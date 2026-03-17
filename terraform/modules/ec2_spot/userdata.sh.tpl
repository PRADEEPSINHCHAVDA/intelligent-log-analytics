#!/bin/bash
# =============================================================================
# Kafka EC2 User Data Script
# Runs on first boot to install + configure Kafka on Amazon Linux 2
# =============================================================================
set -euo pipefail
exec > >(tee /var/log/kafka-userdata.log|logger -t kafka-userdata -s 2>/dev/console) 2>&1

echo "=== Starting Kafka installation ==="
KAFKA_VERSION="${kafka_version}"
SCALA_VERSION="2.13"
KAFKA_HOME="/opt/kafka"
KAFKA_USER="kafka"
KAFKA_DATA_DIR="/var/kafka/data"

# ---------------------------------------------------------------------------
# System prep
# ---------------------------------------------------------------------------
yum update -y
yum install -y java-11-amazon-corretto wget curl jq

# Create kafka user (no login shell)
useradd -r -s /bin/false $KAFKA_USER || true

# ---------------------------------------------------------------------------
# Download and install Kafka
# ---------------------------------------------------------------------------
cd /tmp
KAFKA_URL="https://downloads.apache.org/kafka/$${KAFKA_VERSION}/kafka_$${SCALA_VERSION}-$${KAFKA_VERSION}.tgz"
wget -q "$KAFKA_URL" -O kafka.tgz
tar -xzf kafka.tgz
mv "kafka_$${SCALA_VERSION}-$${KAFKA_VERSION}" $KAFKA_HOME
chown -R $KAFKA_USER:$KAFKA_USER $KAFKA_HOME
rm -f kafka.tgz

# ---------------------------------------------------------------------------
# Get instance metadata (IMDSv2)
# ---------------------------------------------------------------------------
TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
INSTANCE_ID=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/instance-id)
PRIVATE_IP=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/local-ipv4)
AZ=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/placement/availability-zone)

# Broker ID from last octet of private IP (unique per instance)
BROKER_ID=$(echo $PRIVATE_IP | awk -F. '{print $4}')

echo "Instance: $INSTANCE_ID | IP: $PRIVATE_IP | AZ: $AZ | Broker ID: $BROKER_ID"

# ---------------------------------------------------------------------------
# Create data directory
# ---------------------------------------------------------------------------
mkdir -p $KAFKA_DATA_DIR
chown -R $KAFKA_USER:$KAFKA_USER $KAFKA_DATA_DIR

# ---------------------------------------------------------------------------
# Configure Kafka server.properties
# ---------------------------------------------------------------------------
cat > $KAFKA_HOME/config/server.properties << EOF
broker.id=$BROKER_ID
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://$PRIVATE_IP:9092
log.dirs=$KAFKA_DATA_DIR
num.partitions=6
default.replication.factor=1
min.insync.replicas=1
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
zookeeper.connect=localhost:2181
zookeeper.connection.timeout.ms=18000
auto.create.topics.enable=true
delete.topic.enable=true
group.initial.rebalance.delay.ms=3000
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
# Performance tuning
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
EOF

# ---------------------------------------------------------------------------
# Configure Zookeeper (single-node for dev; use ensemble for prod)
# ---------------------------------------------------------------------------
mkdir -p /var/zookeeper
chown -R $KAFKA_USER:$KAFKA_USER /var/zookeeper
echo "1" > /var/zookeeper/myid

cat > $KAFKA_HOME/config/zookeeper.properties << EOF
dataDir=/var/zookeeper
clientPort=2181
maxClientCnxns=60
admin.enableServer=false
tickTime=2000
initLimit=10
syncLimit=5
EOF

# ---------------------------------------------------------------------------
# Create Kafka topics
# ---------------------------------------------------------------------------
create_topics() {
  sleep 30  # Wait for Kafka to start
  TOPICS=(
    "logs.user-service"
    "logs.payment-service"
    "logs.order-service"
    "logs.notification-service"
    "logs.inventory-service"
    "logs.auth-service"
    "logs.api-gateway-service"
    "logs.reporting-service"
  )
  for TOPIC in "$${TOPICS[@]}"; do
    $KAFKA_HOME/bin/kafka-topics.sh \
      --create \
      --if-not-exists \
      --bootstrap-server localhost:9092 \
      --replication-factor 1 \
      --partitions 6 \
      --topic "$TOPIC" || true
    echo "Created topic: $TOPIC"
  done
}

# ---------------------------------------------------------------------------
# Systemd: Zookeeper
# ---------------------------------------------------------------------------
cat > /etc/systemd/system/zookeeper.service << EOF
[Unit]
Description=Apache Zookeeper
After=network.target

[Service]
Type=simple
User=$KAFKA_USER
ExecStart=$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
ExecStop=$KAFKA_HOME/bin/zookeeper-server-stop.sh
Restart=on-failure
RestartSec=10
LimitNOFILE=65536
Environment="JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto"

[Install]
WantedBy=multi-user.target
EOF

# ---------------------------------------------------------------------------
# Systemd: Kafka
# ---------------------------------------------------------------------------
cat > /etc/systemd/system/kafka.service << EOF
[Unit]
Description=Apache Kafka
After=zookeeper.service
Requires=zookeeper.service

[Service]
Type=simple
User=$KAFKA_USER
ExecStart=$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties
ExecStop=$KAFKA_HOME/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=10
LimitNOFILE=65536
Environment="JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto"
Environment="KAFKA_HEAP_OPTS=-Xmx512m -Xms256m"

[Install]
WantedBy=multi-user.target
EOF

# ---------------------------------------------------------------------------
# Spot interruption handler (checks IMDS every 5 seconds for rebalance notice)
# ---------------------------------------------------------------------------
cat > /usr/local/bin/spot-interruption-handler.sh << 'HANDLER_EOF'
#!/bin/bash
# Polls EC2 metadata for Spot interruption notice
# Gracefully shuts down Kafka when a 2-minute warning is received

TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 30" 2>/dev/null)

while true; do
  ACTION=$(curl -sf \
    -H "X-aws-ec2-metadata-token: $TOKEN" \
    "http://169.254.169.254/latest/meta-data/spot/termination-time" 2>/dev/null || echo "")

  if [[ -n "$ACTION" ]]; then
    logger -t spot-handler "⚠️  SPOT INTERRUPTION detected at $ACTION"
    logger -t spot-handler "Gracefully stopping Kafka..."

    # Stop producing new messages (signal producers)
    systemctl stop kafka

    # Wait for in-flight messages to flush (up to 90 seconds)
    sleep 90

    # Stop Zookeeper
    systemctl stop zookeeper

    logger -t spot-handler "Kafka stopped. Instance will terminate soon."
    exit 0
  fi

  sleep 5
done
HANDLER_EOF
chmod +x /usr/local/bin/spot-interruption-handler.sh

cat > /etc/systemd/system/spot-handler.service << EOF
[Unit]
Description=EC2 Spot Interruption Handler
After=kafka.service

[Service]
Type=simple
ExecStart=/usr/local/bin/spot-interruption-handler.sh
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

# ---------------------------------------------------------------------------
# Start services
# ---------------------------------------------------------------------------
systemctl daemon-reload
systemctl enable zookeeper kafka spot-handler
systemctl start zookeeper
sleep 15
systemctl start kafka
sleep 20
systemctl start spot-handler

# Create topics in background
create_topics &

echo "=== Kafka installation complete ==="
echo "Broker ID: $BROKER_ID | IP: $PRIVATE_IP"
