#!/usr/bin/env bash
# Package Lambda function for deployment to real AWS
# Run this before `terraform apply`
set -euo pipefail

echo "Packaging Lambda function..."
cd "$(dirname "$0")/../lambda/log_consumer"

# Install dependencies into package directory
pip3 install -r requirements.txt -t package/ --quiet

# Copy function code
cp lambda_function.py package/

# Create zip
cd package
zip -r ../function.zip . -x "*.pyc" "__pycache__/*" >/dev/null
cd ..
rm -rf package/

echo "✓ Lambda packaged: lambda/log_consumer/function.zip ($(du -sh function.zip | cut -f1))"
