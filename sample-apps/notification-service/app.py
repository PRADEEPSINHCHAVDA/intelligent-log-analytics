"""
Notification Service - Sends emails, SMS, push notifications.
Highest error rate (8%) due to unreliable external providers.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from base_service import BaseMicroservice


class NotificationService(BaseMicroservice):
    SERVICE_NAME = "notification-service"
    SERVICE_VERSION = "1.8.4"

    OPERATIONS = [
        {
            "name": "Send Email Notification",
            "method": "POST",
            "path": "/api/v1/notifications/email",
            "avg_ms": 300,
            "critical": False,
            "errors": [
                "SendGrid API key invalid",
                "Email bounce: address not found",
                "SMTP connection timeout",
                "Spam filter rejected message",
                "Daily email quota exceeded",
            ],
            "metadata": {"provider": "sendgrid", "template": "order_confirmation"},
        },
        {
            "name": "Send SMS Notification",
            "method": "POST",
            "path": "/api/v1/notifications/sms",
            "avg_ms": 250,
            "critical": False,
            "errors": [
                "Twilio API error: invalid number",
                "SMS to this region not supported",
                "Monthly SMS limit reached",
                "Phone number opted out of SMS",
            ],
            "metadata": {"provider": "twilio"},
        },
        {
            "name": "Send Push Notification",
            "method": "POST",
            "path": "/api/v1/notifications/push",
            "avg_ms": 150,
            "critical": False,
            "errors": [
                "FCM device token expired",
                "APNs certificate expired",
                "Device token not registered",
            ],
            "metadata": {"provider": "fcm", "platform": "android"},
        },
        {
            "name": "Get Notification History",
            "method": "GET",
            "path": "/api/v1/users/{user_id}/notifications",
            "avg_ms": 70,
            "critical": False,
            "errors": ["Query result too large"],
            "metadata": {},
        },
        {
            "name": "Update Notification Preferences",
            "method": "PUT",
            "path": "/api/v1/users/{user_id}/notification-preferences",
            "avg_ms": 100,
            "critical": False,
            "errors": ["Invalid preference combination"],
            "metadata": {},
        },
        {
            "name": "Batch Notification Dispatch",
            "method": "POST",
            "path": "/api/v1/notifications/batch",
            "avg_ms": 2500,
            "critical": False,
            "errors": [
                "Batch size exceeds 10000 limit",
                "External provider rate limited",
                "Partial batch failure (>20% failed)",
            ],
            "metadata": {"batch_size": 500, "async": True},
        },
    ]


if __name__ == "__main__":
    NotificationService().run()
