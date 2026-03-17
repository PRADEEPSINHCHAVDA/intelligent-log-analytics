"""
Payment Service - Processes payments, refunds, and billing.
Most critical service: 2% error rate, higher avg latency.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from base_service import BaseMicroservice


class PaymentService(BaseMicroservice):
    SERVICE_NAME = "payment-service"
    SERVICE_VERSION = "3.1.0"

    OPERATIONS = [
        {
            "name": "Process Payment",
            "method": "POST",
            "path": "/api/v1/payments/charge",
            "avg_ms": 450,
            "critical": True,
            "errors": [
                "Card declined by issuing bank",
                "Insufficient funds",
                "Stripe API timeout after 30s",
                "Payment processor unavailable",
                "Card number failed Luhn check",
                "CVV mismatch",
            ],
            "metadata": {
                "processor": "stripe",
                "currency": "USD",
                "payment_method": "credit_card",
            },
        },
        {
            "name": "Initiate Refund",
            "method": "POST",
            "path": "/api/v1/payments/{payment_id}/refund",
            "avg_ms": 600,
            "critical": True,
            "errors": [
                "Refund window expired (>90 days)",
                "Payment already refunded",
                "Stripe refund API error",
            ],
            "metadata": {"refund_reason": "customer_request"},
        },
        {
            "name": "Get Payment Status",
            "method": "GET",
            "path": "/api/v1/payments/{payment_id}",
            "avg_ms": 60,
            "critical": False,
            "errors": ["Payment record not found"],
            "metadata": {"cache_ttl": 300},
        },
        {
            "name": "List Payment Methods",
            "method": "GET",
            "path": "/api/v1/users/{user_id}/payment-methods",
            "avg_ms": 80,
            "critical": False,
            "errors": ["Database read replica lag too high"],
            "metadata": {"vault": "stripe"},
        },
        {
            "name": "Add Payment Method",
            "method": "POST",
            "path": "/api/v1/users/{user_id}/payment-methods",
            "avg_ms": 300,
            "critical": False,
            "errors": [
                "Card tokenization failed",
                "Duplicate card already on file",
                "Card expired",
            ],
            "metadata": {"tokenizer": "stripe_elements"},
        },
        {
            "name": "Webhook: Payment Confirmed",
            "method": "POST",
            "path": "/api/v1/webhooks/stripe",
            "avg_ms": 150,
            "critical": True,
            "errors": [
                "Webhook signature verification failed",
                "Duplicate webhook event",
                "Order service unavailable for fulfillment",
            ],
            "metadata": {"event_type": "payment_intent.succeeded"},
        },
        {
            "name": "Generate Invoice",
            "method": "POST",
            "path": "/api/v1/invoices/generate",
            "avg_ms": 800,
            "critical": False,
            "errors": [
                "PDF generation service timeout",
                "S3 upload failed",
            ],
            "metadata": {"format": "PDF", "storage": "S3"},
        },
    ]


if __name__ == "__main__":
    PaymentService().run()
