"""
Order Service - Manages order lifecycle: creation, fulfillment, tracking.
8 logs/second, 4% error rate, integrates with inventory + payment.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from base_service import BaseMicroservice


class OrderService(BaseMicroservice):
    SERVICE_NAME = "order-service"
    SERVICE_VERSION = "4.0.2"

    OPERATIONS = [
        {
            "name": "Create Order",
            "method": "POST",
            "path": "/api/v1/orders",
            "avg_ms": 250,
            "critical": True,
            "errors": [
                "Item out of stock",
                "Payment authorization failed",
                "Cart session expired",
                "Shipping address validation failed",
                "Minimum order amount not met",
            ],
            "metadata": {"workflow": "create_order_saga"},
        },
        {
            "name": "Get Order Details",
            "method": "GET",
            "path": "/api/v1/orders/{order_id}",
            "avg_ms": 55,
            "critical": False,
            "errors": ["Order not found", "Access denied to order"],
            "metadata": {"cache_hit_rate": 0.75},
        },
        {
            "name": "Update Order Status",
            "method": "PATCH",
            "path": "/api/v1/orders/{order_id}/status",
            "avg_ms": 180,
            "critical": True,
            "errors": [
                "Invalid status transition",
                "Order already cancelled",
                "Fulfillment service unavailable",
            ],
            "metadata": {"valid_transitions": ["pending→confirmed", "confirmed→shipped"]},
        },
        {
            "name": "Cancel Order",
            "method": "DELETE",
            "path": "/api/v1/orders/{order_id}",
            "avg_ms": 400,
            "critical": True,
            "errors": [
                "Order already shipped, cannot cancel",
                "Refund initiation failed",
                "Inventory rollback failed",
            ],
            "metadata": {"requires_refund": True},
        },
        {
            "name": "List Orders",
            "method": "GET",
            "path": "/api/v1/users/{user_id}/orders",
            "avg_ms": 90,
            "critical": False,
            "errors": ["Database query timeout", "Too many results (pagination required)"],
            "metadata": {"page_size": 20},
        },
        {
            "name": "Apply Discount Code",
            "method": "POST",
            "path": "/api/v1/orders/{order_id}/discounts",
            "avg_ms": 120,
            "critical": False,
            "errors": [
                "Discount code expired",
                "Code already used by this user",
                "Minimum cart value not met for discount",
            ],
            "metadata": {},
        },
        {
            "name": "Schedule Delivery",
            "method": "POST",
            "path": "/api/v1/orders/{order_id}/delivery",
            "avg_ms": 350,
            "critical": False,
            "errors": [
                "No delivery slots available",
                "Address outside delivery zone",
                "Logistics partner API down",
            ],
            "metadata": {"logistics_partner": "fedex"},
        },
        {
            "name": "Track Shipment",
            "method": "GET",
            "path": "/api/v1/orders/{order_id}/tracking",
            "avg_ms": 200,
            "critical": False,
            "errors": [
                "Tracking number not yet assigned",
                "Carrier API rate limited",
            ],
            "metadata": {"carrier": "fedex"},
        },
    ]


if __name__ == "__main__":
    OrderService().run()
