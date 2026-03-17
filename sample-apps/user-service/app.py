"""
User Service - Handles authentication, profile management, and user sessions.
Generates ~10 logs/second with 5% error rate.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from base_service import BaseMicroservice


class UserService(BaseMicroservice):
    SERVICE_NAME = "user-service"
    SERVICE_VERSION = "2.3.1"

    OPERATIONS = [
        {
            "name": "User Login",
            "method": "POST",
            "path": "/api/v1/auth/login",
            "avg_ms": 85,
            "critical": False,
            "errors": [
                "Invalid credentials for user",
                "Account locked after 5 failed attempts",
                "MFA verification failed",
                "Session token expired",
            ],
            "metadata": {"auth_type": "password", "mfa_required": True},
        },
        {
            "name": "Get User Profile",
            "method": "GET",
            "path": "/api/v1/users/{user_id}/profile",
            "avg_ms": 45,
            "critical": False,
            "errors": ["User not found", "Profile data corrupted"],
            "metadata": {"cache_enabled": True},
        },
        {
            "name": "Update User Profile",
            "method": "PUT",
            "path": "/api/v1/users/{user_id}/profile",
            "avg_ms": 120,
            "critical": False,
            "errors": [
                "Email already taken by another user",
                "Invalid phone number format",
                "Profile update validation failed",
            ],
            "metadata": {"fields_updated": ["email", "name", "preferences"]},
        },
        {
            "name": "Refresh Auth Token",
            "method": "POST",
            "path": "/api/v1/auth/refresh",
            "avg_ms": 30,
            "critical": False,
            "errors": ["Refresh token expired", "Token blacklisted"],
            "metadata": {"token_type": "JWT"},
        },
        {
            "name": "User Logout",
            "method": "POST",
            "path": "/api/v1/auth/logout",
            "avg_ms": 25,
            "critical": False,
            "errors": ["Session not found"],
            "metadata": {},
        },
        {
            "name": "Password Reset Request",
            "method": "POST",
            "path": "/api/v1/auth/password-reset",
            "avg_ms": 200,
            "critical": False,
            "errors": [
                "Email service unavailable",
                "Too many reset attempts (rate limited)",
            ],
            "metadata": {"channel": "email"},
        },
        {
            "name": "List User Sessions",
            "method": "GET",
            "path": "/api/v1/users/{user_id}/sessions",
            "avg_ms": 65,
            "critical": False,
            "errors": ["Database connection timeout"],
            "metadata": {"max_sessions": 5},
        },
        {
            "name": "User Registration",
            "method": "POST",
            "path": "/api/v1/auth/register",
            "avg_ms": 350,
            "critical": False,
            "errors": [
                "Email already registered",
                "Username taken",
                "Password does not meet complexity requirements",
                "CAPTCHA verification failed",
            ],
            "metadata": {"send_verification_email": True},
        },
    ]


if __name__ == "__main__":
    UserService().run()
