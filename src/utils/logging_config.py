"""
Centralized logging configuration using loguru.

This module provides standardized logging setup for the TagInsights project,
with built-in rotation, retention, and formatting capabilities.
"""

import os
import sys
from pathlib import Path
from typing import Optional

from loguru import logger


def setup_logging(
    level: str = "INFO",
    log_dir: str = "logs",
    enable_file_logging: bool = True,
    enable_console_logging: bool = True,
    rotation: str = "100 MB",
    retention: str = "30 days",
) -> None:
    """
    Configure loguru logging for the entire application.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_dir: Directory for log files
        enable_file_logging: Whether to enable file logging
        enable_console_logging: Whether to enable console logging
        rotation: File rotation policy (size or time based)
        retention: How long to keep old log files
    """
    # Detect Lambda environment early
    is_lambda = os.getenv("AWS_LAMBDA_FUNCTION_NAME") is not None

    # Remove default handler
    logger.remove()

    # Console logging (disable colors in Lambda to avoid ANSI codes in CloudWatch)
    if enable_console_logging:
        if is_lambda:
            # Lambda environment - no colors for clean CloudWatch logs
            console_format = (
                "{time:YYYY-MM-DD HH:mm:ss} | "
                "{level: <8} | "
                "{name}:{function}:{line} | "
                "{message}"
            )
            logger.add(
                sys.stdout,
                format=console_format,
                level=level,
                colorize=False,  # Disable colors for Lambda
                backtrace=True,
                diagnose=False,
            )
            print("✓ Logging configured successfully for Lambda environment")
        else:
            # Local environment - colors enabled for better readability
            console_format = (
                "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                "<level>{level: <8}</level> | "
                "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
                "<level>{message}</level>"
            )
            logger.add(
                sys.stdout,
                format=console_format,
                level=level,
                colorize=True,  # Enable colors for local development
                backtrace=True,
                diagnose=True,
            )

    # File logging with rotation and retention - but not in Lambda
    if enable_file_logging and not is_lambda:
        try:
            # Create log directory (only if not in Lambda)
            log_path = Path(log_dir)
            log_path.mkdir(exist_ok=True)

            # General application logs
            app_log_format = (
                "{time:YYYY-MM-DD HH:mm:ss.SSS} | "
                "{level: <8} | "
                "{name}:{function}:{line} | "
                "{message}"
            )

            logger.add(
                log_path / "taginsights.log",
                format=app_log_format,
                level=level,
                rotation=rotation,
                retention=retention,
                compression="gz",
                backtrace=True,
                diagnose=True,
            )

            # Error-only logs for quick debugging
            logger.add(
                log_path / "errors.log",
                format=app_log_format,
                level="ERROR",
                rotation=rotation,
                retention=retention,
                compression="gz",
                backtrace=True,
                diagnose=True,
            )

            # AWS API call logs (structured for analysis)
            logger.add(
                log_path / "aws_api.log",
                format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {message}",
                level="DEBUG",
                rotation=rotation,
                retention=retention,
                compression="gz",
                filter=lambda record: "aws_api" in record["extra"],
            )
        except (OSError, PermissionError) as e:
            # If file logging fails (e.g., in Lambda), just log to console
            print(f"Warning: Could not set up file logging: {e}")
            print("Continuing with console logging only")
    elif enable_file_logging and is_lambda:
        print(
            "Lambda environment detected - file logging disabled (using CloudWatch Logs)"
        )


def setup_aws_api_logging() -> None:
    """
    Configure specialized logging for AWS API calls.
    This creates structured logs for monitoring AWS operations.
    """
    pass  # The filter in setup_logging handles this


def get_aws_logger(service_name: str):
    """
    Get a logger with AWS service context.

    Args:
        service_name: AWS service name (e.g., 'ec2', 'iam', 's3')

    Returns:
        Logger with service context
    """
    return logger.bind(aws_service=service_name, aws_api=True)


def log_aws_api_call(
    service: str,
    operation: str,
    resource_arn: Optional[str] = None,
    duration_ms: Optional[float] = None,
    success: bool = True,
    error_message: Optional[str] = None,
    tag_count: Optional[int] = None,
):
    """
    Log AWS API call with structured information.

    Args:
        service: AWS service name
        operation: API operation name
        resource_arn: Resource ARN (if applicable)
        duration_ms: Operation duration in milliseconds
        success: Whether the operation succeeded
        error_message: Error message if failed
        tag_count: Number of tags retrieved (if applicable)
    """
    log_data = {
        "aws_api": True,
        "service": service,
        "operation": operation,
        "success": success,
    }

    if resource_arn:
        log_data["resource_arn"] = resource_arn
    if duration_ms is not None:
        log_data["duration_ms"] = duration_ms
    if tag_count is not None:
        log_data["tag_count"] = tag_count
    if error_message:
        log_data["error"] = error_message

    # Create message
    message = f"AWS {service}.{operation}"
    if resource_arn:
        message += f" for {resource_arn}"
    if duration_ms is not None:
        message += f" ({duration_ms:.2f}ms)"
    if tag_count is not None:
        message += f" -> {tag_count} tags"
    if not success and error_message:
        message += f" FAILED: {error_message}"

    # Log with context
    logger.bind(**log_data).info(message)


def setup_development_logging():
    """Setup logging optimized for development (with colors)"""
    setup_logging(
        level="DEBUG",
        enable_console_logging=True,
        enable_file_logging=True,
        rotation="10 MB",
        retention="7 days",
    )


def setup_production_logging():
    """Setup logging optimized for production (colors disabled in Lambda)"""
    # Detect Lambda environment
    is_lambda = os.getenv("AWS_LAMBDA_FUNCTION_NAME") is not None

    setup_logging(
        level="INFO",
        enable_console_logging=True,  # Always enable for Lambda (CloudWatch Logs)
        enable_file_logging=not is_lambda,  # Disable file logging in Lambda (read-only FS)
        rotation="100 MB",
        retention="30 days",
    )


def setup_testing_logging():
    """Setup minimal logging for testing"""
    setup_logging(
        level="WARNING", enable_console_logging=True, enable_file_logging=False
    )


# Environment-based auto-configuration
def configure_logging_from_environment():
    """
    Automatically configure logging based on environment variables.

    Environment variables:
    - LOG_LEVEL: Logging level (default: INFO)
    - LOG_DIR: Log directory (default: logs)
    - ENVIRONMENT: dev/prod/test for preset configurations
    - AWS_LAMBDA_FUNCTION_NAME: Detected automatically (disables colors)
    """
    environment = os.getenv("ENVIRONMENT", "").lower()

    if environment == "dev" or environment == "development":
        setup_development_logging()
    elif environment == "prod" or environment == "production":
        setup_production_logging()
    elif environment == "test" or environment == "testing":
        setup_testing_logging()
    else:
        # Default configuration
        level = os.getenv("LOG_LEVEL", "INFO").upper()
        log_dir = os.getenv("LOG_DIR", "logs")

        setup_logging(
            level=level,
            log_dir=log_dir,
            enable_console_logging=True,
            enable_file_logging=True,
        )


# Note: Logging initialization moved to runtime in Lambda handlers
# to avoid read-only filesystem issues in AWS Lambda
# Call configure_logging_from_environment() explicitly in your application entry points
