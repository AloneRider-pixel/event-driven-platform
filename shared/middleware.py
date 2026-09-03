"""
Shared middleware for all services.
Provides structured logging, correlation IDs, and metrics.
"""
import logging
import time
import uuid

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """
    Adds a correlation ID to every request for distributed tracing.
    Propagates through service boundaries via headers.
    """

    async def dispatch(self, request: Request, call_next):
        # Get or generate correlation ID
        correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
        
        # Add to request state and logging context
        request.state.correlation_id = correlation_id
        
        # Process request
        start_time = time.time()
        response = await call_next(request)
        latency_ms = (time.time() - start_time) * 1000
        
        # Add correlation ID and timing to response
        response.headers["X-Correlation-ID"] = correlation_id
        response.headers["X-Response-Time"] = f"{latency_ms:.0f}ms"
        
        return response


class StructuredLoggingMiddleware(BaseHTTPMiddleware):
    """
    Logs every request with structured JSON output.
    Includes correlation ID, timing, and user context.
    """

    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
        start_time = time.time()
        
        logger = logging.getLogger("access")
        
        try:
            response = await call_next(request)
            latency_ms = (time.time() - start_time) * 1000
            
            log_data = {
                "event": "http_request",
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "latency_ms": round(latency_ms, 2),
                "client_ip": request.client.host if request.client else "unknown",
            }
            
            if response.status_code >= 500:
                logger.error("Request failed", extra=log_data)
            elif response.status_code >= 400:
                logger.warning("Request client error", extra=log_data)
            else:
                logger.info("Request completed", extra=log_data)
            
            response.headers["X-Request-ID"] = request_id
            return response
        
        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            logger.error(
                "Request exception",
                extra={
                    "event": "http_error",
                    "request_id": request_id,
                    "method": request.method,
                    "path": request.url.path,
                    "error": str(e),
                    "latency_ms": round(latency_ms, 2),
                },
            )
            raise


class MetricsMiddleware(BaseHTTPMiddleware):
    """
    Tracks request metrics for monitoring.
    Counts requests by status code and tracks latency distribution.
    """

    def __init__(self, app, service_name: str = "unknown"):
        super().__init__(app)
        self.service_name = service_name
        self._request_count = 0
        self._error_count = 0
        self._latencies = []

    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        self._request_count += 1
        
        response = await call_next(request)
        latency = (time.time() - start_time) * 1000
        
        self._latencies.append(latency)
        if len(self._latencies) > 1000:
            self._latencies = self._latencies[-1000:]
        
        if response.status_code >= 400:
            self._error_count += 1
        
        return response

    def get_metrics(self) -> dict:
        """Get current metrics snapshot."""
        latencies = sorted(self._latencies) if self._latencies else [0]
        n = len(latencies)
        
        return {
            "service": self.service_name,
            "total_requests": self._request_count,
            "total_errors": self._error_count,
            "error_rate": self._error_count / max(self._request_count, 1),
            "latency_p50": latencies[n // 2],
            "latency_p95": latencies[min(int(n * 0.95), n - 1)],
            "latency_p99": latencies[min(int(n * 0.99), n - 1)],
        }
