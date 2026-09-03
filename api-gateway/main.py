"""
API Gateway - Entry point for all client requests.
Handles authentication, rate limiting, routing, and API versioning.
"""
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime

import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from auth import create_token, verify_token, hash_password
from rate_limiter import RateLimiter

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("api-gateway")

# ─── Configuration ───
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
ORDER_SERVICE_URL = os.getenv("ORDER_SERVICE_URL", "http://order-service:8001")
INVENTORY_SERVICE_URL = os.getenv("INVENTORY_SERVICE_URL", "http://inventory-service:8003")
JWT_SECRET = os.getenv("JWT_SECRET_KEY", "dev-secret-change-me")

redis_client = None
rate_limiter = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, rate_limiter
    redis_client = redis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}", decode_responses=True)
    rate_limiter = RateLimiter(redis_client)
    logger.info("API Gateway started ✓")
    yield
    await redis_client.close()
    logger.info("API Gateway stopped")


app = FastAPI(
    title="Event-Driven E-Commerce API Gateway",
    version="1.0.0",
    lifespan=lifespan,
)

# ─── Middleware ───
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Auth Dependencies ───
async def get_current_user(request: Request) -> dict:
    """Extract and verify JWT from Authorization header."""
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid token")
    
    token = auth_header.split(" ")[1]
    user = verify_token(token, JWT_SECRET)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return user


# ─── Health Checks ───
@app.get("/health")
async def health():
    """Gateway health check."""
    try:
        await redis_client.ping()
        redis_status = "healthy"
    except Exception:
        redis_status = "unhealthy"
    
    return {
        "status": "healthy",
        "service": "api-gateway",
        "version": "1.0.0",
        "timestamp": datetime.utcnow().isoformat(),
        "dependencies": {"redis": redis_status},
    }


@app.get("/ready")
async def readiness():
    """Readiness probe."""
    return {"status": "ready"}


# ─── Auth Routes ───
@app.post("/api/v1/auth/register")
async def register(request: Request):
    """Register a new user."""
    body = await request.json()
    email = body.get("email")
    password = body.get("password")
    
    if not email or not password:
        raise HTTPException(status_code=400, detail="Email and password required")
    
    # Store user in Redis
    user_key = f"user:{email}"
    existing = await redis_client.get(user_key)
    if existing:
        raise HTTPException(status_code=409, detail="Email already registered")
    
    hashed = hash_password(password)
    await redis_client.hset(user_key, mapping={
        "email": email,
        "password": hashed,
        "role": "user",
        "created_at": datetime.utcnow().isoformat(),
    })
    
    token = create_token({"sub": email, "role": "user"}, JWT_SECRET)
    return {"access_token": token, "token_type": "bearer"}


@app.post("/api/v1/auth/login")
async def login(request: Request):
    """Authenticate user and return JWT."""
    body = await request.json()
    email = body.get("email")
    password = body.get("password")
    
    user_key = f"user:{email}"
    user_data = await redis_client.hgetall(user_key)
    
    if not user_data:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    from auth import check_password
    if not check_password(password, user_data["password"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    token = create_token({"sub": email, "role": user_data.get("role", "user")}, JWT_SECRET)
    return {"access_token": token, "token_type": "bearer"}


# ─── Order Routes ───
@app.post("/api/v1/orders")
async def create_order(request: Request, user: dict = Depends(get_current_user)):
    """Create a new order - routes to Order Service."""
    # Rate limit check
    allowed = await rate_limiter.check(user["sub"])
    if not allowed:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    
    body = await request.json()
    
    async with __import__("httpx").AsyncClient() as client:
        response = await client.post(
            f"{ORDER_SERVICE_URL}/api/v1/orders",
            json=body,
            headers={"X-Correlation-ID": request.headers.get("X-Correlation-ID", "")},
            timeout=10.0,
        )
    
    return JSONResponse(status_code=response.status_code, content=response.json())


@app.get("/api/v1/orders/{order_id}")
async def get_order(order_id: str, user: dict = Depends(get_current_user)):
    """Get order details."""
    async with __import__("httpx").AsyncClient() as client:
        response = await client.get(
            f"{ORDER_SERVICE_URL}/api/v1/orders/{order_id}",
            timeout=10.0,
        )
    
    if response.status_code == 404:
        raise HTTPException(status_code=404, detail="Order not found")
    
    return response.json()


@app.get("/api/v1/orders")
async def list_orders(
    page: int = 1,
    page_size: int = 20,
    user: dict = Depends(get_current_user),
):
    """List orders with pagination."""
    async with __import__("httpx").AsyncClient() as client:
        response = await client.get(
            f"{ORDER_SERVICE_URL}/api/v1/orders",
            params={"page": page, "page_size": page_size, "customer_id": user["sub"]},
            timeout=10.0,
        )
    
    return response.json()


@app.put("/api/v1/orders/{order_id}/cancel")
async def cancel_order(order_id: str, request: Request, user: dict = Depends(get_current_user)):
    """Cancel an order."""
    body = await request.json() if request.headers.get("content-type") else {}
    
    async with __import__("httpx").AsyncClient() as client:
        response = await client.put(
            f"{ORDER_SERVICE_URL}/api/v1/orders/{order_id}/cancel",
            json=body,
            timeout=10.0,
        )
    
    return JSONResponse(status_code=response.status_code, content=response.json())


# ─── Inventory Routes ───
@app.get("/api/v1/inventory/{product_id}")
async def get_inventory(product_id: str):
    """Check product inventory."""
    async with __import__("httpx").AsyncClient() as client:
        response = await client.get(
            f"{INVENTORY_SERVICE_URL}/api/v1/inventory/{product_id}",
            timeout=10.0,
        )
    
    if response.status_code == 404:
        raise HTTPException(status_code=404, detail="Product not found")
    
    return response.json()


# ─── Metrics ───
@app.get("/metrics")
async def metrics():
    """Expose basic metrics."""
    return {
        "service": "api-gateway",
        "timestamp": datetime.utcnow().isoformat(),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
