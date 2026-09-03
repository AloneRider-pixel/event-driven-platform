"""
Load testing script using Locust.
Tests the API Gateway under concurrent load.
"""
import random
import string
from locust import HttpUser, task, between


class ECommerceUser(HttpUser):
    """Simulates a customer using the e-commerce platform."""
    wait_time = between(0.5, 2)
    
    def on_start(self):
        """Register and login before starting tasks."""
        email = f"user_{''.join(random.choices(string.ascii_lowercase, k=8))}@test.com"
        self.email = email
        self.password = "testpass123"
        
        # Register
        self.client.post("/api/v1/auth/register", json={
            "email": email,
            "password": self.password,
        })
        
        # Login
        response = self.client.post("/api/v1/auth/login", json={
            "email": email,
            "password": self.password,
        })
        if response.status_code == 200:
            self.token = response.json().get("access_token")
            self.headers = {"Authorization": f"Bearer {self.token}"}
        else:
            self.token = None
            self.headers = {}
    
    @task(5)
    def create_order(self):
        """Create a new order."""
        if not self.token:
            return
        
        self.client.post("/api/v1/orders", json={
            "customer_id": self.email,
            "product_id": f"PROD-{random.randint(1, 5):03d}",
            "quantity": random.randint(1, 3),
        }, headers=self.headers)
    
    @task(3)
    def list_orders(self):
        """List orders."""
        if not self.token:
            return
        self.client.get("/api/v1/orders", headers=self.headers)
    
    @task(2)
    def check_inventory(self):
        """Check product inventory."""
        product_id = f"PROD-{random.randint(1, 5):03d}"
        self.client.get(f"/api/v1/inventory/{product_id}")
    
    @task(1)
    def health_check(self):
        """Check service health."""
        self.client.get("/health")


# Run with: locust -f scripts/load_test.py --host http://localhost:8000
