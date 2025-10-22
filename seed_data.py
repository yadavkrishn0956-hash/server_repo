"""Seed data for demo purposes on Vercel"""
from datetime import datetime, timedelta
import random

SEED_DATASETS = [
    {
        "title": "Global Financial Markets Data",
        "description": "High-quality synthetic market data including stock prices, trading volumes, and market indicators. Essential for quantitative finance research.",
        "category": "Finance",
        "uploader": "0x742d35Cc6634C0532925a3b8D4C0C8b3C2e1e416",
        "price": 99.99,
        "quality_score": 96,
        "tags": ["stocks", "trading", "markets", "quantitative", "premium"],
        "rows": 10000,
        "columns": 7,
        "file_size": 598000
    },
    {
        "title": "E-Commerce Customer Behavior Dataset",
        "description": "Comprehensive customer behavior data for e-commerce analytics including purchase patterns and demographics.",
        "category": "Business",
        "uploader": "0x8f3B9C1d4E2A5F6c7D8E9F0A1B2C3D4E5F6A7B8C",
        "price": 49.99,
        "quality_score": 88,
        "tags": ["ecommerce", "customers", "analytics", "behavior"],
        "rows": 5000,
        "columns": 12,
        "file_size": 450000
    },
    {
        "title": "Medical Patient Records (Synthetic)",
        "description": "Synthetic patient health records for medical research and ML model training. HIPAA-compliant synthetic data.",
        "category": "Medical",
        "uploader": "0x1A2B3C4D5E6F7A8B9C0D1E2F3A4B5C6D7E8F9A0B",
        "price": 149.99,
        "quality_score": 94,
        "tags": ["medical", "healthcare", "patients", "research", "premium"],
        "rows": 8000,
        "columns": 15,
        "file_size": 1200000
    },
    {
        "title": "Retail Sales Transaction Data",
        "description": "Synthetic retail sales data with product information, timestamps, and customer demographics.",
        "category": "Retail",
        "uploader": "0x9E8D7C6B5A4F3E2D1C0B9A8F7E6D5C4B3A2F1E0D",
        "price": 29.99,
        "quality_score": 85,
        "tags": ["retail", "sales", "transactions", "products"],
        "rows": 15000,
        "columns": 8,
        "file_size": 890000
    },
    {
        "title": "Social Media Engagement Metrics",
        "description": "Synthetic social media engagement data including likes, shares, comments, and user interactions.",
        "category": "Business",
        "uploader": "0x2F3E4D5C6B7A8F9E0D1C2B3A4F5E6D7C8B9A0F1E",
        "price": 0,
        "quality_score": 78,
        "tags": ["social", "engagement", "metrics", "free"],
        "rows": 20000,
        "columns": 10,
        "file_size": 1100000
    },
    {
        "title": "Weather and Climate Dataset",
        "description": "Comprehensive weather data with temperature, precipitation, and atmospheric conditions.",
        "category": "Science",
        "uploader": "0x3A4B5C6D7E8F9A0B1C2D3E4F5A6B7C8D9E0F1A2B",
        "price": 0,
        "quality_score": 92,
        "tags": ["weather", "climate", "science", "free"],
        "rows": 50000,
        "columns": 6,
        "file_size": 2500000
    },
    {
        "title": "Product Image Classification Dataset",
        "description": "Synthetic product images with labels for computer vision and ML training.",
        "category": "Image",
        "uploader": "0x4B5C6D7E8F9A0B1C2D3E4F5A6B7C8D9E0F1A2B3C",
        "price": 79.99,
        "quality_score": 90,
        "tags": ["images", "classification", "ml", "computer-vision"],
        "rows": 5000,
        "columns": 3,
        "file_size": 3500000
    },
    {
        "title": "Cryptocurrency Trading History",
        "description": "Historical cryptocurrency trading data with prices, volumes, and market cap information.",
        "category": "Finance",
        "uploader": "0x5C6D7E8F9A0B1C2D3E4F5A6B7C8D9E0F1A2B3C4D",
        "price": 59.99,
        "quality_score": 87,
        "tags": ["crypto", "trading", "blockchain", "finance"],
        "rows": 30000,
        "columns": 9,
        "file_size": 1800000
    }
]

def generate_seed_cid(index: int) -> str:
    """Generate a consistent CID for seed data"""
    base = f"seed{index:04d}"
    return f"{base}{'a' * (64 - len(base))}"

def get_seed_datasets():
    """Get seed datasets with generated CIDs and timestamps"""
    datasets = []
    base_time = datetime.utcnow() - timedelta(days=30)
    
    for i, seed in enumerate(SEED_DATASETS):
        cid = generate_seed_cid(i)
        timestamp = (base_time + timedelta(days=i*3, hours=random.randint(0, 23))).isoformat()
        
        dataset = {
            "cid": cid,
            **seed,
            "timestamp": timestamp,
            "quality_metrics": {
                "completeness": random.randint(85, 100),
                "consistency": random.randint(80, 98),
                "accuracy": random.randint(85, 99),
                "uniqueness": random.randint(75, 95)
            },
            "quality_explanation": [
                "High data completeness with minimal missing values",
                "Consistent formatting across all fields",
                "Validated data types and ranges"
            ],
            "quality_recommendations": []
        }
        
        datasets.append(dataset)
    
    return datasets
