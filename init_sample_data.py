"""
Initialize sample datasets for the marketplace
"""
import json
import random
from datetime import datetime, timedelta
from services.ipfs_mimic import ipfs
from services.quality_assessment import quality_service

def generate_sample_csv_data(category: str, rows: int = 100) -> bytes:
    """Generate sample CSV data based on category"""
    
    if category == "Medical":
        headers = "patient_id,age,gender,blood_pressure,heart_rate,diagnosis,treatment"
        data_rows = []
        for i in range(rows):
            data_rows.append(
                f"P{i+1000},{random.randint(18, 85)},{random.choice(['M', 'F'])},"
                f"{random.randint(90, 180)}/{random.randint(60, 120)},"
                f"{random.randint(60, 100)},{random.choice(['Hypertension', 'Diabetes', 'Healthy', 'Flu'])},"
                f"{random.choice(['Medication', 'Surgery', 'Therapy', 'None'])}"
            )
    
    elif category == "Finance":
        headers = "transaction_id,account_id,amount,type,category,date,status"
        data_rows = []
        for i in range(rows):
            data_rows.append(
                f"TX{i+5000},ACC{random.randint(1000, 9999)},{random.uniform(10, 5000):.2f},"
                f"{random.choice(['debit', 'credit'])},{random.choice(['groceries', 'utilities', 'entertainment', 'salary'])},"
                f"2024-{random.randint(1, 12):02d}-{random.randint(1, 28):02d},"
                f"{random.choice(['completed', 'pending', 'failed'])}"
            )
    
    elif category == "Business":
        headers = "employee_id,name,department,salary,performance_score,years_experience,position"
        data_rows = []
        for i in range(rows):
            data_rows.append(
                f"E{i+1000},Employee_{i},{random.choice(['Sales', 'Engineering', 'Marketing', 'HR'])},"
                f"{random.randint(40000, 150000)},{random.uniform(3.0, 5.0):.1f},"
                f"{random.randint(0, 20)},{random.choice(['Junior', 'Senior', 'Manager', 'Director'])}"
            )
    
    elif category == "Retail":
        headers = "product_id,name,category,price,stock,sales,rating"
        data_rows = []
        for i in range(rows):
            data_rows.append(
                f"PROD{i+1000},Product_{i},{random.choice(['Electronics', 'Clothing', 'Food', 'Books'])},"
                f"{random.uniform(5, 500):.2f},{random.randint(0, 1000)},"
                f"{random.randint(0, 500)},{random.uniform(1.0, 5.0):.1f}"
            )
    
    else:  # Image or default
        headers = "image_id,width,height,format,size_kb,category,tags"
        data_rows = []
        for i in range(rows):
            data_rows.append(
                f"IMG{i+1000},{random.choice([256, 512, 1024])},{random.choice([256, 512, 1024])},"
                f"{random.choice(['jpg', 'png', 'webp'])},{random.randint(50, 5000)},"
                f"{random.choice(['nature', 'urban', 'portrait', 'abstract'])},"
                f"{random.choice(['outdoor', 'indoor', 'landscape', 'people'])}"
            )
    
    csv_content = headers + "\n" + "\n".join(data_rows)
    return csv_content.encode('utf-8')

def create_sample_datasets():
    """Create sample datasets for the marketplace"""
    
    sample_datasets = [
        {
            "title": "Healthcare Patient Records 2024",
            "description": "Comprehensive synthetic patient data including vital signs, diagnoses, and treatment information. Perfect for healthcare analytics and ML model training.",
            "category": "Medical",
            "price": 49.99,
            "uploader": "0xHealthDataLabs",
            "tags": ["healthcare", "patients", "medical-records", "synthetic", "ml-ready"],
            "rows": 1000
        },
        {
            "title": "Financial Transaction Dataset",
            "description": "Realistic financial transaction data with multiple account types, transaction categories, and temporal patterns. Ideal for fraud detection and financial analysis.",
            "category": "Finance",
            "price": 79.99,
            "uploader": "0xFinTechAnalytics",
            "tags": ["finance", "transactions", "banking", "fraud-detection", "time-series"],
            "rows": 5000
        },
        {
            "title": "Employee Performance Metrics",
            "description": "Synthetic HR dataset with employee information, performance scores, and departmental data. Great for HR analytics and workforce planning.",
            "category": "Business",
            "price": 39.99,
            "uploader": "0xHRInsights",
            "tags": ["hr", "employees", "performance", "business-analytics", "workforce"],
            "rows": 500
        },
        {
            "title": "E-Commerce Product Catalog",
            "description": "Complete product dataset with pricing, inventory, sales data, and customer ratings. Perfect for retail analytics and recommendation systems.",
            "category": "Retail",
            "price": 29.99,
            "uploader": "0xRetailDataHub",
            "tags": ["ecommerce", "products", "retail", "sales", "inventory"],
            "rows": 2000
        },
        {
            "title": "Medical Imaging Metadata",
            "description": "Comprehensive metadata for medical imaging datasets including dimensions, formats, and categorization. Useful for medical AI applications.",
            "category": "Image",
            "price": 0,  # Free dataset
            "uploader": "0xMedicalAI",
            "tags": ["medical", "imaging", "metadata", "free", "ai-training"],
            "rows": 1500
        },
        {
            "title": "Global Financial Markets Data",
            "description": "High-quality synthetic market data including stock prices, trading volumes, and market indicators. Essential for quantitative finance research.",
            "category": "Finance",
            "price": 99.99,
            "uploader": "0xQuantFinance",
            "tags": ["stocks", "trading", "markets", "quantitative", "premium"],
            "rows": 10000
        },
        {
            "title": "Customer Behavior Analytics",
            "description": "Synthetic customer journey data with purchase patterns, browsing behavior, and demographic information. Perfect for marketing analytics.",
            "category": "Business",
            "price": 59.99,
            "uploader": "0xMarketingPro",
            "tags": ["customers", "behavior", "marketing", "analytics", "segmentation"],
            "rows": 3000
        },
        {
            "title": "Clinical Trial Results",
            "description": "Synthetic clinical trial data with patient outcomes, treatment efficacy, and safety metrics. Ideal for pharmaceutical research and analysis.",
            "category": "Medical",
            "price": 149.99,
            "uploader": "0xPharmaResearch",
            "tags": ["clinical-trials", "pharmaceutical", "research", "outcomes", "premium"],
            "rows": 800
        },
        {
            "title": "Retail Sales Forecasting Dataset",
            "description": "Time-series retail sales data with seasonal patterns, promotional effects, and external factors. Great for demand forecasting models.",
            "category": "Retail",
            "price": 0,  # Free dataset
            "uploader": "0xDataScience",
            "tags": ["sales", "forecasting", "time-series", "free", "ml-ready"],
            "rows": 2500
        },
        {
            "title": "Enterprise IT Infrastructure Logs",
            "description": "Synthetic IT system logs with performance metrics, error patterns, and security events. Perfect for DevOps and security analytics.",
            "category": "Business",
            "price": 69.99,
            "uploader": "0xDevOpsData",
            "tags": ["it", "logs", "infrastructure", "security", "monitoring"],
            "rows": 5000
        }
    ]
    
    print("🚀 Initializing sample datasets...")
    created_count = 0
    
    for dataset_info in sample_datasets:
        try:
            # Generate sample data
            rows = dataset_info.pop("rows", 1000)
            csv_data = generate_sample_csv_data(dataset_info["category"], rows)
            
            # Perform quality assessment
            quality_assessment = quality_service.assess_dataset_quality(
                csv_data, 
                dataset_info["category"]
            )
            
            # Prepare metadata
            timestamp = datetime.utcnow() - timedelta(days=random.randint(1, 30))
            metadata = {
                **dataset_info,
                "timestamp": timestamp.isoformat(),
                "quality_score": quality_assessment.overall_score,
                "quality_metrics": quality_assessment.metrics.dict(),
                "quality_explanation": quality_assessment.explanation,
                "quality_recommendations": quality_assessment.recommendations,
                "file_name": f"{dataset_info['title'].replace(' ', '_').lower()}.csv",
                "file_size": len(csv_data),
                "rows": rows,
                "columns": len(csv_data.decode('utf-8').split('\n')[0].split(','))
            }
            
            # Store in IPFS mimic
            cid = ipfs.store_file(csv_data, metadata)
            
            print(f"✅ Created: {dataset_info['title']} (CID: {cid[:16]}...)")
            created_count += 1
            
        except Exception as e:
            print(f"❌ Failed to create {dataset_info['title']}: {str(e)}")
    
    print(f"\n🎉 Successfully created {created_count}/{len(sample_datasets)} sample datasets!")
    print(f"📊 Total storage: {ipfs.get_storage_stats()['total_size_mb']} MB")

if __name__ == "__main__":
    create_sample_datasets()
