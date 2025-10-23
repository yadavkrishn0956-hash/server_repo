from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from typing import Optional, List
from datetime import datetime
import random

from models import APIResponse, DatasetMetadata, PurchaseRequest, Transaction
from services.ipfs_mimic import ipfs
from services.quality_assessment import quality_service
from services.blockchain_ledger import blockchain
from config import MAX_FILE_SIZE, ALLOWED_FILE_TYPES

router = APIRouter(prefix="/api", tags=["Marketplace"])

# Seed data for demo
def get_seed_datasets():
    """Get seed datasets for demo purposes"""
    base_time = datetime.utcnow()
    return [
        {
            "cid": "seed0001" + "a" * 56,
            "title": "Global Financial Markets Data",
            "description": "High-quality synthetic market data including stock prices, trading volumes, and market indicators.",
            "category": "Finance",
            "uploader": "0x742d35Cc6634C0532925a3b8D4C0C8b3C2e1e416",
            "price": 99.99,
            "quality_score": 96,
            "tags": ["stocks", "trading", "markets", "quantitative", "premium"],
            "rows": 10000,
            "columns": 7,
            "file_size": 598000,
            "timestamp": base_time.isoformat(),
            "quality_metrics": {"completeness": 98, "consistency": 96, "accuracy": 97, "uniqueness": 94}
        },
        {
            "cid": "seed0002" + "b" * 56,
            "title": "E-Commerce Customer Behavior Dataset",
            "description": "Comprehensive customer behavior data for e-commerce analytics.",
            "category": "Business",
            "uploader": "0x8f3B9C1d4E2A5F6c7D8E9F0A1B2C3D4E5F6A7B8C",
            "price": 49.99,
            "quality_score": 88,
            "tags": ["ecommerce", "customers", "analytics", "behavior"],
            "rows": 5000,
            "columns": 12,
            "file_size": 450000,
            "timestamp": base_time.isoformat(),
            "quality_metrics": {"completeness": 90, "consistency": 88, "accuracy": 89, "uniqueness": 85}
        },
        {
            "cid": "seed0003" + "c" * 56,
            "title": "Medical Patient Records (Synthetic)",
            "description": "Synthetic patient health records for medical research and ML model training.",
            "category": "Medical",
            "uploader": "0x1A2B3C4D5E6F7A8B9C0D1E2F3A4B5C6D7E8F9A0B",
            "price": 149.99,
            "quality_score": 94,
            "tags": ["medical", "healthcare", "patients", "research", "premium"],
            "rows": 8000,
            "columns": 15,
            "file_size": 1200000,
            "timestamp": base_time.isoformat(),
            "quality_metrics": {"completeness": 96, "consistency": 94, "accuracy": 95, "uniqueness": 92}
        },
        {
            "cid": "seed0004" + "d" * 56,
            "title": "Retail Sales Transaction Data",
            "description": "Synthetic retail sales data with product information and customer demographics.",
            "category": "Retail",
            "uploader": "0x9E8D7C6B5A4F3E2D1C0B9A8F7E6D5C4B3A2F1E0D",
            "price": 29.99,
            "quality_score": 85,
            "tags": ["retail", "sales", "transactions", "products"],
            "rows": 15000,
            "columns": 8,
            "file_size": 890000,
            "timestamp": base_time.isoformat(),
            "quality_metrics": {"completeness": 87, "consistency": 85, "accuracy": 86, "uniqueness": 82}
        },
        {
            "cid": "seed0005" + "e" * 56,
            "title": "Social Media Engagement Metrics",
            "description": "Synthetic social media engagement data including likes, shares, and comments.",
            "category": "Business",
            "uploader": "0x2F3E4D5C6B7A8F9E0D1C2B3A4F5E6D7C8B9A0F1E",
            "price": 0,
            "quality_score": 78,
            "tags": ["social", "engagement", "metrics", "free"],
            "rows": 20000,
            "columns": 10,
            "file_size": 1100000,
            "timestamp": base_time.isoformat(),
            "quality_metrics": {"completeness": 80, "consistency": 78, "accuracy": 79, "uniqueness": 75}
        },
        {
            "cid": "seed0006" + "f" * 56,
            "title": "Weather and Climate Dataset",
            "description": "Comprehensive weather data with temperature, precipitation, and atmospheric conditions.",
            "category": "Science",
            "uploader": "0x3A4B5C6D7E8F9A0B1C2D3E4F5A6B7C8D9E0F1A2B",
            "price": 0,
            "quality_score": 92,
            "tags": ["weather", "climate", "science", "free"],
            "rows": 50000,
            "columns": 6,
            "file_size": 2500000,
            "timestamp": base_time.isoformat(),
            "quality_metrics": {"completeness": 94, "consistency": 92, "accuracy": 93, "uniqueness": 90}
        }
    ]

@router.post("/upload", response_model=APIResponse)
async def upload_dataset(
    file: UploadFile = File(...),
    title: str = Form(...),
    description: str = Form(...),
    category: str = Form(...),
    price: float = Form(ge=0),
    uploader: str = Form(...),
    tags: Optional[str] = Form("")
):
    """Upload dataset for selling with quality assessment"""
    
    try:
        # Validate file size
        file_content = await file.read()
        if len(file_content) > MAX_FILE_SIZE:
            raise HTTPException(
                status_code=413, 
                detail=f"File too large. Maximum size is {MAX_FILE_SIZE // (1024*1024)}MB"
            )
        
        # Validate file type
        file_extension = file.filename.split('.')[-1].lower() if file.filename else ""
        if f".{file_extension}" not in ALLOWED_FILE_TYPES:
            raise HTTPException(
                status_code=400,
                detail=f"File type not allowed. Supported types: {', '.join(ALLOWED_FILE_TYPES)}"
            )
        
        # Perform quality assessment
        quality_assessment = quality_service.assess_dataset_quality(file_content, category)
        
        # Prepare metadata
        tags_list = [tag.strip() for tag in tags.split(",") if tag.strip()] if tags else []
        tags_list.extend([category.lower(), "uploaded"])
        
        metadata = {
            "title": title,
            "description": description,
            "category": category,
            "uploader": uploader,
            "timestamp": datetime.utcnow().isoformat(),
            "price": price,
            "tags": tags_list,
            "quality_score": quality_assessment.overall_score,
            "quality_metrics": quality_assessment.metrics.dict(),
            "quality_explanation": quality_assessment.explanation,
            "quality_recommendations": quality_assessment.recommendations,
            "file_name": file.filename,
            "file_size": len(file_content)
        }
        
        # Store in IPFS mimic
        cid = ipfs.store_file(file_content, metadata)
        
        # Create response data
        response_data = {
            "cid": cid,
            "quality_assessment": quality_assessment.dict(),
            "metadata": metadata,
            "file_size_mb": round(len(file_content) / (1024 * 1024), 2),
            "quality_color": quality_service.get_quality_indicator_color(quality_assessment.overall_score)
        }
        
        return APIResponse(
            success=True,
            message="Dataset uploaded and assessed successfully",
            data=response_data
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@router.get("/datasets", response_model=APIResponse)
async def list_datasets(
    category: Optional[str] = None,
    min_quality: Optional[int] = None,
    max_price: Optional[float] = None,
    search: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
):
    """List all available datasets with filtering options"""
    
    try:
        # Always return seed data on Vercel (no persistent storage)
        seed_datasets = get_seed_datasets()
            
        # Apply filters to seed data
        filtered_datasets = []
        for dataset in seed_datasets:
                if category and dataset.get("category", "").lower() != category.lower():
                    continue
                if min_quality and dataset.get("quality_score", 0) < min_quality:
                    continue
                if max_price is not None and dataset.get("price", 0) > max_price:
                    continue
                if search:
                    search_text = f"{dataset.get('title', '')} {dataset.get('description', '')} {' '.join(dataset.get('tags', []))}".lower()
                    if search.lower() not in search_text:
                        continue
                filtered_datasets.append(dataset)
            
        # Apply pagination
        total_count = len(filtered_datasets)
        paginated_datasets = filtered_datasets[offset:offset + limit]
        
        return APIResponse(
            success=True,
            message=f"Found {total_count} datasets",
            data={
                "datasets": paginated_datasets,
                "total_count": total_count,
                "limit": limit,
                "offset": offset,
                "has_more": offset + limit < total_count
            }
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list datasets: {str(e)}")



@router.get("/metadata/{cid}", response_model=APIResponse)
async def get_dataset_metadata(cid: str):
    """Get detailed metadata for a specific dataset"""
    
    try:
        metadata = ipfs.get_metadata(cid)
        
        # If not found in IPFS, check seed data
        if not metadata and cid.startswith("seed"):
            seed_datasets = get_seed_datasets()
            for dataset in seed_datasets:
                if dataset["cid"] == cid:
                    metadata = dataset
                    break
        
        if not metadata:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        # Add quality color indicator
        metadata["quality_color"] = quality_service.get_quality_indicator_color(
            metadata.get("quality_score", 0)
        )
        
        # Add transaction history
        transactions = blockchain.get_dataset_transactions(cid)
        metadata["transaction_history"] = transactions
        metadata["total_sales"] = len([tx for tx in transactions if tx["status"] == "completed"])
        
        return APIResponse(
            success=True,
            message="Metadata retrieved successfully",
            data=metadata
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get metadata: {str(e)}")

@router.get("/categories", response_model=APIResponse)
async def get_available_categories():
    """Get list of available dataset categories"""
    
    try:
        # Get all datasets and extract unique categories
        all_cids = ipfs.list_all_cids()
        categories = set()
        
        for cid in all_cids:
            metadata = ipfs.get_metadata(cid)
            if metadata and metadata.get("category"):
                categories.add(metadata["category"])
        
        # Add standard categories
        standard_categories = ["Medical", "Finance", "Business", "Retail", "Image"]
        categories.update(standard_categories)
        
        return APIResponse(
            success=True,
            message="Categories retrieved successfully",
            data={"categories": sorted(list(categories))}
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get categories: {str(e)}")

@router.get("/search", response_model=APIResponse)
async def search_datasets(
    q: str,
    category: Optional[str] = None,
    min_quality: Optional[int] = None,
    max_price: Optional[float] = None,
    limit: int = 20
):
    """Search datasets by title, description, and tags"""
    
    try:
        # Use the existing list_datasets function with search parameter
        result = await list_datasets(
            category=category,
            min_quality=min_quality,
            max_price=max_price,
            search=q,
            limit=limit,
            offset=0
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")

@router.get("/stats", response_model=APIResponse)
async def get_marketplace_stats():
    """Get overall marketplace statistics"""
    
    try:
        # Get dataset statistics
        all_cids = ipfs.list_all_cids()
        total_datasets = len(all_cids)
        
        # Category distribution
        category_counts = {}
        quality_distribution = {"high": 0, "medium": 0, "low": 0}
        total_size = 0
        
        for cid in all_cids:
            metadata = ipfs.get_metadata(cid)
            if metadata:
                # Category count
                category = metadata.get("category", "Unknown")
                category_counts[category] = category_counts.get(category, 0) + 1
                
                # Quality distribution
                quality_score = metadata.get("quality_score", 0)
                if quality_score >= 80:
                    quality_distribution["high"] += 1
                elif quality_score >= 60:
                    quality_distribution["medium"] += 1
                else:
                    quality_distribution["low"] += 1
                
                # Total size
                total_size += metadata.get("file_size", 0)
        
        # Get blockchain statistics
        blockchain_stats = blockchain.get_ledger_stats()
        
        # Get IPFS statistics
        ipfs_stats = ipfs.get_storage_stats()
        
        stats = {
            "datasets": {
                "total_count": total_datasets,
                "category_distribution": category_counts,
                "quality_distribution": quality_distribution,
                "total_size_mb": round(total_size / (1024 * 1024), 2)
            },
            "transactions": blockchain_stats,
            "storage": ipfs_stats
        }
        
        return APIResponse(
            success=True,
            message="Marketplace statistics retrieved",
            data=stats
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get stats: {str(e)}")