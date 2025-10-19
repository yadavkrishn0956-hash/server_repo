from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from typing import Optional, List
from datetime import datetime

from models import APIResponse, DatasetMetadata, PurchaseRequest, Transaction
from services.ipfs_mimic import ipfs
from services.quality_assessment import quality_service
from services.blockchain_ledger import blockchain
from config import MAX_FILE_SIZE, ALLOWED_FILE_TYPES

router = APIRouter(prefix="/api", tags=["Marketplace"])

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
        # Get all CIDs
        all_cids = ipfs.list_all_cids()
        
        datasets = []
        for cid in all_cids:
            metadata = ipfs.get_metadata(cid)
            if not metadata:
                continue
            
            # Apply filters
            if category and metadata.get("category", "").lower() != category.lower():
                continue
            
            if min_quality and metadata.get("quality_score", 0) < min_quality:
                continue
            
            if max_price is not None and metadata.get("price", 0) > max_price:
                continue
            
            if search:
                search_text = f"{metadata.get('title', '')} {metadata.get('description', '')} {' '.join(metadata.get('tags', []))}".lower()
                if search.lower() not in search_text:
                    continue
            
            # Create dataset info
            dataset_info = {
                "cid": cid,
                "title": metadata.get("title", "Untitled Dataset"),
                "description": metadata.get("description", ""),
                "category": metadata.get("category", "Unknown"),
                "uploader": metadata.get("uploader", "Anonymous"),
                "timestamp": metadata.get("timestamp", ""),
                "quality_score": metadata.get("quality_score", 0),
                "price": metadata.get("price", 0),
                "file_size": metadata.get("file_size", 0),
                "tags": metadata.get("tags", []),
                "quality_color": quality_service.get_quality_indicator_color(metadata.get("quality_score", 0))
            }
            
            datasets.append(dataset_info)
        
        # Sort by: free datasets first, then by quality score (descending), then by timestamp (newest first)
        datasets.sort(key=lambda x: (x["price"] != 0, -x["quality_score"], x["timestamp"]), reverse=False)
        
        # Apply pagination
        total_count = len(datasets)
        paginated_datasets = datasets[offset:offset + limit]
        
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