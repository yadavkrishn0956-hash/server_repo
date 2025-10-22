from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse
from typing import Optional
import io

from models import DatasetGenerationRequest, APIResponse
from services.dataset_service import dataset_service
from services.blockchain_ledger import blockchain

router = APIRouter(prefix="/api", tags=["Dataset Generation"])

@router.post("/generate", response_model=APIResponse)
async def generate_dataset(request: DatasetGenerationRequest):
    """Generate synthetic dataset based on parameters"""
    
    try:
        # Validate request parameters
        if request.rows <= 0 or request.rows > 100000:
            raise HTTPException(status_code=400, detail="Rows must be between 1 and 100,000")
        
        if request.columns <= 0 or request.columns > 1000:
            raise HTTPException(status_code=400, detail="Columns must be between 1 and 1,000")
        
        # Generate the dataset
        result = dataset_service.generate_dataset(request)
        
        return APIResponse(
            success=True,
            message="Dataset generated successfully",
            data=result
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Dataset generation failed: {str(e)}")

@router.get("/preview/{cid}", response_model=APIResponse)
async def get_dataset_preview(cid: str):
    """Get preview of a generated dataset"""
    
    try:
        preview_data = dataset_service.get_dataset_preview(cid)
        
        if not preview_data:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        return APIResponse(
            success=True,
            message="Preview retrieved successfully",
            data=preview_data
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Preview generation failed: {str(e)}")

@router.get("/download/{cid}")
async def download_dataset(cid: str, format: str = "zip", buyer: Optional[str] = None):
    """Download dataset by CID (with authorization check for purchased datasets)"""
    
    try:
        # Check if dataset exists
        metadata = dataset_service.ipfs.get_metadata(cid)
        
        # If not found in IPFS, check seed data
        if not metadata and cid.startswith("seed"):
            from seed_data import get_seed_datasets
            seed_datasets = get_seed_datasets()
            for dataset in seed_datasets:
                if dataset["cid"] == cid:
                    metadata = dataset
                    break
        
        if not metadata:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        # Check if this is a paid dataset and if user has purchased it
        if metadata.get("price", 0) > 0 and buyer:
            if not blockchain.is_dataset_purchased(cid, buyer):
                raise HTTPException(
                    status_code=403, 
                    detail="Dataset not purchased. Please complete payment first."
                )
        
        # Get the dataset file
        file_data = dataset_service.download_dataset(cid, format)
        
        # If seed dataset, generate demo CSV
        if not file_data and cid.startswith("seed"):
            import csv
            from io import StringIO
            csv_buffer = StringIO()
            writer = csv.writer(csv_buffer)
            writer.writerow(['id', 'value', 'category', 'timestamp'])
            for i in range(10):
                writer.writerow([i, f'demo_value_{i}', metadata.get('category', 'Demo'), f'2024-01-{i+1:02d}'])
            file_data = csv_buffer.getvalue().encode('utf-8')
        
        if not file_data:
            raise HTTPException(status_code=404, detail="Dataset file not found")
        
        # Determine content type and filename based on format
        if format.lower() == "csv":
            # Try to extract CSV from ZIP
            csv_data = dataset_service.extract_csv_from_zip(file_data)
            if csv_data:
                file_data = csv_data
                content_type = "text/csv"
                filename = f"dataset_{cid[:8]}.csv"
            else:
                content_type = "application/zip"
                filename = f"dataset_{cid[:8]}.zip"
        else:
            content_type = "application/zip"
            filename = f"dataset_{cid[:8]}.zip"
        
        # Return file as streaming response
        return StreamingResponse(
            io.BytesIO(file_data),
            media_type=content_type,
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Download failed: {str(e)}")

@router.get("/formats/{cid}", response_model=APIResponse)
async def get_available_formats(cid: str):
    """Get available download formats for a dataset"""
    
    try:
        formats = dataset_service.get_dataset_formats(cid)
        
        if not formats:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        return APIResponse(
            success=True,
            message="Available formats retrieved",
            data={"formats": formats}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get formats: {str(e)}")

@router.get("/stats/{cid}", response_model=APIResponse)
async def get_dataset_statistics(cid: str):
    """Get detailed statistics for a dataset"""
    
    try:
        stats = dataset_service.get_dataset_statistics(cid)
        
        if not stats:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        return APIResponse(
            success=True,
            message="Dataset statistics retrieved",
            data=stats
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get statistics: {str(e)}")