import pandas as pd
import numpy as np
import io
import zipfile
import json
from typing import Dict, Any, Optional, Union, List
from datetime import datetime

from .data_generator import data_generator
from .image_generator import image_generator
from .ipfs_mimic import ipfs
from models import DatasetGenerationRequest, DatasetPreview

class DatasetService:
    """Service for handling dataset generation, preview, and download functionality"""
    
    def __init__(self):
        self.data_generator = data_generator
        self.image_generator = image_generator
        self.ipfs = ipfs
    
    def generate_dataset(self, request: DatasetGenerationRequest) -> Dict[str, Any]:
        """Generate dataset based on request parameters"""
        
        if request.category == "Image":
            return self._generate_image_dataset(request)
        else:
            return self._generate_structured_dataset(request)
    
    def _generate_structured_dataset(self, request: DatasetGenerationRequest) -> Dict[str, Any]:
        """Generate structured (tabular) dataset"""
        
        # Generate the dataset
        df = self.data_generator.generate_dataset(
            category=request.category,
            rows=request.rows,
            columns=request.columns
        )
        
        # Add some realistic imperfections
        df = self.data_generator.add_noise_and_missing_values(df)
        
        # Generate preview
        preview_data = self.data_generator.get_dataset_preview(df)
        
        # Convert to CSV bytes for storage
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_bytes = csv_buffer.getvalue().encode('utf-8')
        
        # Create ZIP with CSV and metadata
        zip_bytes = self._create_structured_zip(df, csv_bytes, request)
        
        # Perform quality assessment
        from .quality_assessment import quality_service
        quality_assessment = quality_service.assess_dataset_quality(csv_bytes, request.category)
        
        # Store in IPFS mimic
        metadata = {
            "title": request.title or f"{request.category} Dataset",
            "description": request.description or f"Synthetic {request.category.lower()} dataset with {request.rows} rows and {request.columns} columns",
            "category": request.category,
            "uploader": "system",  # In real system, this would be the authenticated user
            "timestamp": datetime.utcnow().isoformat(),
            "rows": request.rows,
            "columns": request.columns,
            "format": "CSV",
            "quality_score": quality_assessment.overall_score,
            "quality_metrics": quality_assessment.metrics.dict(),
            "quality_explanation": quality_assessment.explanation,
            "quality_recommendations": quality_assessment.recommendations,
            "tags": [request.category.lower(), "synthetic", "generated"]
        }
        
        cid = self.ipfs.store_file(zip_bytes, metadata)
        
        return {
            "cid": cid,
            "preview": preview_data,
            "metadata": metadata,
            "file_size_mb": round(len(zip_bytes) / (1024 * 1024), 2)
        }
    
    def _generate_image_dataset(self, request: DatasetGenerationRequest) -> Dict[str, Any]:
        """Generate image dataset"""
        
        # For image datasets, 'rows' represents number of images
        num_images = request.rows
        
        # Generate images
        images = self.image_generator.generate_image_dataset(num_images)
        
        # Generate preview
        preview_data = self.image_generator.get_image_preview_data(images)
        
        # Convert to ZIP bytes
        zip_bytes = self.image_generator.images_to_zip_bytes(images)
        
        # Store in IPFS mimic
        metadata = {
            "title": request.title or f"Image Dataset ({num_images} images)",
            "description": request.description or f"Synthetic image dataset with {num_images} 32x32 pixel images",
            "category": request.category,
            "uploader": "system",
            "timestamp": datetime.utcnow().isoformat(),
            "rows": num_images,  # Number of images
            "columns": 3072,  # 32*32*3 pixels
            "format": "PNG (ZIP archive)",
            "tags": ["image", "synthetic", "32x32", "generated"]
        }
        
        cid = self.ipfs.store_file(zip_bytes, metadata)
        
        return {
            "cid": cid,
            "preview": preview_data,
            "metadata": metadata,
            "file_size_mb": round(len(zip_bytes) / (1024 * 1024), 2)
        }
    
    def _create_structured_zip(self, df: pd.DataFrame, csv_bytes: bytes, 
                             request: DatasetGenerationRequest) -> bytes:
        """Create ZIP file containing CSV and metadata for structured datasets"""
        
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # Add CSV file
            zip_file.writestr('dataset.csv', csv_bytes)
            
            # Add metadata
            metadata = {
                "dataset_info": {
                    "category": request.category,
                    "rows": len(df),
                    "columns": len(df.columns),
                    "column_names": list(df.columns),
                    "data_types": {col: str(dtype) for col, dtype in df.dtypes.items()},
                    "generated_at": datetime.utcnow().isoformat()
                },
                "statistics": {
                    "missing_values": df.isnull().sum().to_dict(),
                    "numeric_columns": list(df.select_dtypes(include=[np.number]).columns),
                    "categorical_columns": list(df.select_dtypes(include=['object']).columns)
                }
            }
            
            zip_file.writestr('metadata.json', json.dumps(metadata, indent=2, default=str))
            
            # Add data dictionary/schema
            schema = {
                "columns": []
            }
            
            for col in df.columns:
                col_info = {
                    "name": col,
                    "type": str(df[col].dtype),
                    "description": f"Generated {col} data for {request.category} category"
                }
                
                if df[col].dtype in ['int64', 'float64']:
                    col_info.update({
                        "min": float(df[col].min()) if not df[col].isnull().all() else None,
                        "max": float(df[col].max()) if not df[col].isnull().all() else None,
                        "mean": float(df[col].mean()) if not df[col].isnull().all() else None
                    })
                
                schema["columns"].append(col_info)
            
            zip_file.writestr('schema.json', json.dumps(schema, indent=2, default=str))
        
        zip_buffer.seek(0)
        return zip_buffer.getvalue()
    
    def get_dataset_preview(self, cid: str) -> Optional[Dict[str, Any]]:
        """Get preview of an existing dataset"""
        
        metadata = self.ipfs.get_metadata(cid)
        if not metadata:
            return None
        
        # For now, return metadata as preview
        # In a full implementation, we might extract and preview the actual data
        return {
            "cid": cid,
            "metadata": metadata,
            "preview_available": True
        }
    
    def download_dataset(self, cid: str, format_type: str = "zip") -> Optional[bytes]:
        """Download dataset by CID"""
        
        file_data = self.ipfs.retrieve_file(cid)
        if not file_data:
            return None
        
        # For now, we always return the stored ZIP file
        # In a full implementation, we might convert between formats
        return file_data
    
    def get_dataset_formats(self, cid: str) -> List[str]:
        """Get available download formats for a dataset"""
        
        metadata = self.ipfs.get_metadata(cid)
        if not metadata:
            return []
        
        # For now, we support ZIP format
        # Could be extended to support CSV, JSON, etc.
        available_formats = ["zip"]
        
        if metadata.get("category") != "Image":
            available_formats.extend(["csv"])
        
        return available_formats
    
    def extract_csv_from_zip(self, zip_bytes: bytes) -> Optional[bytes]:
        """Extract CSV file from ZIP archive"""
        
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes), 'r') as zip_file:
                if 'dataset.csv' in zip_file.namelist():
                    return zip_file.read('dataset.csv')
        except Exception:
            pass
        
        return None
    
    def get_dataset_statistics(self, cid: str) -> Optional[Dict[str, Any]]:
        """Get detailed statistics for a dataset"""
        
        metadata = self.ipfs.get_metadata(cid)
        if not metadata:
            return None
        
        file_data = self.ipfs.retrieve_file(cid)
        if not file_data:
            return None
        
        stats = {
            "cid": cid,
            "file_size_bytes": len(file_data),
            "file_size_mb": round(len(file_data) / (1024 * 1024), 2),
            "created_at": metadata.get("timestamp"),
            "category": metadata.get("category"),
            "format": metadata.get("format", "ZIP")
        }
        
        # Add category-specific stats
        if metadata.get("category") == "Image":
            stats.update({
                "total_images": metadata.get("rows", 0),
                "image_dimensions": "32x32",
                "channels": 3
            })
        else:
            stats.update({
                "total_rows": metadata.get("rows", 0),
                "total_columns": metadata.get("columns", 0)
            })
        
        return stats

# Global instance
dataset_service = DatasetService()