from pydantic import BaseModel, Field
from typing import List, Optional, Literal
from datetime import datetime

# Dataset generation models
class DatasetGenerationRequest(BaseModel):
    category: Literal["Medical", "Finance", "Business", "Retail", "Image"]
    rows: int = Field(ge=1, le=100000, default=1000)
    columns: int = Field(ge=1, le=1000, default=10)
    title: Optional[str] = None
    description: Optional[str] = None

class DatasetPreview(BaseModel):
    sample_data: List[dict]
    total_rows: int
    total_columns: int
    file_size_mb: float

# Quality assessment models
class QualityMetrics(BaseModel):
    completeness: float = Field(ge=0, le=100)
    statistical_consistency: float = Field(ge=0, le=100)
    class_balance: float = Field(ge=0, le=100)
    duplicates: float = Field(ge=0, le=100)
    outliers: float = Field(ge=0, le=100)
    schema_match: float = Field(ge=0, le=100)

class QualityAssessment(BaseModel):
    overall_score: int = Field(ge=0, le=100)
    metrics: QualityMetrics
    explanation: List[str]
    recommendations: List[str]

# Dataset metadata models
class DatasetMetadata(BaseModel):
    cid: str
    title: str
    category: Literal["Medical", "Finance", "Business", "Retail", "Image"]
    uploader: str
    timestamp: datetime
    quality_score: int = Field(ge=0, le=100)
    rows: int
    columns: int
    file_size: int
    price: float = Field(ge=0)
    description: str
    tags: List[str] = []

# Transaction models
class PurchaseRequest(BaseModel):
    cid: str
    buyer: str
    amount: float = Field(ge=0)

class Transaction(BaseModel):
    tx_id: str
    cid: str
    buyer: str
    seller: str
    amount: float
    timestamp: datetime
    status: Literal["pending", "completed", "failed"]
    escrow_released: bool = False

# API response models
class APIResponse(BaseModel):
    success: bool
    message: str
    data: Optional[dict] = None

class ErrorResponse(BaseModel):
    error: dict