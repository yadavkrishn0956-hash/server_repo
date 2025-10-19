from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os
from config import CORS_ORIGINS
from models import APIResponse, ErrorResponse

app = FastAPI(
    title="Decentralized Synthetic Data Market API",
    description="Backend API for the synthetic data marketplace",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            error={
                "code": "INTERNAL_SERVER_ERROR",
                "message": "An unexpected error occurred",
                "details": str(exc),
                "timestamp": "2024-01-15T10:30:00Z"
            }
        ).dict()
    )

@app.get("/", response_model=APIResponse)
async def root():
    return APIResponse(
        success=True,
        message="Decentralized Synthetic Data Market API",
        data={"version": "1.0.0", "status": "running"}
    )

@app.get("/health", response_model=APIResponse)
async def health_check():
    return APIResponse(
        success=True,
        message="Service is healthy",
        data={"status": "healthy", "service": "data-market-api"}
    )

# Import route modules
from routes.generator import router as generator_router
from routes.marketplace import router as marketplace_router
from routes.transactions import router as transactions_router

# Include routers
app.include_router(generator_router)
app.include_router(marketplace_router)
app.include_router(transactions_router)

# Initialize sample data on startup
@app.on_event("startup")
async def startup_event():
    """Initialize sample datasets if none exist"""
    from services.ipfs_mimic import ipfs
    
    # Check if we already have datasets
    existing_cids = ipfs.list_all_cids()
    
    if len(existing_cids) == 0:
        print("📦 No datasets found. Initializing sample data...")
        try:
            from init_sample_data import create_sample_datasets
            create_sample_datasets()
        except Exception as e:
            print(f"⚠️  Failed to initialize sample data: {str(e)}")
    else:
        print(f"✅ Found {len(existing_cids)} existing datasets")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)