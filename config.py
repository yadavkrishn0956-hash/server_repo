import os
from pathlib import Path

# Storage configuration
STORAGE_PATH = Path(os.getenv("STORAGE_PATH", "./storage"))
DATASETS_PATH = STORAGE_PATH / "datasets"
METADATA_PATH = STORAGE_PATH / "metadata"
LEDGER_PATH = STORAGE_PATH / "ledger"

# Ensure directories exist
DATASETS_PATH.mkdir(parents=True, exist_ok=True)
METADATA_PATH.mkdir(parents=True, exist_ok=True)
LEDGER_PATH.mkdir(parents=True, exist_ok=True)

# API configuration
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",")
# Add wildcard for Vercel deployments
if os.getenv("VERCEL"):
    CORS_ORIGINS.append("*")
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB
ALLOWED_FILE_TYPES = {".csv", ".json", ".zip"}

# Dataset generation limits
MAX_ROWS = 100000
MAX_COLUMNS = 1000
DEFAULT_ROWS = 1000
DEFAULT_COLUMNS = 10