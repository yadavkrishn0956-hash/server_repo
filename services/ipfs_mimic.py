import hashlib
import json
import os
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime
from config import DATASETS_PATH, METADATA_PATH

class IPFSMimic:
    """
    Simulates IPFS functionality with local file storage and content addressing
    """
    
    def __init__(self):
        self.datasets_path = DATASETS_PATH
        self.metadata_path = METADATA_PATH
        
    def compute_cid(self, data_bytes: bytes) -> str:
        """Generate SHA-256 hash as Content Identifier (CID)"""
        return hashlib.sha256(data_bytes).hexdigest()
    
    def store_file(self, file_bytes: bytes, metadata: Dict[str, Any]) -> str:
        """
        Store file with content addressing and metadata
        Returns the CID of the stored file
        """
        # Generate CID from file content
        cid = self.compute_cid(file_bytes)
        
        # Store the actual file
        file_path = self.datasets_path / f"{cid}.bin"
        with open(file_path, "wb") as f:
            f.write(file_bytes)
        
        # Store metadata
        metadata_enhanced = {
            **metadata,
            "cid": cid,
            "file_size": len(file_bytes),
            "stored_at": datetime.utcnow().isoformat(),
            "file_path": str(file_path)
        }
        
        metadata_path = self.metadata_path / f"{cid}.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata_enhanced, f, indent=2, default=str)
        
        return cid
    
    def retrieve_file(self, cid: str) -> Optional[bytes]:
        """Retrieve file by CID"""
        file_path = self.datasets_path / f"{cid}.bin"
        
        if not file_path.exists():
            return None
            
        with open(file_path, "rb") as f:
            return f.read()
    
    def get_metadata(self, cid: str) -> Optional[Dict[str, Any]]:
        """Get metadata for a CID"""
        metadata_path = self.metadata_path / f"{cid}.json"
        
        if not metadata_path.exists():
            return None
            
        with open(metadata_path, "r") as f:
            return json.load(f)
    
    def list_all_cids(self) -> list[str]:
        """List all stored CIDs"""
        cids = []
        for metadata_file in self.metadata_path.glob("*.json"):
            cid = metadata_file.stem
            cids.append(cid)
        return cids
    
    def delete_file(self, cid: str) -> bool:
        """Delete file and metadata by CID"""
        file_path = self.datasets_path / f"{cid}.bin"
        metadata_path = self.metadata_path / f"{cid}.json"
        
        deleted = False
        
        if file_path.exists():
            file_path.unlink()
            deleted = True
            
        if metadata_path.exists():
            metadata_path.unlink()
            deleted = True
            
        return deleted
    
    def verify_integrity(self, cid: str) -> bool:
        """Verify file integrity by recomputing CID"""
        file_data = self.retrieve_file(cid)
        if file_data is None:
            return False
            
        computed_cid = self.compute_cid(file_data)
        return computed_cid == cid
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics"""
        total_files = len(list(self.datasets_path.glob("*.bin")))
        total_size = sum(f.stat().st_size for f in self.datasets_path.glob("*.bin"))
        
        return {
            "total_files": total_files,
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "datasets_path": str(self.datasets_path),
            "metadata_path": str(self.metadata_path)
        }

# Global instance
ipfs = IPFSMimic()