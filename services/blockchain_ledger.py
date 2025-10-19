import json
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
from models import Transaction, PurchaseRequest
from config import LEDGER_PATH

class MockBlockchainLedger:
    """
    Mock blockchain ledger that simulates escrow and payment verification
    """
    
    def __init__(self):
        self.ledger_path = LEDGER_PATH
        self.transactions_file = self.ledger_path / "transactions.json"
        self.escrow_file = self.ledger_path / "escrow.json"
        
        # Initialize files if they don't exist
        self._init_ledger_files()
    
    def _init_ledger_files(self):
        """Initialize ledger files with empty data"""
        if not self.transactions_file.exists():
            with open(self.transactions_file, "w") as f:
                json.dump([], f)
        
        if not self.escrow_file.exists():
            with open(self.escrow_file, "w") as f:
                json.dump({}, f)
    
    def _load_transactions(self) -> List[Dict]:
        """Load all transactions from file"""
        with open(self.transactions_file, "r") as f:
            return json.load(f)
    
    def _save_transactions(self, transactions: List[Dict]):
        """Save transactions to file"""
        with open(self.transactions_file, "w") as f:
            json.dump(transactions, f, indent=2, default=str)
    
    def _load_escrow(self) -> Dict:
        """Load escrow data from file"""
        with open(self.escrow_file, "r") as f:
            return json.load(f)
    
    def _save_escrow(self, escrow_data: Dict):
        """Save escrow data to file"""
        with open(self.escrow_file, "w") as f:
            json.dump(escrow_data, f, indent=2, default=str)
    
    def generate_tx_id(self, cid: str, buyer: str, amount: float) -> str:
        """Generate transaction ID by hashing CID + buyer + amount"""
        data = f"{cid}{buyer}{amount}{datetime.utcnow().isoformat()}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]
    
    def create_transaction(self, purchase_request: PurchaseRequest, seller: str) -> Transaction:
        """Create a new transaction and add to ledger"""
        tx_id = self.generate_tx_id(
            purchase_request.cid, 
            purchase_request.buyer, 
            purchase_request.amount
        )
        
        transaction = Transaction(
            tx_id=tx_id,
            cid=purchase_request.cid,
            buyer=purchase_request.buyer,
            seller=seller,
            amount=purchase_request.amount,
            timestamp=datetime.utcnow(),
            status="pending",
            escrow_released=False
        )
        
        # Add to transactions ledger
        transactions = self._load_transactions()
        transactions.append(transaction.dict())
        self._save_transactions(transactions)
        
        # Add to escrow
        self._add_to_escrow(transaction)
        
        return transaction
    
    def _add_to_escrow(self, transaction: Transaction):
        """Add transaction to escrow system"""
        escrow_data = self._load_escrow()
        
        escrow_data[transaction.tx_id] = {
            "cid": transaction.cid,
            "buyer": transaction.buyer,
            "seller": transaction.seller,
            "amount": transaction.amount,
            "created_at": transaction.timestamp.isoformat(),
            "status": "held",
            "released": False
        }
        
        self._save_escrow(escrow_data)
    
    def verify_payment(self, tx_id: str, payment_amount: float) -> bool:
        """
        Mock payment verification - in real system this would verify with payment processor
        For demo purposes, any payment amount > 0 is considered valid
        """
        if payment_amount <= 0:
            return False
        
        # In a real system, you would verify with payment processor here
        # For now, we'll simulate successful payment verification
        return True
    
    def complete_transaction(self, tx_id: str, payment_amount: float) -> bool:
        """Complete transaction and release escrow"""
        if not self.verify_payment(tx_id, payment_amount):
            return False
        
        # Update transaction status
        transactions = self._load_transactions()
        transaction_updated = False
        
        for tx in transactions:
            if tx["tx_id"] == tx_id:
                tx["status"] = "completed"
                tx["escrow_released"] = True
                tx["completed_at"] = datetime.utcnow().isoformat()
                transaction_updated = True
                break
        
        if not transaction_updated:
            return False
        
        self._save_transactions(transactions)
        
        # Release escrow
        escrow_data = self._load_escrow()
        if tx_id in escrow_data:
            escrow_data[tx_id]["status"] = "released"
            escrow_data[tx_id]["released"] = True
            escrow_data[tx_id]["released_at"] = datetime.utcnow().isoformat()
            self._save_escrow(escrow_data)
        
        return True
    
    def fail_transaction(self, tx_id: str, reason: str = "Payment failed") -> bool:
        """Mark transaction as failed and release escrow back to buyer"""
        transactions = self._load_transactions()
        transaction_updated = False
        
        for tx in transactions:
            if tx["tx_id"] == tx_id:
                tx["status"] = "failed"
                tx["failure_reason"] = reason
                tx["failed_at"] = datetime.utcnow().isoformat()
                transaction_updated = True
                break
        
        if not transaction_updated:
            return False
        
        self._save_transactions(transactions)
        
        # Update escrow
        escrow_data = self._load_escrow()
        if tx_id in escrow_data:
            escrow_data[tx_id]["status"] = "refunded"
            escrow_data[tx_id]["refunded_at"] = datetime.utcnow().isoformat()
            self._save_escrow(escrow_data)
        
        return True
    
    def get_transaction(self, tx_id: str) -> Optional[Dict]:
        """Get transaction by ID"""
        transactions = self._load_transactions()
        for tx in transactions:
            if tx["tx_id"] == tx_id:
                return tx
        return None
    
    def get_user_transactions(self, user: str) -> List[Dict]:
        """Get all transactions for a user (as buyer or seller)"""
        transactions = self._load_transactions()
        user_transactions = []
        
        for tx in transactions:
            if tx["buyer"] == user or tx["seller"] == user:
                user_transactions.append(tx)
        
        return user_transactions
    
    def get_dataset_transactions(self, cid: str) -> List[Dict]:
        """Get all transactions for a specific dataset"""
        transactions = self._load_transactions()
        dataset_transactions = []
        
        for tx in transactions:
            if tx["cid"] == cid:
                dataset_transactions.append(tx)
        
        return dataset_transactions
    
    def is_dataset_purchased(self, cid: str, buyer: str) -> bool:
        """Check if a user has successfully purchased a dataset"""
        transactions = self._load_transactions()
        
        for tx in transactions:
            if (tx["cid"] == cid and 
                tx["buyer"] == buyer and 
                tx["status"] == "completed"):
                return True
        
        return False
    
    def get_ledger_stats(self) -> Dict[str, Any]:
        """Get blockchain ledger statistics"""
        transactions = self._load_transactions()
        escrow_data = self._load_escrow()
        
        total_transactions = len(transactions)
        completed_transactions = len([tx for tx in transactions if tx["status"] == "completed"])
        pending_transactions = len([tx for tx in transactions if tx["status"] == "pending"])
        failed_transactions = len([tx for tx in transactions if tx["status"] == "failed"])
        
        total_volume = sum(tx["amount"] for tx in transactions if tx["status"] == "completed")
        
        return {
            "total_transactions": total_transactions,
            "completed_transactions": completed_transactions,
            "pending_transactions": pending_transactions,
            "failed_transactions": failed_transactions,
            "total_volume": total_volume,
            "active_escrow_count": len([e for e in escrow_data.values() if e["status"] == "held"]),
            "ledger_file": str(self.transactions_file),
            "escrow_file": str(self.escrow_file)
        }

# Global instance
blockchain = MockBlockchainLedger()