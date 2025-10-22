from fastapi import APIRouter, HTTPException
from typing import List, Optional

from models import APIResponse, PurchaseRequest, Transaction
from services.blockchain_ledger import blockchain
from services.ipfs_mimic import ipfs

router = APIRouter(prefix="/api", tags=["Transactions"])

@router.post("/purchase", response_model=APIResponse)
async def initiate_purchase(request: PurchaseRequest):
    """Initiate dataset purchase with escrow"""
    
    try:
        # Validate dataset exists
        metadata = ipfs.get_metadata(request.cid)
        
        # If not found in IPFS, check seed data
        if not metadata and request.cid.startswith("seed"):
            # Import from marketplace route
            import sys
            sys.path.append('..')
            from routes.marketplace import get_seed_datasets
            seed_datasets = get_seed_datasets()
            for dataset in seed_datasets:
                if dataset["cid"] == request.cid:
                    metadata = dataset
                    break
        
        if not metadata:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        # Get seller from metadata
        seller = metadata.get("uploader", "unknown")
        
        # Validate purchase amount matches dataset price
        dataset_price = metadata.get("price", 0)
        if request.amount < dataset_price:
            raise HTTPException(
                status_code=400, 
                detail=f"Insufficient payment. Required: {dataset_price}, Provided: {request.amount}"
            )
        
        # Check if user already purchased this dataset
        if blockchain.is_dataset_purchased(request.cid, request.buyer):
            raise HTTPException(
                status_code=400,
                detail="Dataset already purchased by this user"
            )
        
        # Create transaction and add to escrow
        transaction = blockchain.create_transaction(request, seller)
        
        return APIResponse(
            success=True,
            message="Purchase initiated successfully. Payment is held in escrow.",
            data={
                "transaction": transaction.dict(),
                "next_step": "complete_payment",
                "payment_instructions": "Call /api/pay endpoint with transaction ID and payment confirmation"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Purchase initiation failed: {str(e)}")

@router.post("/pay", response_model=APIResponse)
async def complete_payment(tx_id: str, payment_amount: float):
    """Complete payment and release escrow"""
    
    try:
        # Get transaction
        transaction = blockchain.get_transaction(tx_id)
        if not transaction:
            raise HTTPException(status_code=404, detail="Transaction not found")
        
        if transaction["status"] != "pending":
            raise HTTPException(
                status_code=400, 
                detail=f"Transaction is not pending. Current status: {transaction['status']}"
            )
        
        # Complete the transaction
        success = blockchain.complete_transaction(tx_id, payment_amount)
        
        if not success:
            # If payment verification failed, mark transaction as failed
            blockchain.fail_transaction(tx_id, "Payment verification failed")
            raise HTTPException(status_code=400, detail="Payment verification failed")
        
        # Get updated transaction
        updated_transaction = blockchain.get_transaction(tx_id)
        
        return APIResponse(
            success=True,
            message="Payment completed successfully. Dataset access granted.",
            data={
                "transaction": updated_transaction,
                "download_url": f"/api/download/{transaction['cid']}?buyer={transaction['buyer']}",
                "access_granted": True
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Payment completion failed: {str(e)}")

@router.get("/transaction/{tx_id}", response_model=APIResponse)
async def get_transaction(tx_id: str):
    """Get transaction details by ID"""
    
    try:
        transaction = blockchain.get_transaction(tx_id)
        
        if not transaction:
            raise HTTPException(status_code=404, detail="Transaction not found")
        
        # Add dataset metadata for context
        dataset_metadata = ipfs.get_metadata(transaction["cid"])
        
        return APIResponse(
            success=True,
            message="Transaction retrieved successfully",
            data={
                "transaction": transaction,
                "dataset": dataset_metadata
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get transaction: {str(e)}")

@router.get("/transactions/user/{user}", response_model=APIResponse)
async def get_user_transactions(user: str, status: Optional[str] = None):
    """Get all transactions for a user (as buyer or seller)"""
    
    try:
        transactions = blockchain.get_user_transactions(user)
        
        # Filter by status if provided
        if status:
            transactions = [tx for tx in transactions if tx["status"] == status]
        
        # Add dataset metadata for each transaction
        enriched_transactions = []
        for tx in transactions:
            dataset_metadata = ipfs.get_metadata(tx["cid"])
            enriched_tx = {
                **tx,
                "dataset_title": dataset_metadata.get("title", "Unknown") if dataset_metadata else "Unknown",
                "dataset_category": dataset_metadata.get("category", "Unknown") if dataset_metadata else "Unknown"
            }
            enriched_transactions.append(enriched_tx)
        
        return APIResponse(
            success=True,
            message=f"Found {len(enriched_transactions)} transactions for user {user}",
            data={
                "transactions": enriched_transactions,
                "user": user,
                "total_count": len(enriched_transactions)
            }
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get user transactions: {str(e)}")

@router.get("/transactions/dataset/{cid}", response_model=APIResponse)
async def get_dataset_transactions(cid: str):
    """Get all transactions for a specific dataset"""
    
    try:
        # Validate dataset exists
        metadata = ipfs.get_metadata(cid)
        if not metadata:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        transactions = blockchain.get_dataset_transactions(cid)
        
        return APIResponse(
            success=True,
            message=f"Found {len(transactions)} transactions for dataset {cid}",
            data={
                "transactions": transactions,
                "dataset": metadata,
                "total_sales": len([tx for tx in transactions if tx["status"] == "completed"]),
                "total_revenue": sum(tx["amount"] for tx in transactions if tx["status"] == "completed")
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get dataset transactions: {str(e)}")

@router.post("/transaction/{tx_id}/cancel", response_model=APIResponse)
async def cancel_transaction(tx_id: str, reason: Optional[str] = "Cancelled by user"):
    """Cancel a pending transaction"""
    
    try:
        transaction = blockchain.get_transaction(tx_id)
        if not transaction:
            raise HTTPException(status_code=404, detail="Transaction not found")
        
        if transaction["status"] != "pending":
            raise HTTPException(
                status_code=400,
                detail=f"Cannot cancel transaction with status: {transaction['status']}"
            )
        
        # Mark transaction as failed (cancelled)
        success = blockchain.fail_transaction(tx_id, reason)
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to cancel transaction")
        
        updated_transaction = blockchain.get_transaction(tx_id)
        
        return APIResponse(
            success=True,
            message="Transaction cancelled successfully",
            data={"transaction": updated_transaction}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Transaction cancellation failed: {str(e)}")

@router.get("/purchases/{buyer}", response_model=APIResponse)
async def get_user_purchases(buyer: str):
    """Get all successful purchases by a user with download access"""
    
    try:
        transactions = blockchain.get_user_transactions(buyer)
        
        # Filter for completed purchases where user is the buyer
        purchases = [
            tx for tx in transactions 
            if tx["buyer"] == buyer and tx["status"] == "completed"
        ]
        
        # Add dataset metadata and download links
        enriched_purchases = []
        for purchase in purchases:
            dataset_metadata = ipfs.get_metadata(purchase["cid"])
            enriched_purchase = {
                **purchase,
                "dataset_title": dataset_metadata.get("title", "Unknown") if dataset_metadata else "Unknown",
                "dataset_category": dataset_metadata.get("category", "Unknown") if dataset_metadata else "Unknown",
                "download_url": f"/api/download/{purchase['cid']}?buyer={buyer}",
                "can_download": True
            }
            enriched_purchases.append(enriched_purchase)
        
        return APIResponse(
            success=True,
            message=f"Found {len(enriched_purchases)} purchases for user {buyer}",
            data={
                "purchases": enriched_purchases,
                "buyer": buyer,
                "total_spent": sum(p["amount"] for p in purchases)
            }
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get user purchases: {str(e)}")

@router.get("/sales/{seller}", response_model=APIResponse)
async def get_user_sales(seller: str):
    """Get all sales by a user (datasets they uploaded and sold)"""
    
    try:
        transactions = blockchain.get_user_transactions(seller)
        
        # Filter for completed sales where user is the seller
        sales = [
            tx for tx in transactions 
            if tx["seller"] == seller and tx["status"] == "completed"
        ]
        
        # Add dataset metadata
        enriched_sales = []
        for sale in sales:
            dataset_metadata = ipfs.get_metadata(sale["cid"])
            enriched_sale = {
                **sale,
                "dataset_title": dataset_metadata.get("title", "Unknown") if dataset_metadata else "Unknown",
                "dataset_category": dataset_metadata.get("category", "Unknown") if dataset_metadata else "Unknown"
            }
            enriched_sales.append(enriched_sale)
        
        return APIResponse(
            success=True,
            message=f"Found {len(enriched_sales)} sales for user {seller}",
            data={
                "sales": enriched_sales,
                "seller": seller,
                "total_earned": sum(s["amount"] for s in sales),
                "unique_datasets_sold": len(set(s["cid"] for s in sales))
            }
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get user sales: {str(e)}")