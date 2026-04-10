"""
NOWPayments Payout Service for USDT TRC20 Withdrawals
API Documentation: https://documenter.getpostman.com/view/7907941/2s93JusNJt
"""

import os
import httpx
from typing import Optional, Dict, Any
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime, timedelta

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

NOWPAYMENTS_API_KEY = os.environ.get("NOWPAYMENTS_API_KEY", "")
NOWPAYMENTS_EMAIL = os.environ.get("NOWPAYMENTS_EMAIL", "")
NOWPAYMENTS_PASSWORD = os.environ.get("NOWPAYMENTS_PASSWORD", "")
NOWPAYMENTS_IPN_SECRET = os.environ.get("NOWPAYMENTS_IPN_SECRET", "")
NOWPAYMENTS_BASE_URL = "https://api.nowpayments.io/v1"

class NOWPaymentsService:
    """Service class for NOWPayments Payout API with JWT Authentication"""
    
    def __init__(self):
        self.api_key = NOWPAYMENTS_API_KEY
        self.email = NOWPAYMENTS_EMAIL
        self.password = NOWPAYMENTS_PASSWORD
        self.base_url = NOWPAYMENTS_BASE_URL
        self.jwt_token = None
        self.jwt_expires_at = None
    
    def _get_base_headers(self) -> Dict[str, str]:
        """Get base headers with API key"""
        return {
            "x-api-key": self.api_key,
            "Content-Type": "application/json"
        }
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """Get headers with both API key and JWT token"""
        headers = self._get_base_headers()
        if self.jwt_token:
            headers["Authorization"] = f"Bearer {self.jwt_token}"
        return headers
    
    async def authenticate(self) -> bool:
        """
        Authenticate with NOWPayments to get JWT token
        Required for payout operations
        """
        try:
            # Check if we have a valid token
            if self.jwt_token and self.jwt_expires_at:
                if datetime.now() < self.jwt_expires_at:
                    return True
            
            print(f"[NOWPayments] Authenticating with email: {self.email}")
            
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.post(
                    f"{self.base_url}/auth",
                    headers={"Content-Type": "application/json"},
                    json={
                        "email": self.email,
                        "password": self.password
                    }
                )
                
                data = response.json()
                print(f"[NOWPayments] Auth response: {response.status_code}")
                
                if response.status_code == 200 and data.get("token"):
                    self.jwt_token = data["token"]
                    # Token typically valid for 24 hours, refresh after 23 hours
                    self.jwt_expires_at = datetime.now() + timedelta(hours=23)
                    print("[NOWPayments] Authentication successful")
                    return True
                else:
                    print(f"[NOWPayments] Auth failed: {data}")
                    return False
                    
        except Exception as e:
            print(f"[NOWPayments] Auth error: {e}")
            return False
    
    async def get_status(self) -> Dict[str, Any]:
        """Check NOWPayments API status"""
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    f"{self.base_url}/status",
                    headers=self._get_base_headers()
                )
                return response.json()
        except Exception as e:
            return {"error": str(e)}
    
    async def get_balance(self) -> Dict[str, Any]:
        """Get NOWPayments account balance (requires JWT)"""
        try:
            await self.authenticate()
            
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    f"{self.base_url}/balance",
                    headers=self._get_auth_headers()
                )
                return response.json()
        except Exception as e:
            return {"error": str(e)}
    
    async def validate_address(self, address: str, currency: str = "usdttrc20") -> Dict[str, Any]:
        """Validate a crypto address before payout"""
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.post(
                    f"{self.base_url}/payout/validate-address",
                    headers=self._get_base_headers(),
                    json={
                        "address": address,
                        "currency": currency
                    }
                )
                return response.json()
        except Exception as e:
            return {"error": str(e), "result": False}
    
    async def create_payout(
        self,
        address: str,
        amount: float,
        currency: str = "usdttrc20",
        ipn_callback_url: Optional[str] = None,
        unique_external_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a payout request to send USDT TRC20
        Requires JWT authentication
        
        Args:
            address: Recipient's TRC20 wallet address (starts with T)
            amount: Amount in USDT to send
            currency: Currency code (default: usdttrc20)
            ipn_callback_url: URL to receive payment notifications
            unique_external_id: Your unique identifier for this payout
        
        Returns:
            Payout response with id, status, etc.
        """
        try:
            # First authenticate to get JWT token
            auth_success = await self.authenticate()
            if not auth_success:
                return {
                    "success": False,
                    "error": "Failed to authenticate with NOWPayments"
                }
            
            # Build payout payload
            # NOWPayments uses "withdrawals" array for batch payouts
            payload = {
                "ipn_callback_url": ipn_callback_url,
                "withdrawals": [
                    {
                        "address": address,
                        "currency": currency,
                        "amount": amount,
                        "unique_external_id": unique_external_id
                    }
                ]
            }
            
            # Remove None values from withdrawal
            payload["withdrawals"][0] = {k: v for k, v in payload["withdrawals"][0].items() if v is not None}
            if not ipn_callback_url:
                del payload["ipn_callback_url"]
            
            print(f"[NOWPayments] Creating payout: {payload}")
            
            async with httpx.AsyncClient(timeout=60) as client:
                response = await client.post(
                    f"{self.base_url}/payout",
                    headers=self._get_auth_headers(),
                    json=payload
                )
                
                data = response.json()
                print(f"[NOWPayments] Payout response ({response.status_code}): {data}")
                
                # Handle successful response
                if response.status_code in [200, 201]:
                    # Response contains "withdrawals" array
                    withdrawals = data.get("withdrawals", [])
                    if withdrawals:
                        withdrawal = withdrawals[0]
                        return {
                            "success": True,
                            "status_code": response.status_code,
                            "data": {
                                "id": withdrawal.get("id"),
                                "batch_withdrawal_id": withdrawal.get("batch_withdrawal_id"),
                                "status": withdrawal.get("status"),
                                "amount": withdrawal.get("amount"),
                                "currency": withdrawal.get("currency"),
                                "address": withdrawal.get("address"),
                                "hash": withdrawal.get("hash"),
                                "unique_external_id": withdrawal.get("unique_external_id")
                            }
                        }
                    return {
                        "success": True,
                        "status_code": response.status_code,
                        "data": data
                    }
                else:
                    return {
                        "success": False,
                        "status_code": response.status_code,
                        "data": data,
                        "error": data.get("message") or data.get("error") or "Payout creation failed"
                    }
                    
        except Exception as e:
            print(f"[NOWPayments] Payout error: {e}")
            import traceback
            traceback.print_exc()
            return {
                "success": False,
                "error": str(e)
            }
    
    async def get_payout_status(self, payout_id: str) -> Dict[str, Any]:
        """
        Get the status of a payout (requires JWT)
        
        Args:
            payout_id: The NOWPayments payout ID
            
        Returns:
            Payout details including status
        """
        try:
            await self.authenticate()
            
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    f"{self.base_url}/payout/{payout_id}",
                    headers=self._get_auth_headers()
                )
                return response.json()
        except Exception as e:
            return {"error": str(e)}
    
    async def get_payouts_list(
        self,
        limit: int = 10,
        page: int = 0,
        status: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get list of payouts (requires JWT)
        
        Args:
            limit: Number of results per page
            page: Page number
            status: Filter by status (waiting, confirming, sending, finished, failed)
        """
        try:
            await self.authenticate()
            
            params = {
                "limit": limit,
                "page": page
            }
            if status:
                params["status"] = status
                
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    f"{self.base_url}/payout",
                    headers=self._get_auth_headers(),
                    params=params
                )
                return response.json()
        except Exception as e:
            return {"error": str(e)}


# Singleton instance
nowpayments_service = NOWPaymentsService()


# Helper functions for quick access
async def create_usdt_payout(
    address: str,
    amount: float,
    external_id: Optional[str] = None,
    callback_url: Optional[str] = None
) -> Dict[str, Any]:
    """
    Quick helper to create USDT TRC20 payout
    
    Args:
        address: TRC20 wallet address
        amount: Amount in USDT
        external_id: Your unique reference ID
        callback_url: Webhook URL for status updates
    """
    return await nowpayments_service.create_payout(
        address=address,
        amount=amount,
        currency="usdttrc20",
        unique_external_id=external_id,
        ipn_callback_url=callback_url
    )


async def check_payout_status(payout_id: str) -> Dict[str, Any]:
    """Quick helper to check payout status"""
    return await nowpayments_service.get_payout_status(payout_id)


async def validate_trc20_address(address: str) -> bool:
    """Validate if address is a valid TRC20 address"""
    # Basic TRC20 address validation
    if not address or len(address) != 34 or not address.startswith('T'):
        return False
    return True


async def get_nowpayments_balance() -> Dict[str, Any]:
    """Get NOWPayments account balance"""
    return await nowpayments_service.get_balance()
