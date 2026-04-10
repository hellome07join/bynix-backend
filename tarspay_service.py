"""
TarsPay Payment Gateway Service for Bangladesh (bKash, Nagad)
API Documentation: https://apifox.com/apidoc/shared-4c352e19-e446-4150-bb64-ea713bdd5667/doc-5106899
"""

import os
import time
import hashlib
import httpx
import asyncio
from typing import Optional, Dict, Any
from ecdsa import SigningKey, VerifyingKey, SECP256k1, BadSignatureError
from ecdsa.util import sigencode_der, sigdecode_der
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# TarsPay Configuration
TARSPAY_BASE_URL = os.getenv("TARSPAY_API_URL", "https://payment.tarspay.com")
TARSPAY_MCH_NO = os.getenv("TARSPAY_MERCHANT_ID", "M1023")
TARSPAY_PRIVATE_KEY = os.getenv("TARSPAY_PRIVATE_KEY", "")
TARSPAY_PUBLIC_KEY = os.getenv("TARSPAY_PUBLIC_KEY", "")
TARSPAY_SYSTEM_PUBLIC_KEY = os.getenv("TARSPAY_SYSTEM_PUBLIC_KEY", "03029c655932f22aee81034d109795fbd7e23ca173ca27e195091d434e593a2e0f")

# Exchange rates (fixed rates as per user request)
USD_TO_BDT = int(os.getenv("BDT_TO_USD_RATE", "127"))
USD_TO_INR = int(os.getenv("INR_TO_USD_RATE", "84"))  # 1 USD = 84 INR
USD_TO_PKR = int(os.getenv("PKR_TO_USD_RATE", "278"))  # 1 USD = 278 PKR

async def fetch_live_exchange_rate() -> float:
    """Return fixed exchange rate (127 BDT per USD as configured)"""
    return float(USD_TO_BDT)

def get_current_rate() -> float:
    """Get current fixed exchange rate"""
    return float(USD_TO_BDT)

def get_rate_for_currency(currency: str) -> float:
    """Get exchange rate for specific currency"""
    rates = {
        "BDT": float(USD_TO_BDT),
        "INR": float(USD_TO_INR),
        "PKR": float(USD_TO_PKR)
    }
    return rates.get(currency, 127.0)

# Payment channels configuration
# Bangladesh (BDT) - Min $10 USD
TARSPAY_CHANNELS_BD = {
    "bkash": {
        "wayCode": "EWALLET_BKASH",
        "name": "bKash",
        "currency": "BDT",
        "country": "bd",
        "min_local": 1270,  # $10 * 127
        "max_local": 30000,
        "min_usd": 10,
        "logo": "https://defipay.oss-ap-southeast-1.aliyuncs.com/bKash.png"
    },
    "nagad": {
        "wayCode": "EWALLET_NAGAD",
        "name": "Nagad",
        "currency": "BDT",
        "country": "bd",
        "min_local": 1270,
        "max_local": 30000,
        "min_usd": 10,
        "logo": "https://defipay.oss-ap-southeast-1.aliyuncs.com/nagad.png"
    }
}

# India (INR) - Min $10 USD
TARSPAY_CHANNELS_IN = {
    "upi": {
        "wayCode": "UPI",
        "name": "UPI",
        "currency": "INR",
        "country": "in",
        "min_local": 840,  # $10 * 84
        "max_local": 50000,
        "min_usd": 10,
        "logo": "https://tarspay.oss-ap-southeast-1.aliyuncs.com/tarspay_v_1.0/upi.svg"
    }
}

# Pakistan (PKR) - Min $10 USD
TARSPAY_CHANNELS_PK = {
    "jazzcash": {
        "wayCode": "jazzcash",
        "name": "JazzCash",
        "currency": "PKR",
        "country": "pk",
        "min_local": 2780,  # $10 * 278
        "max_local": 50000,
        "min_usd": 10,
        "logo": "https://upload.wikimedia.org/wikipedia/en/thumb/a/a6/JazzCash_logo.png/220px-JazzCash_logo.png"
    },
    "easypaisa": {
        "wayCode": "easypaisa",
        "name": "EasyPaisa",
        "currency": "PKR",
        "country": "pk",
        "min_local": 2780,
        "max_local": 50000,
        "min_usd": 10,
        "logo": "https://defipay.oss-ap-southeast-1.aliyuncs.com/easypaisa.png"
    }
}

# Combined channels for backward compatibility
TARSPAY_CHANNELS = {**TARSPAY_CHANNELS_BD, **TARSPAY_CHANNELS_IN, **TARSPAY_CHANNELS_PK}

# All channels by country
ALL_CHANNELS = {
    "bd": TARSPAY_CHANNELS_BD,
    "in": TARSPAY_CHANNELS_IN,
    "pk": TARSPAY_CHANNELS_PK
}


class TarsPayService:
    def __init__(self):
        self.base_url = TARSPAY_BASE_URL
        self.mch_no = TARSPAY_MCH_NO
        self.private_key_hex = TARSPAY_PRIVATE_KEY
        self.public_key_hex = TARSPAY_PUBLIC_KEY
        self.system_public_key_hex = TARSPAY_SYSTEM_PUBLIC_KEY
        
        # Initialize signing key
        try:
            private_key_bytes = bytes.fromhex(self.private_key_hex)
            self.signing_key = SigningKey.from_string(private_key_bytes, curve=SECP256k1)
            print(f"TarsPay: Signing key initialized successfully")
        except Exception as e:
            print(f"TarsPay: Error initializing signing key: {e}")
            self.signing_key = None
    
    def _sha256(self, data: bytes) -> bytes:
        """Single SHA256 hash as required by TarsPay"""
        return hashlib.sha256(data).digest()
    
    def _sort_params(self, params: Dict[str, Any]) -> str:
        """Sort parameters alphabetically and create query string"""
        # Filter out None/empty values
        filtered = {k: v for k, v in params.items() if v is not None and v != ""}
        # Sort by key
        sorted_items = sorted(filtered.items(), key=lambda x: x[0])
        # Create query string
        return "&".join([f"{k}={v}" for k, v in sorted_items])
    
    def _create_signature(self, method: str, path: str, timestamp: int, params: Dict[str, Any]) -> str:
        """
        Create ECDSA signature for TarsPay API
        Format: METHOD|PATH|TIMESTAMP|PARAMS
        
        TarsPay Java code does:
        1. SHA256 the data first (Utils.sha256(message))
        2. Then sign with SHA256withECDSA (which does another SHA256)
        So effectively it's double SHA256
        """
        if not self.signing_key:
            raise Exception("Signing key not initialized")
        
        # Build signature string
        params_str = self._sort_params(params)
        sign_data = f"{method}|{path}|{timestamp}|{params_str}"
        print(f"TarsPay Sign Data: {sign_data}")
        
        # Double SHA256 as per Java implementation
        first_hash = hashlib.sha256(sign_data.encode('utf-8')).digest()
        double_hash = hashlib.sha256(first_hash).digest()
        
        # Sign the double hash with ECDSA
        signature = self.signing_key.sign_digest(double_hash, sigencode=sigencode_der)
        
        return signature.hex()
    
    def verify_callback_signature(self, content: str, signature_hex: str) -> bool:
        """Verify callback signature from TarsPay"""
        try:
            # Use system public key for verification
            pub_key_bytes = bytes.fromhex(self.system_public_key_hex)
            verifying_key = VerifyingKey.from_string(pub_key_bytes, curve=SECP256k1)
            
            # Single SHA256
            data_hash = self._sha256(content.encode('utf-8'))
            signature = bytes.fromhex(signature_hex)
            
            return verifying_key.verify(signature, data_hash, sigdecode=sigdecode_der)
        except BadSignatureError:
            return False
        except Exception as e:
            print(f"TarsPay: Signature verification error: {e}")
            return False
    
    async def create_deposit_order(
        self,
        order_id: str,
        amount_usd: float,
        channel: str = "bkash",
        customer_phone: Optional[str] = None,
        notify_url: str = "",
        return_url: str = ""
    ) -> Dict[str, Any]:
        """
        Create a deposit order with TarsPay
        
        Args:
            order_id: Unique merchant order ID
            amount_usd: Amount in USD
            channel: Payment channel (bkash, nagad, upi, jazzcash, easypaisa)
            customer_phone: Customer's phone/wallet number
            notify_url: Callback URL for payment notification
            return_url: URL to redirect after payment
        
        Returns:
            API response with payment URL and order details
        """
        # Get channel config
        channel_config = TARSPAY_CHANNELS.get(channel)
        if not channel_config:
            return {"success": False, "error": f"Invalid channel: {channel}"}
        
        way_code = channel_config["wayCode"]
        currency = channel_config.get("currency", "BDT")
        country = channel_config.get("country", "bd")
        
        # Get exchange rate for this currency
        exchange_rate = get_rate_for_currency(currency)
        
        # Convert USD to local currency
        amount_local = int(amount_usd * exchange_rate)
        
        # Validate amount limits
        min_local = channel_config.get("min_local", 1000)
        max_local = channel_config.get("max_local", 50000)
        min_usd = channel_config.get("min_usd", 10)
        
        if amount_usd < min_usd:
            return {
                "success": False,
                "error": f"Minimum amount is ${min_usd} USD"
            }
        if amount_local > max_local:
            return {
                "success": False,
                "error": f"Maximum amount is {max_local} {currency}"
            }
        
        # Prepare request
        path = "/api/pay/unifiedOrder"
        timestamp = int(time.time() * 1000)
        
        params = {
            "amount": str(amount_local),
            "currency": currency,
            "mchNo": self.mch_no,
            "mchOrderNo": order_id,
            "notifyUrl": notify_url,
            "wayCode": way_code
        }
        
        # Add step parameter for Pakistan channels
        if country == "pk":
            params["step"] = 0  # Use TarsPay cashier
        
        if customer_phone:
            params["customerContact"] = customer_phone
        if return_url:
            params["returnUrl"] = return_url
        
        # Create signature
        try:
            signature = self._create_signature("POST", path, timestamp, params)
        except Exception as e:
            print(f"TarsPay: Signature creation error: {e}")
            return {"success": False, "error": f"Signature error: {str(e)}"}
        
        # Make API request
        headers = {
            "Content-Type": "application/json",
            "X-API-KEY": self.public_key_hex,
            "X-API-NONCE": str(timestamp),
            "X-API-SIGNATURE": signature
        }
        
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.post(
                    f"{self.base_url}{path}",
                    json=params,
                    headers=headers
                )
                
                data = response.json()
                print(f"TarsPay Response: {data}")
                
                if data.get("code") == 0:
                    resp_data = data.get("data", {})
                    return {
                        "success": True,
                        "payment_id": resp_data.get("payOrderId"),
                        "order_id": order_id,
                        "amount_local": amount_local,
                        "amount_usd": amount_usd,
                        "currency": currency,
                        "country": country,
                        "pay_url": resp_data.get("payUrl"),
                        "pay_data": resp_data.get("payData"),
                        "pay_data_type": resp_data.get("payDataType"),
                        "channel": channel,
                        "channel_name": channel_config["name"],
                        "status": "pending",
                        "expired_time": resp_data.get("expiredTime")
                    }
                else:
                    return {
                        "success": False,
                        "error": data.get("msg", "Unknown error"),
                        "code": data.get("code")
                    }
        except Exception as e:
            print(f"TarsPay: API request error: {e}")
            return {"success": False, "error": str(e)}
    
    async def get_order_status(self, order_id: str) -> Dict[str, Any]:
        """
        Get deposit order status using TarsPay API
        
        Args:
            order_id: Merchant order ID (mchOrderNo)
        
        Returns:
            Order status information
        """
        # CORRECT ENDPOINT: /api/payInInfo (NOT /api/pay/query)
        path = "/api/payInInfo"
        timestamp = int(time.time() * 1000)
        
        params = {
            "mchNo": self.mch_no,
            "mchOrderNo": order_id
        }
        
        try:
            signature = self._create_signature("POST", path, timestamp, params)
        except Exception as e:
            return {"success": False, "error": f"Signature error: {str(e)}"}
        
        headers = {
            "Content-Type": "application/json",
            "X-API-KEY": self.public_key_hex,
            "X-API-NONCE": str(timestamp),
            "X-API-SIGNATURE": signature
        }
        
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.post(
                    f"{self.base_url}{path}",
                    json=params,
                    headers=headers
                )
                
                print(f"[TarsPay Query] Order: {order_id}, Status: {response.status_code}")
                data = response.json()
                print(f"[TarsPay Query] Response: {data}")
                
                if data.get("code") == 0:
                    resp_data = data.get("data", {})
                    # Order states: 2=Success, 3=Failure, 5=Refund, 6=Timeout, 9=Partial
                    order_state = resp_data.get("state", 0)
                    status_map = {2: "success", 3: "failed", 5: "refund", 6: "timeout", 9: "partial"}
                    
                    return {
                        "success": True,
                        "payment_id": resp_data.get("payOrderId"),
                        "order_id": order_id,
                        "status": status_map.get(order_state, "pending"),
                        "order_amount": resp_data.get("orderAmount"),
                        "pay_amount": resp_data.get("payAmount"),
                        "currency": resp_data.get("currency"),
                        "fee": resp_data.get("fee"),
                        "state": order_state,
                        "paid": order_state == 2
                    }
                else:
                    return {"success": False, "error": data.get("msg", "Unknown error"), "code": data.get("code")}
                    
        except Exception as e:
            print(f"[TarsPay Query] Error: {e}")
            return {"success": False, "error": str(e)}
    
    def get_channels(self) -> list:
        """Get available payment channels with limits for all countries"""
        channels = []
        for key, config in TARSPAY_CHANNELS.items():
            currency = config.get("currency", "BDT")
            rate = get_rate_for_currency(currency)
            channels.append({
                "id": key,
                "name": config["name"],
                "wayCode": config["wayCode"],
                "currency": currency,
                "country": config.get("country", "bd"),
                "min_usd": config.get("min_usd", 10),
                "max_usd": round(config.get("max_local", 50000) / rate, 2),
                "min_local": config.get("min_local", 1000),
                "max_local": config.get("max_local", 50000),
                "exchange_rate": rate,
                "logo": config.get("logo", "")
            })
        return channels

    def get_withdrawal_channels(self) -> list:
        """Get available withdrawal channels for bKash/Nagad"""
        rate = get_rate_for_currency("BDT")
        return [
            {
                "id": "bkash",
                "name": "bKash",
                "wayCode": "EWALLET_BKASH",
                "currency": "BDT",
                "country": "bd",
                "min_local": 100,  # Min 100 BDT
                "max_local": 50000,  # Max 50,000 BDT
                "min_usd": round(100 / rate, 2),
                "max_usd": round(50000 / rate, 2),
                "exchange_rate": rate,
                "fee_percent": 1.5,  # 1.5% fee
                "logo": "https://customer-assets.emergentagent.com/job_bynix-markets/artifacts/7xb7yj94_IMG_3475.png"
            },
            {
                "id": "nagad",
                "name": "Nagad",
                "wayCode": "EWALLET_NAGAD",
                "currency": "BDT",
                "country": "bd",
                "min_local": 100,
                "max_local": 50000,
                "min_usd": round(100 / rate, 2),
                "max_usd": round(50000 / rate, 2),
                "exchange_rate": rate,
                "fee_percent": 1.5,
                "logo": "https://customer-assets.emergentagent.com/job_bynix-markets/artifacts/remcqmc2_IMG_3476.png"
            }
        ]

    async def create_withdrawal(
        self,
        order_id: str,
        amount_bdt: int,
        wallet_id: str,
        way_code: str,
        notify_url: str
    ) -> Dict[str, Any]:
        """
        Create a withdrawal order to bKash/Nagad
        
        Args:
            order_id: Unique merchant order ID
            amount_bdt: Amount in BDT (integer, min 100, max 50000)
            wallet_id: bKash/Nagad wallet number (11 digits starting with 0)
            way_code: EWALLET_BKASH or EWALLET_NAGAD
            notify_url: Callback URL for payment status
        
        Returns:
            Result with payment order info or error
        """
        path = "/api/payOut/unifiedOrder"
        timestamp = int(time.time() * 1000)
        
        # Validate wallet ID format
        if not wallet_id or len(wallet_id) != 11 or not wallet_id.startswith("0"):
            return {"success": False, "error": "Invalid wallet ID. Must be 11 digits starting with 0"}
        
        # Validate amount
        if amount_bdt < 100:
            return {"success": False, "error": "Minimum withdrawal is ৳100 BDT"}
        if amount_bdt > 50000:
            return {"success": False, "error": "Maximum withdrawal is ৳50,000 BDT"}
        
        params = {
            "mchNo": self.mch_no,
            "mchOrderNo": order_id,
            "wayCode": way_code,
            "currency": "BDT",
            "amount": str(amount_bdt),
            "notifyUrl": notify_url,
            "walletId": wallet_id
        }
        
        try:
            signature = self._create_signature("POST", path, timestamp, params)
        except Exception as e:
            print(f"[TarsPay Withdraw] Signature error: {e}")
            return {"success": False, "error": f"Signature error: {str(e)}"}
        
        headers = {
            "Content-Type": "application/json",
            "X-API-KEY": self.public_key_hex,
            "X-API-NONCE": str(timestamp),
            "X-API-SIGNATURE": signature
        }
        
        try:
            print(f"[TarsPay Withdraw] Creating withdrawal: {order_id}, Amount: ৳{amount_bdt}, Wallet: {wallet_id}, Way: {way_code}")
            
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.post(
                    f"{self.base_url}{path}",
                    json=params,
                    headers=headers
                )
                
                print(f"[TarsPay Withdraw] Response Status: {response.status_code}")
                data = response.json()
                print(f"[TarsPay Withdraw] Response: {data}")
                
                if data.get("code") == 0:
                    resp_data = data.get("data", {})
                    return {
                        "success": True,
                        "payment_id": resp_data.get("payOrderId"),
                        "order_id": order_id,
                        "mch_order_no": resp_data.get("mchOrderNo"),
                        "amount_bdt": amount_bdt,
                        "wallet_id": wallet_id,
                        "way_code": way_code
                    }
                else:
                    return {
                        "success": False,
                        "error": data.get("msg", "Unknown error"),
                        "code": data.get("code")
                    }
                    
        except Exception as e:
            print(f"[TarsPay Withdraw] Error: {e}")
            return {"success": False, "error": str(e)}

    async def get_withdrawal_status(self, order_id: str) -> Dict[str, Any]:
        """
        Query withdrawal order status
        
        Args:
            order_id: Merchant order ID
        
        Returns:
            Withdrawal status information
        """
        path = "/api/payOutInfo"
        timestamp = int(time.time() * 1000)
        
        params = {
            "mchNo": self.mch_no,
            "mchOrderNo": order_id
        }
        
        try:
            signature = self._create_signature("POST", path, timestamp, params)
        except Exception as e:
            return {"success": False, "error": f"Signature error: {str(e)}"}
        
        headers = {
            "Content-Type": "application/json",
            "X-API-KEY": self.public_key_hex,
            "X-API-NONCE": str(timestamp),
            "X-API-SIGNATURE": signature
        }
        
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.post(
                    f"{self.base_url}{path}",
                    json=params,
                    headers=headers
                )
                
                print(f"[TarsPay Withdraw Query] Order: {order_id}, Status: {response.status_code}")
                data = response.json()
                print(f"[TarsPay Withdraw Query] Response: {data}")
                
                if data.get("code") == 0:
                    resp_data = data.get("data", {})
                    # Status: 2=Success, 3=Failure, 5=Refund, 8=Rejection
                    state = resp_data.get("state", 0)
                    status_map = {2: "success", 3: "failed", 5: "refund", 8: "rejected"}
                    
                    return {
                        "success": True,
                        "payment_id": resp_data.get("payOrderId"),
                        "order_id": order_id,
                        "status": status_map.get(state, "pending"),
                        "state": state,
                        "order_amount": resp_data.get("orderAmount"),
                        "pay_amount": resp_data.get("payAmount"),
                        "currency": resp_data.get("currency"),
                        "fee": resp_data.get("fee"),
                        "completed": state == 2,
                        "failed": state in [3, 8]
                    }
                else:
                    return {"success": False, "error": data.get("msg", "Unknown error"), "code": data.get("code")}
                    
        except Exception as e:
            print(f"[TarsPay Withdraw Query] Error: {e}")
            return {"success": False, "error": str(e)}


# Global instance
tarspay_service = TarsPayService()
