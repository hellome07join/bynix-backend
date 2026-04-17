from fastapi import FastAPI, APIRouter, HTTPException, Request, Response, Header, Body
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId
import os
import math
import logging
from pathlib import Path
from pydantic import BaseModel, Field, EmailStr
from typing import List, Optional, Dict, Any
import uuid
from datetime import datetime, timedelta, timezone
from passlib.context import CryptContext
import jwt
import random
import httpx
import socketio
import asyncio
import base64
# OpenAI for KYC verification (AWS deployment - no emergentintegrations)
from openai import OpenAI
from tarspay_service import tarspay_service, fetch_live_exchange_rate, get_current_rate, get_rate_for_currency, TARSPAY_CHANNELS, ALL_CHANNELS
from email_service import send_verification_otp, verify_otp as verify_email_otp, resend_otp
from nowpayments_service import nowpayments_service, create_usdt_payout, check_payout_status, validate_trc20_address
from marketing_service import marketing_service, EMAIL_TEMPLATES

# Demo-only assets - These 6 Forex assets are ONLY available for Demo trading
# Real balance can trade all OTHER assets (including USD/CHF)
DEMO_ONLY_ASSETS = [
    "EURUSD", "EUR/USD", "EUR/USD OTC",
    "GBPUSD", "GBP/USD", "GBP/USD OTC",
    "USDJPY", "USD/JPY", "USD/JPY OTC",
    "AUDUSD", "AUD/USD", "AUD/USD OTC",
    "NZDUSD", "NZD/USD", "NZD/USD OTC",
    "USDCAD", "USD/CAD", "USD/CAD OTC",
]

def is_demo_only_asset(symbol: str) -> bool:
    """Check if an asset is demo-only (restricted from real trading)"""
    clean_symbol = symbol.upper().replace(" ", "").replace("/", "").replace("OTC", "").strip()
    for demo_asset in DEMO_ONLY_ASSETS:
        clean_demo = demo_asset.upper().replace(" ", "").replace("/", "").replace("OTC", "").strip()
        if clean_symbol == clean_demo:
            return True
    return False

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# JWT Configuration
SECRET_KEY = os.environ.get("JWT_SECRET", "your-secret-key-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_DAYS = 7

# Create the main app
app = FastAPI()
api_router = APIRouter(prefix="/api")

# Socket.IO for WebSocket
sio = socketio.AsyncServer(
    async_mode='asgi',
    cors_allowed_origins='*',
    logger=True,
    engineio_logger=True
)
socket_app = socketio.ASGIApp(sio, socketio_path='socket.io')
app.mount('/ws', socket_app)

# ============= Models =============

class User(BaseModel):
    user_id: str
    email: EmailStr
    name: str
    picture: Optional[str] = None
    chart_picture: Optional[str] = None  # Separate picture for chart background
    demo_balance: float = 10000.0
    real_balance: float = 0.0
    # Separate balance tracking for withdrawal rules
    deposit_balance: float = 0.0  # Amount deposited
    bonus_balance: float = 0.0    # Bonus received (not withdrawable)
    profit_balance: float = 0.0   # Profit from trades (withdrawable)
    has_withdrawn: bool = False   # If user has ever withdrawn, bonus is forfeited
    is_admin: bool = False
    created_at: datetime

class UserCreate(BaseModel):
    email: EmailStr
    password: str
    name: str
    country: Optional[str] = None
    country_flag: Optional[str] = None
    referred_by: Optional[str] = None  # Affiliate referral code

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class UserSession(BaseModel):
    user_id: str
    session_token: str
    expires_at: datetime
    created_at: datetime

class Trade(BaseModel):
    trade_id: str
    user_id: str
    asset: str  # e.g., "BTC/USD"
    trade_type: str  # "call" or "put"
    amount: float
    entry_price: float
    exit_price: Optional[float] = None
    duration: int  # in seconds
    payout_percentage: float = 80.0
    status: str = "pending"  # pending, won, lost
    profit_loss: float = 0.0
    account_type: str = "demo"  # demo or real
    created_at: datetime
    settled_at: Optional[datetime] = None

class TradeCreate(BaseModel):
    asset: str
    trade_type: str
    direction: str  # 'up' or 'down'
    amount: float
    duration: int
    entry_price: float
    account_type: str = "demo"
    payout_percentage: float = 80.0  # Frontend sends asset's payout

class Asset(BaseModel):
    asset_id: str
    symbol: str  # e.g., "BTC/USD"
    name: str
    category: str  # crypto, forex, stocks
    payout_percentage: float = 80.0
    is_active: bool = True

class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    type: str  # deposit, withdrawal
    amount: float
    status: str = "pending"  # pending, completed, rejected
    currency: str = "USDT"  # USDT, BTC, ETH, LTC
    network: str = "TRC-20"  # TRC-20, Bitcoin, ERC20, Litecoin
    crypto_address: Optional[str] = None
    txn_hash: Optional[str] = None
    account_type: str = "real"
    created_at: datetime
    completed_at: Optional[datetime] = None

class DepositRequest(BaseModel):
    amount: float

class WithdrawalRequest(BaseModel):
    amount: float
    crypto_address: str

class OTPVerification(BaseModel):
    email: EmailStr
    otp: str

class PasswordReset(BaseModel):
    email: EmailStr
    otp: str
    new_password: str

class TradeSettle(BaseModel):
    exit_price: float

# Chart Data Models
class ChartTick(BaseModel):
    time: int
    open: float
    high: float
    low: float
    close: float

class ChartDataRequest(BaseModel):
    symbol: str
    ticks: List[ChartTick]

class ChartDataResponse(BaseModel):
    symbol: str
    ticks: List[ChartTick]
    last_updated: datetime

# Notification Models
class Notification(BaseModel):
    notification_id: str
    user_id: str
    title: str
    message: str
    type: str  # trade, deposit, withdrawal, system, promo
    is_read: bool = False
    data: Optional[dict] = None
    created_at: datetime

class NotificationCreate(BaseModel):
    title: str
    message: str
    type: str = "system"
    data: Optional[dict] = None

# ============= Helper Functions =============

def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def create_access_token(data: dict) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(days=ACCESS_TOKEN_EXPIRE_DAYS)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def generate_otp() -> str:
    return str(random.randint(100000, 999999))

def generate_crypto_address() -> str:
    """Generate a mock cryptocurrency address"""
    return "0x" + "".join(random.choices("0123456789abcdef", k=40))

async def get_current_user(authorization: Optional[str] = Header(None), request: Request = None) -> User:
    """Get current user from session token (cookie or header)"""
    token = None
    
    # Try to get from cookie first
    if request:
        token = request.cookies.get("session_token")
    
    # Fallback to Authorization header
    if not token and authorization:
        if authorization.startswith("Bearer "):
            token = authorization.replace("Bearer ", "")
        else:
            token = authorization
    
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    try:
        # Check if it's a JWT token
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            user_id = payload.get("sub")
            if not user_id:
                raise HTTPException(status_code=401, detail="Invalid token")
        except jwt.InvalidTokenError:
            # If not JWT, treat as session token from OAuth
            session_doc = await db.user_sessions.find_one({"session_token": token}, {"_id": 0})
            if not session_doc:
                raise HTTPException(status_code=401, detail="Session not found")
            
            # Check expiry
            expires_at = session_doc["expires_at"]
            if isinstance(expires_at, str):
                expires_at = datetime.fromisoformat(expires_at)
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
            if expires_at < datetime.now(timezone.utc):
                raise HTTPException(status_code=401, detail="Session expired")
            
            user_id = session_doc["user_id"]
        
        # Get user from database
        user_doc = await db.users.find_one({"user_id": user_id}, {"_id": 0})
        if not user_doc:
            raise HTTPException(status_code=401, detail="User not found")
        
        return User(**user_doc)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Authentication failed: {str(e)}")

# ============= Authentication Routes =============

class SendOTPRequest(BaseModel):
    email: EmailStr

class VerifyEmailOTPRequest(BaseModel):
    email: EmailStr
    otp: str

@api_router.post("/auth/signup")
async def signup(user: UserCreate):
    """Register a new user with email and password - sends OTP for verification"""
    # Check if user exists
    existing_user = await db.users.find_one({"email": user.email})
    if existing_user:
        if existing_user.get("is_verified"):
            raise HTTPException(status_code=400, detail="Email already registered")
        else:
            # User exists but not verified - resend OTP
            success, message = send_verification_otp(user.email)
            if success:
                return {
                    "message": "Verification code sent to your email",
                    "requires_verification": True,
                    "email": user.email
                }
            else:
                raise HTTPException(status_code=500, detail=message)
    
    # Create user
    user_id = f"user_{uuid.uuid4().hex[:12]}"
    hashed_password = hash_password(user.password)
    
    # Generate unique account ID (incremental starting from 10000001)
    last_user = await db.users.find_one(
        {"account_id": {"$exists": True}},
        sort=[("account_id", -1)]
    )
    if last_user and last_user.get("account_id"):
        try:
            account_id = int(last_user["account_id"]) + 1
        except:
            account_id = 10000001
    else:
        account_id = 10000001
    
    new_user = {
        "user_id": user_id,
        "account_id": str(account_id),
        "display_id": str(account_id),  # Store display_id for easy lookup
        "email": user.email,
        "name": user.name,
        "full_name": user.name,
        "nickname": None,
        "country": user.country,
        "country_flag": user.country_flag or "🌍",
        "password": hashed_password,
        "picture": None,
        "demo_balance": 10000.0,
        "real_balance": 0.0,
        "bonus_balance": 0.0,
        "is_admin": False,
        "is_verified": False,
        "referred_by": user.referred_by,  # Store affiliate referral code
        "created_at": datetime.now(timezone.utc)
    }
    
    await db.users.insert_one(new_user)
    
    # If referred by affiliate, track the registration properly
    if user.referred_by:
        affiliate = await db.affiliates.find_one({"ref_code": user.referred_by})
        link_id = None
        
        # If not found by ref_code, check affiliate_links collection for custom link codes
        if not affiliate:
            link = await db.affiliate_links.find_one({"code": user.referred_by})
            if link:
                link_id = link.get("link_id")
                affiliate = await db.affiliates.find_one({"affiliate_id": link.get("affiliate_id")})
                print(f"[REFERRAL] Found affiliate via link code {user.referred_by}: {affiliate.get('affiliate_id') if affiliate else 'None'}")
        
        if affiliate:
            # Increment affiliate's referral count
            await db.affiliates.update_one(
                {"affiliate_id": affiliate.get("affiliate_id")},
                {"$inc": {"total_referrals": 1, "total_registrations": 1}}
            )
            # Also update the link if it was used
            await db.affiliate_links.update_one(
                {"code": user.referred_by},
                {"$inc": {"registrations": 1}}
            )
            
            # Create referral record in referrals collection
            referral_doc = {
                "referral_id": f"ref_{uuid.uuid4().hex[:12]}",
                "affiliate_id": affiliate.get("affiliate_id"),
                "referred_user_id": user_id,
                "user_email": user.email,
                "link_id": link_id,  # Track which link was used
                "link_code": user.referred_by,  # Track the code used
                "is_ftd": False,
                "total_deposited": 0,
                "total_traded": 0,
                "commission_earned": 0,
                "created_at": datetime.now(timezone.utc)
            }
            await db.referrals.insert_one(referral_doc)
            
            # Also create entry in affiliate_referrals for tracking
            aff_ref_doc = {
                "user_id": user_id,
                "referred_user_id": user_id,  # Keep consistent naming
                "affiliate_id": affiliate.get("affiliate_id"),
                "link_id": link_id,
                "link_code": user.referred_by,
                "program": affiliate.get("commission_type", "revenue_share"),
                "has_deposited": False,
                "total_deposits": 0,
                "commission_earned": 0,
                "total_volume": 0,
                "status": "active",
                "created_at": datetime.now(timezone.utc)
            }
            await db.affiliate_referrals.insert_one(aff_ref_doc)
            print(f"[REFERRAL] Successfully tracked referral: user={user_id}, affiliate={affiliate.get('affiliate_id')}, link_code={user.referred_by}")
    
    # Send OTP email
    success, message = send_verification_otp(user.email)
    
    if success:
        return {
            "message": "Account created! Verification code sent to your email.",
            "requires_verification": True,
            "email": user.email
        }
    else:
        # Delete user if email failed
        await db.users.delete_one({"user_id": user_id})
        raise HTTPException(status_code=500, detail=f"Failed to send verification email: {message}")

@api_router.post("/auth/verify-email")
async def verify_email_otp_endpoint(request: VerifyEmailOTPRequest):
    """Verify email with OTP code"""
    # Check OTP
    success, message = verify_email_otp(request.email, request.otp)
    
    if not success:
        raise HTTPException(status_code=400, detail=message)
    
    # Get user
    user = await db.users.find_one({"email": request.email})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Update user as verified
    await db.users.update_one(
        {"email": request.email},
        {"$set": {"is_verified": True}}
    )
    
    # Generate access token
    access_token = create_access_token({"sub": user["user_id"]})
    
    return {
        "message": "Email verified successfully!",
        "access_token": access_token,
        "token_type": "bearer",
        "user": {
            "user_id": user["user_id"],
            "email": user["email"],
            "name": user.get("name", ""),
            "demo_balance": user.get("demo_balance", 10000.0),
            "real_balance": user.get("real_balance", 0.0),
            "bonus_balance": user.get("bonus_balance", 0.0),
            "is_admin": user.get("is_admin", False)
        }
    }

@api_router.post("/auth/resend-otp")
async def resend_verification_otp(request: SendOTPRequest):
    """Resend OTP to email"""
    # Check if user exists
    user = await db.users.find_one({"email": request.email})
    if not user:
        raise HTTPException(status_code=404, detail="Email not found")
    
    if user.get("is_verified"):
        raise HTTPException(status_code=400, detail="Email already verified")
    
    success, message = resend_otp(request.email)
    
    if success:
        return {"message": "Verification code sent to your email"}
    else:
        raise HTTPException(status_code=400, detail=message)

@api_router.post("/auth/verify-otp")
async def verify_otp(verification: OTPVerification):
    """Verify OTP and activate account"""
    user_doc = await db.users.find_one({"email": verification.email})
    if not user_doc:
        raise HTTPException(status_code=404, detail="User not found")
    
    if user_doc.get("is_verified"):
        raise HTTPException(status_code=400, detail="User already verified")
    
    if user_doc.get("otp") != verification.otp:
        raise HTTPException(status_code=400, detail="Invalid OTP")
    
    # Check OTP expiry (10 minutes)
    otp_created = user_doc.get("otp_created_at")
    if isinstance(otp_created, str):
        otp_created = datetime.fromisoformat(otp_created)
    if otp_created.tzinfo is None:
        otp_created = otp_created.replace(tzinfo=timezone.utc)
    
    if datetime.now(timezone.utc) - otp_created > timedelta(minutes=10):
        raise HTTPException(status_code=400, detail="OTP expired")
    
    # Mark as verified
    await db.users.update_one(
        {"email": verification.email},
        {"$set": {"is_verified": True}, "$unset": {"otp": "", "otp_created_at": ""}}
    )
    
    # Create JWT token
    access_token = create_access_token(data={"sub": user_doc["user_id"]})
    
    return {
        "message": "Email verified successfully",
        "access_token": access_token,
        "token_type": "bearer"
    }

@api_router.post("/auth/login")
async def login(credentials: UserLogin):
    """Login with email and password"""
    user_doc = await db.users.find_one({"email": credentials.email})
    if not user_doc:
        raise HTTPException(status_code=401, detail="Invalid email or password")
    
    if not verify_password(credentials.password, user_doc["password"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    
    # Check if user is deleted
    if user_doc.get("is_deleted", False):
        raise HTTPException(status_code=403, detail="This account has been deleted by the owner")
    
    # Check if user is banned
    if user_doc.get("is_banned", False):
        ban_reason = user_doc.get("ban_reason", "This account is suspended for violation of company rules")
        raise HTTPException(status_code=403, detail=ban_reason)
    
    if not user_doc.get("is_verified", False):
        raise HTTPException(status_code=401, detail="Please verify your email first")
    
    # Create JWT token
    access_token = create_access_token(data={"sub": user_doc["user_id"]})
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": {
            "user_id": user_doc["user_id"],
            "email": user_doc["email"],
            "name": user_doc["name"],
            "demo_balance": user_doc["demo_balance"],
            "real_balance": user_doc["real_balance"],
            "is_admin": user_doc.get("is_admin", False)
        }
    }

@api_router.post("/auth/request-password-reset")
async def request_password_reset(email: EmailStr):
    """Request password reset OTP"""
    user_doc = await db.users.find_one({"email": email})
    if not user_doc:
        # Don't reveal if email exists
        return {"message": "If email exists, OTP has been sent"}
    
    otp = generate_otp()
    await db.users.update_one(
        {"email": email},
        {"$set": {"reset_otp": otp, "reset_otp_created_at": datetime.now(timezone.utc)}}
    )
    
    # In production, send OTP via email
    return {"message": "OTP sent to email", "otp": otp}  # Remove otp in production

@api_router.post("/auth/reset-password")
async def reset_password(reset: PasswordReset):
    """Reset password with OTP"""
    user_doc = await db.users.find_one({"email": reset.email})
    if not user_doc:
        raise HTTPException(status_code=404, detail="User not found")
    
    if user_doc.get("reset_otp") != reset.otp:
        raise HTTPException(status_code=400, detail="Invalid OTP")
    
    # Check OTP expiry
    otp_created = user_doc.get("reset_otp_created_at")
    if isinstance(otp_created, str):
        otp_created = datetime.fromisoformat(otp_created)
    if otp_created.tzinfo is None:
        otp_created = otp_created.replace(tzinfo=timezone.utc)
    
    if datetime.now(timezone.utc) - otp_created > timedelta(minutes=10):
        raise HTTPException(status_code=400, detail="OTP expired")
    
    # Update password
    hashed_password = hash_password(reset.new_password)
    await db.users.update_one(
        {"email": reset.email},
        {"$set": {"password": hashed_password}, "$unset": {"reset_otp": "", "reset_otp_created_at": ""}}
    )
    
    return {"message": "Password reset successfully"}

# REMINDER: DO NOT HARDCODE THE URL, OR ADD ANY FALLBACKS OR REDIRECT URLS, THIS BREAKS THE AUTH
@api_router.get("/auth/google/session")
async def google_session(session_id: str = Header(None, alias="X-Session-ID")):
    """Exchange session_id for user data (OAuth callback)"""
    if not session_id:
        raise HTTPException(status_code=400, detail="Missing session_id")
    
    try:
        # Call Emergent Auth API
        async with httpx.AsyncClient() as client:
            response = await client.get(
                "https://demobackend.emergentagent.com/auth/v1/env/oauth/session-data",
                headers={"X-Session-ID": session_id}
            )
            
            if response.status_code != 200:
                raise HTTPException(status_code=401, detail="Invalid session")
            
            session_data = response.json()
        
        # Check if user exists
        user_doc = await db.users.find_one({"email": session_data["email"]}, {"_id": 0})
        
        if user_doc:
            # Update existing user
            user_id = user_doc["user_id"]
            await db.users.update_one(
                {"user_id": user_id},
                {"$set": {"name": session_data["name"], "picture": session_data["picture"]}}
            )
        else:
            # Create new user
            user_id = f"user_{uuid.uuid4().hex[:12]}"
            new_user = {
                "user_id": user_id,
                "email": session_data["email"],
                "name": session_data["name"],
                "picture": session_data["picture"],
                "demo_balance": 10000.0,
                "real_balance": 0.0,
                "is_admin": False,
                "is_verified": True,
                "created_at": datetime.now(timezone.utc)
            }
            await db.users.insert_one(new_user)
        
        # Store session
        session_token = session_data["session_token"]
        expires_at = datetime.now(timezone.utc) + timedelta(days=7)
        
        await db.user_sessions.insert_one({
            "user_id": user_id,
            "session_token": session_token,
            "expires_at": expires_at,
            "created_at": datetime.now(timezone.utc)
        })
        
        # Get updated user
        user_doc = await db.users.find_one({"user_id": user_id}, {"_id": 0, "password": 0})
        
        return {
            "session_token": session_token,
            "user": user_doc
        }
    
    except httpx.HTTPError as e:
        raise HTTPException(status_code=500, detail=f"Failed to authenticate: {str(e)}")

@api_router.get("/auth/me")
async def get_me(authorization: Optional[str] = Header(None), request: Request = None):
    """Get current user info"""
    user = await get_current_user(authorization, request)
    
    # Get user document to fetch all balance fields
    user_doc = await db.users.find_one({"user_id": user.user_id})
    if user_doc:
        deposit_balance = user_doc.get("deposit_balance", 0)
        bonus_balance = user_doc.get("bonus_balance", 0)
        profit_balance = user_doc.get("profit_balance", 0)
        has_withdrawn = user_doc.get("has_withdrawn", False)
    else:
        deposit_balance = 0
        bonus_balance = 0
        profit_balance = 0
        has_withdrawn = False
    
    # Total balance in account (real_balance includes deposit + bonus)
    total_balance = user.real_balance
    
    # Available for withdrawal (real_balance - bonus_balance)
    withdrawable_balance = max(0, user.real_balance - bonus_balance)
    
    return {
        "user_id": user.user_id,
        "email": user.email,
        "name": user.name,
        "picture": user.picture,
        "chart_picture": user_doc.get("chart_picture") if user_doc else None,
        "demo_balance": user.demo_balance,
        "real_balance": user.real_balance,
        "deposit_balance": deposit_balance,
        "bonus_balance": bonus_balance,
        "profit_balance": profit_balance,
        "total_balance": total_balance,
        "withdrawable_balance": withdrawable_balance,
        "has_withdrawn": has_withdrawn,
        "is_admin": user.is_admin
    }

@api_router.post("/auth/logout")
async def logout(request: Request):
    """Logout user"""
    token = request.cookies.get("session_token")
    if token:
        await db.user_sessions.delete_one({"session_token": token})
    return {"message": "Logged out successfully"}

# ============= Trading Routes =============

@api_router.get("/platform/status")
async def get_platform_status():
    """Get public platform status (trading enabled, maintenance, etc.)"""
    god_mode = await db.platform_settings.find_one({"_id": "god_mode"})
    return {
        "trading_enabled": god_mode.get("trading_enabled", True) if god_mode else True,
        "maintenance_mode": god_mode.get("maintenance_mode", False) if god_mode else False
    }

@api_router.get("/assets")
async def get_assets(include_inactive: bool = False):
    """Get all tradeable assets. Use include_inactive=true to get all assets including disabled ones."""
    # Create default assets if fewer than 60 exist (to add all new assets)
    count = await db.assets.count_documents({})
    if count < 60:
        # Clear old assets and add complete list
        await db.assets.delete_many({})
        default_assets = [
            # FOREX - OTC Pairs
            {"asset_id": str(uuid.uuid4()), "symbol": "USD/CHF", "name": "USD/CHF OTC", "category": "forex", "payout_percentage": 86.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "NZD/USD", "name": "NZD/USD OTC", "category": "forex", "payout_percentage": 84.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "USD/CAD", "name": "USD/CAD OTC", "category": "forex", "payout_percentage": 83.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "EUR/JPY", "name": "EUR/JPY OTC", "category": "forex", "payout_percentage": 82.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "GBP/JPY", "name": "GBP/JPY OTC", "category": "forex", "payout_percentage": 81.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "EUR/AUD", "name": "EUR/AUD OTC", "category": "forex", "payout_percentage": 80.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "EUR/CAD", "name": "EUR/CAD OTC", "category": "forex", "payout_percentage": 79.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "EUR/CHF", "name": "EUR/CHF OTC", "category": "forex", "payout_percentage": 78.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "GBP/AUD", "name": "GBP/AUD OTC", "category": "forex", "payout_percentage": 77.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "GBP/CAD", "name": "GBP/CAD OTC", "category": "forex", "payout_percentage": 76.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "AUD/JPY", "name": "AUD/JPY OTC", "category": "forex", "payout_percentage": 75.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "CHF/JPY", "name": "CHF/JPY OTC", "category": "forex", "payout_percentage": 74.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "CAD/JPY", "name": "CAD/JPY OTC", "category": "forex", "payout_percentage": 73.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "NZD/JPY", "name": "NZD/JPY OTC", "category": "forex", "payout_percentage": 72.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "AUD/NZD", "name": "AUD/NZD OTC", "category": "forex", "payout_percentage": 71.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "EUR/USD", "name": "EUR/USD OTC", "category": "forex", "payout_percentage": 85.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "GBP/USD", "name": "GBP/USD OTC", "category": "forex", "payout_percentage": 84.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "AUD/USD", "name": "AUD/USD OTC", "category": "forex", "payout_percentage": 82.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "USD/JPY", "name": "USD/JPY OTC", "category": "forex", "payout_percentage": 83.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "EUR/GBP", "name": "EUR/GBP OTC", "category": "forex", "payout_percentage": 80.0, "is_active": True},
            
            # CRYPTO
            {"asset_id": str(uuid.uuid4()), "symbol": "BTC/USD", "name": "Bitcoin OTC", "category": "crypto", "payout_percentage": 90.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "ETH/USD", "name": "Ethereum OTC", "category": "crypto", "payout_percentage": 89.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "SOL/USD", "name": "SOL/USD OTC", "category": "crypto", "payout_percentage": 88.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "ADA/USD", "name": "ADA/USD OTC", "category": "crypto", "payout_percentage": 87.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "XRP/USD", "name": "XRP/USD OTC", "category": "crypto", "payout_percentage": 86.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "DOT/USD", "name": "DOT/USD OTC", "category": "crypto", "payout_percentage": 85.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "MATIC/USD", "name": "MATIC/USD OTC", "category": "crypto", "payout_percentage": 84.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "LTC/USD", "name": "LTC/USD OTC", "category": "crypto", "payout_percentage": 83.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "AVAX/USD", "name": "AVAX/USD OTC", "category": "crypto", "payout_percentage": 82.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "LINK/USD", "name": "LINK/USD OTC", "category": "crypto", "payout_percentage": 81.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "UNI/USD", "name": "UNI/USD OTC", "category": "crypto", "payout_percentage": 80.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "ATOM/USD", "name": "ATOM/USD OTC", "category": "crypto", "payout_percentage": 79.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "XLM/USD", "name": "XLM/USD OTC", "category": "crypto", "payout_percentage": 78.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "ETC/USD", "name": "ETC/USD OTC", "category": "crypto", "payout_percentage": 77.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "FIL/USD", "name": "FIL/USD OTC", "category": "crypto", "payout_percentage": 76.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "DOGE/USD", "name": "DOGE/USD OTC", "category": "crypto", "payout_percentage": 85.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "SHIB/USD", "name": "SHIB/USD OTC", "category": "crypto", "payout_percentage": 84.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "TRX/USD", "name": "TRX/USD OTC", "category": "crypto", "payout_percentage": 75.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "NEAR/USD", "name": "NEAR/USD OTC", "category": "crypto", "payout_percentage": 74.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "APT/USD", "name": "APT/USD OTC", "category": "crypto", "payout_percentage": 73.0, "is_active": True},
            
            # STOCKS
            {"asset_id": str(uuid.uuid4()), "symbol": "AAPL", "name": "Apple Inc.", "category": "stocks", "payout_percentage": 82.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "GOOGL", "name": "Alphabet Inc.", "category": "stocks", "payout_percentage": 81.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "MSFT", "name": "Microsoft Corp.", "category": "stocks", "payout_percentage": 80.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "AMZN", "name": "Amazon.com Inc.", "category": "stocks", "payout_percentage": 79.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "TSLA", "name": "Tesla Inc.", "category": "stocks", "payout_percentage": 85.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "META", "name": "Meta Platforms", "category": "stocks", "payout_percentage": 78.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "NVDA", "name": "NVIDIA Corp.", "category": "stocks", "payout_percentage": 84.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "NFLX", "name": "Netflix Inc.", "category": "stocks", "payout_percentage": 77.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "SBUX", "name": "Starbucks OTC", "category": "stocks", "payout_percentage": 76.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "V", "name": "Visa OTC", "category": "stocks", "payout_percentage": 75.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "MA", "name": "Mastercard OTC", "category": "stocks", "payout_percentage": 74.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "PYPL", "name": "PayPal OTC", "category": "stocks", "payout_percentage": 73.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "WMT", "name": "Walmart OTC", "category": "stocks", "payout_percentage": 72.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "JPM", "name": "JPMorgan OTC", "category": "stocks", "payout_percentage": 71.0, "is_active": True},
            
            # COMMODITIES
            {"asset_id": str(uuid.uuid4()), "symbol": "GOLD", "name": "Gold", "category": "commodities", "payout_percentage": 88.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "SILVER", "name": "Silver", "category": "commodities", "payout_percentage": 86.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "OIL", "name": "Crude Oil", "category": "commodities", "payout_percentage": 85.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "NATGAS", "name": "Natural Gas OTC", "category": "commodities", "payout_percentage": 78.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "PALLADIUM", "name": "Palladium OTC", "category": "commodities", "payout_percentage": 80.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "COPPER", "name": "Copper OTC", "category": "commodities", "payout_percentage": 76.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "WHEAT", "name": "Wheat OTC", "category": "commodities", "payout_percentage": 74.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "CORN", "name": "Corn OTC", "category": "commodities", "payout_percentage": 72.0, "is_active": True},
            {"asset_id": str(uuid.uuid4()), "symbol": "COFFEE", "name": "Coffee OTC", "category": "commodities", "payout_percentage": 70.0, "is_active": True},
        ]
        await db.assets.insert_many(default_assets)
    
    # Return all assets or only active based on query param
    if include_inactive:
        assets = await db.assets.find({}, {"_id": 0}).to_list(200)
    else:
        assets = await db.assets.find({"is_active": True}, {"_id": 0}).to_list(100)
    
    # Add demo_only flag to each asset
    for asset in assets:
        asset["demo_only"] = is_demo_only_asset(asset.get("symbol", ""))
    
    return assets

@api_router.post("/trades")
async def create_trade(trade: TradeCreate, authorization: Optional[str] = Header(None), request: Request = None):
    """Place a new trade"""
    user = await get_current_user(authorization, request)
    
    # Check if global trading is enabled
    god_mode_settings = await db.platform_settings.find_one({"_id": "god_mode"})
    if god_mode_settings and god_mode_settings.get("trading_enabled") == False:
        raise HTTPException(status_code=403, detail="Trading is currently disabled by administrator")
    
    # Check asset restrictions based on account type
    asset_is_demo_only = is_demo_only_asset(trade.asset)
    
    # REMOVED: Demo can now trade all assets (lock is just UI indication)
    # Demo users can trade any asset - the lock icon is just a recommendation
    
    if trade.account_type == "real" and asset_is_demo_only:
        raise HTTPException(
            status_code=403, 
            detail="This asset is only available for demo trading. Please switch to demo account or select a different asset."
        )
    
    # Get user document to check bonus_balance
    user_doc = await db.users.find_one({"user_id": user.user_id})
    bonus_balance = user_doc.get("bonus_balance", 0) if user_doc else 0
    
    if trade.account_type == "demo":
        # Demo account - simple balance check
        if user.demo_balance < trade.amount:
            raise HTTPException(status_code=400, detail="Insufficient balance")
        
        await db.users.update_one(
            {"user_id": user.user_id},
            {"$inc": {"demo_balance": -trade.amount}}
        )
        deducted_from_real = 0
        deducted_from_bonus = 0
    else:
        # Real account - real_balance already contains deposit + bonus
        # bonus_balance is just for tracking (non-withdrawable portion)
        total_available = user.real_balance
        if total_available < trade.amount:
            raise HTTPException(status_code=400, detail="Insufficient balance")
        
        # ============= CORRECT BONUS LOGIC =============
        # RULE: Trade losses should FIRST come from deposit (real portion), THEN from bonus
        # 
        # Example: User has $30 total (real_balance=30), with $20 bonus (bonus_balance=20)
        # So actual deposit = $30 - $20 = $10 (this is the withdrawable portion)
        # 
        # If user trades $15:
        # - First deduct from deposit: $10 (deposit becomes 0)
        # - Then deduct from bonus: $5 (bonus becomes $15)
        # - Result: real_balance = $15, bonus_balance = $15
        #
        # This ensures deposits are used first, bonus last.
        
        # Calculate deposit (non-bonus) portion
        deposit_portion = max(0, user.real_balance - bonus_balance)  # e.g., 30 - 20 = 10
        
        # Determine deduction split
        if deposit_portion >= trade.amount:
            # Entire trade comes from deposit (no bonus used)
            deducted_from_real_deposit = trade.amount
            deducted_from_bonus = 0
        else:
            # Use all deposit first, then use bonus for the rest
            deducted_from_real_deposit = deposit_portion
            deducted_from_bonus = trade.amount - deposit_portion
        
        # For affiliate commission: only the deposit portion counts (not bonus)
        deducted_from_real = deducted_from_real_deposit
        
        # Deduct from real_balance (total) and proportionally from bonus_balance
        update_fields = {"real_balance": -trade.amount}
        if deducted_from_bonus > 0:
            update_fields["bonus_balance"] = -deducted_from_bonus
        
        await db.users.update_one(
            {"user_id": user.user_id},
            {"$inc": update_fields}
        )
        
        print(f"[TRADE DEDUCTION] deposit_portion=${deposit_portion:.2f}, trade=${trade.amount:.2f}, from_deposit=${deducted_from_real_deposit:.2f}, from_bonus=${deducted_from_bonus:.2f}")
    
    # For DEMO accounts only: Predetermine outcome based on AI win rate settings
    # For REAL accounts: Also use AI win rate if AI is enabled
    predetermined_outcome = None
    
    # Get AI settings
    god_mode_settings = await db.platform_settings.find_one({"_id": "god_mode"})
    ai_enabled = god_mode_settings.get("ai_enabled", True) if god_mode_settings else True
    ai_win_rate = god_mode_settings.get("ai_win_rate", 45) if god_mode_settings else 45
    demo_win_rate = god_mode_settings.get("demo_win_rate", 65) if god_mode_settings else 65
    
    print(f"[TRADE CREATE] account_type={trade.account_type}, ai_enabled={ai_enabled}, ai_win_rate={ai_win_rate}, demo_win_rate={demo_win_rate}")
    
    # Check if the asset is a demo-only asset
    is_demo_asset = is_demo_only_asset(trade.asset)
    print(f"[TRADE CREATE] asset={trade.asset}, is_demo_only_asset={is_demo_asset}")
    
    if trade.account_type == "demo":
        # Check if there's already an active demo trade for consistency
        existing_active_trade = await db.trades.find_one({
            "user_id": user.user_id,
            "status": "pending",
            "account_type": "demo"
        }, sort=[("created_at", -1)])
        
        if existing_active_trade and existing_active_trade.get("predetermined_outcome"):
            predetermined_outcome = existing_active_trade["predetermined_outcome"]
        else:
            # ALL Demo account trades use demo_win_rate (regardless of asset type)
            # Demo win rate applies to the entire demo account experience
            win_probability = demo_win_rate / 100.0 if ai_enabled else 0.90
            print(f"[TRADE CREATE] Using DEMO WIN RATE ({demo_win_rate}%) for demo account")
            
            predetermined_won = random.random() < win_probability
            predetermined_outcome = "won" if predetermined_won else "lost"
        print(f"[TRADE CREATE] DEMO trade, predetermined_outcome={predetermined_outcome}")
    elif trade.account_type == "real" and ai_enabled:
        # Real account with AI enabled: Use AI win rate
        win_probability = ai_win_rate / 100.0
        predetermined_won = random.random() < win_probability
        predetermined_outcome = "won" if predetermined_won else "lost"
        print(f"[TRADE CREATE] REAL trade with AI, predetermined_outcome={predetermined_outcome}")
    else:
        print(f"[TRADE CREATE] REAL trade without AI, no predetermined outcome")
    # Real account with AI disabled: predetermined_outcome stays None - will use actual price
    
    # Create trade
    trade_id = f"trade_{uuid.uuid4().hex[:12]}"
    new_trade = {
        "trade_id": trade_id,
        "user_id": user.user_id,
        "asset": trade.asset,
        "trade_type": trade.trade_type,
        "direction": trade.direction,  # UP or DOWN direction
        "amount": trade.amount,
        "entry_price": trade.entry_price,
        "exit_price": None,
        "duration": trade.duration,
        "payout_percentage": trade.payout_percentage,  # Use frontend's asset payout
        "status": "pending",
        "profit_loss": 0.0,
        "account_type": trade.account_type,
        "predetermined_outcome": predetermined_outcome,  # None for real accounts
        "deducted_from_real": deducted_from_real if trade.account_type == "real" else 0,
        "deducted_from_bonus": deducted_from_bonus if trade.account_type == "real" else 0,
        "created_at": datetime.now(timezone.utc),
        "expires_at": datetime.now(timezone.utc) + timedelta(seconds=trade.duration),
        "settled_at": None
    }
    
    await db.trades.insert_one(new_trade)
    
    return {"trade_id": trade_id, "message": "Trade placed successfully"}

@api_router.get("/trades")
async def get_trades(authorization: Optional[str] = Header(None), request: Request = None, limit: int = 50):
    """Get user's trade history"""
    user = await get_current_user(authorization, request)
    
    trades = await db.trades.find(
        {"user_id": user.user_id},
        {"_id": 0}
    ).sort("created_at", -1).limit(limit).to_list(limit)
    
    return trades

@api_router.get("/trades/history")
async def get_trade_history(
    authorization: Optional[str] = Header(None), 
    request: Request = None, 
    limit: int = 50,
    account_type: Optional[str] = None
):
    """Get user's formatted trade history for display - ONLY completed trades (won/lost), NOT pending"""
    user = await get_current_user(authorization, request)
    
    # Build query filter - EXCLUDE pending trades from history
    query_filter = {
        "user_id": user.user_id,
        "status": {"$in": ["won", "lost"]}  # Only show completed trades
    }
    if account_type and account_type in ["demo", "real"]:
        query_filter["account_type"] = account_type
    
    trades = await db.trades.find(
        query_filter,
        {"_id": 0}
    ).sort("created_at", -1).limit(limit).to_list(limit)
    
    # Format trades for frontend display
    formatted_trades = []
    for trade in trades:
        created_at = trade.get("created_at")
        if created_at:
            # Calculate time ago
            now = datetime.now(timezone.utc)
            if isinstance(created_at, str):
                created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            elif isinstance(created_at, datetime) and created_at.tzinfo is None:
                # Make timezone-naive datetime timezone-aware (assume UTC)
                created_at = created_at.replace(tzinfo=timezone.utc)
            diff = now - created_at
            if diff.days > 0:
                time_ago = f"{diff.days}d ago"
            elif diff.seconds >= 3600:
                time_ago = f"{diff.seconds // 3600}h ago"
            elif diff.seconds >= 60:
                time_ago = f"{diff.seconds // 60}m ago"
            else:
                time_ago = f"{diff.seconds}s ago"
        else:
            time_ago = "just now"
            
        formatted_trades.append({
            "trade_id": trade.get("trade_id"),
            "asset": trade.get("asset", "EUR/USD OTC"),
            "type": trade.get("trade_type", "call"),
            "entry_price": trade.get("entry_price", 0),
            "exit_price": trade.get("exit_price", 0),
            "amount": trade.get("amount", 0),
            "profit_loss": trade.get("profit_loss", 0),
            "status": trade.get("status", "pending"),
            "account_type": trade.get("account_type", "demo"),
            "time_ago": time_ago,
            "created_at": str(created_at) if created_at else None
        })
    
    return {"trades": formatted_trades}

@api_router.get("/trades/stats")
async def get_trade_stats(authorization: Optional[str] = Header(None), request: Request = None, limit: int = 500):
    """Get user's trading statistics"""
    user = await get_current_user(authorization, request)
    
    # Optimized query with projection to only fetch needed fields
    all_trades = await db.trades.find(
        {"user_id": user.user_id}, 
        {"_id": 0, "status": 1, "profit_loss": 1}
    ).limit(limit).to_list(limit)
    
    total_trades = len(all_trades)
    won_trades = len([t for t in all_trades if t.get("status") == "won"])
    lost_trades = len([t for t in all_trades if t.get("status") == "lost"])
    total_profit = sum(t.get("profit_loss", 0) for t in all_trades)
    win_rate = (won_trades / total_trades * 100) if total_trades > 0 else 0
    
    return {
        "total_trades": total_trades,
        "won_trades": won_trades,
        "lost_trades": lost_trades,
        "total_profit": total_profit,
        "win_rate": win_rate
    }

@api_router.post("/trades/{trade_id}/settle")
async def settle_trade(trade_id: str, settle_data: TradeSettle, authorization: Optional[str] = Header(None), request: Request = None):
    """Settle a trade - Uses ACTUAL price movement for win/loss"""
    user = await get_current_user(authorization, request)
    
    trade = await db.trades.find_one({"trade_id": trade_id, "user_id": user.user_id})
    if not trade:
        raise HTTPException(status_code=404, detail="Trade not found")
    
    if trade["status"] != "pending":
        raise HTTPException(status_code=400, detail="Trade already settled")
    
    entry_price = trade["entry_price"]
    trade_type = trade["trade_type"]
    exit_price = settle_data.exit_price
    
    # Use ACTUAL price movement to determine win/loss
    # UP (call) wins if price went UP (exit > entry)
    # DOWN (put) wins if price went DOWN (exit < entry)
    if trade_type == "call":
        won = exit_price > entry_price
    else:  # put
        won = exit_price < entry_price
    
    # Tie goes to house (loss)
    if exit_price == entry_price:
        won = False
    
    print(f"[TRADE SETTLE] id={trade_id}, type={trade_type}, entry={entry_price:.5f}, exit={exit_price:.5f}, won={won}")
    
    status = "won" if won else "lost"
    profit_loss = trade["amount"] * (trade["payout_percentage"] / 100) if won else -trade["amount"]
    
    # Update trade
    await db.trades.update_one(
        {"trade_id": trade_id},
        {"$set": {
            "exit_price": exit_price,
            "status": status,
            "profit_loss": profit_loss,
            "settled_at": datetime.now(timezone.utc)
        }}
    )
    
    # Update user balance
    if trade["account_type"] == "demo":
        # Demo account - simple balance update
        if won:
            payout = trade["amount"] + profit_loss
            await db.users.update_one(
                {"user_id": user.user_id},
                {"$inc": {"demo_balance": payout}}
            )
    else:
        # Real account - profits go to real_balance (withdrawable)
        if won:
            # Payout = original amount + profit
            payout = trade["amount"] + profit_loss
            # ALL winnings go to real_balance (withdrawable)
            await db.users.update_one(
                {"user_id": user.user_id},
                {"$inc": {"real_balance": payout}}
            )
        # If lost, the amount was already deducted when trade was placed
        
        # ============= AFFILIATE COMMISSION CALCULATION =============
        # Check if user was referred by an affiliate
        user_data = await db.users.find_one({"user_id": user.user_id})
        referred_by = user_data.get("referred_by") if user_data else None
        
        if referred_by:
            # Find the affiliate by ref_code or link code
            affiliate = await db.affiliates.find_one({"ref_code": referred_by})
            
            # If not found by ref_code, check affiliate_links
            if not affiliate:
                link = await db.affiliate_links.find_one({"code": referred_by})
                if link:
                    affiliate = await db.affiliates.find_one({"affiliate_id": link.get("affiliate_id")})
                    print(f"[COMMISSION] Found affiliate via link code {referred_by}: {affiliate.get('affiliate_id') if affiliate else 'None'}")
            
            if affiliate:
                # Get affiliate's level based on FTDs
                total_ftds = affiliate.get("total_ftds", 0)
                level_info = get_affiliate_level(total_ftds)
                
                # Get referral info to determine commission program type
                referral = await db.affiliate_referrals.find_one({
                    "user_id": user.user_id,
                    "affiliate_id": affiliate["affiliate_id"]
                })
                program_type = referral.get("program", "revenue_sharing") if referral else "revenue_sharing"
                
                # Calculate commission based on program type
                # IMPORTANT: Commission is only calculated on REAL deposit amount
                # NOT on bonus amount - affiliate only earns from actual deposits
                trade_amount_for_commission = trade.get("deducted_from_real", trade["amount"])
                
                # If trade was from demo account or no real balance was used, skip commission
                if trade.get("account_type") == "demo" or trade_amount_for_commission <= 0:
                    print(f"[COMMISSION] Skipping - demo account or no real balance used")
                else:
                    commission = calculate_commission(
                        affiliate["affiliate_id"],
                        level_info,
                        trade_amount_for_commission,  # Only real deposit portion
                        status,
                        program_type
                    )
                    
                    # Handle both positive (user loss) and negative (user win) commissions
                    if commission != 0:
                        # Credit/Debit commission to affiliate's HOLD balance (released on Monday 6 AM SGT)
                        # Track turnover and revenue commissions separately
                        if program_type == "turnover":
                            # Turnover is always positive
                            await db.affiliates.update_one(
                                {"affiliate_id": affiliate["affiliate_id"]},
                                {"$inc": {"hold_balance": commission, "hold_balance_turnover": commission, "total_earnings": commission}}
                            )
                        else:
                            # Revenue share: positive when user loses, negative when user wins
                            await db.affiliates.update_one(
                                {"affiliate_id": affiliate["affiliate_id"]},
                                {"$inc": {"hold_balance": commission, "hold_balance_revenue": commission, "total_earnings": commission}}
                            )
                        
                        # Update referral commission earned
                        if referral:
                            await db.affiliate_referrals.update_one(
                                {"user_id": user.user_id, "affiliate_id": affiliate["affiliate_id"]},
                                {"$inc": {"commission_earned": commission, "total_volume": trade_amount_for_commission}}
                            )
                        
                        # Log commission transaction (including negative ones)
                        await db.affiliate_commissions.insert_one({
                            "commission_id": str(uuid.uuid4()),
                            "affiliate_id": affiliate["affiliate_id"],
                            "user_id": user.user_id,
                            "trade_id": trade_id,
                            "trade_amount": trade["amount"],
                            "real_amount_used": trade_amount_for_commission,
                            "bonus_amount_used": trade.get("deducted_from_bonus", 0),
                            "trade_result": status,
                            "program_type": program_type,
                            "affiliate_level": level_info["level"],
                            "commission_rate": level_info["revenue_share"] if program_type == "revenue_sharing" else level_info["turnover_share"],
                            "commission_amount": commission,
                            "is_deduction": commission < 0,
                            "created_at": datetime.now(timezone.utc)
                        })
                        
                        print(f"[COMMISSION] Affiliate {affiliate['affiliate_id']}: ${commission:.2f} (from ${trade_amount_for_commission} real, ${trade.get('deducted_from_bonus', 0)} bonus skipped)")
    
    return {"message": "Trade settled", "status": status, "profit_loss": profit_loss, "exit_price": exit_price, "entry_price": entry_price}

# ============= Wallet Routes =============

@api_router.post("/wallet/deposit")
async def request_deposit(deposit: DepositRequest, authorization: Optional[str] = Header(None), request: Request = None):
    """Request crypto deposit (mock)"""
    user = await get_current_user(authorization, request)
    
    # Generate mock deposit address
    crypto_address = generate_crypto_address()
    transaction_id = f"txn_{uuid.uuid4().hex[:12]}"
    
    new_transaction = {
        "transaction_id": transaction_id,
        "user_id": user.user_id,
        "type": "deposit",
        "amount": deposit.amount,
        "status": "pending",
        "crypto_address": crypto_address,
        "txn_hash": None,
        "account_type": "real",
        "created_at": datetime.now(timezone.utc),
        "completed_at": None
    }
    
    await db.transactions.insert_one(new_transaction)
    
    return {
        "transaction_id": transaction_id,
        "crypto_address": crypto_address,
        "amount": deposit.amount,
        "message": "Send crypto to this address"
    }

@api_router.post("/wallet/withdraw")
async def request_withdrawal(withdrawal: WithdrawalRequest, authorization: Optional[str] = Header(None), request: Request = None):
    """Request USDT TRC20 withdrawal via NOWPayments"""
    user = await get_current_user(authorization, request)
    
    # Check if user has any locked withdrawal
    locked_withdrawal = await db.transactions.find_one({
        "user_id": user.user_id,
        "type": "withdrawal",
        "status": "locked"
    })
    
    if locked_withdrawal:
        raise HTTPException(
            status_code=400, 
            detail=f"You have a locked withdrawal pending KYC verification. Please submit the required document ({locked_withdrawal.get('kyc_requirement', 'Bank Statement')}) before creating new withdrawal requests."
        )
    
    # Validate TRC20 address format
    crypto_address = withdrawal.crypto_address.strip()
    if not crypto_address or len(crypto_address) != 34 or not crypto_address.startswith('T'):
        raise HTTPException(
            status_code=400,
            detail="Invalid TRC20 address. Address must be 34 characters and start with 'T'"
        )
    
    # Get current balance and bonus
    user_doc = await db.users.find_one({"user_id": user.user_id})
    real_balance = user_doc.get("real_balance", 0)
    bonus_balance = user_doc.get("bonus_balance", 0)
    
    # Withdrawable = real_balance - bonus_balance
    withdrawable = real_balance - bonus_balance
    
    # Minimum withdrawal $10, Network fee $1
    min_withdrawal = 10.0
    network_fee = 1.0
    
    # AUTO-APPROVAL THRESHOLD: $100 or less = auto-approved, more than $100 = admin approval required
    AUTO_APPROVAL_LIMIT = 100.0
    
    if withdrawal.amount < min_withdrawal:
        raise HTTPException(
            status_code=400,
            detail=f"Minimum withdrawal is ${min_withdrawal}"
        )
    
    if withdrawal.amount > withdrawable:
        raise HTTPException(
            status_code=400, 
            detail=f"Insufficient withdrawable balance. You have ${withdrawable:.2f} available. (Bonus of ${bonus_balance:.2f} cannot be withdrawn)"
        )
    
    # Calculate net amount after fee
    net_amount = withdrawal.amount - network_fee
    
    transaction_id = f"txn_{uuid.uuid4().hex[:12]}"
    
    # Determine if auto-approval or admin approval required
    requires_admin_approval = withdrawal.amount > AUTO_APPROVAL_LIMIT
    
    # Initialize payout variables
    payout_id = None
    payout_status = "pending_approval" if requires_admin_approval else "pending"
    payout_error = None
    
    if not requires_admin_approval:
        # AUTO-APPROVED: Process via NOWPayments immediately
        # Get IPN callback URL
        integration_proxy = os.environ.get("INTEGRATION_PROXY_URL", "")
        host = request.headers.get("host", "localhost")
        scheme = "https" if any(x in host for x in ["preview.emergentagent.com", "preview.emergentcf.cloud", "emergent.host"]) else request.url.scheme
        base_url = f"{scheme}://{host}"
        callback_url = f"{integration_proxy}/api/nowpayments/withdrawal/callback" if integration_proxy else f"{base_url}/api/nowpayments/withdrawal/callback"
        
        # Create payout via NOWPayments API
        print(f"[NOWPayments] Auto-approved withdrawal (${withdrawal.amount} <= ${AUTO_APPROVAL_LIMIT}): amount={net_amount}, address={crypto_address}")
        payout_result = await create_usdt_payout(
            address=crypto_address,
            amount=net_amount,
            external_id=transaction_id,
            callback_url=callback_url
        )
        
        print(f"[NOWPayments] Payout result: {payout_result}")
        
        # Determine status based on NOWPayments response
        if payout_result.get("success"):
            payout_data = payout_result.get("data", {})
            payout_id = payout_data.get("id") or payout_data.get("payout_id")
            # NOWPayments statuses: waiting, confirming, sending, finished, failed, refunded
            np_status = payout_data.get("status", "waiting")
            if np_status in ["waiting", "confirming", "sending"]:
                payout_status = "processing"
            elif np_status == "finished":
                payout_status = "completed"
            elif np_status == "failed":
                payout_status = "failed"
                payout_error = payout_data.get("error") or "Payout failed"
        else:
            # If NOWPayments API failed, still create record but mark as pending for manual review
            payout_error = payout_result.get("error") or payout_result.get("data", {}).get("message") or "NOWPayments API error"
            print(f"[NOWPayments] API Error: {payout_error}")
            # Keep as pending for admin to manually process
            payout_status = "pending"
    else:
        # ADMIN APPROVAL REQUIRED: Don't call NOWPayments yet
        print(f"[NOWPayments] Withdrawal requires admin approval (${withdrawal.amount} > ${AUTO_APPROVAL_LIMIT})")
    
    new_transaction = {
        "transaction_id": transaction_id,
        "user_id": user.user_id,
        "type": "withdrawal",
        "payment_type": "nowpayments",
        "currency": "USDT",
        "network": "TRC20",
        "amount": withdrawal.amount,
        "net_amount": net_amount,
        "network_fee": network_fee,
        "status": payout_status,
        "requires_admin_approval": requires_admin_approval,
        "auto_approval_limit": AUTO_APPROVAL_LIMIT,
        "crypto_address": crypto_address,
        "nowpayments_payout_id": payout_id,
        "nowpayments_error": payout_error,
        "txn_hash": None,
        "account_type": "real",
        "created_at": datetime.now(timezone.utc),
        "completed_at": None,
        "bonus_forfeited": bonus_balance if bonus_balance > 0 else 0
    }
    
    await db.transactions.insert_one(new_transaction)
    
    # Also store in withdrawals collection for consistency
    await db.withdrawals.insert_one({
        **new_transaction,
        "order_id": transaction_id
    })
    
    # Deduct from balance immediately
    # IMPORTANT: If user has bonus, forfeit the bonus on any withdrawal
    if bonus_balance > 0:
        # Deduct withdrawal amount + forfeit entire bonus
        await db.users.update_one(
            {"user_id": user.user_id},
            {
                "$inc": {"real_balance": -(withdrawal.amount + bonus_balance)},
                "$set": {"bonus_balance": 0}
            }
        )
        # Create notification about bonus forfeit
        await create_notification(
            user.user_id,
            "⚠️ Bonus Forfeited",
            f"Your bonus of ${bonus_balance:.2f} has been forfeited due to withdrawal. Bonuses can only be used for trading.",
            "warning"
        )
    else:
        await db.users.update_one(
            {"user_id": user.user_id},
            {"$inc": {"real_balance": -withdrawal.amount}}
        )
    
    # Create appropriate notification
    if requires_admin_approval:
        await create_notification(
            user.user_id,
            "⏳ Withdrawal Pending Approval",
            f"Your withdrawal of ${net_amount:.2f} USDT TRC20 requires admin approval (amount > ${AUTO_APPROVAL_LIMIT}). You'll be notified once approved.",
            "info"
        )
    else:
        await create_notification(
            user.user_id,
            "💰 Withdrawal Processing",
            f"Your withdrawal of ${net_amount:.2f} USDT TRC20 to {crypto_address[:8]}...{crypto_address[-6:]} is being processed.",
            "success"
        )
    
    return {
        "success": True,
        "transaction_id": transaction_id,
        "payout_id": payout_id,
        "amount": withdrawal.amount,
        "net_amount": net_amount,
        "fee": network_fee,
        "status": payout_status,
        "requires_admin_approval": requires_admin_approval,
        "message": f"Withdrawal requires admin approval (amount > ${AUTO_APPROVAL_LIMIT})" if requires_admin_approval else ("Withdrawal request submitted" if payout_status == "pending" else "Withdrawal is being processed via NOWPayments")
    }

@api_router.get("/wallet/transactions")
async def get_transactions(authorization: Optional[str] = Header(None), request: Request = None):
    """Get user's transaction history with summary stats"""
    user = await get_current_user(authorization, request)
    
    # Fetch from transactions collection
    transactions = await db.transactions.find(
        {"user_id": user.user_id},
        {"_id": 0}
    ).sort("created_at", -1).to_list(100)
    
    # Also fetch from deposits collection (NOWPayments deposits)
    deposits = await db.deposits.find(
        {"user_id": user.user_id},
        {"_id": 0}
    ).sort("created_at", -1).to_list(100)
    
    # Merge deposits into transactions format
    for dep in deposits:
        # Handle both crypto (NOWPayments) and fiat (TarsPay) deposits
        is_tarspay = dep.get("payment_type") == "tarspay"
        
        tx = {
            "transaction_id": dep.get("payment_id") or dep.get("order_id") or dep.get("transaction_id"),
            "user_id": dep.get("user_id"),
            "type": "deposit",
            "amount": dep.get("amount_usd") or dep.get("amount", 0),
            "status": dep.get("status", "pending"),
            "payment_type": dep.get("payment_type", "crypto"),
            "channel": dep.get("channel"),
            "channel_name": dep.get("channel_name"),
            "currency": dep.get("channel_name") or dep.get("pay_currency", "USDT") if is_tarspay else dep.get("pay_currency", "USDT"),
            "network": "" if is_tarspay else dep.get("network", "TRC20"),
            "crypto_address": dep.get("pay_address"),
            "created_at": dep.get("created_at"),
            "bonus_amount": dep.get("bonus_amount", 0),
            "total_credit": dep.get("total_credited") or dep.get("total_credit") or dep.get("amount_usd") or dep.get("amount", 0)
        }
        # Only add if not already in transactions
        if not any(t.get("transaction_id") == tx["transaction_id"] for t in transactions):
            transactions.append(tx)
    
    # Also fetch from withdrawals collection (E-Wallet withdrawals)
    withdrawals = await db.withdrawals.find(
        {"user_id": user.user_id},
        {"_id": 0}
    ).sort("created_at", -1).to_list(100)
    
    # Merge withdrawals into transactions format
    for wd in withdrawals:
        is_ewallet = wd.get("payment_type") == "tarspay" or wd.get("type") == "ewallet"
        
        tx = {
            "transaction_id": wd.get("payment_id") or wd.get("order_id") or wd.get("transaction_id"),
            "user_id": wd.get("user_id"),
            "type": "withdrawal",
            "amount": wd.get("amount_usd") or wd.get("amount", 0),
            "status": wd.get("status", "pending"),
            "payment_type": wd.get("payment_type", "crypto"),
            "channel": wd.get("channel"),
            "channel_name": wd.get("channel_name"),
            "wallet_id": wd.get("wallet_id"),
            "currency": wd.get("channel_name") if is_ewallet else "USDT",
            "network": "" if is_ewallet else wd.get("network", "TRC20"),
            "crypto_address": wd.get("crypto_address") or wd.get("wallet_id"),
            "created_at": wd.get("created_at"),
            "fee_usd": wd.get("fee_usd", 0),
            "fee_bdt": wd.get("fee_bdt", 0),
            "net_amount_bdt": wd.get("net_amount_bdt", 0)
        }
        # Only add if not already in transactions
        if not any(t.get("transaction_id") == tx["transaction_id"] for t in transactions):
            transactions.append(tx)
    
    # Sort all by created_at descending
    transactions.sort(key=lambda x: x.get("created_at") or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    
    # Calculate summary statistics
    total_deposits = 0
    total_deposit_amount = 0.0
    total_withdrawals = 0
    total_withdrawal_amount = 0.0
    
    for tx in transactions:
        if tx.get("type") == "deposit":
            total_deposits += 1
            if tx.get("status") in ["completed", "confirmed", "finished"]:
                total_deposit_amount += tx.get("amount") or 0
        elif tx.get("type") == "withdrawal":
            total_withdrawals += 1
            if tx.get("status") in ["completed", "success", "paid"]:
                total_withdrawal_amount += tx.get("amount") or 0
    
    return {
        "transactions": transactions,
        "summary": {
            "total_deposits": total_deposits,
            "total_deposit_amount": total_deposit_amount,
            "total_withdrawals": total_withdrawals,
            "total_withdrawal_amount": total_withdrawal_amount
        }
    }

# ============= Notification Routes =============

@api_router.get("/notifications")
async def get_notifications(authorization: Optional[str] = Header(None), request: Request = None):
    """Get user's notifications"""
    user = await get_current_user(authorization, request)
    
    notifications = await db.notifications.find(
        {"user_id": user.user_id},
        {"_id": 0}
    ).sort("created_at", -1).limit(50).to_list(50)
    
    # Convert datetime to ISO format with Z suffix for proper timezone handling
    for notif in notifications:
        if 'created_at' in notif and notif['created_at']:
            if isinstance(notif['created_at'], datetime):
                notif['created_at'] = notif['created_at'].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    
    # Count unread
    unread_count = await db.notifications.count_documents({
        "user_id": user.user_id,
        "is_read": False
    })
    
    return {
        "notifications": notifications,
        "unread_count": unread_count
    }

@api_router.post("/notifications/read/{notification_id}")
async def mark_notification_read(notification_id: str, authorization: Optional[str] = Header(None), request: Request = None):
    """Mark a notification as read"""
    user = await get_current_user(authorization, request)
    
    result = await db.notifications.update_one(
        {"notification_id": notification_id, "user_id": user.user_id},
        {"$set": {"is_read": True}}
    )
    
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Notification not found")
    
    return {"success": True}

@api_router.post("/notifications/read-all")
async def mark_all_notifications_read(authorization: Optional[str] = Header(None), request: Request = None):
    """Mark all notifications as read"""
    user = await get_current_user(authorization, request)
    
    await db.notifications.update_many(
        {"user_id": user.user_id, "is_read": False},
        {"$set": {"is_read": True}}
    )
    
    return {"success": True}

@api_router.delete("/notifications/{notification_id}")
async def delete_notification(notification_id: str, authorization: Optional[str] = Header(None), request: Request = None):
    """Delete a notification"""
    user = await get_current_user(authorization, request)
    
    result = await db.notifications.delete_one(
        {"notification_id": notification_id, "user_id": user.user_id}
    )
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Notification not found")
    
    return {"success": True}

# Helper function to create notifications
async def create_notification(user_id: str, title: str, message: str, notif_type: str, data: dict = None):
    """Create a notification for a user"""
    notification = {
        "notification_id": f"notif_{uuid.uuid4().hex[:12]}",
        "user_id": user_id,
        "title": title,
        "message": message,
        "type": notif_type,
        "is_read": False,
        "data": data,
        "created_at": datetime.now(timezone.utc)
    }
    await db.notifications.insert_one(notification)
    return notification

# ============= Leaderboard Routes =============

@api_router.get("/leaderboard")
async def get_leaderboard():
    """Get top traders leaderboard for last 24 hours - ONLY REAL BALANCE TRADES"""
    # Calculate the time 24 hours ago
    time_24h_ago = datetime.now(timezone.utc) - timedelta(hours=24)
    
    # Aggregate trades to get profit/loss per user in last 24 hours
    # ONLY count REAL balance trades (not demo)
    pipeline = [
        {
            "$match": {
                "settled_at": {"$gte": time_24h_ago},
                "status": {"$in": ["won", "lost"]},
                "account_type": "real",  # Only real balance trades count for leaderboard
            }
        },
        {
            "$group": {
                "_id": "$user_id",
                "total_profit": {"$sum": "$profit_loss"},
                "total_trades": {"$sum": 1},
                "won_trades": {
                    "$sum": {"$cond": [{"$eq": ["$status", "won"]}, 1, 0]}
                },
                "total_volume": {"$sum": "$amount"},
            }
        },
        {
            "$sort": {"total_profit": -1}  # Sort by profit descending (highest profit first, then losses)
        },
        {
            "$limit": 100
        }
    ]
    
    results = await db.trades.aggregate(pipeline).to_list(100)
    
    # Fetch user details for each result
    leaderboard = []
    for i, result in enumerate(results):
        user = await db.users.find_one(
            {"user_id": result["_id"]},
            {"_id": 0, "user_id": 1, "name": 1, "full_name": 1, "nickname": 1, "account_id": 1, "country": 1, "country_flag": 1, "picture": 1, "profile_picture": 1}
        )
        
        if user:
            win_rate = (result["won_trades"] / result["total_trades"] * 100) if result["total_trades"] > 0 else 0
            # Use nickname if set, otherwise use ID: {account_id}
            display_name = user.get("nickname")
            if not display_name:
                account_id = user.get("account_id", result['_id'][-8:])
                display_name = f"ID: {account_id}"
            # Get profile picture - check both picture and profile_picture fields
            profile_pic = user.get("picture") or user.get("profile_picture")
            leaderboard.append({
                "rank": i + 1,
                "user_id": result["_id"],
                "name": display_name,
                "country": user.get("country", "Unknown"),
                "country_flag": user.get("country_flag", "🌍"),
                "picture": profile_pic,
                "profit": round(result["total_profit"], 2),
                "is_profit": result["total_profit"] >= 0,  # True for profit, False for loss
                "total_trades": result["total_trades"],
                "win_rate": round(win_rate, 1),
                "volume": round(result["total_volume"], 2),
            })
    
    return {
        "leaderboard": leaderboard,
        "total_traders": len(leaderboard),
        "updated_at": datetime.now(timezone.utc).isoformat()
    }

@api_router.get("/leaderboard/my-stats")
async def get_my_leaderboard_stats(authorization: Optional[str] = Header(None), request: Request = None):
    """Get current user's leaderboard stats and position"""
    user = await get_current_user(authorization, request)
    
    # Get user's profile for nickname and country
    user_doc = await db.users.find_one(
        {"user_id": user.user_id},
        {"nickname": 1, "account_id": 1, "country": 1, "country_flag": 1, "name": 1, "full_name": 1, "picture": 1, "profile_picture": 1}
    )
    
    # Determine display name (nickname or ID: account_id)
    display_name = user_doc.get("nickname") if user_doc else None
    if not display_name:
        account_id = user_doc.get("account_id") if user_doc else None
        if account_id:
            display_name = f"ID: {account_id}"
        else:
            display_name = user_doc.get("full_name") or user_doc.get("name") or user.name
    
    country_flag = user_doc.get("country_flag", "🌍") if user_doc else "🌍"
    profile_pic = user_doc.get("picture") or user_doc.get("profile_picture") if user_doc else None
    
    # Calculate the time 24 hours ago
    time_24h_ago = datetime.now(timezone.utc) - timedelta(hours=24)
    
    # Get user's stats for last 24 hours - ONLY REAL BALANCE TRADES
    user_stats_pipeline = [
        {
            "$match": {
                "user_id": user.user_id,
                "settled_at": {"$gte": time_24h_ago},
                "status": {"$in": ["won", "lost"]},
                "account_type": "real",  # Only real balance trades count
            }
        },
        {
            "$group": {
                "_id": "$user_id",
                "total_profit": {"$sum": "$profit_loss"},
                "total_trades": {"$sum": 1},
                "won_trades": {
                    "$sum": {"$cond": [{"$eq": ["$status", "won"]}, 1, 0]}
                },
                "total_volume": {"$sum": "$amount"},
            }
        }
    ]
    
    user_results = await db.trades.aggregate(user_stats_pipeline).to_list(1)
    
    if not user_results:
        return {
            "user_id": user.user_id,
            "name": display_name,
            "country_flag": country_flag,
            "picture": profile_pic,
            "profit": 0,
            "total_trades": 0,
            "win_rate": 0,
            "volume": 0,
            "position": "100+",  # User hasn't traded in last 24h
        }
    
    user_stats = user_results[0]
    win_rate = (user_stats["won_trades"] / user_stats["total_trades"] * 100) if user_stats["total_trades"] > 0 else 0
    
    # Calculate user's position by counting users with higher profit - ONLY REAL TRADES
    position_pipeline = [
        {
            "$match": {
                "settled_at": {"$gte": time_24h_ago},
                "status": {"$in": ["won", "lost"]},
                "account_type": "real",  # Only real balance trades count
            }
        },
        {
            "$group": {
                "_id": "$user_id",
                "total_profit": {"$sum": "$profit_loss"},
            }
        },
        {
            "$match": {
                "total_profit": {"$gt": user_stats["total_profit"]}
            }
        },
        {
            "$count": "higher_ranked"
        }
    ]
    
    position_result = await db.trades.aggregate(position_pipeline).to_list(1)
    position = (position_result[0]["higher_ranked"] + 1) if position_result else 1
    
    # Format position
    position_str = str(position) if position <= 100 else "100+"
    
    return {
        "user_id": user.user_id,
        "name": display_name,
        "country_flag": country_flag,
        "picture": profile_pic,
        "profit": round(user_stats["total_profit"], 2),
        "total_trades": user_stats["total_trades"],
        "win_rate": round(win_rate, 1),
        "volume": round(user_stats["total_volume"], 2),
        "position": position_str,
    }

@api_router.get("/leaderboard/user/{user_id}")
async def get_leaderboard_user_profile(user_id: str):
    """Get detailed profile of a user for leaderboard popup"""
    
    # Get user's profile info
    user_doc = await db.users.find_one(
        {"user_id": user_id},
        {
            "user_id": 1, "account_id": 1, "nickname": 1, "full_name": 1, "name": 1,
            "country": 1, "country_flag": 1, "picture": 1, "demo_balance": 1,
            "real_balance": 1, "created_at": 1
        }
    )
    
    if not user_doc:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Determine display name
    display_name = user_doc.get("nickname")
    if not display_name:
        account_id = user_doc.get("account_id")
        if account_id:
            display_name = f"ID: {account_id}"
        else:
            display_name = user_doc.get("full_name") or user_doc.get("name") or "Trader"
    
    # Get all-time trading stats
    stats_pipeline = [
        {
            "$match": {
                "user_id": user_id,
                "status": {"$in": ["won", "lost"]},
            }
        },
        {
            "$group": {
                "_id": "$user_id",
                "total_trades": {"$sum": 1},
                "won_trades": {
                    "$sum": {"$cond": [{"$eq": ["$status", "won"]}, 1, 0]}
                },
                "total_profit": {"$sum": "$profit_loss"},
                "total_volume": {"$sum": "$amount"},
                "min_amount": {"$min": "$amount"},
                "max_amount": {"$max": "$amount"},
            }
        }
    ]
    
    stats_results = await db.trades.aggregate(stats_pipeline).to_list(1)
    
    # Default stats if no trades
    if not stats_results:
        stats = {
            "total_trades": 0,
            "won_trades": 0,
            "total_profit": 0,
            "total_volume": 0,
            "min_amount": 0,
            "max_amount": 0,
        }
    else:
        stats = stats_results[0]
    
    # Calculate derived stats
    total_trades = stats.get("total_trades", 0)
    won_trades = stats.get("won_trades", 0)
    total_profit = stats.get("total_profit", 0)
    avg_profit = total_profit / total_trades if total_trades > 0 else 0
    
    # Determine account level based on volume
    total_volume = stats.get("total_volume", 0)
    if total_volume >= 100000:
        account_level = "VIP Diamond"
        level_color = "#00E5FF"
    elif total_volume >= 50000:
        account_level = "VIP Gold"
        level_color = "#FFD700"
    elif total_volume >= 10000:
        account_level = "VIP Silver"
        level_color = "#C0C0C0"
    elif total_volume >= 1000:
        account_level = "Bronze"
        level_color = "#CD7F32"
    else:
        account_level = "Starter"
        level_color = "#00E55A"
    
    return {
        "user_id": user_id,
        "name": display_name,
        "country": user_doc.get("country", "Unknown"),
        "country_flag": user_doc.get("country_flag", "🌍"),
        "picture": user_doc.get("picture"),
        "account_level": account_level,
        "level_color": level_color,
        "trades_count": total_trades,
        "profitable_trades": won_trades,
        "trades_profit": round(total_profit, 2),
        "average_profit": round(avg_profit, 2),
        "min_trade_amount": round(stats.get("min_amount", 0), 2),
        "max_trade_amount": round(stats.get("max_amount", 0), 2),
    }

# ============= Profile Stats Routes =============

@api_router.get("/profile/stats")
async def get_profile_stats(authorization: Optional[str] = Header(None), request: Request = None):
    """Get user's trading statistics"""
    user = await get_current_user(authorization, request)
    
    # Get user's profile info
    user_doc = await db.users.find_one(
        {"user_id": user.user_id},
        {"account_id": 1, "nickname": 1, "full_name": 1, "name": 1, "country": 1, "country_flag": 1, "is_verified": 1}
    )
    
    # Ensure user has account_id (migration for existing users)
    if user_doc and not user_doc.get("account_id"):
        last_user = await db.users.find_one(
            {"account_id": {"$exists": True}},
            sort=[("account_id", -1)]
        )
        if last_user and last_user.get("account_id"):
            try:
                new_account_id = int(last_user["account_id"]) + 1
            except:
                new_account_id = 10000001
        else:
            new_account_id = 10000001
        
        await db.users.update_one(
            {"user_id": user.user_id},
            {"$set": {"account_id": str(new_account_id)}}
        )
        user_doc["account_id"] = str(new_account_id)
    
    # Get all-time stats
    all_time_pipeline = [
        {
            "$match": {
                "user_id": user.user_id,
                "status": {"$in": ["won", "lost"]},
            }
        },
        {
            "$group": {
                "_id": "$user_id",
                "total_trades": {"$sum": 1},
                "won_trades": {
                    "$sum": {"$cond": [{"$eq": ["$status", "won"]}, 1, 0]}
                },
                "total_volume": {"$sum": "$amount"},
                "net_pnl": {"$sum": "$profit_loss"},
            }
        }
    ]
    
    results = await db.trades.aggregate(all_time_pipeline).to_list(1)
    
    if not results:
        return {
            "total_trades": 0,
            "win_rate": 0,
            "volume": 0,
            "net_pnl": 0,
            "account_id": user_doc.get("account_id") if user_doc else None,
            "nickname": user_doc.get("nickname") if user_doc else None,
            "country": user_doc.get("country") if user_doc else None,
            "country_flag": user_doc.get("country_flag", "🌍") if user_doc else "🌍",
            "is_verified": user_doc.get("is_verified", False) if user_doc else False,
        }
    
    stats = results[0]
    win_rate = (stats["won_trades"] / stats["total_trades"] * 100) if stats["total_trades"] > 0 else 0
    
    return {
        "total_trades": stats["total_trades"],
        "win_rate": round(win_rate, 1),
        "volume": round(stats["total_volume"], 2),
        "net_pnl": round(stats["net_pnl"], 2),
        "account_id": user_doc.get("account_id") if user_doc else None,
        "nickname": user_doc.get("nickname") if user_doc else None,
        "country": user_doc.get("country") if user_doc else None,
        "country_flag": user_doc.get("country_flag", "🌍") if user_doc else "🌍",
        "is_verified": user_doc.get("is_verified", False) if user_doc else False,
    }

@api_router.put("/profile/nickname")
async def update_nickname(nickname: str, authorization: Optional[str] = Header(None), request: Request = None):
    """Update user's nickname for leaderboard"""
    user = await get_current_user(authorization, request)
    
    # Validate nickname
    if len(nickname) < 3 or len(nickname) > 20:
        raise HTTPException(status_code=400, detail="Nickname must be 3-20 characters")
    
    # Check if nickname is already taken
    existing = await db.users.find_one({"nickname": nickname, "user_id": {"$ne": user.user_id}})
    if existing:
        raise HTTPException(status_code=400, detail="Nickname already taken")
    
    await db.users.update_one(
        {"user_id": user.user_id},
        {"$set": {"nickname": nickname}}
    )
    
    return {"success": True, "nickname": nickname}

@api_router.put("/profile/country")
async def update_country(country: str, country_flag: str = "🌍", authorization: Optional[str] = Header(None), request: Request = None):
    """Update user's country for leaderboard"""
    user = await get_current_user(authorization, request)
    
    await db.users.update_one(
        {"user_id": user.user_id},
        {"$set": {"country": country, "country_flag": country_flag}}
    )
    
    return {"success": True, "country": country, "country_flag": country_flag}

@api_router.post("/profile/change-password")
async def change_password(password_data: dict, authorization: Optional[str] = Header(None), request: Request = None):
    """Change user password"""
    user = await get_current_user(authorization, request)
    
    current_password = password_data.get("current_password")
    new_password = password_data.get("new_password")
    
    if not current_password or not new_password:
        raise HTTPException(status_code=400, detail="Both current and new password required")
    
    if len(new_password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters")
    
    # Get user from database
    user_doc = await db.users.find_one({"user_id": user.user_id})
    if not user_doc:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Verify current password
    if not verify_password(current_password, user_doc.get("password", "")):
        raise HTTPException(status_code=400, detail="Current password is incorrect")
    
    # Update password
    hashed_new_password = hash_password(new_password)
    await db.users.update_one(
        {"user_id": user.user_id},
        {"$set": {
            "password": hashed_new_password,
            "password_changed_at": datetime.now(timezone.utc)
        }}
    )
    
    return {"success": True, "message": "Password changed successfully"}

@api_router.post("/profile/toggle-2fa")
async def toggle_2fa(data: dict, authorization: Optional[str] = Header(None), request: Request = None):
    """Enable or disable 2FA"""
    user = await get_current_user(authorization, request)
    
    enable = data.get("enable", False)
    
    await db.users.update_one(
        {"user_id": user.user_id},
        {"$set": {"is_2fa_enabled": enable}}
    )
    
    return {
        "success": True,
        "is_2fa_enabled": enable,
        "message": "2FA enabled successfully" if enable else "2FA disabled successfully"
    }

@api_router.post("/auth/send-verification")
async def send_verification_code(authorization: Optional[str] = Header(None), request: Request = None):
    """Send email verification code"""
    user = await get_current_user(authorization, request)
    
    # Check if already verified
    user_doc = await db.users.find_one({"user_id": user.user_id})
    if user_doc and user_doc.get("is_email_verified"):
        return {"success": True, "message": "Email already verified"}
    
    # Generate a 6-digit code
    verification_code = str(random.randint(100000, 999999))
    
    # Store the code with expiration
    await db.verification_codes.update_one(
        {"user_id": user.user_id, "type": "email"},
        {"$set": {
            "code": verification_code,
            "created_at": datetime.now(timezone.utc),
            "expires_at": datetime.now(timezone.utc) + timedelta(minutes=10),
            "used": False
        }},
        upsert=True
    )
    
    # In production, send email here
    # For demo, just return success
    return {
        "success": True,
        "message": "Verification code sent to your email",
        "dev_code": verification_code  # Remove in production
    }

@api_router.put("/profile/notification-settings")
async def update_notification_settings(data: dict, authorization: Optional[str] = Header(None), request: Request = None):
    """Update user notification settings"""
    user = await get_current_user(authorization, request)
    
    setting = data.get("setting")
    enabled = data.get("enabled", False)
    
    valid_settings = ["email", "tradeAlerts", "depositUpdates", "withdrawalUpdates", "securityAlerts"]
    if setting not in valid_settings:
        raise HTTPException(status_code=400, detail="Invalid setting")
    
    # Update the specific notification setting
    await db.users.update_one(
        {"user_id": user.user_id},
        {"$set": {f"notification_settings.{setting}": enabled}}
    )
    
    return {
        "success": True,
        "setting": setting,
        "enabled": enabled
    }

@api_router.get("/profile/notification-settings")
async def get_notification_settings(authorization: Optional[str] = Header(None), request: Request = None):
    """Get user notification settings"""
    user = await get_current_user(authorization, request)
    
    user_doc = await db.users.find_one(
        {"user_id": user.user_id},
        {"notification_settings": 1}
    )
    
    # Default settings if not set
    default_settings = {
        "email": True,
        "tradeAlerts": True,
        "depositUpdates": True,
        "withdrawalUpdates": True,
        "securityAlerts": True
    }
    
    settings = user_doc.get("notification_settings", default_settings) if user_doc else default_settings
    
    return {
        "success": True,
        "settings": settings
    }

@api_router.post("/profile/delete-request")
async def request_account_deletion(authorization: Optional[str] = Header(None), request: Request = None):
    """Request account deletion"""
    user = await get_current_user(authorization, request)
    
    # Check if there's already a pending deletion request
    existing_request = await db.deletion_requests.find_one({
        "user_id": user.user_id,
        "status": "pending"
    })
    
    if existing_request:
        raise HTTPException(status_code=400, detail="You already have a pending deletion request")
    
    # Create deletion request
    await db.deletion_requests.insert_one({
        "user_id": user.user_id,
        "email": user.email,
        "requested_at": datetime.now(timezone.utc),
        "status": "pending",
        "scheduled_deletion": datetime.now(timezone.utc) + timedelta(days=30)  # 30-day grace period
    })
    
    # Mark user as pending deletion
    await db.users.update_one(
        {"user_id": user.user_id},
        {"$set": {"deletion_requested": True, "deletion_requested_at": datetime.now(timezone.utc)}}
    )
    
    return {
        "success": True,
        "message": "Account deletion request submitted. Your account will be deleted in 30 days. You can cancel this request by contacting support."
    }

@api_router.post("/profile/photo")
async def upload_profile_photo(photo_data: dict, authorization: Optional[str] = Header(None), request: Request = None):
    """Upload profile photo"""
    user = await get_current_user(authorization, request)
    
    photo_base64 = photo_data.get("photo_base64")
    if not photo_base64:
        raise HTTPException(status_code=400, detail="No photo provided")
    
    # Store the photo as base64 (in production, upload to S3/storage)
    photo_url = f"data:image/jpeg;base64,{photo_base64}"
    
    await db.users.update_one(
        {"user_id": user.user_id},
        {"$set": {"picture": photo_url}}
    )
    
    return {"success": True, "picture": photo_url}

@api_router.post("/profile/chart-picture")
async def upload_chart_picture(photo_data: dict, authorization: Optional[str] = Header(None), request: Request = None):
    """Upload chart background picture"""
    user = await get_current_user(authorization, request)
    
    photo_base64 = photo_data.get("photo_base64")
    if not photo_base64:
        raise HTTPException(status_code=400, detail="No photo provided")
    
    # Store the photo as base64
    photo_url = f"data:image/jpeg;base64,{photo_base64}"
    
    await db.users.update_one(
        {"user_id": user.user_id},
        {"$set": {"chart_picture": photo_url}}
    )
    
    return {"success": True, "chart_picture": photo_url}

@api_router.delete("/profile/chart-picture")
async def delete_chart_picture(authorization: Optional[str] = Header(None), request: Request = None):
    """Delete chart background picture"""
    user = await get_current_user(authorization, request)
    
    await db.users.update_one(
        {"user_id": user.user_id},
        {"$set": {"chart_picture": None}}
    )
    
    return {"success": True}


# ============= KYC Document Verification Routes =============

class KYCDocumentSubmission(BaseModel):
    full_name: str
    nationality: str
    date_of_birth: Optional[str] = None
    id_type: str  # Passport, National ID Card, Driver's License
    id_number: str
    front_image_base64: str
    back_image_base64: Optional[str] = None

# ============= DIDIT KYC INTEGRATION =============

# Get Didit configuration from environment
DIDIT_API_KEY = os.environ.get('DIDIT_API_KEY', '')
DIDIT_WEBHOOK_SECRET = os.environ.get('DIDIT_WEBHOOK_SECRET', '')
DIDIT_VERIFICATION_URL = os.environ.get('DIDIT_VERIFICATION_URL', 'https://verify.didit.me/u/xHb89pbETh2txYbwaQyOcg')

# Workflow ID for Didit API - this is the UUID from Didit Console
DIDIT_WORKFLOW_ID = os.environ.get('DIDIT_WORKFLOW_ID', 'c476fcf6-96c4-4e1d-adc5-86f0690c8e72')

@api_router.get("/kyc/didit/start")
async def start_didit_kyc(authorization: Optional[str] = Header(None), request: Request = None):
    """Start Didit KYC verification - creates a session via Didit API and returns the verification URL"""
    user = await get_current_user(authorization, request)
    
    # Check if user already has verified KYC
    existing_kyc = await db.kyc_submissions.find_one({
        "user_id": user.user_id,
        "status": "verified"
    })
    
    if existing_kyc:
        return {
            "success": False,
            "message": "Your KYC is already verified",
            "status": "verified"
        }
    
    # Check if there's a pending verification with a valid session URL
    pending_kyc = await db.kyc_submissions.find_one({
        "user_id": user.user_id,
        "status": "pending",
        "provider": "didit",
        "didit_verification_url": {"$exists": True, "$ne": None}
    })
    
    if pending_kyc and pending_kyc.get("didit_verification_url"):
        return {
            "success": True,
            "verification_url": pending_kyc.get("didit_verification_url"),
            "session_id": pending_kyc.get("didit_session_id"),
            "message": "Continue your pending verification",
            "status": "pending"
        }
    
    # Create a new verification session via Didit API
    try:
        import httpx
        
        # Didit API endpoint for creating sessions
        didit_api_url = "https://verification.didit.me/v3/session/"
        
        # Get base URL for callback and redirect
        base_url = os.environ.get('EXPO_PUBLIC_BACKEND_URL', 'https://bynix-markets.preview.emergentagent.com')
        frontend_url = base_url  # Same domain for frontend
        
        # Prepare request data with callback for webhook and redirect URL
        session_data = {
            "workflow_id": DIDIT_WORKFLOW_ID,
            "vendor_data": user.user_id,
            "callback": f"{base_url}/api/kyc/didit/webhook",  # Webhook URL for status updates
        }
        
        print(f"[DIDIT] Creating session for user {user.user_id} with workflow {DIDIT_WORKFLOW_ID}")
        print(f"[DIDIT] Callback URL: {session_data['callback']}")
        
        # Make API request to Didit
        async with httpx.AsyncClient() as client:
            response = await client.post(
                didit_api_url,
                json=session_data,
                headers={
                    "x-api-key": DIDIT_API_KEY,
                    "Content-Type": "application/json",
                    "Accept": "application/json"
                },
                timeout=30.0
            )
            
            print(f"[DIDIT] Response status: {response.status_code}")
            print(f"[DIDIT] Response body: {response.text}")
            
            if response.status_code == 200 or response.status_code == 201:
                didit_response = response.json()
                
                # Get the verification URL from response
                verification_url = didit_response.get("url") or didit_response.get("verification_url") or didit_response.get("session_url")
                session_id = didit_response.get("session_id") or didit_response.get("id") or f"didit_{uuid.uuid4().hex[:16]}"
                
                if not verification_url:
                    # If no URL in response, construct it from session_id
                    verification_url = f"https://verify.didit.me/s/{session_id}" if session_id else None
                
                if verification_url:
                    # Delete any old pending KYC for this user
                    await db.kyc_submissions.delete_many({
                        "user_id": user.user_id,
                        "status": "pending",
                        "provider": "didit"
                    })
                    
                    # Create new KYC submission record with the session info
                    kyc_record = {
                        "user_id": user.user_id,
                        "provider": "didit",
                        "didit_session_id": session_id,
                        "didit_verification_url": verification_url,
                        "didit_response": didit_response,
                        "status": "pending",
                        "created_at": datetime.now(timezone.utc),
                        "updated_at": datetime.now(timezone.utc)
                    }
                    
                    await db.kyc_submissions.insert_one(kyc_record)
                    
                    return {
                        "success": True,
                        "verification_url": verification_url,
                        "session_id": session_id,
                        "message": "Please complete the verification on Didit",
                        "status": "pending"
                    }
                else:
                    print(f"[DIDIT] No verification URL in response: {didit_response}")
                    raise Exception("No verification URL received from Didit")
            else:
                error_msg = response.text
                print(f"[DIDIT] API error: {error_msg}")
                raise Exception(f"Didit API error: {response.status_code} - {error_msg}")
                
    except Exception as e:
        print(f"[DIDIT] Exception: {str(e)}")
        
        # Fallback to static URL if API fails
        print(f"[DIDIT] Falling back to static URL")
        
        session_id = f"didit_{uuid.uuid4().hex[:16]}"
        verification_url = f"{DIDIT_VERIFICATION_URL}?vendor_data={user.user_id}"
        
        # Delete any old pending KYC
        await db.kyc_submissions.delete_many({
            "user_id": user.user_id,
            "status": "pending",
            "provider": "didit"
        })
        
        kyc_record = {
            "user_id": user.user_id,
            "provider": "didit",
            "didit_session_id": session_id,
            "didit_verification_url": verification_url,
            "status": "pending",
            "error": str(e),
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
        
        await db.kyc_submissions.insert_one(kyc_record)
        
        return {
            "success": True,
            "verification_url": verification_url,
            "session_id": session_id,
            "message": "Please complete the verification on Didit",
            "status": "pending"
        }


@api_router.get("/kyc/didit/callback")
async def didit_callback(request: Request):
    """Handle redirect from Didit after KYC verification - redirects user back to profile"""
    from fastapi.responses import HTMLResponse, RedirectResponse
    
    # Get any query parameters Didit might send
    params = dict(request.query_params)
    print(f"[DIDIT CALLBACK] Received callback with params: {params}")
    
    # Create a nice redirect page that shows success and redirects to profile
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>KYC Verification Complete - Bynix</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                background: linear-gradient(135deg, #0a0a0a 0%, #1a1a2e 100%);
                min-height: 100vh;
                display: flex;
                justify-content: center;
                align-items: center;
                color: white;
            }
            .container {
                text-align: center;
                padding: 40px;
                max-width: 400px;
            }
            .success-icon {
                width: 100px;
                height: 100px;
                background: linear-gradient(135deg, #00E55A 0%, #00C853 100%);
                border-radius: 50%;
                display: flex;
                justify-content: center;
                align-items: center;
                margin: 0 auto 30px;
                animation: pulse 2s infinite;
            }
            .success-icon svg {
                width: 50px;
                height: 50px;
                fill: white;
            }
            @keyframes pulse {
                0%, 100% { transform: scale(1); box-shadow: 0 0 0 0 rgba(0, 229, 90, 0.4); }
                50% { transform: scale(1.05); box-shadow: 0 0 20px 10px rgba(0, 229, 90, 0.2); }
            }
            h1 {
                font-size: 28px;
                margin-bottom: 15px;
                background: linear-gradient(90deg, #00E55A, #00C853);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
            }
            p {
                color: #888;
                font-size: 16px;
                line-height: 1.6;
                margin-bottom: 30px;
            }
            .btn {
                display: inline-block;
                background: linear-gradient(135deg, #00E55A 0%, #00C853 100%);
                color: #0a0a0a;
                padding: 15px 40px;
                border-radius: 12px;
                text-decoration: none;
                font-weight: 600;
                font-size: 16px;
                transition: transform 0.2s, box-shadow 0.2s;
            }
            .btn:hover {
                transform: translateY(-2px);
                box-shadow: 0 10px 30px rgba(0, 229, 90, 0.3);
            }
            .redirect-text {
                color: #666;
                font-size: 14px;
                margin-top: 20px;
            }
            .loader {
                width: 20px;
                height: 20px;
                border: 2px solid #333;
                border-top-color: #00E55A;
                border-radius: 50%;
                animation: spin 1s linear infinite;
                display: inline-block;
                margin-right: 8px;
                vertical-align: middle;
            }
            @keyframes spin {
                to { transform: rotate(360deg); }
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="success-icon">
                <svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                    <path d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z"/>
                </svg>
            </div>
            <h1>Verification Complete!</h1>
            <p>Your identity has been successfully verified. You can now access all features including withdrawals.</p>
            <a href="/" class="btn">Return to Bynix</a>
            <p class="redirect-text">
                <span class="loader"></span>
                Redirecting automatically in <span id="countdown">5</span> seconds...
            </p>
        </div>
        <script>
            let count = 5;
            const countdown = document.getElementById('countdown');
            const timer = setInterval(() => {
                count--;
                countdown.textContent = count;
                if (count <= 0) {
                    clearInterval(timer);
                    window.location.href = '/';
                }
            }, 1000);
        </script>
    </body>
    </html>
    """
    
    return HTMLResponse(content=html_content, status_code=200)


@api_router.get("/kyc/didit/webhook")
async def didit_webhook_redirect(request: Request):
    """Handle GET redirect from Didit after KYC verification - shows success page"""
    from fastapi.responses import HTMLResponse
    
    # Get query parameters from Didit
    params = dict(request.query_params)
    session_id = params.get("verificationSessionId", "")
    status = params.get("status", "").lower()
    
    print(f"[DIDIT REDIRECT] Session: {session_id}, Status: {status}")
    
    # Determine message based on status
    if status in ["approved", "completed", "verified"]:
        title = "Verification Complete!"
        message = "Your identity has been successfully verified. You can now access all features including withdrawals."
        icon_color = "#00E55A"
        icon_svg = '<path d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z"/>'
    elif status in ["declined", "rejected", "failed"]:
        title = "Verification Failed"
        message = "Unfortunately, your verification was not successful. Please try again with valid documents."
        icon_color = "#F44336"
        icon_svg = '<path d="M19 6.41L17.59 5 12 10.59 6.41 5 5 6.41 10.59 12 5 17.59 6.41 19 12 13.41 17.59 19 19 17.59 13.41 12z"/>'
    else:
        title = "Verification Pending"
        message = "Your verification is being processed. Please check back later."
        icon_color = "#FF9800"
        icon_svg = '<path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z"/>'
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{title} - Bynix</title>
        <style>
            * {{ margin: 0; padding: 0; box-sizing: border-box; }}
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                background: linear-gradient(135deg, #0a0a0a 0%, #1a1a2e 100%);
                min-height: 100vh;
                display: flex;
                justify-content: center;
                align-items: center;
                color: white;
            }}
            .container {{
                text-align: center;
                padding: 40px 20px;
                max-width: 400px;
            }}
            .success-icon {{
                width: 100px;
                height: 100px;
                background: linear-gradient(135deg, {icon_color} 0%, {icon_color}dd 100%);
                border-radius: 50%;
                display: flex;
                justify-content: center;
                align-items: center;
                margin: 0 auto 30px;
                animation: pulse 2s infinite;
            }}
            .success-icon svg {{
                width: 50px;
                height: 50px;
                fill: white;
            }}
            @keyframes pulse {{
                0%, 100% {{ transform: scale(1); box-shadow: 0 0 0 0 rgba(0, 229, 90, 0.4); }}
                50% {{ transform: scale(1.05); box-shadow: 0 0 20px 10px rgba(0, 229, 90, 0.2); }}
            }}
            h1 {{
                font-size: 28px;
                margin-bottom: 15px;
                background: linear-gradient(90deg, {icon_color}, {icon_color}dd);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                background-clip: text;
            }}
            p {{
                color: #888;
                font-size: 16px;
                line-height: 1.6;
                margin-bottom: 30px;
            }}
            .btn {{
                display: inline-block;
                background: linear-gradient(135deg, #00E55A 0%, #00C853 100%);
                color: #0a0a0a;
                padding: 15px 40px;
                border-radius: 12px;
                text-decoration: none;
                font-weight: 600;
                font-size: 16px;
                transition: transform 0.2s, box-shadow 0.2s;
            }}
            .btn:hover {{
                transform: translateY(-2px);
                box-shadow: 0 10px 30px rgba(0, 229, 90, 0.3);
            }}
            .redirect-text {{
                color: #666;
                font-size: 14px;
                margin-top: 20px;
            }}
            .loader {{
                width: 20px;
                height: 20px;
                border: 2px solid #333;
                border-top-color: #00E55A;
                border-radius: 50%;
                animation: spin 1s linear infinite;
                display: inline-block;
                margin-right: 8px;
                vertical-align: middle;
            }}
            @keyframes spin {{
                to {{ transform: rotate(360deg); }}
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="success-icon">
                <svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                    {icon_svg}
                </svg>
            </div>
            <h1>{title}</h1>
            <p>{message}</p>
            <a href="/(tabs)/trade" class="btn">Start Trading</a>
            <p class="redirect-text">
                <span class="loader"></span>
                Redirecting to trading in <span id="countdown">5</span> seconds...
            </p>
        </div>
        <script>
            let count = 5;
            const countdown = document.getElementById('countdown');
            const timer = setInterval(() => {{
                count--;
                countdown.textContent = count;
                if (count <= 0) {{
                    clearInterval(timer);
                    window.location.href = '/(tabs)/trade';
                }}
            }}, 1000);
        </script>
    </body>
    </html>
    """
    
    return HTMLResponse(content=html_content, status_code=200)


@api_router.post("/kyc/didit/webhook")
async def didit_webhook(request: Request):
    """Handle Didit KYC verification webhook callbacks"""
    try:
        body = await request.json()
        
        # Log webhook for debugging
        print(f"Didit Webhook received: {body}")
        
        # Verify webhook signature if secret is configured
        if DIDIT_WEBHOOK_SECRET:
            signature = request.headers.get("x-signature") or request.headers.get("x-webhook-signature")
            # TODO: Implement signature verification
        
        # Extract data from webhook
        session_id = body.get("session_id") or body.get("id")
        status = body.get("status", "").lower()
        vendor_data = body.get("vendor_data", "")  # This is the user_id we sent
        decision = body.get("decision", {})
        
        # Map Didit status to our status
        kyc_status = "pending"
        if status in ["approved", "completed", "verified"]:
            kyc_status = "verified"
        elif status in ["declined", "rejected", "failed"]:
            kyc_status = "rejected"
            
            # Check for duplicate user - auto-approve if previous account is deleted
            if decision and isinstance(decision, dict):
                id_verifications = decision.get("id_verifications", [])
                for verification in (id_verifications or []):
                    warnings = verification.get("warnings", [])
                    for warning in (warnings or []):
                        if warning.get("risk") == "POSSIBLE_DUPLICATED_USER":
                            print(f"[DIDIT] Duplicate user detected for {vendor_data}")
                            
                            # Get the duplicated session info
                            additional_data = warning.get("additional_data", {})
                            duplicated_session_id = additional_data.get("duplicated_session_id")
                            
                            if duplicated_session_id:
                                # Find the previous KYC submission with this session
                                prev_kyc = await db.kyc_submissions.find_one({
                                    "didit_session_id": duplicated_session_id
                                })
                                
                                if prev_kyc:
                                    prev_user_id = prev_kyc.get("user_id")
                                    
                                    # Check if the previous user's account is deleted
                                    prev_user = await db.users.find_one({"user_id": prev_user_id})
                                    
                                    if prev_user and prev_user.get("is_deleted", False):
                                        print(f"[DIDIT] Previous account {prev_user_id} is deleted - AUTO-APPROVING new user {vendor_data}")
                                        kyc_status = "verified"
                                        
                                        # Send notification about auto-approval
                                        await create_notification(
                                            vendor_data,
                                            "KYC Auto-Approved! ✅",
                                            "Your identity has been verified. Your previous account was deleted, so we've approved your new verification.",
                                            "kyc"
                                        )
                                    else:
                                        print(f"[DIDIT] Previous account {prev_user_id} is NOT deleted - keeping as rejected")
                            
                            # Also check matches array for duplicate info
                            matches = verification.get("matches", [])
                            for match in (matches or []):
                                match_vendor_data = match.get("vendor_data")
                                match_session_id = match.get("session_id")
                                
                                if match_vendor_data:
                                    # Check if this user is deleted
                                    matched_user = await db.users.find_one({"user_id": match_vendor_data})
                                    
                                    if matched_user and matched_user.get("is_deleted", False):
                                        print(f"[DIDIT] Matched user {match_vendor_data} is deleted - AUTO-APPROVING new user {vendor_data}")
                                        kyc_status = "verified"
                                        break
        
        # Find and update the KYC record
        update_query = {}
        if vendor_data:
            update_query["user_id"] = vendor_data
        if session_id:
            update_query["$or"] = [
                {"didit_session_id": session_id},
                {"didit_session_id": {"$regex": session_id[:16]}}
            ]
        
        if not update_query:
            return {"success": False, "message": "Could not identify user"}
        
        # Update KYC submission
        result = await db.kyc_submissions.update_one(
            {"user_id": vendor_data, "provider": "didit"},
            {
                "$set": {
                    "status": kyc_status,
                    "didit_response": body,
                    "verified_at": datetime.now(timezone.utc) if kyc_status == "verified" else None,
                    "updated_at": datetime.now(timezone.utc)
                }
            }
        )
        
        # Also update user's KYC status
        if kyc_status == "verified":
            await db.users.update_one(
                {"user_id": vendor_data},
                {"$set": {"kyc_verified": True, "kyc_verified_at": datetime.now(timezone.utc)}}
            )
            
            # Send notification (if not already sent for auto-approval)
            existing_notification = await db.notifications.find_one({
                "user_id": vendor_data,
                "title": {"$regex": "KYC.*Approved"},
                "created_at": {"$gte": datetime.now(timezone.utc) - timedelta(minutes=1)}
            })
            
            if not existing_notification:
                await create_notification(
                    vendor_data,
                    "KYC Verified! ✅",
                    "Your identity has been successfully verified. You can now make withdrawals.",
                    "kyc"
                )
        elif kyc_status == "rejected":
            await create_notification(
                vendor_data,
                "KYC Rejected ❌",
                "Your identity verification was not successful. Please try again with valid documents.",
                "kyc"
            )
        
        print(f"[DIDIT] Final KYC status for {vendor_data}: {kyc_status}")
        return {"success": True, "status": kyc_status}
        
    except Exception as e:
        print(f"Didit webhook error: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}


# ============= DIDIT KYC ADMIN ENDPOINTS =============

@api_router.get("/admin/kyc/didit/review-sessions")
async def get_didit_review_sessions(
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """
    Fetch KYC sessions that are "In Review" from Didit API.
    These are sessions where duplicate documents were detected.
    """
    try:
        user = await get_current_user(authorization, request)
    except HTTPException:
        raise HTTPException(status_code=401, detail="Admin authentication required")
    
    if not DIDIT_API_KEY:
        return {"success": False, "error": "Didit API key not configured", "sessions": []}
    
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            # Fetch "In Review" sessions from Didit
            response = await client.get(
                "https://verification.didit.me/v3/sessions",
                params={
                    "status": "In Review",
                    "limit": 50
                },
                headers={
                    "x-api-key": DIDIT_API_KEY
                }
            )
            
            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])
                
                # Enrich with our database info
                enriched_sessions = []
                for session in results:
                    vendor_data = session.get("vendor_data", "")
                    
                    # Get user info from our database
                    user_info = await db.users.find_one({"user_id": vendor_data})
                    kyc_record = await db.kyc_submissions.find_one({
                        "didit_session_id": session.get("session_id")
                    })
                    
                    enriched_sessions.append({
                        "session_id": session.get("session_id"),
                        "session_number": session.get("session_number"),
                        "session_url": session.get("session_url"),
                        "status": session.get("status"),
                        "full_name": session.get("full_name"),
                        "document_type": session.get("document_type"),
                        "country": session.get("country"),
                        "portrait_image": session.get("portrait_image"),
                        "created_at": session.get("created_at"),
                        "features": session.get("features", []),
                        "vendor_data": vendor_data,
                        "user_email": user_info.get("email") if user_info else None,
                        "user_name": user_info.get("name") or user_info.get("full_name") if user_info else None,
                        "our_status": kyc_record.get("status") if kyc_record else "unknown"
                    })
                
                return {
                    "success": True,
                    "count": len(enriched_sessions),
                    "sessions": enriched_sessions
                }
            else:
                print(f"[DIDIT Admin] API error: {response.status_code} - {response.text}")
                return {
                    "success": False,
                    "error": f"Didit API error: {response.status_code}",
                    "sessions": []
                }
                
    except Exception as e:
        print(f"[DIDIT Admin] Error fetching review sessions: {e}")
        return {"success": False, "error": str(e), "sessions": []}


@api_router.get("/admin/kyc/didit/session/{session_id}")
async def get_didit_session_details(
    session_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get full details of a Didit KYC session"""
    try:
        user = await get_current_user(authorization, request)
    except HTTPException:
        raise HTTPException(status_code=401, detail="Admin authentication required")
    
    if not DIDIT_API_KEY:
        return {"success": False, "error": "Didit API key not configured"}
    
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                f"https://verification.didit.me/v3/session/{session_id}/decision/",
                headers={
                    "x-api-key": DIDIT_API_KEY
                }
            )
            
            if response.status_code == 200:
                data = response.json()
                return {"success": True, "session": data}
            else:
                return {"success": False, "error": f"Didit API error: {response.status_code}"}
                
    except Exception as e:
        return {"success": False, "error": str(e)}


@api_router.post("/admin/kyc/didit/approve/{session_id}")
async def approve_didit_kyc_session(
    session_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """
    Approve a KYC session from admin dashboard.
    This updates our database and notifies the user.
    Note: Didit console approval is separate - this is for our internal records.
    """
    try:
        admin = await get_current_user(authorization, request)
    except HTTPException:
        raise HTTPException(status_code=401, detail="Admin authentication required")
    
    # Find the KYC record by session_id
    kyc_record = await db.kyc_submissions.find_one({
        "didit_session_id": session_id
    })
    
    if not kyc_record:
        # Try to find by partial match
        kyc_record = await db.kyc_submissions.find_one({
            "didit_session_id": {"$regex": session_id[:16]}
        })
    
    if not kyc_record:
        raise HTTPException(status_code=404, detail="KYC session not found in our database")
    
    user_id = kyc_record.get("user_id")
    
    # Update KYC submission to verified
    await db.kyc_submissions.update_one(
        {"_id": kyc_record["_id"]},
        {
            "$set": {
                "status": "verified",
                "verified_at": datetime.now(timezone.utc),
                "admin_approved_by": admin.user_id,
                "admin_approved_at": datetime.now(timezone.utc),
                "admin_approval_note": "Manually approved by admin after review"
            }
        }
    )
    
    # Update user KYC status
    await db.users.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "kyc_status": "verified",
                "kyc_verified_at": datetime.now(timezone.utc)
            }
        }
    )
    
    # Notify user
    await create_notification(
        user_id,
        "KYC Verified! ✅",
        "Your identity has been verified by our team. You now have full access to all features.",
        "kyc"
    )
    
    return {
        "success": True,
        "message": "KYC approved successfully",
        "user_id": user_id
    }


@api_router.post("/admin/kyc/didit/reject/{session_id}")
async def reject_didit_kyc_session(
    session_id: str,
    reason: str = "Duplicate account detected",
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Reject a KYC session from admin dashboard"""
    try:
        admin = await get_current_user(authorization, request)
    except HTTPException:
        raise HTTPException(status_code=401, detail="Admin authentication required")
    
    # Find the KYC record
    kyc_record = await db.kyc_submissions.find_one({
        "didit_session_id": session_id
    })
    
    if not kyc_record:
        kyc_record = await db.kyc_submissions.find_one({
            "didit_session_id": {"$regex": session_id[:16]}
        })
    
    if not kyc_record:
        raise HTTPException(status_code=404, detail="KYC session not found")
    
    user_id = kyc_record.get("user_id")
    
    # Update KYC submission to rejected
    await db.kyc_submissions.update_one(
        {"_id": kyc_record["_id"]},
        {
            "$set": {
                "status": "rejected",
                "rejected_at": datetime.now(timezone.utc),
                "admin_rejected_by": admin.user_id,
                "rejection_reason": reason
            }
        }
    )
    
    # Update user KYC status
    await db.users.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "kyc_status": "rejected",
                "kyc_rejection_reason": reason
            }
        }
    )
    
    # Notify user
    await create_notification(
        user_id,
        "KYC Rejected ❌",
        f"Your identity verification was rejected. Reason: {reason}",
        "kyc"
    )
    
    return {
        "success": True,
        "message": "KYC rejected",
        "user_id": user_id
    }


@api_router.get("/kyc/didit/status")
async def get_didit_kyc_status(authorization: Optional[str] = Header(None), request: Request = None):
    """Check user's Didit KYC verification status - also checks Didit API for updates"""
    user = await get_current_user(authorization, request)
    
    # Check for existing KYC submission
    kyc = await db.kyc_submissions.find_one({
        "user_id": user.user_id,
        "provider": "didit"
    })
    
    if kyc and kyc.get("status") == "pending" and kyc.get("didit_session_id"):
        # Check Didit API for updated status
        try:
            import httpx
            session_id = kyc.get("didit_session_id")
            
            # Call Didit API to get session status
            didit_status_url = f"https://verification.didit.me/v3/session/{session_id}/"
            
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    didit_status_url,
                    headers={
                        "x-api-key": DIDIT_API_KEY,
                        "Accept": "application/json"
                    },
                    timeout=10.0
                )
                
                print(f"[DIDIT STATUS CHECK] Session: {session_id}, Response: {response.status_code}")
                
                if response.status_code == 200:
                    didit_data = response.json()
                    didit_status = didit_data.get("status", "").lower()
                    print(f"[DIDIT STATUS CHECK] Didit status: {didit_status}")
                    
                    # Map Didit status to our status
                    new_status = kyc.get("status")
                    if didit_status in ["approved", "completed", "verified"]:
                        new_status = "verified"
                    elif didit_status in ["declined", "rejected", "failed"]:
                        new_status = "rejected"
                    elif didit_status in ["expired"]:
                        new_status = "expired"
                    
                    # Update our database if status changed
                    if new_status != kyc.get("status"):
                        await db.kyc_submissions.update_one(
                            {"_id": kyc["_id"]},
                            {
                                "$set": {
                                    "status": new_status,
                                    "didit_api_response": didit_data,
                                    "verified_at": datetime.now(timezone.utc) if new_status == "verified" else None,
                                    "updated_at": datetime.now(timezone.utc)
                                }
                            }
                        )
                        
                        # Update user's kyc_verified flag if verified
                        if new_status == "verified":
                            await db.users.update_one(
                                {"user_id": user.user_id},
                                {"$set": {"kyc_verified": True, "kyc_verified_at": datetime.now(timezone.utc)}}
                            )
                            
                            # Send notification
                            await create_notification(
                                user.user_id,
                                "KYC Verified!",
                                "Congratulations! Your identity has been verified. You can now make withdrawals.",
                                "kyc"
                            )
                        
                        kyc["status"] = new_status
                        
        except Exception as e:
            print(f"[DIDIT STATUS CHECK] Error: {str(e)}")
            # Continue with cached status if API fails
    
    if not kyc:
        # Check legacy KYC (non-Didit)
        legacy_kyc = await db.kyc_submissions.find_one({
            "user_id": user.user_id,
            "status": "verified"
        })
        if legacy_kyc:
            return {
                "status": "verified",
                "provider": "legacy",
                "verified_at": str(legacy_kyc.get("verified_at", ""))
            }
        return {"status": "not_started", "provider": None}
    
    return {
        "status": kyc.get("status", "pending"),
        "provider": "didit",
        "session_id": kyc.get("didit_session_id"),
        "created_at": str(kyc.get("created_at", "")),
        "verified_at": str(kyc.get("verified_at", "")) if kyc.get("verified_at") else None
    }


@api_router.post("/kyc/submit")
async def submit_kyc_documents(submission: KYCDocumentSubmission, authorization: Optional[str] = Header(None), request: Request = None):
    """Submit KYC documents for AI verification"""
    user = await get_current_user(authorization, request)
    
    try:
        # Check if ID number already used by another account
        existing_kyc = await db.kyc_submissions.find_one({
            "id_number": submission.id_number,
            "user_id": {"$ne": user.user_id},
            "status": "verified"
        })
        
        if existing_kyc:
            return {
                "success": False,
                "status": "rejected",
                "ai_result": {
                    "is_valid_document": False,
                    "reason": "This ID number is already registered with another account"
                },
                "message": "This ID number is already registered with another account. Each document can only be used once."
            }
        
        # Get the OpenAI API key (use OPENAI_API_KEY for AWS deployment)
        openai_key = os.environ.get('OPENAI_API_KEY') or os.environ.get('EMERGENT_LLM_KEY')
        if not openai_key:
            raise HTTPException(status_code=500, detail="AI verification service not configured")
        
        # Initialize OpenAI client for document verification
        openai_client = OpenAI(api_key=openai_key)
        
        system_message = """You are a professional KYC document verification AI. Your job is to analyze identity documents and verify:
1. If the document is a valid government-issued ID (Passport, National ID Card, or Driver's License)
2. Which country issued the document
3. If the document appears authentic (not obviously fake, edited, or a screenshot)
4. If the NAME on the document matches or is similar to the provided name
5. If any ID NUMBER is visible on the document

You must respond ONLY in this exact JSON format:
{
    "is_valid_document": true/false,
    "document_type": "Passport" | "National ID Card" | "Driver's License" | "Unknown",
    "country": "Country Name" | "Unknown",
    "country_code": "XX" | "Unknown",
    "confidence": "high" | "medium" | "low",
    "reason": "Brief explanation",
    "name_on_document": "Name visible on document or 'Not visible'",
    "name_matches": true/false,
    "id_number_visible": true/false,
    "detected_id_number": "ID number if visible or 'Not visible'"
}

IMPORTANT: 
- If the name on document doesn't match the provided name, set is_valid_document to false
- If you cannot read the document clearly, set is_valid_document to false
- Be strict about authenticity"""
        
        # Create the verification prompt with personal info to match
        prompt = f"""Please analyze this identity document and verify the information:

INFORMATION PROVIDED BY USER:
- Full Name: {submission.full_name}
- Nationality: {submission.nationality}
- Document Type: {submission.id_type}
- ID Number: {submission.id_number}

YOUR TASK:
1. Verify this is a real {submission.id_type} from {submission.nationality}
2. Check if the NAME on the document matches "{submission.full_name}"
3. Check if any ID number is visible on the document
4. Verify the document appears authentic (not edited, screenshot, or fake)

Analyze the image carefully and respond with the JSON verification result."""

        # Prepare image for OpenAI Vision API
        image_data = submission.front_image_base64
        if image_data.startswith('data:'):
            # Extract base64 part from data URL
            image_data = image_data.split(',')[1] if ',' in image_data else image_data
        
        # Send message with image using OpenAI Vision API
        try:
            response = openai_client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": system_message},
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": prompt},
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/png;base64,{image_data}"
                                }
                            }
                        ]
                    }
                ],
                max_tokens=1000
            )
            ai_response_text = response.choices[0].message.content
        except Exception as ai_error:
            # If AI service fails, provide a fallback response for testing
            logging.error(f"AI service error: {str(ai_error)}")
            
            # For now, provide a mock response to allow testing of the flow
            # In production, this should be handled differently
            ai_result = {
                "is_valid_document": False,
                "document_type": "Unknown",
                "country": "Unknown",
                "confidence": "low",
                "reason": f"AI service temporarily unavailable: {str(ai_error)[:100]}",
                "name_on_document": "Not visible",
                "name_matches": False,
                "id_number_visible": False,
                "detected_id_number": "Not visible"
            }
            
            # Store KYC submission with error status
            kyc_record = {
                "kyc_id": f"kyc_{uuid.uuid4().hex[:12]}",
                "user_id": user.user_id,
                "full_name": submission.full_name,
                "nationality": submission.nationality,
                "date_of_birth": submission.date_of_birth,
                "id_type": submission.id_type,
                "id_number": submission.id_number,
                "ai_verification": ai_result,
                "status": "rejected",
                "submitted_at": datetime.now(timezone.utc),
                "rejected_at": datetime.now(timezone.utc),
                "rejection_reason": ai_result["reason"]
            }
            
            await db.kyc_submissions.insert_one(kyc_record)
            
            return {
                "success": False,
                "kyc_id": kyc_record["kyc_id"],
                "status": "rejected",
                "ai_result": ai_result,
                "message": f"AI verification service error: {ai_result['reason']}"
            }
        
        # Parse AI response
        import json
        try:
            # Extract JSON from response - use ai_response_text from OpenAI
            response_text = ai_response_text.strip()
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0].strip()
            
            ai_result = json.loads(response_text)
        except json.JSONDecodeError:
            ai_result = {
                "is_valid_document": False,
                "document_type": "Unknown",
                "country": "Unknown",
                "confidence": "low",
                "reason": "Could not analyze the document properly. Please upload a clearer image."
            }
        
        # Check if name matches (additional validation)
        name_matches = ai_result.get("name_matches", True)
        if not name_matches:
            ai_result["is_valid_document"] = False
            ai_result["reason"] = f"Name on document does not match the provided name '{submission.full_name}'"
        
        # Store KYC submission in database (always store for records)
        kyc_record = {
            "kyc_id": f"kyc_{uuid.uuid4().hex[:12]}",
            "user_id": user.user_id,
            "full_name": submission.full_name,
            "nationality": submission.nationality,
            "date_of_birth": submission.date_of_birth,
            "id_type": submission.id_type,
            "id_number": submission.id_number,
            "ai_verification": ai_result,
            "status": "pending",
            "submitted_at": datetime.now(timezone.utc),
            "verified_at": None,
        }
        
        # Determine verification status
        is_verified = ai_result.get("is_valid_document") and ai_result.get("confidence") in ["high", "medium"]
        
        if is_verified:
            # VERIFIED - AI confirmed the document and info matches
            kyc_record["status"] = "verified"
            kyc_record["verified_at"] = datetime.now(timezone.utc)
            
            # Update user's KYC status and store personal info
            await db.users.update_one(
                {"user_id": user.user_id},
                {"$set": {
                    "kyc_status": "verified",
                    "kyc_verified_at": datetime.now(timezone.utc),
                    "is_kyc_verified": True,
                    "kyc_full_name": submission.full_name,
                    "kyc_nationality": submission.nationality,
                    "kyc_id_type": submission.id_type,
                    "kyc_id_number": submission.id_number,
                    "kyc_date_of_birth": submission.date_of_birth
                }}
            )
            
            # Create success notification
            await create_notification(
                user.user_id,
                "KYC Verified! ✅",
                f"Your {submission.id_type} from {ai_result.get('country', submission.nationality)} has been verified. You now have full access!",
                "system"
            )
            
            await db.kyc_submissions.insert_one(kyc_record)
            
            return {
                "success": True,
                "kyc_id": kyc_record["kyc_id"],
                "status": "verified",
                "ai_result": ai_result,
                "message": "Identity verified successfully!"
            }
        else:
            # REJECTED - AI could not verify
            kyc_record["status"] = "rejected"
            kyc_record["rejected_at"] = datetime.now(timezone.utc)
            kyc_record["rejection_reason"] = ai_result.get("reason", "Document could not be verified")
            
            # Update user's KYC status
            await db.users.update_one(
                {"user_id": user.user_id},
                {"$set": {
                    "kyc_status": "rejected",
                    "is_kyc_verified": False
                }}
            )
            
            # Create rejection notification
            await create_notification(
                user.user_id,
                "KYC Verification Failed ❌",
                f"Reason: {ai_result.get('reason', 'Invalid document')}. Please try again with a valid document.",
                "system"
            )
            
            await db.kyc_submissions.insert_one(kyc_record)
            
            return {
                "success": False,
                "kyc_id": kyc_record["kyc_id"],
                "status": "rejected",
                "ai_result": ai_result,
                "message": f"Verification failed: {ai_result.get('reason', 'Invalid document')}"
            }
        
    except Exception as e:
        logging.error(f"KYC submission error: {str(e)}")
        return {
            "success": False,
            "status": "error",
            "ai_result": None,
            "message": f"Error processing documents: {str(e)}"
        }

@api_router.get("/kyc/status")
async def get_kyc_status(authorization: Optional[str] = Header(None), request: Request = None):
    """Get user's KYC status - also checks Didit API for pending verifications"""
    user = await get_current_user(authorization, request)
    
    # Get latest KYC submission
    kyc_record = await db.kyc_submissions.find_one(
        {"user_id": user.user_id},
        sort=[("created_at", -1)]
    )
    
    if not kyc_record:
        return {
            "status": "not_submitted",
            "is_verified": False
        }
    
    # If it's a Didit KYC and pending, check Didit API for status update
    if kyc_record.get("provider") == "didit" and kyc_record.get("status") == "pending":
        session_id = kyc_record.get("didit_session_id")
        if session_id and not session_id.startswith("didit_"):  # Only real Didit session IDs
            try:
                import httpx
                didit_status_url = f"https://verification.didit.me/v3/session/{session_id}/"
                
                async with httpx.AsyncClient() as client:
                    response = await client.get(
                        didit_status_url,
                        headers={
                            "x-api-key": DIDIT_API_KEY,
                            "Accept": "application/json"
                        },
                        timeout=10.0
                    )
                    
                    print(f"[KYC STATUS] Checking Didit session {session_id[:20]}..., Response: {response.status_code}")
                    
                    if response.status_code == 200:
                        didit_data = response.json()
                        didit_status = didit_data.get("status", "").lower()
                        print(f"[KYC STATUS] Didit status: {didit_status}")
                        
                        # Map Didit status to our status
                        new_status = kyc_record.get("status")
                        if didit_status in ["approved", "completed", "verified"]:
                            new_status = "verified"
                        elif didit_status in ["declined", "rejected", "failed"]:
                            new_status = "rejected"
                        elif didit_status in ["expired"]:
                            new_status = "expired"
                        
                        # Update database if status changed
                        if new_status != kyc_record.get("status"):
                            print(f"[KYC STATUS] Updating status from {kyc_record.get('status')} to {new_status}")
                            
                            await db.kyc_submissions.update_one(
                                {"_id": kyc_record["_id"]},
                                {
                                    "$set": {
                                        "status": new_status,
                                        "didit_api_response": didit_data,
                                        "verified_at": datetime.now(timezone.utc) if new_status == "verified" else None,
                                        "updated_at": datetime.now(timezone.utc)
                                    }
                                }
                            )
                            
                            # Update user's kyc_verified flag
                            if new_status == "verified":
                                await db.users.update_one(
                                    {"user_id": user.user_id},
                                    {"$set": {"kyc_verified": True, "kyc_verified_at": datetime.now(timezone.utc)}}
                                )
                                
                                # Send notification
                                await create_notification(
                                    user.user_id,
                                    "KYC Verified!",
                                    "Congratulations! Your identity has been verified. You can now make withdrawals.",
                                    "kyc"
                                )
                            
                            kyc_record["status"] = new_status
                            
            except Exception as e:
                print(f"[KYC STATUS] Error checking Didit: {str(e)}")
                # Continue with cached status
    
    # Calculate remaining time for auto-approval (legacy)
    remaining_seconds = None
    if kyc_record.get("status") == "auto_approved" and kyc_record.get("auto_approve_at"):
        now = datetime.now(timezone.utc)
        auto_approve_at = kyc_record["auto_approve_at"]
        if isinstance(auto_approve_at, datetime):
            remaining = (auto_approve_at - now).total_seconds()
            remaining_seconds = max(0, int(remaining))
    
    return {
        "kyc_id": kyc_record.get("kyc_id"),
        "status": kyc_record.get("status"),
        "is_verified": kyc_record.get("status") == "verified",
        "submitted_at": kyc_record.get("submitted_at") or kyc_record.get("created_at"),
        "verified_at": kyc_record.get("verified_at"),
        "ai_result": kyc_record.get("ai_verification"),
        "remaining_seconds": remaining_seconds,
        "provider": kyc_record.get("provider", "legacy")
    }

# ============= Old Admin Routes (Legacy - Kept for compatibility) =============
# Note: New admin routes are at the end of this file

# ============= WebSocket Events =============

@sio.event
async def connect(sid, environ):
    print(f"Client connected: {sid}")

@sio.event
async def subscribe_market(sid, data):
    """Subscribe to market price updates"""
    asset = data.get("asset", "BTC/USD")
    print(f"Client {sid} subscribed to {asset}")
    # In production, start sending real-time price updates

# ============= Binance Proxy Routes (for CORS bypass) =============

@api_router.get("/binance/klines")
async def binance_klines_proxy(symbol: str, interval: str = "1m", limit: int = 50):
    """Proxy Binance klines API to bypass CORS"""
    try:
        url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=10.0)
            if response.status_code == 200:
                return response.json()
            else:
                raise HTTPException(status_code=response.status_code, detail="Binance API error")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch Binance data: {str(e)}")

# WebSocket proxy for Binance streams
@sio.event
async def subscribe_binance(sid, data):
    """Subscribe to Binance WebSocket and relay to client"""
    symbol = data.get('symbol', 'BTCUSDT').lower()
    interval = data.get('interval', '1m')
    
    print(f"Client {sid} subscribing to Binance {symbol}@kline_{interval}")
    
    # In production, establish WebSocket connection to Binance and relay data
    # For now, send mock updates every 2 seconds
    import asyncio
    
    async def send_binance_updates():
        try:
            # Fetch initial price from Binance
            url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol.upper()}"
            async with httpx.AsyncClient() as client:
                response = await client.get(url, timeout=5.0)
                if response.status_code == 200:
                    price_data = response.json()
                    base_price = float(price_data['price'])
                else:
                    base_price = 50000.0  # Fallback
            
            # Send updates every 2 seconds
            for _ in range(30):  # Send 30 updates (1 minute)
                await asyncio.sleep(2)
                
                # Generate mock candle update
                import random
                change = (random.random() - 0.5) * (base_price * 0.001)
                new_price = base_price + change
                
                candle = {
                    'time': int(asyncio.get_event_loop().time() * 1000),
                    'open': base_price,
                    'high': max(base_price, new_price) + random.random() * base_price * 0.0005,
                    'low': min(base_price, new_price) - random.random() * base_price * 0.0005,
                    'close': new_price,
                    'volume': random.uniform(100, 1000)
                }
                
                await sio.emit('binance_update', candle, room=sid)
                base_price = new_price
        except Exception as e:
            print(f"Error sending Binance updates: {e}")
    
    # Start sending updates in background
    asyncio.create_task(send_binance_updates())

# ============= OTC Market Data Generator =============

# Store active market subscriptions
active_subscriptions = {}

# OTC Market base prices
OTC_BASE_PRICES = {
    'EUR/USD OTC': 1.0850,
    'GBP/USD OTC': 1.2650,
    'USD/JPY OTC': 149.50,
    'AUD/USD OTC': 0.6550,
    'USD/CHF OTC': 0.8750,
    'EUR/GBP OTC': 0.8550,
    'NZD/USD OTC': 0.6150,
    'USD/CAD OTC': 1.3550,
    'EUR/JPY OTC': 162.50,
    'GBP/JPY OTC': 189.50,
}

# Store current prices for each market
current_market_prices = {asset: price for asset, price in OTC_BASE_PRICES.items()}

def generate_historical_candles(asset: str, count: int = 15000, interval_seconds: int = 60):
    """Generate fake historical OHLC data for OTC markets - 10+ days of data"""
    base_price = OTC_BASE_PRICES.get(asset, 1.0)
    candles = []
    current_time = int(datetime.now(timezone.utc).timestamp()) - (count * interval_seconds)
    price = base_price
    
    for i in range(count):
        # Random walk with trend
        volatility = base_price * 0.0003  # 0.03% volatility per candle
        change = (random.random() - 0.5) * volatility * 2
        
        open_price = price
        close_price = price + change
        high_price = max(open_price, close_price) + random.random() * volatility
        low_price = min(open_price, close_price) - random.random() * volatility
        volume = random.uniform(100, 1000)
        
        candles.append({
            'time': current_time + (i * interval_seconds),
            'open': round(open_price, 5),
            'high': round(high_price, 5),
            'low': round(low_price, 5),
            'close': round(close_price, 5),
            'volume': round(volume, 2)
        })
        
        price = close_price
    
    # Update current price
    current_market_prices[asset] = price
    return candles

@api_router.get("/otc/history")
async def get_otc_history(asset: str = "EUR/USD OTC", count: int = 15000, interval: str = "1m"):
    """Get historical candle data for OTC markets - supports 10+ days of data"""
    interval_map = {
        '15s': 15,
        '1m': 60,
        '5m': 300,
        '15m': 900,
        '30m': 1800,
        '1h': 3600,
        '4h': 14400,
        '1d': 86400
    }
    interval_seconds = interval_map.get(interval, 60)
    candles = generate_historical_candles(asset, count, interval_seconds)
    return {"asset": asset, "interval": interval, "candles": candles}

@api_router.get("/otc/price")
async def get_otc_price(asset: str = "EUR/USD OTC"):
    """Get current price for OTC market"""
    price = current_market_prices.get(asset, OTC_BASE_PRICES.get(asset, 1.0))
    return {"asset": asset, "price": round(price, 5)}

@sio.event
async def subscribe_otc(sid, data):
    """Subscribe to OTC market real-time updates"""
    import asyncio
    
    asset = data.get('asset', 'EUR/USD OTC')
    print(f"Client {sid} subscribing to OTC market: {asset}")
    
    # Cancel any existing subscription for this client
    if sid in active_subscriptions:
        active_subscriptions[sid]['active'] = False
    
    subscription = {'active': True, 'asset': asset}
    active_subscriptions[sid] = subscription
    
    async def send_otc_updates():
        try:
            price = current_market_prices.get(asset, OTC_BASE_PRICES.get(asset, 1.0))
            last_candle_time = int(datetime.now(timezone.utc).timestamp())
            
            while subscription['active']:
                await asyncio.sleep(0.5)  # Update every 500ms for smooth movement
                
                if not subscription['active']:
                    break
                
                # Generate price movement
                volatility = price * 0.0001  # 0.01% per tick
                change = (random.random() - 0.5) * volatility * 2
                price += change
                current_market_prices[asset] = price
                
                current_time = int(datetime.now(timezone.utc).timestamp())
                
                # Emit tick update
                tick_data = {
                    'asset': asset,
                    'price': round(price, 5),
                    'time': current_time,
                    'change': round(change, 7)
                }
                await sio.emit('otc_tick', tick_data, room=sid)
                
                # Every 60 seconds, emit a new candle
                if current_time - last_candle_time >= 60:
                    candle = {
                        'time': current_time,
                        'open': round(price - change, 5),
                        'high': round(price + abs(change), 5),
                        'low': round(price - abs(change), 5),
                        'close': round(price, 5),
                        'volume': round(random.uniform(100, 500), 2)
                    }
                    await sio.emit('otc_candle', candle, room=sid)
                    last_candle_time = current_time
                    
        except Exception as e:
            print(f"Error in OTC updates for {sid}: {e}")
        finally:
            if sid in active_subscriptions:
                del active_subscriptions[sid]
    
    asyncio.create_task(send_otc_updates())

@sio.event
async def unsubscribe_otc(sid, data=None):
    """Unsubscribe from OTC market updates"""
    if sid in active_subscriptions:
        active_subscriptions[sid]['active'] = False
        print(f"Client {sid} unsubscribed from OTC market")

@sio.event
async def disconnect(sid):
    """Handle client disconnect"""
    if sid in active_subscriptions:
        active_subscriptions[sid]['active'] = False
    print(f"Client disconnected: {sid}")

# ============= NOWPayments Integration =============

NOWPAYMENTS_API_KEY = os.environ.get("NOWPAYMENTS_API_KEY", "")
NOWPAYMENTS_API_URL = "https://api.nowpayments.io/v1"

class NOWPaymentsService:
    """Service for interacting with NOWPayments API"""
    
    def __init__(self):
        self.api_url = NOWPAYMENTS_API_URL
        self.api_key = NOWPAYMENTS_API_KEY
        self.timeout = 30
    
    def _get_headers(self):
        return {
            "x-api-key": self.api_key,
            "Content-Type": "application/json"
        }
    
    async def check_api_status(self):
        """Check if NOWPayments API is operational"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.api_url}/status",
                    headers=self._get_headers(),
                    timeout=self.timeout
                )
            return response.status_code == 200
        except Exception as e:
            print(f"API status check failed: {e}")
            return False
    
    async def get_minimum_amount(self, currency_from: str, currency_to: str):
        """Get minimum payment amount for a currency pair"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.api_url}/min-amount",
                    params={
                        "currency_from": currency_from.lower(),
                        "currency_to": currency_to.lower()
                    },
                    headers=self._get_headers(),
                    timeout=self.timeout
                )
            if response.status_code == 200:
                return response.json()
            return {"min_amount": 10}
        except Exception as e:
            print(f"Failed to get minimum amount: {e}")
            return {"min_amount": 10}
    
    async def create_payment(
        self,
        price_amount: float,
        price_currency: str,
        pay_currency: str,
        order_id: str = None,
        order_description: str = None,
        ipn_callback_url: str = None
    ):
        """Create a new payment - generates deposit address"""
        payload = {
            "price_amount": price_amount,
            "price_currency": price_currency.lower(),
            "pay_currency": pay_currency.lower(),
            "is_fee_paid_by_user": False,  # Merchant pays fees - user pays exact amount
            "is_fixed_rate": True,  # Lock the exchange rate
        }
        
        if order_id:
            payload["order_id"] = order_id
        if order_description:
            payload["order_description"] = order_description
        if ipn_callback_url:
            payload["ipn_callback_url"] = ipn_callback_url
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.api_url}/payment",
                    json=payload,
                    headers=self._get_headers(),
                    timeout=self.timeout
                )
            
            if response.status_code == 200 or response.status_code == 201:
                data = response.json()
                print(f"Payment created: {data}")
                return {
                    "success": True,
                    "payment_id": data.get("payment_id"),
                    "payment_status": data.get("payment_status"),
                    "pay_address": data.get("pay_address"),
                    "pay_amount": data.get("pay_amount"),
                    "pay_currency": data.get("pay_currency"),
                    "price_amount": data.get("price_amount"),
                    "price_currency": data.get("price_currency"),
                    "expiration_estimate_date": data.get("expiration_estimate_date"),
                    "network": data.get("network", "TRC20")
                }
            else:
                print(f"Payment creation failed: {response.text}")
                return {
                    "success": False,
                    "error": response.json().get("message", "Payment creation failed")
                }
        except Exception as e:
            print(f"Failed to create payment: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def get_payment_status(self, payment_id: int):
        """Get the current status of a payment"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.api_url}/payment/{payment_id}",
                    headers=self._get_headers(),
                    timeout=self.timeout
                )
            
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Failed to get payment status: {e}")
            return None
    
    async def get_available_currencies(self):
        """Get list of available currencies"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.api_url}/currencies",
                    headers=self._get_headers(),
                    timeout=self.timeout
                )
            if response.status_code == 200:
                return response.json()
            return {"currencies": ["usdttrc20"]}
        except Exception as e:
            print(f"Failed to get currencies: {e}")
            return {"currencies": ["usdttrc20"]}

# Initialize NOWPayments service
nowpayments_service = NOWPaymentsService()

# Deposit models
class CreateDepositRequest(BaseModel):
    amount: float = Field(..., gt=0, description="Amount in USD to deposit")
    network: str = Field(default="TRC20", description="Crypto network")
    promo_code: Optional[str] = Field(default=None, description="Promo code for bonus")

class DepositResponse(BaseModel):
    success: bool
    payment_id: Optional[int] = None
    pay_address: Optional[str] = None
    pay_amount: Optional[float] = None
    pay_currency: Optional[str] = None
    network: Optional[str] = None
    expiration_estimate_date: Optional[str] = None
    bonus_percentage: Optional[int] = None
    bonus_amount: Optional[float] = None
    total_credit: Optional[float] = None
    is_first_deposit: Optional[bool] = None
    error: Optional[str] = None

# Supported crypto networks
CRYPTO_NETWORKS = {
    "TRC20": {"currency": "usdttrc20", "name": "USDT (TRC20)", "fee": "No fee"},
    "ERC20": {"currency": "usdterc20", "name": "USDT (ERC20)", "fee": "No fee"},
    "BEP20": {"currency": "usdtbsc", "name": "USDT (BEP20/BSC)", "fee": "No fee"},
    "SOL": {"currency": "usdtsol", "name": "USDT (Solana)", "fee": "No fee"},
    "MATIC": {"currency": "usdtmatic", "name": "USDT (Polygon)", "fee": "No fee"},
}

# First time deposit bonus - DISABLED (only promo codes give bonus now)
# FIRST_DEPOSIT_BONUS_PERCENTAGE = 200

# ============= Deposit Endpoints =============

@api_router.get("/deposit/status")
async def check_nowpayments_status():
    """Check NOWPayments API status"""
    is_online = await nowpayments_service.check_api_status()
    return {"status": "online" if is_online else "offline"}

@api_router.get("/deposit/min-amount")
async def get_deposit_min_amount():
    """Get minimum deposit amount"""
    result = await nowpayments_service.get_minimum_amount("usd", "usdttrc20")
    # Add a small buffer to ensure we're above minimum
    min_amount = result.get("min_amount", 20)
    return {"min_amount": round(min_amount + 1, 2), "currency": "USD"}

@api_router.get("/deposit/networks")
async def get_available_networks():
    """Get available crypto networks for deposit"""
    networks = []
    for key, value in CRYPTO_NETWORKS.items():
        networks.append({
            "id": key,
            "name": value["name"],
            "fee": value["fee"]
        })
    return {"networks": networks}

@api_router.post("/deposit/create", response_model=DepositResponse)
async def create_deposit(
    request: CreateDepositRequest,
    authorization: Optional[str] = Header(None),
    req: Request = None
):
    """Create a deposit request - generates USDT address with bonus calculation"""
    # Get current user
    try:
        user = await get_current_user(authorization, req)
    except HTTPException:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    # Minimum amount check
    if request.amount < 10:
        raise HTTPException(status_code=400, detail="Minimum deposit amount is $10")
    
    # Get network configuration
    network_config = CRYPTO_NETWORKS.get(request.network, CRYPTO_NETWORKS["TRC20"])
    
    # Check if first deposit
    existing_deposits = await db.deposits.count_documents({
        "user_id": user.user_id,
        "status": {"$in": ["completed", "confirmed"]}
    })
    is_first_deposit = existing_deposits == 0
    
    # Calculate bonus - ONLY from promo codes now (from database)
    bonus_percentage = 0
    bonus_amount = 0
    promo_error = None
    
    # Check promo code from database
    if request.promo_code:
        promo_upper = request.promo_code.upper().strip()
        
        # Look up promo code in database instead of hardcoded dict
        promo = await db.promo_codes.find_one({"code": promo_upper, "is_active": True})
        
        if promo:
            # Check expiry
            if promo.get("expires_at"):
                expires_at = promo["expires_at"]
                if expires_at.tzinfo is None:
                    expires_at = expires_at.replace(tzinfo=timezone.utc)
                if datetime.now(timezone.utc) > expires_at:
                    promo_error = f"Promo code {promo_upper} has expired"
            
            # Check usage limit
            if not promo_error and promo.get("usage_limit", 0) > 0:
                if promo.get("usage_count", 0) >= promo["usage_limit"]:
                    promo_error = f"Promo code {promo_upper} has reached its usage limit"
            
            # Check if user already used this code
            if not promo_error:
                already_used = await db.promo_usage.find_one({
                    "user_id": user.user_id,
                    "promo_code": promo_upper
                })
                if already_used:
                    promo_error = f"You have already used promo code {promo_upper}"
            
            # Check minimum deposit from database
            if not promo_error:
                min_deposit = promo.get("min_deposit", 0)
                if request.amount < min_deposit:
                    promo_error = f"Minimum deposit for {promo_upper} is ${min_deposit}"
            
            # Apply promo bonus if no errors
            if not promo_error:
                bonus_type = promo.get("bonus_type", "percentage")
                bonus_value = promo.get("bonus_value", 0)
                max_bonus = promo.get("max_bonus", 0)
                
                if bonus_type == "percentage":
                    bonus_percentage = bonus_value
                    bonus_amount = request.amount * (bonus_value / 100)
                    if max_bonus > 0 and bonus_amount > max_bonus:
                        bonus_amount = max_bonus
                else:  # fixed
                    bonus_percentage = 0
                    bonus_amount = bonus_value
        else:
            promo_error = "Invalid promo code"
    
    # If promo code error, raise exception
    if promo_error:
        raise HTTPException(status_code=400, detail=promo_error)
    
    total_credit = request.amount + bonus_amount
    
    # Create unique order ID
    order_id = f"DEP_{user.user_id}_{uuid.uuid4().hex[:8]}"
    
    # Create payment with NOWPayments
    result = await nowpayments_service.create_payment(
        price_amount=request.amount,
        price_currency="usd",
        pay_currency=network_config["currency"],
        order_id=order_id,
        order_description=f"Deposit for user {user.email}"
    )
    
    if result.get("success"):
        # Store deposit record in database with bonus info
        deposit_record = {
            "transaction_id": str(uuid.uuid4()),
            "user_id": user.user_id,
            "payment_id": result.get("payment_id"),
            "order_id": order_id,
            "type": "deposit",
            "amount": request.amount,
            "bonus_percentage": bonus_percentage,
            "bonus_amount": bonus_amount,
            "total_credit": total_credit,
            "is_first_deposit": is_first_deposit,
            "promo_code": request.promo_code,
            "pay_amount": result.get("pay_amount"),
            "pay_currency": result.get("pay_currency", "USDT"),
            "pay_address": result.get("pay_address"),
            "network": request.network,
            "status": "pending",
            "created_at": datetime.now(timezone.utc),
            "expiration_date": result.get("expiration_estimate_date")
        }
        await db.deposits.insert_one(deposit_record)
        
        return DepositResponse(
            success=True,
            payment_id=result.get("payment_id"),
            pay_address=result.get("pay_address"),
            pay_amount=result.get("pay_amount"),
            pay_currency="USDT",
            network=request.network,
            expiration_estimate_date=result.get("expiration_estimate_date"),
            bonus_percentage=bonus_percentage,
            bonus_amount=bonus_amount,
            total_credit=total_credit,
            is_first_deposit=is_first_deposit
        )
    else:
        return DepositResponse(
            success=False,
            error=result.get("error", "Failed to create deposit")
        )

@api_router.get("/deposit/check/{payment_id}")
async def check_deposit_status(
    payment_id: str,
    authorization: Optional[str] = Header(None),
    req: Request = None
):
    """Check the status of a deposit"""
    try:
        user = await get_current_user(authorization, req)
    except HTTPException:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    # Get payment status from NOWPayments
    status = await nowpayments_service.get_payment_status(int(payment_id))
    
    if status:
        payment_status = status.get("payment_status")
        actually_paid = float(status.get("actually_paid", 0))
        
        # Credit balance for finished or partially_paid payments
        if payment_status in ["finished", "partially_paid", "confirmed", "sending"]:
            # Find the deposit record (payment_id stored as string)
            deposit = await db.deposits.find_one({
                "payment_id": str(payment_id),
                "user_id": user.user_id
            })
            
            if deposit and deposit.get("status") not in ["completed", "credited"]:
                # Calculate the USD value of what was actually paid
                # Since they paid in USDT (1:1 with USD approximately)
                credit_amount = actually_paid if actually_paid > 0 else deposit.get("amount", 0)
                bonus_amount = deposit.get("bonus_amount", 0)
                
                # Update deposit status - always mark as "completed" if payment was received
                # Both finished and partially_paid should be shown as completed since fund is credited
                await db.deposits.update_one(
                    {"payment_id": str(payment_id)},
                    {
                        "$set": {
                            "status": "completed",
                            "payment_status": payment_status,  # Store original NOWPayments status
                            "actually_paid": actually_paid,
                            "credit_amount": credit_amount,
                            "completed_at": datetime.now(timezone.utc)
                        }
                    }
                )
                
                # Add to user's real balance (deposit amount + bonus for trading)
                # Note: bonus is also added to bonus_balance separately for tracking
                total_credit = credit_amount + bonus_amount
                update_fields = {"$inc": {"real_balance": total_credit}}
                if bonus_amount > 0:
                    update_fields["$inc"]["bonus_balance"] = bonus_amount
                
                await db.users.update_one(
                    {"user_id": user.user_id},
                    update_fields
                )
                
                # Create notification about deposit
                total_credited = credit_amount + bonus_amount
                bonus_msg = f" + ${bonus_amount:.2f} bonus!" if bonus_amount > 0 else ""
                await create_notification(
                    user.user_id,
                    "Deposit Successful! 💰",
                    f"${credit_amount:.2f} has been credited to your account{bonus_msg}",
                    "deposit"
                )
                
                print(f"Credited ${credit_amount} + ${bonus_amount} bonus to user {user.user_id} for payment {payment_id}")
                
                # ========== AFFILIATE TRACKING FOR DEPOSITS ==========
                # Update affiliate stats when referred user makes a deposit
                user_data = await db.users.find_one({"user_id": user.user_id})
                if user_data and user_data.get("referred_by"):
                    referred_by = user_data.get("referred_by")
                    
                    # Find affiliate by ref_code or link code
                    affiliate = await db.affiliates.find_one({"ref_code": referred_by})
                    if not affiliate:
                        link = await db.affiliate_links.find_one({"code": referred_by})
                        if link:
                            affiliate = await db.affiliates.find_one({"affiliate_id": link.get("affiliate_id")})
                    
                    if affiliate:
                        affiliate_id = affiliate.get("affiliate_id")
                        
                        # Check if this is user's first deposit (FTD)
                        existing_deposits = await db.deposits.count_documents({
                            "user_id": user.user_id,
                            "status": "completed"
                        })
                        is_ftd = existing_deposits == 1  # This is first completed deposit
                        
                        # Update affiliate_referrals
                        await db.affiliate_referrals.update_one(
                            {"user_id": user.user_id, "affiliate_id": affiliate_id},
                            {
                                "$set": {"has_deposited": True, "status": "active"},
                                "$inc": {"total_deposits": credit_amount, "deposits_count": 1}
                            }
                        )
                        
                        # Update referrals collection
                        update_data = {
                            "$set": {"is_ftd": True if is_ftd else None},
                            "$inc": {"total_deposited": credit_amount}
                        }
                        await db.referrals.update_one(
                            {"referred_user_id": user.user_id, "affiliate_id": affiliate_id},
                            update_data
                        )
                        
                        # Update user's total deposits tracking
                        await db.users.update_one(
                            {"user_id": user.user_id},
                            {"$inc": {"total_deposits": credit_amount, "total_deposits_count": 1}}
                        )
                        
                        # Update affiliate's FTD count if this is first deposit
                        if is_ftd:
                            await db.affiliates.update_one(
                                {"affiliate_id": affiliate_id},
                                {"$inc": {"total_ftds": 1, "total_deposits": 1}}
                            )
                            # Also update the link FTD count if used
                            await db.affiliate_links.update_one(
                                {"code": referred_by},
                                {"$inc": {"ftds": 1}}
                            )
                        else:
                            await db.affiliates.update_one(
                                {"affiliate_id": affiliate_id},
                                {"$inc": {"total_deposits": 1}}
                            )
                        
                        print(f"[AFFILIATE] Updated affiliate {affiliate_id} - FTD: {is_ftd}, Deposit: ${credit_amount}")
        
        # Handle expired/failed payments - update local deposit status
        elif payment_status in ["expired", "failed", "refunded"]:
            await db.deposits.update_one(
                {"payment_id": str(payment_id)},
                {
                    "$set": {
                        "status": "expired",
                        "payment_status": payment_status,
                        "expired_at": datetime.now(timezone.utc)
                    }
                }
            )
        
        return {
            "payment_id": payment_id,
            "status": payment_status,
            "actually_paid": actually_paid,
            "pay_amount": status.get("pay_amount"),
            "pay_currency": status.get("pay_currency"),
            "credited": payment_status in ["finished", "partially_paid", "confirmed", "sending"]
        }
    
    raise HTTPException(status_code=404, detail="Payment not found")

@api_router.get("/deposit/history")
async def get_deposit_history(
    authorization: Optional[str] = Header(None),
    req: Request = None
):
    """Get user's deposit history"""
    try:
        user = await get_current_user(authorization, req)
    except HTTPException:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    # First, auto-expire pending deposits older than 12 minutes
    twelve_minutes_ago = datetime.now(timezone.utc) - timedelta(minutes=12)
    await db.deposits.update_many(
        {
            "user_id": user.user_id,
            "status": "pending",
            "created_at": {"$lt": twelve_minutes_ago}
        },
        {
            "$set": {
                "status": "expired",
                "expired_at": datetime.now(timezone.utc)
            }
        }
    )
    
    deposits = await db.deposits.find(
        {"user_id": user.user_id}
    ).sort("created_at", -1).limit(50).to_list(50)
    
    # Convert ObjectId to string
    for dep in deposits:
        dep["_id"] = str(dep["_id"])
        if dep.get("created_at"):
            dep["created_at"] = dep["created_at"].isoformat()
        if dep.get("completed_at"):
            dep["completed_at"] = dep["completed_at"].isoformat()
        if dep.get("expired_at"):
            dep["expired_at"] = dep["expired_at"].isoformat()
    
    return {"deposits": deposits}

# ============= TarsPay Payment Gateway (bKash, Nagad) =============

class TarsPayDepositRequest(BaseModel):
    amount: float = Field(..., description="Amount in USD")
    channel: str = Field(default="bkash", description="Payment channel: bkash, nagad")
    phone: Optional[str] = Field(None, description="Customer phone/wallet number")
    promo_code: Optional[str] = Field(None, description="Promo code for bonus")

@api_router.get("/tarspay/channels")
async def get_tarspay_channels():
    """Get available TarsPay payment channels for all countries"""
    channels = tarspay_service.get_channels()
    return {
        "success": True,
        "channels": channels,
        "exchange_rates": {
            "BDT": get_rate_for_currency("BDT"),
            "INR": get_rate_for_currency("INR"),
            "PKR": get_rate_for_currency("PKR")
        },
        "countries": {
            "bd": {"name": "Bangladesh", "currency": "BDT", "flag": "🇧🇩"},
            "in": {"name": "India", "currency": "INR", "flag": "🇮🇳"},
            "pk": {"name": "Pakistan", "currency": "PKR", "flag": "🇵🇰"}
        }
    }

@api_router.post("/tarspay/deposit/create")
async def create_tarspay_deposit(
    request: TarsPayDepositRequest,
    authorization: Optional[str] = Header(None),
    req: Request = None
):
    """Create a deposit order using TarsPay (bKash/Nagad)"""
    try:
        user = await get_current_user(authorization, req)
    except HTTPException:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    # Generate unique order ID
    order_id = f"BYNIX{user.user_id[:8]}{int(datetime.now().timestamp())}"
    
    # Get base URL for callbacks
    # Use INTEGRATION_PROXY_URL for webhook callbacks (bypasses Kubernetes ingress auth)
    integration_proxy = os.environ.get("INTEGRATION_PROXY_URL", "")
    host = req.headers.get("host", "localhost")
    scheme = "https" if any(x in host for x in ["preview.emergentagent.com", "preview.emergentcf.cloud", "emergent.host"]) else req.url.scheme
    base_url = f"{scheme}://{host}"
    
    # Use integration proxy for webhook callbacks, regular URL for user redirects
    notify_url = f"{integration_proxy}/api/tarspay/callback" if integration_proxy else f"{base_url}/api/tarspay/callback"
    return_url = base_url  # Root URL redirects to trade page
    
    print(f"[TarsPay] Creating order - notifyUrl: {notify_url}, returnUrl: {return_url}")
    
    # Create TarsPay order
    result = await tarspay_service.create_deposit_order(
        order_id=order_id,
        amount_usd=request.amount,
        channel=request.channel,
        customer_phone=request.phone,
        notify_url=notify_url,
        return_url=return_url
    )
    
    if result.get("success"):
        # Save deposit record with promo code
        deposit_record = {
            "user_id": user.user_id,
            "order_id": order_id,
            "payment_id": result.get("payment_id"),
            "amount_usd": request.amount,
            "amount_bdt": result.get("amount_bdt"),
            "channel": request.channel,
            "channel_name": result.get("channel_name"),
            "pay_url": result.get("pay_url"),
            "promo_code": request.promo_code,
            "status": "pending",
            "payment_type": "tarspay",
            "created_at": datetime.now(timezone.utc),
        }
        await db.deposits.insert_one(deposit_record)
        
        return {
            "success": True,
            "order_id": order_id,
            "payment_id": result.get("payment_id"),
            "amount_usd": request.amount,
            "amount_bdt": result.get("amount_bdt"),
            "pay_url": result.get("pay_url"),
            "channel": request.channel,
            "channel_name": result.get("channel_name")
        }
    else:
        return {
            "success": False,
            "error": result.get("error", "Failed to create payment")
        }

@api_router.get("/tarspay/check-pending")
async def check_pending_tarspay_deposits(
    authorization: Optional[str] = Header(None),
    req: Request = None
):
    """Check all pending TarsPay deposits for current user and auto-credit completed ones"""
    try:
        user = await get_current_user(authorization, req)
    except HTTPException:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    # Find all pending TarsPay deposits
    pending_deposits = await db.deposits.find({
        "user_id": user.user_id,
        "status": "pending",
        "payment_type": "tarspay"
    }).sort("created_at", -1).to_list(20)
    
    credited = []
    still_pending = []
    
    for deposit in pending_deposits:
        order_id = deposit.get("order_id")
        if not order_id or not order_id.startswith("BYNIX"):
            continue
        
        # Query TarsPay for actual payment status
        result = await tarspay_service.get_order_status(order_id)
        
        if result.get("success") and result.get("paid"):
            # Payment confirmed! Credit the user
            amount_usd = deposit.get("amount_usd", 0)
            promo_code = deposit.get("promo_code", "")
            
            # Calculate bonus
            bonus_amount = 0
            if promo_code:
                promo = await db.promo_codes.find_one({"code": promo_code.upper(), "is_active": True})
                if promo:
                    min_deposit = promo.get("min_deposit", 0)
                    if amount_usd >= min_deposit:
                        if promo.get("bonus_type") == "percentage":
                            bonus_amount = amount_usd * (promo.get("bonus_value", 0) / 100)
                        else:
                            bonus_amount = promo.get("bonus_value", 0)
                        max_bonus = promo.get("max_bonus")
                        if max_bonus and bonus_amount > max_bonus:
                            bonus_amount = max_bonus
            
            total_credit = amount_usd + bonus_amount
            
            # Update deposit status
            await db.deposits.update_one(
                {"order_id": order_id},
                {
                    "$set": {
                        "status": "completed",
                        "completed_at": datetime.now(timezone.utc),
                        "bonus_amount": bonus_amount,
                        "total_credited": total_credit,
                        "tarspay_response": result
                    }
                }
            )
            
            # Credit user's balance
            update_fields = {"real_balance": total_credit}
            if bonus_amount > 0:
                update_fields["bonus_balance"] = bonus_amount
            
            await db.users.update_one(
                {"user_id": user.user_id},
                {"$inc": update_fields}
            )
            
            # Create notification
            await create_notification(
                user.user_id,
                "Deposit Successful! 💰",
                f"${amount_usd:.2f}" + (f" + ${bonus_amount:.2f} bonus" if bonus_amount > 0 else "") + " has been credited!",
                "deposit"
            )
            
            credited.append({
                "order_id": order_id,
                "amount": amount_usd,
                "bonus": bonus_amount,
                "total": total_credit,
                "channel": deposit.get("channel_name", deposit.get("channel"))
            })
            
            print(f"[AUTO-CREDIT] Credited ${total_credit} to user {user.user_id} from order {order_id}")
        else:
            # Still pending or failed
            still_pending.append({
                "order_id": order_id,
                "amount_usd": deposit.get("amount_usd"),
                "channel": deposit.get("channel_name", deposit.get("channel")),
                "status": result.get("status", "unknown") if result.get("success") else "checking",
                "created_at": deposit.get("created_at").isoformat() if deposit.get("created_at") else None
            })
    
    # Get updated balance
    updated_user = await db.users.find_one({"user_id": user.user_id})
    
    return {
        "success": True,
        "credited_count": len(credited),
        "credited": credited,
        "pending_count": len(still_pending),
        "pending": still_pending,
        "new_balance": updated_user.get("real_balance", 0) if updated_user else 0
    }

@api_router.get("/tarspay/deposit/status/{order_id}")
async def get_tarspay_deposit_status(
    order_id: str,
    authorization: Optional[str] = Header(None),
    req: Request = None
):
    """Check TarsPay deposit order status"""
    try:
        user = await get_current_user(authorization, req)
    except HTTPException:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    # Get status from TarsPay
    result = await tarspay_service.get_order_status(order_id)
    
    if result.get("success"):
        # If payment is successful, credit user's balance
        if result.get("paid") and result.get("status") == "success":
            deposit = await db.deposits.find_one({
                "order_id": order_id,
                "user_id": user.user_id
            })
            
            if deposit and deposit.get("status") != "completed":
                amount_usd = deposit.get("amount_usd", 0)
                promo_code = deposit.get("promo_code")
                bonus_amount = 0
                
                # Check promo code and calculate bonus
                if promo_code:
                    promo = await db.promo_codes.find_one({
                        "code": promo_code.upper(),
                        "active": True
                    })
                    if promo:
                        min_deposit = promo.get("min_deposit", 0)
                        if amount_usd >= min_deposit:
                            if promo.get("bonus_type") == "percentage":
                                bonus_amount = amount_usd * (promo.get("bonus_value", 0) / 100)
                            else:
                                bonus_amount = promo.get("bonus_value", 0)
                            
                            # Cap bonus if max_bonus is set
                            max_bonus = promo.get("max_bonus")
                            if max_bonus and bonus_amount > max_bonus:
                                bonus_amount = max_bonus
                
                total_credit = amount_usd + bonus_amount
                
                # Update deposit status
                await db.deposits.update_one(
                    {"order_id": order_id},
                    {
                        "$set": {
                            "status": "completed",
                            "completed_at": datetime.now(timezone.utc),
                            "bonus_amount": bonus_amount
                        }
                    }
                )
                
                # Credit user's balance (deposit + bonus to real_balance, bonus tracked in bonus_balance)
                update_fields = {"real_balance": total_credit}
                if bonus_amount > 0:
                    update_fields["bonus_balance"] = bonus_amount
                
                await db.users.update_one(
                    {"user_id": user.user_id},
                    {"$inc": update_fields}
                )
                
                # Create notification
                if bonus_amount > 0:
                    await create_notification(
                        user.user_id,
                        "Deposit Successful! 💰🎉",
                        f"${amount_usd:.2f} + ${bonus_amount:.2f} bonus (Total: ${total_credit:.2f}) credited via {deposit.get('channel_name', 'bKash/Nagad')}",
                        "deposit"
                    )
                else:
                    await create_notification(
                        user.user_id,
                        "Deposit Successful! 💰",
                        f"${amount_usd:.2f} has been credited to your account via {deposit.get('channel_name', 'bKash/Nagad')}",
                        "deposit"
                    )
                
                print(f"TarsPay: Credited ${total_credit} (deposit: ${amount_usd}, bonus: ${bonus_amount}) to user {user.user_id}")
        
        return {
            "success": True,
            "order_id": order_id,
            "status": result.get("status"),
            "paid": result.get("paid", False)
        }
    else:
        return {
            "success": False,
            "error": result.get("error", "Failed to get status")
        }

@api_router.post("/tarspay/callback")
async def tarspay_callback(request: Request):
    """Handle TarsPay payment callback notifications"""
    try:
        body = await request.json()
        signature = request.headers.get("X-RESP-SIGNATURE", "")
        
        print(f"TarsPay Callback: {body}")
        
        # Verify signature (optional but recommended)
        # content = json.dumps(body, separators=(',', ':'))
        # if not tarspay_service.verify_callback_signature(content, signature):
        #     return Response(content="DENY", status_code=200)
        
        order_id = body.get("mchOrderNo")
        order_state = body.get("orderState")  # 2 = success
        
        if order_id and order_state == 2:
            # Find deposit record
            deposit = await db.deposits.find_one({"order_id": order_id})
            
            if deposit and deposit.get("status") != "completed":
                user_id = deposit.get("user_id")
                amount_usd = deposit.get("amount_usd", 0)
                promo_code = deposit.get("promo_code", "")
                
                # Calculate promo bonus
                bonus = 0
                if promo_code == "BYNIX":
                    bonus = amount_usd * 1.0  # 100% bonus
                elif promo_code == "VIP50" or promo_code == "WELCOME50":
                    bonus = amount_usd * 0.5  # 50% bonus
                
                total_credit = amount_usd + bonus
                
                # Update deposit status
                await db.deposits.update_one(
                    {"order_id": order_id},
                    {
                        "$set": {
                            "status": "completed",
                            "completed_at": datetime.now(timezone.utc),
                            "callback_data": body,
                            "bonus_amount": bonus,
                            "total_credited": total_credit
                        }
                    }
                )
                
                # Credit user's balance (real_balance includes bonus, bonus_balance tracks bonus separately)
                await db.users.update_one(
                    {"user_id": user_id},
                    {"$inc": {"real_balance": total_credit, "bonus_balance": bonus}}
                )
                
                # Create notification
                notification_msg = f"${amount_usd:.2f} has been credited to your account"
                if bonus > 0:
                    notification_msg = f"${amount_usd:.2f} + ${bonus:.2f} bonus = ${total_credit:.2f} credited!"
                
                await create_notification(
                    user_id,
                    "Deposit Successful! 💰",
                    notification_msg,
                    "deposit"
                )
                
                print(f"[TarsPay Callback] Credited ${total_credit} (${amount_usd} + ${bonus} bonus) to user {user_id}")
        
        # Return OK to acknowledge receipt
        return Response(content="OK", status_code=200)
        
    except Exception as e:
        print(f"TarsPay Callback Error: {e}")
        return Response(content="OK", status_code=200)

# ============= TarsPay E-Wallet Withdrawal (bKash, Nagad) =============

class EWalletWithdrawRequest(BaseModel):
    amount: float = Field(..., description="Amount in USD to withdraw")
    channel: str = Field(..., description="Withdrawal channel: bkash or nagad")
    wallet_id: str = Field(..., description="bKash/Nagad wallet number (11 digits starting with 0)")

@api_router.get("/tarspay/withdrawal/channels")
async def get_ewallet_withdrawal_channels():
    """Get available E-Wallet withdrawal channels (bKash, Nagad)"""
    return {
        "success": True,
        "channels": tarspay_service.get_withdrawal_channels(),
        "exchange_rate": get_current_rate(),
        "currency": "BDT"
    }

@api_router.post("/tarspay/withdrawal/create")
async def create_ewallet_withdrawal(
    request: EWalletWithdrawRequest,
    authorization: Optional[str] = Header(None),
    req: Request = None
):
    """Create E-Wallet withdrawal request to bKash/Nagad"""
    try:
        user = await get_current_user(authorization, req)
    except HTTPException:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    amount_usd = request.amount
    channel = request.channel.lower()
    wallet_id = request.wallet_id
    
    # Validate channel
    if channel not in ["bkash", "nagad"]:
        return {"success": False, "error": "Invalid channel. Use 'bkash' or 'nagad'"}
    
    way_code = "EWALLET_BKASH" if channel == "bkash" else "EWALLET_NAGAD"
    channel_name = "bKash" if channel == "bkash" else "Nagad"
    
    # Get exchange rate and calculate BDT amount
    rate = get_current_rate()
    amount_bdt = int(amount_usd * rate)
    
    # Validate limits
    if amount_bdt < 100:
        return {"success": False, "error": f"Minimum withdrawal is ৳100 BDT (~${round(100/rate, 2)} USD)"}
    if amount_bdt > 50000:
        return {"success": False, "error": f"Maximum withdrawal is ৳50,000 BDT (~${round(50000/rate, 2)} USD)"}
    
    # Validate wallet ID
    if not wallet_id or len(wallet_id) != 11 or not wallet_id.startswith("0"):
        return {"success": False, "error": "Invalid wallet number. Must be 11 digits starting with 0 (e.g., 01712345678)"}
    
    # Check user balance (only real_balance minus bonus_balance is withdrawable)
    user_doc = await db.users.find_one({"user_id": user.user_id})
    real_balance = user_doc.get("real_balance", 0)
    bonus_balance = user_doc.get("bonus_balance", 0)
    
    # Withdrawable = real_balance - bonus_balance (bonus cannot be withdrawn directly)
    withdrawable = real_balance - bonus_balance
    
    if amount_usd > withdrawable:
        return {
            "success": False, 
            "error": f"Insufficient withdrawable balance. You have ${withdrawable:.2f} available for withdrawal. (Note: Bonus balance of ${bonus_balance:.2f} cannot be withdrawn directly)"
        }
    
    # Calculate fee (1.5%)
    fee_percent = 1.5
    fee_bdt = int(amount_bdt * fee_percent / 100)
    net_amount_bdt = amount_bdt - fee_bdt
    
    # Generate unique order ID
    order_id = f"WBYNIX{user.user_id[:6]}{int(datetime.now().timestamp())}"
    
    # AUTO-APPROVAL THRESHOLD: $100 or less = auto-approved, more than $100 = admin approval required
    AUTO_APPROVAL_LIMIT = 100.0
    requires_admin_approval = amount_usd > AUTO_APPROVAL_LIMIT
    
    # Get notify URL
    integration_proxy = os.environ.get("INTEGRATION_PROXY_URL", "")
    host = req.headers.get("host", "localhost")
    scheme = "https" if any(x in host for x in ["preview.emergentagent.com", "preview.emergentcf.cloud", "emergent.host"]) else req.url.scheme
    base_url = f"{scheme}://{host}"
    notify_url = f"{integration_proxy}/api/tarspay/withdrawal/callback" if integration_proxy else f"{base_url}/api/tarspay/withdrawal/callback"
    
    # Deduct balance immediately (will be refunded if withdrawal fails)
    # IMPORTANT: If user has bonus, forfeit the bonus on any withdrawal
    bonus_forfeited = 0
    if bonus_balance > 0:
        bonus_forfeited = bonus_balance
        # Deduct withdrawal amount + forfeit entire bonus
        await db.users.update_one(
            {"user_id": user.user_id},
            {
                "$inc": {"real_balance": -(amount_usd + bonus_balance)},
                "$set": {"bonus_balance": 0}
            }
        )
        # Create notification about bonus forfeit
        await create_notification(
            user.user_id,
            "⚠️ Bonus Forfeited",
            f"Your bonus of ${bonus_balance:.2f} has been forfeited due to withdrawal. Bonuses can only be used for trading.",
            "warning"
        )
    else:
        # No bonus, just deduct withdrawal amount
        await db.users.update_one(
            {"user_id": user.user_id},
            {"$inc": {"real_balance": -amount_usd}}
        )
    
    # Create withdrawal record in database
    withdrawal_doc = {
        "order_id": order_id,
        "user_id": user.user_id,
        "type": "ewallet",
        "payment_type": "tarspay",
        "channel": channel,
        "channel_name": channel_name,
        "way_code": way_code,
        "wallet_id": wallet_id,
        "amount_usd": amount_usd,
        "amount_bdt": amount_bdt,
        "fee_bdt": fee_bdt,
        "net_amount_bdt": net_amount_bdt,
        "exchange_rate": rate,
        "status": "pending_approval" if requires_admin_approval else "processing",
        "requires_admin_approval": requires_admin_approval,
        "auto_approval_limit": AUTO_APPROVAL_LIMIT,
        "bonus_forfeited": bonus_forfeited,
        "created_at": datetime.now(timezone.utc)
    }
    await db.withdrawals.insert_one(withdrawal_doc)
    
    # If requires admin approval, don't call TarsPay API yet
    if requires_admin_approval:
        print(f"[TarsPay] Withdrawal requires admin approval (${amount_usd} > ${AUTO_APPROVAL_LIMIT})")
        await create_notification(
            user.user_id,
            "⏳ Withdrawal Pending Approval",
            f"Your withdrawal of ৳{net_amount_bdt} to {channel_name} ({wallet_id}) requires admin approval (amount > ${AUTO_APPROVAL_LIMIT}). You'll be notified once approved.",
            "info"
        )
        return {
            "success": True,
            "order_id": order_id,
            "amount_usd": amount_usd,
            "amount_bdt": amount_bdt,
            "fee_bdt": fee_bdt,
            "net_amount_bdt": net_amount_bdt,
            "wallet_id": wallet_id,
            "channel": channel_name,
            "status": "pending_approval",
            "requires_admin_approval": True,
            "message": f"Withdrawal of ৳{net_amount_bdt} to {channel_name} requires admin approval (amount > ${AUTO_APPROVAL_LIMIT})"
        }
    
    # AUTO-APPROVED: Call TarsPay API to create withdrawal
    print(f"[TarsPay] Auto-approved withdrawal (${amount_usd} <= ${AUTO_APPROVAL_LIMIT})")
    result = await tarspay_service.create_withdrawal(
        order_id=order_id,
        amount_bdt=net_amount_bdt,  # Send net amount (after fee)
        wallet_id=wallet_id,
        way_code=way_code,
        notify_url=notify_url
    )
    
    if result.get("success"):
        # Update withdrawal with payment ID
        await db.withdrawals.update_one(
            {"order_id": order_id},
            {"$set": {"payment_id": result.get("payment_id"), "status": "pending"}}
        )
        
        return {
            "success": True,
            "order_id": order_id,
            "payment_id": result.get("payment_id"),
            "amount_usd": amount_usd,
            "amount_bdt": amount_bdt,
            "fee_bdt": fee_bdt,
            "net_amount_bdt": net_amount_bdt,
            "wallet_id": wallet_id,
            "channel": channel_name,
            "requires_admin_approval": False,
            "message": f"Withdrawal of ৳{net_amount_bdt} to {channel_name} ({wallet_id}) is being processed"
        }
    else:
        # Refund balance if TarsPay API failed
        await db.users.update_one(
            {"user_id": user.user_id},
            {"$inc": {"real_balance": amount_usd}}
        )
        
        # Update withdrawal status to failed
        await db.withdrawals.update_one(
            {"order_id": order_id},
            {"$set": {"status": "failed", "error": result.get("error")}}
        )
        
        return {
            "success": False,
            "error": result.get("error", "Failed to create withdrawal")
        }

@api_router.get("/tarspay/withdrawal/status/{order_id}")
async def get_ewallet_withdrawal_status(
    order_id: str,
    authorization: Optional[str] = Header(None),
    req: Request = None
):
    """Get E-Wallet withdrawal status"""
    try:
        user = await get_current_user(authorization, req)
    except HTTPException:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    # Find withdrawal in database
    withdrawal = await db.withdrawals.find_one({
        "order_id": order_id,
        "user_id": user.user_id
    })
    
    if not withdrawal:
        return {"success": False, "error": "Withdrawal not found"}
    
    # Query TarsPay for latest status
    if withdrawal.get("status") == "pending":
        result = await tarspay_service.get_withdrawal_status(order_id)
        
        if result.get("success"):
            new_status = withdrawal.get("status")
            
            if result.get("completed"):
                new_status = "completed"
                # Create notification
                await create_notification(
                    user.user_id,
                    "Withdrawal Successful! 💸",
                    f"৳{withdrawal.get('net_amount_bdt')} has been sent to your {withdrawal.get('channel_name')} ({withdrawal.get('wallet_id')})",
                    "withdrawal"
                )
            elif result.get("failed"):
                new_status = "failed"
                # Refund balance
                await db.users.update_one(
                    {"user_id": user.user_id},
                    {"$inc": {"real_balance": withdrawal.get("amount_usd", 0)}}
                )
                # Create notification
                await create_notification(
                    user.user_id,
                    "Withdrawal Failed ❌",
                    f"Your withdrawal of ৳{withdrawal.get('amount_bdt')} has failed. Balance has been refunded.",
                    "withdrawal"
                )
            
            # Update database
            await db.withdrawals.update_one(
                {"order_id": order_id},
                {"$set": {"status": new_status, "tarspay_status": result}}
            )
            withdrawal["status"] = new_status
    
    return {
        "success": True,
        "order_id": order_id,
        "status": withdrawal.get("status"),
        "amount_usd": withdrawal.get("amount_usd"),
        "amount_bdt": withdrawal.get("amount_bdt"),
        "fee_bdt": withdrawal.get("fee_bdt"),
        "net_amount_bdt": withdrawal.get("net_amount_bdt"),
        "wallet_id": withdrawal.get("wallet_id"),
        "channel": withdrawal.get("channel_name"),
        "created_at": withdrawal.get("created_at").isoformat() if withdrawal.get("created_at") else None
    }

@api_router.post("/tarspay/withdrawal/callback")
async def tarspay_withdrawal_callback(request: Request):
    """Handle TarsPay withdrawal callback"""
    try:
        body = await request.json()
        print(f"[TarsPay Withdrawal Callback] Received: {body}")
        
        order_id = body.get("mchOrderNo")
        order_state = body.get("orderState") or body.get("state")
        
        if order_id and order_state:
            withdrawal = await db.withdrawals.find_one({"order_id": order_id})
            
            if withdrawal and withdrawal.get("status") == "pending":
                user_id = withdrawal.get("user_id")
                
                if order_state == 2:  # Success
                    await db.withdrawals.update_one(
                        {"order_id": order_id},
                        {"$set": {"status": "completed", "completed_at": datetime.now(timezone.utc), "callback_data": body}}
                    )
                    await create_notification(
                        user_id,
                        "Withdrawal Successful! 💸",
                        f"৳{withdrawal.get('net_amount_bdt')} has been sent to your {withdrawal.get('channel_name')}",
                        "withdrawal"
                    )
                    print(f"[TarsPay Withdrawal Callback] Completed: {order_id}")
                    
                elif order_state in [3, 8]:  # Failed or Rejected
                    # Refund balance
                    await db.users.update_one(
                        {"user_id": user_id},
                        {"$inc": {"real_balance": withdrawal.get("amount_usd", 0)}}
                    )
                    await db.withdrawals.update_one(
                        {"order_id": order_id},
                        {"$set": {"status": "failed", "callback_data": body}}
                    )
                    await create_notification(
                        user_id,
                        "Withdrawal Failed ❌",
                        f"Your withdrawal has failed. ${withdrawal.get('amount_usd'):.2f} has been refunded.",
                        "withdrawal"
                    )
                    print(f"[TarsPay Withdrawal Callback] Failed: {order_id}")
        
        return Response(content="OK", status_code=200)
        
    except Exception as e:
        print(f"[TarsPay Withdrawal Callback] Error: {e}")
        return Response(content="OK", status_code=200)

@api_router.get("/tarspay/check-pending-withdrawals")
async def check_pending_withdrawals(
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """
    Check and update status of all pending TarsPay withdrawals for the user.
    This is a fallback for when webhooks don't reach us.
    """
    try:
        user = await get_current_user(authorization, request)
    except HTTPException:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    # Find all pending/processing TarsPay withdrawals for this user
    pending_withdrawals = await db.withdrawals.find({
        "user_id": user.user_id,
        "payment_type": "tarspay",
        "status": {"$in": ["pending", "processing"]}
    }).to_list(50)
    
    if not pending_withdrawals:
        return {"success": True, "message": "No pending withdrawals", "updated": 0}
    
    updated_count = 0
    results = []
    
    for withdrawal in pending_withdrawals:
        order_id = withdrawal.get("order_id")
        if not order_id:
            continue
        
        # Query TarsPay for current status
        status_result = await tarspay_service.get_withdrawal_status(order_id)
        
        if status_result.get("success"):
            tarspay_status = status_result.get("status")
            tarspay_state = status_result.get("state")
            
            # Update if status changed
            if tarspay_status == "success" and withdrawal.get("status") != "completed":
                await db.withdrawals.update_one(
                    {"order_id": order_id},
                    {
                        "$set": {
                            "status": "completed",
                            "completed_at": datetime.now(timezone.utc),
                            "tarspay_query_data": status_result
                        }
                    }
                )
                await create_notification(
                    user.user_id,
                    "Withdrawal Successful! 💸",
                    f"৳{withdrawal.get('net_amount_bdt')} has been sent to your {withdrawal.get('channel_name')}",
                    "withdrawal"
                )
                updated_count += 1
                results.append({"order_id": order_id, "old_status": withdrawal.get("status"), "new_status": "completed"})
                
            elif tarspay_status in ["failed", "rejected", "refund"]:
                # Refund balance
                await db.users.update_one(
                    {"user_id": user.user_id},
                    {"$inc": {"real_balance": withdrawal.get("amount_usd", 0)}}
                )
                await db.withdrawals.update_one(
                    {"order_id": order_id},
                    {
                        "$set": {
                            "status": "failed",
                            "tarspay_query_data": status_result
                        }
                    }
                )
                await create_notification(
                    user.user_id,
                    "Withdrawal Failed ❌",
                    f"Your withdrawal has failed. ${withdrawal.get('amount_usd'):.2f} has been refunded.",
                    "withdrawal"
                )
                updated_count += 1
                results.append({"order_id": order_id, "old_status": withdrawal.get("status"), "new_status": "failed"})
        else:
            results.append({"order_id": order_id, "error": status_result.get("error", "Query failed")})
    
    return {
        "success": True,
        "message": f"Checked {len(pending_withdrawals)} pending withdrawals, updated {updated_count}",
        "updated": updated_count,
        "results": results
    }

# ============= NOWPayments Withdrawal Callback =============

@api_router.post("/nowpayments/withdrawal/callback")
async def nowpayments_withdrawal_callback(request: Request):
    """Handle NOWPayments payout IPN callback"""
    try:
        body = await request.json()
        print(f"[NOWPayments Callback] Received: {body}")
        
        # NOWPayments IPN fields
        payout_id = body.get("id") or body.get("payout_id")
        status = body.get("status")  # waiting, confirming, sending, finished, failed, refunded
        external_id = body.get("unique_external_id")  # Our transaction_id
        batch_withdrawal_id = body.get("batch_withdrawal_id")
        hash_value = body.get("hash")  # Transaction hash when finished
        
        if not payout_id and not external_id:
            print("[NOWPayments Callback] Missing payout_id and external_id")
            return Response(content="OK", status_code=200)
        
        # Find withdrawal by NOWPayments payout_id or our transaction_id
        query = {}
        if payout_id:
            query["nowpayments_payout_id"] = str(payout_id)
        elif external_id:
            query["transaction_id"] = external_id
            
        withdrawal = await db.transactions.find_one(query)
        
        if not withdrawal:
            # Try withdrawals collection
            withdrawal = await db.withdrawals.find_one(query)
        
        if not withdrawal:
            print(f"[NOWPayments Callback] Withdrawal not found: payout_id={payout_id}, external_id={external_id}")
            return Response(content="OK", status_code=200)
        
        transaction_id = withdrawal.get("transaction_id") or withdrawal.get("order_id")
        user_id = withdrawal.get("user_id")
        current_status = withdrawal.get("status")
        
        # Map NOWPayments status to our status
        new_status = current_status
        if status == "finished":
            new_status = "completed"
        elif status == "failed":
            new_status = "failed"
        elif status == "refunded":
            new_status = "refunded"
        elif status in ["waiting", "confirming", "sending"]:
            new_status = "processing"
        
        # Only update if status changed
        if new_status != current_status:
            update_data = {
                "status": new_status,
                "nowpayments_callback_data": body
            }
            
            if hash_value:
                update_data["txn_hash"] = hash_value
            
            if new_status == "completed":
                update_data["completed_at"] = datetime.now(timezone.utc)
            
            # Update both collections
            await db.transactions.update_one(
                {"transaction_id": transaction_id},
                {"$set": update_data}
            )
            await db.withdrawals.update_one(
                {"$or": [{"transaction_id": transaction_id}, {"order_id": transaction_id}]},
                {"$set": update_data}
            )
            
            # Create notification based on status
            if new_status == "completed":
                await create_notification(
                    user_id,
                    "✅ Withdrawal Successful!",
                    f"Your USDT TRC20 withdrawal has been completed. TxHash: {hash_value[:12]}..." if hash_value else "Your withdrawal has been sent.",
                    "withdrawal"
                )
                print(f"[NOWPayments Callback] Completed: {transaction_id}")
                
            elif new_status == "failed" or new_status == "refunded":
                # Refund user's balance
                amount = withdrawal.get("amount", 0)
                await db.users.update_one(
                    {"user_id": user_id},
                    {"$inc": {"real_balance": amount}}
                )
                await create_notification(
                    user_id,
                    "❌ Withdrawal Failed",
                    f"Your withdrawal of ${amount:.2f} has failed. The amount has been refunded to your balance.",
                    "withdrawal"
                )
                print(f"[NOWPayments Callback] Failed/Refunded: {transaction_id}, refunded ${amount}")
        
        return Response(content="OK", status_code=200)
        
    except Exception as e:
        print(f"[NOWPayments Callback] Error: {e}")
        import traceback
        traceback.print_exc()
        return Response(content="OK", status_code=200)

@api_router.get("/nowpayments/withdrawal/status/{transaction_id}")
async def get_nowpayments_withdrawal_status(
    transaction_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get NOWPayments withdrawal status and sync with API"""
    try:
        user = await get_current_user(authorization, request)
    except HTTPException:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    # Find withdrawal in transactions collection
    withdrawal = await db.transactions.find_one({
        "transaction_id": transaction_id,
        "user_id": user.user_id,
        "type": "withdrawal"
    })
    
    if not withdrawal:
        # Try withdrawals collection
        withdrawal = await db.withdrawals.find_one({
            "$or": [{"transaction_id": transaction_id}, {"order_id": transaction_id}],
            "user_id": user.user_id
        })
    
    if not withdrawal:
        return {"success": False, "error": "Withdrawal not found"}
    
    # If has NOWPayments payout_id and not completed, check live status
    payout_id = withdrawal.get("nowpayments_payout_id")
    if payout_id and withdrawal.get("status") not in ["completed", "failed", "refunded"]:
        live_status = await check_payout_status(payout_id)
        
        if live_status and not live_status.get("error"):
            np_status = live_status.get("status")
            hash_value = live_status.get("hash")
            
            # Update if status changed
            new_status = withdrawal.get("status")
            if np_status == "finished":
                new_status = "completed"
            elif np_status == "failed":
                new_status = "failed"
            elif np_status in ["waiting", "confirming", "sending"]:
                new_status = "processing"
            
            if new_status != withdrawal.get("status"):
                update_data = {"status": new_status}
                if hash_value:
                    update_data["txn_hash"] = hash_value
                if new_status == "completed":
                    update_data["completed_at"] = datetime.now(timezone.utc)
                
                await db.transactions.update_one(
                    {"transaction_id": transaction_id},
                    {"$set": update_data}
                )
                await db.withdrawals.update_one(
                    {"$or": [{"transaction_id": transaction_id}, {"order_id": transaction_id}]},
                    {"$set": update_data}
                )
                
                withdrawal["status"] = new_status
                if hash_value:
                    withdrawal["txn_hash"] = hash_value
    
    return {
        "success": True,
        "transaction_id": transaction_id,
        "payout_id": payout_id,
        "status": withdrawal.get("status"),
        "amount": withdrawal.get("amount"),
        "net_amount": withdrawal.get("net_amount"),
        "fee": withdrawal.get("network_fee"),
        "crypto_address": withdrawal.get("crypto_address"),
        "txn_hash": withdrawal.get("txn_hash"),
        "created_at": withdrawal.get("created_at").isoformat() if withdrawal.get("created_at") else None,
        "completed_at": withdrawal.get("completed_at").isoformat() if withdrawal.get("completed_at") else None
    }

# ============= Chart Data API (Synced across all devices) =============

def get_base_price(symbol: str) -> float:
    """Get base price for a symbol"""
    # Normalize symbol: remove OTC, spaces, slashes, and uppercase
    symbol_clean = symbol.upper().replace(" OTC", "").replace("OTC", "").replace("/", "").replace(" ", "").replace("_", "")
    
    # Match against normalized symbols
    if 'EURUSD' in symbol_clean: return 1.0850
    if 'GBPUSD' in symbol_clean: return 1.2650
    if 'USDJPY' in symbol_clean: return 149.50
    if 'AUDUSD' in symbol_clean: return 0.6550
    if 'USDCHF' in symbol_clean: return 0.8750
    if 'EURGBP' in symbol_clean: return 0.8550
    if 'NZDUSD' in symbol_clean: return 0.6150
    if 'USDCAD' in symbol_clean: return 1.3550
    if 'EURJPY' in symbol_clean: return 162.50
    if 'GBPJPY' in symbol_clean: return 189.50
    if 'AUDJPY' in symbol_clean: return 98.50
    if 'CADJPY' in symbol_clean: return 110.50
    if 'CHFJPY' in symbol_clean: return 170.50
    if 'NZDJPY' in symbol_clean: return 92.50
    if 'EURAUD' in symbol_clean: return 1.66
    if 'EURCHF' in symbol_clean: return 0.95
    if 'EURCAD' in symbol_clean: return 1.47
    if 'EURNZD' in symbol_clean: return 1.77
    if 'GBPAUD' in symbol_clean: return 1.93
    if 'GBPCAD' in symbol_clean: return 1.71
    if 'GBPCHF' in symbol_clean: return 1.11
    if 'GBPNZD' in symbol_clean: return 2.06
    if 'AUDCAD' in symbol_clean: return 0.89
    if 'AUDCHF' in symbol_clean: return 0.57
    if 'AUDNZD' in symbol_clean: return 1.07
    if 'CADCHF' in symbol_clean: return 0.64
    if 'NZDCAD' in symbol_clean: return 0.83
    if 'NZDCHF' in symbol_clean: return 0.54
    if 'BTC' in symbol_clean: return 67500
    if 'ETH' in symbol_clean: return 3500
    if 'XRP' in symbol_clean: return 0.55
    if 'SOL' in symbol_clean: return 145
    if 'ADA' in symbol_clean: return 0.45
    if 'DOGE' in symbol_clean: return 0.12
    if 'BNB' in symbol_clean: return 580
    if 'AAPL' in symbol_clean: return 178
    if 'GOOGL' in symbol_clean: return 141
    if 'MSFT' in symbol_clean: return 378
    if 'AMZN' in symbol_clean: return 178
    if 'TSLA' in symbol_clean: return 245
    if 'META' in symbol_clean: return 485
    if 'NVDA' in symbol_clean: return 890
    if 'NFLX' in symbol_clean: return 620
    # Commodities
    if 'XAUUSD' in symbol_clean or 'GOLD' in symbol_clean: return 2350
    if 'XAGUSD' in symbol_clean or 'SILVER' in symbol_clean: return 28.50
    if 'USOIL' in symbol_clean or 'CRUDEOIL' in symbol_clean: return 78.50
    
    return 1.0850

def generate_server_chart_data(symbol: str, days: int = 7) -> list:
    """Generate chart data on the server (consistent across all devices)
    
    Args:
        symbol: Trading pair symbol
        days: Number of days of historical data (1-30, default 7)
    
    Lazy loading: Initial load is 7 days, user can scroll to load up to 30 days
    """
    import hashlib
    import math
    
    base_price = get_base_price(symbol)
    ticks = []
    now = int(datetime.now(timezone.utc).timestamp())
    
    # Use symbol hash as seed for deterministic randomness
    seed = int(hashlib.md5(symbol.encode()).hexdigest()[:8], 16)
    random.seed(seed)
    
    price = base_price
    
    # Calculate total ticks based on requested days
    # days × 24 hours × 60 minutes × 60 seconds
    TOTAL_TICKS = days * 24 * 60 * 60
    
    # Generate ticks from (now - TOTAL_TICKS) to (now - 1)
    # The last tick is 1 second ago, so real-time tick at 'now' will update current candle
    for i in range(TOTAL_TICKS, 0, -1):
        tick_time = now - i
        
        # ========== ULTRA SLOW SMOOTH MOVEMENT (Pocket Option Style) ==========
        volatility = base_price * 0.0000006  # ULTRA TINY - almost imperceptible per tick
        
        tick_second = tick_time % 86400  # Seconds since midnight
        
        # Very slow amplitude changes
        amplitude_cycle = (tick_time // 180) % 100  # Changes every 3 minutes
        random.seed(seed + amplitude_cycle)
        amp_variation = 0.7 + random.random() * 0.6
        
        # Slow phase shifts
        phase_shift = (tick_time // 90) * 0.15 + (seed % 500)
        
        # ULTRA SLOW wave frequencies (creates very gradual movement)
        fast_wave = math.sin((tick_second * 0.008) + phase_shift) * 0.3 * amp_variation
        medium_wave = math.sin((tick_second * 0.003) + phase_shift * 0.4) * 0.4 * (1.4 - amp_variation * 0.25)
        slow_wave = math.sin((tick_second * 0.0008) + phase_shift * 0.15) * 0.3
        
        # Combine for ultra smooth directional movement
        trend_direction = fast_wave + medium_wave + slow_wave
        
        # Mean reversion keeps price stable
        mean_reversion = (base_price - price) * 0.0005
        change = (trend_direction * volatility * 1.5) + mean_reversion
        
        # Tiny noise
        random.seed(seed + tick_time)
        noise = (random.random() - 0.5) * volatility * 0.08
        change += noise
        
        open_price = price
        close_price = open_price + change
        
        # Clamp price within reasonable range (±3% of base for tighter control)
        max_price = base_price * 1.03
        min_price = base_price * 0.97
        close_price = max(min_price, min(max_price, close_price))
        
        # Very small high/low variance for smooth candles
        high_price = max(open_price, close_price) + abs(volatility * 0.2)
        low_price = min(open_price, close_price) - abs(volatility * 0.2)
        
        # Ensure high/low are within bounds
        high_price = min(high_price, max_price)
        low_price = max(low_price, min_price)
        
        ticks.append({
            "time": tick_time,
            "open": round(open_price, 6),
            "high": round(high_price, 6),
            "low": round(low_price, 6),
            "close": round(close_price, 6)
        })
        
        price = close_price
    
    # Add final tick at current timestamp to ensure current candle exists
    final_volatility = base_price * 0.000003
    ticks.append({
        "time": now,
        "open": round(price, 6),
        "high": round(price + abs(final_volatility * 0.2), 6),
        "low": round(price - abs(final_volatility * 0.2), 6),
        "close": round(price, 6)
    })
    
    # Reset random seed
    random.seed()
    
    return ticks

# In-memory chart data cache (avoid MongoDB 16MB document limit)
chart_data_memory_cache = {}
chart_data_cache_timestamps = {}

@api_router.post("/chart/clear-cache")
async def clear_chart_cache():
    """Clear all chart data cache to regenerate fresh data"""
    global chart_data_memory_cache, chart_data_cache_timestamps
    chart_data_memory_cache = {}
    chart_data_cache_timestamps = {}
    return {"success": True, "message": "Chart cache cleared"}

def aggregate_ticks_to_candles(ticks: list, interval_seconds: int) -> list:
    """Aggregate raw ticks into OHLC candles based on interval"""
    if not ticks:
        return []
    
    candles = []
    current_candle = None
    candle_start_time = 0
    
    for tick in ticks:
        tick_candle_start = (tick["time"] // interval_seconds) * interval_seconds
        
        if current_candle is None or tick_candle_start != candle_start_time:
            # Start new candle
            if current_candle is not None:
                candles.append(current_candle)
            candle_start_time = tick_candle_start
            current_candle = {
                "time": tick_candle_start,
                "open": tick["open"],
                "high": tick["high"],
                "low": tick["low"],
                "close": tick["close"],
            }
        else:
            # Update current candle
            current_candle["high"] = max(current_candle["high"], tick["high"])
            current_candle["low"] = min(current_candle["low"], tick["low"])
            current_candle["close"] = tick["close"]
    
    # Add the last candle
    if current_candle is not None:
        candles.append(current_candle)
    
    return candles

@api_router.get("/chart/data/{symbol}")
async def get_chart_data(symbol: str, interval: str = "1m", days: int = 7):
    """Get chart data for a symbol - returns pre-aggregated candles based on interval
    
    Interval options: 15s, 1m, 5m, 15m, 30m, 1h, 4h
    Days: Number of days of historical data (1-30, default 7)
    Returns candles instead of raw ticks for better frontend performance
    
    Lazy loading: Start with 7 days, load more (up to 30) when user scrolls back
    """
    global chart_data_memory_cache, chart_data_cache_timestamps
    
    # Clamp days to valid range
    days = max(1, min(30, days))
    
    # Interval mapping
    interval_map = {
        '15s': 15,
        '1m': 60,
        '5m': 300,
        '15m': 900,
        '30m': 1800,
        '1h': 3600,
        '4h': 14400
    }
    interval_seconds = interval_map.get(interval, 60)
    
    # Normalize symbol
    symbol_key = symbol.replace("/", "_").replace(" ", "_").upper()
    
    # Cache key includes days for different data sizes
    cache_key = f"{symbol_key}_{days}d"
    
    # Check in-memory cache for raw ticks first
    if cache_key not in chart_data_memory_cache:
        # Generate new raw data
        print(f"[CHART] Generating {days}-day data for {symbol_key}...")
        ticks = generate_server_chart_data(symbol, days=days)
        chart_data_memory_cache[cache_key] = ticks
        chart_data_cache_timestamps[cache_key] = datetime.now(timezone.utc)
        print(f"[CHART] Generated {len(ticks)} ticks for {cache_key}")
    else:
        cache_time = chart_data_cache_timestamps.get(cache_key, datetime.min.replace(tzinfo=timezone.utc))
        age = datetime.now(timezone.utc) - cache_time
        
        # Refresh cache if older than 30 minutes
        if age.total_seconds() > 1800:
            print(f"[CHART] Refreshing {days}-day data for {symbol_key}...")
            ticks = generate_server_chart_data(symbol, days=days)
            chart_data_memory_cache[cache_key] = ticks
            chart_data_cache_timestamps[cache_key] = datetime.now(timezone.utc)
    
    ticks = chart_data_memory_cache[cache_key]
    
    # Aggregate ticks into candles based on requested interval
    candles = aggregate_ticks_to_candles(ticks, interval_seconds)
    
    print(f"[CHART] Serving {cache_key} interval={interval}: {len(candles)} candles from {len(ticks)} ticks")
    
    return {
        "symbol": symbol,
        "interval": interval,
        "days": days,
        "ticks": candles,  # Return candles as "ticks" for frontend compatibility
        "candle_count": len(candles),
        "max_days_available": 30,
        "last_updated": chart_data_cache_timestamps.get(cache_key, datetime.now(timezone.utc)).isoformat()
    }

@api_router.post("/chart/tick/{symbol}")
async def add_chart_tick(symbol: str, authorization: Optional[str] = Header(None), request: Request = None):
    """Add a new tick to the chart data - called periodically to keep data fresh
    If user has active trades, bias the price movement based on predetermined outcome
    
    Uses in-memory cache to avoid MongoDB 16MB document limit
    """
    global chart_data_memory_cache, chart_data_cache_timestamps
    
    symbol_key = symbol.replace("/", "_").replace(" ", "_").upper()
    
    # Get AI settings first
    god_mode_settings = await db.platform_settings.find_one({"_id": "god_mode"})
    ai_enabled = god_mode_settings.get("ai_enabled", True) if god_mode_settings else True
    ai_win_rate = god_mode_settings.get("ai_win_rate", 45) if god_mode_settings else 45
    
    # Check if user has active trades on this asset to bias price movement
    active_trade = None
    try:
        user = await get_current_user(authorization, request)
        if user:
            # Find active (pending) trade for this user and symbol
            symbol_clean = symbol.replace("_", "").replace(" ", "").upper()  # USDCHF
            
            # For forex pairs like USDCHF, split into USD/CHF
            if len(symbol_clean) == 6:
                symbol_with_slash = symbol_clean[:3] + "/" + symbol_clean[3:]  # USD/CHF
            else:
                symbol_with_slash = symbol_clean
            
            print(f"[TRADE SEARCH] Looking for pending trades - user={user.user_id}, symbol_clean={symbol_clean}, symbol_with_slash={symbol_with_slash}")
            
            active_trade = await db.trades.find_one({
                "user_id": user.user_id,
                "status": "pending",
                "$or": [
                    {"asset": {"$regex": symbol_clean, "$options": "i"}},
                    {"asset": {"$regex": symbol_with_slash, "$options": "i"}}
                ]
            })
            
            if active_trade:
                print(f"[TRADE SEARCH] Found active trade: {active_trade.get('trade_id')} for asset {active_trade.get('asset')}")
            else:
                print(f"[TRADE SEARCH] No pending trade found for {symbol_clean}")
            
            
            # If no predetermined_outcome but AI is enabled with 100%, force it
            if active_trade and not active_trade.get("predetermined_outcome") and ai_enabled and ai_win_rate >= 100:
                # For 100% win rate, ALWAYS win
                predetermined_outcome = "won"
                # Update the trade with predetermined_outcome
                await db.trades.update_one(
                    {"_id": active_trade["_id"]},
                    {"$set": {"predetermined_outcome": predetermined_outcome}}
                )
                active_trade["predetermined_outcome"] = predetermined_outcome
                print(f"[PRICE CONTROL] Updated trade with predetermined_outcome=won for 100% AI")
    except Exception as e:
        pass  # No auth or error, proceed without trade bias
    
    # Get existing data from memory cache (NOT MongoDB)
    ticks = chart_data_memory_cache.get(symbol_key, [])
    
    if len(ticks) == 0:
        # No cached data, generate it first
        print(f"[CHART TICK] No cache for {symbol_key}, generating...")
        ticks = generate_server_chart_data(symbol)
        chart_data_memory_cache[symbol_key] = ticks
        chart_data_cache_timestamps[symbol_key] = datetime.now(timezone.utc)
    
    if len(ticks) == 0:
        return {"error": "No chart data found"}
    
    # Generate new tick based on last tick
    last_tick = ticks[-1]
    now = int(datetime.now(timezone.utc).timestamp())
    
    # Handle last_tick["time"] being either int or datetime
    last_tick_time = last_tick["time"]
    if isinstance(last_tick_time, datetime):
        last_tick_time = int(last_tick_time.timestamp())
    
    # Only add new tick if at least 1 second has passed
    if now <= last_tick_time:
        # Return the current last tick so all clients stay synced
        return {
            "message": "Synced", 
            "new_tick": last_tick, 
            "ticks_count": len(ticks),
            "synced": True
        }
    
    # Use deterministic random based on timestamp so all requests get same result
    random.seed(now + hash(symbol_key))
    
    base_price = last_tick["close"]
    volatility = base_price * 0.0000008  # ULTRA TINY volatility - 0.00008%
    
    # ========== ULTRA SLOW SMOOTH MOVEMENT (Pocket Option Style) ==========
    current_second = now % 86400  # Seconds since midnight
    
    # Very slow amplitude changes (every 3-5 minutes)
    amplitude_cycle = (now // 200) % 100
    random.seed(hash(symbol_key) + amplitude_cycle)
    amp_variation = 0.7 + random.random() * 0.6  # 0.7 to 1.3x amplitude
    
    # Slow phase shifts
    phase_shift = (now // 100) * 0.2 + (hash(symbol_key) % 500)
    
    # ULTRA SLOW wave frequencies (10x slower than before)
    # These create very gradual, smooth movement
    fast_wave = math.sin((current_second * 0.012) + phase_shift) * 0.3 * amp_variation
    medium_wave = math.sin((current_second * 0.004) + phase_shift * 0.5) * 0.4 * (1.5 - amp_variation * 0.3)
    slow_wave = math.sin((current_second * 0.001) + phase_shift * 0.2) * 0.3
    
    # Combine waves - very smooth directional movement
    combined_direction = fast_wave + medium_wave + slow_wave
    
    # Ultra small change for smooth appearance
    change = combined_direction * volatility * 2
    
    # Tiny noise for natural micro-movement
    random.seed(now + hash(symbol_key))
    noise = (random.random() - 0.5) * volatility * 0.1
    change += noise
    
    # ========== PER-USER PRICE MANIPULATION ==========
    # Each user's active trade gets price manipulation based on their predetermined_outcome
    # This ensures AI Win Rate works for each user independently
    
    if active_trade:
        entry_price = active_trade.get("entry_price", base_price)
        trade_type = active_trade.get("trade_type")  # 'call' or 'put'
        predetermined_outcome = active_trade.get("predetermined_outcome")
        expires_at = active_trade.get("expires_at")
        
        # Calculate time remaining
        time_remaining = float('inf')
        if expires_at:
            now_dt = datetime.now(timezone.utc)
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
            time_remaining = (expires_at - now_dt).total_seconds()
        
        # Only manipulate in last 2 seconds before expiry
        if time_remaining <= 2:
            should_go_up = None
            
            # Use predetermined outcome to decide price direction
            if predetermined_outcome:
                if predetermined_outcome == "won":
                    should_go_up = (trade_type == "call")
                else:
                    should_go_up = (trade_type != "call")
            
            # Apply SUBTLE price manipulation - natural looking candles
            if should_go_up is not None:
                # Calculate how much we need to move to be profitable
                # Use very small, natural-looking increments
                min_profit_diff = entry_price * 0.00005  # Just 0.005% above/below entry (very small)
                
                # Use deterministic random for manipulation (seed is still set)
                manip_random = random.random()
                
                if should_go_up:
                    # Need price to be slightly ABOVE entry
                    target_price = entry_price + min_profit_diff
                    if base_price < target_price:
                        # Gradually move up - small natural change
                        change = min(volatility * 1.2, target_price - base_price + volatility * 0.3)
                    else:
                        # Already above entry, just add small positive movement
                        change = abs(manip_random * volatility * 0.8)
                else:
                    # Need price to be slightly BELOW entry
                    target_price = entry_price - min_profit_diff
                    if base_price > target_price:
                        # Gradually move down - small natural change
                        change = -min(volatility * 1.2, base_price - target_price + volatility * 0.3)
                    else:
                        # Already below entry, just add small negative movement
                        change = -abs(manip_random * volatility * 0.8)
    
    # Create new tick with deterministic values (while seed is still set)
    high_offset = abs((random.random() - 0.5) * volatility * 0.3)
    low_offset = abs((random.random() - 0.5) * volatility * 0.3)
    
    new_tick = {
        "time": now,
        "open": round(base_price, 6),
        "high": round(max(base_price, base_price + change) + high_offset, 6),
        "low": round(min(base_price, base_price + change) - low_offset, 6),
        "close": round(base_price + change, 6)
    }
    
    # Reset random seed AFTER creating the tick
    random.seed()
    
    ticks.append(new_tick)
    
    # Keep only last 900000 ticks in memory (enough for 10+ days)
    if len(ticks) > 900000:
        ticks = ticks[-900000:]
    
    # Update memory cache (NOT MongoDB - avoids 16MB limit)
    chart_data_memory_cache[symbol_key] = ticks
    
    return {"message": "Tick added", "new_tick": new_tick, "ticks_count": len(ticks), "synced": True}

# Note: app.include_router(api_router) moved to end of file to include all routes

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()

# ============= ADMIN API ENDPOINTS =============

@api_router.get("/admin/stats")
async def admin_get_stats(
    authorization: Optional[str] = Header(None), 
    request: Request = None,
    period: str = "all"  # 24h, 7d, 30d, 90d, all
):
    """Get admin dashboard statistics with time period filter"""
    user = await get_current_user(authorization, request)
    
    # Calculate date filter based on period
    date_filter = {}
    now = datetime.now(timezone.utc)
    
    if period == "24h":
        start_date = now - timedelta(hours=24)
        date_filter = {"created_at": {"$gte": start_date}}
    elif period == "7d":
        start_date = now - timedelta(days=7)
        date_filter = {"created_at": {"$gte": start_date}}
    elif period == "30d":
        start_date = now - timedelta(days=30)
        date_filter = {"created_at": {"$gte": start_date}}
    elif period == "90d":
        start_date = now - timedelta(days=90)
        date_filter = {"created_at": {"$gte": start_date}}
    # 'all' = no date filter
    
    # Get total users (registered in period for filtered, or all)
    if date_filter:
        total_users = await db.users.count_documents(date_filter)
    else:
        total_users = await db.users.count_documents({})
    
    # Get total trades in period
    trades_filter = date_filter.copy() if date_filter else {}
    total_trades = await db.trades.count_documents(trades_filter)
    
    # Get total volume in period
    volume_pipeline = [
        {"$match": trades_filter} if trades_filter else {"$match": {}},
        {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
    ]
    volume_result = await db.trades.aggregate(volume_pipeline).to_list(1)
    total_volume = volume_result[0]["total"] if volume_result else 0
    
    # Get total deposits in period
    deposits_filter = {"status": "completed"}
    if date_filter:
        deposits_filter.update(date_filter)
    deposits_pipeline = [
        {"$match": deposits_filter},
        {"$group": {"_id": None, "total": {"$sum": {"$ifNull": ["$amount_usd", {"$ifNull": ["$amount", 0]}]}}}}
    ]
    deposits_result = await db.deposits.aggregate(deposits_pipeline).to_list(1)
    total_deposits = deposits_result[0]["total"] if deposits_result else 0
    
    # Get total withdrawals in period - BOTH collections
    withdrawals_filter_base = {"status": "completed"}
    if date_filter:
        withdrawals_filter_base.update(date_filter)
    
    # From transactions collection (USDT/NOWPayments)
    tx_withdrawals_pipeline = [
        {"$match": {**withdrawals_filter_base, "type": "withdrawal"}},
        {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
    ]
    tx_withdrawals_result = await db.transactions.aggregate(tx_withdrawals_pipeline).to_list(1)
    tx_total = tx_withdrawals_result[0]["total"] if tx_withdrawals_result else 0
    
    # From withdrawals collection (TarsPay/E-Wallet)
    wd_withdrawals_pipeline = [
        {"$match": withdrawals_filter_base},
        {"$group": {"_id": None, "total": {"$sum": {"$ifNull": ["$amount_usd", "$amount"]}}}}
    ]
    wd_withdrawals_result = await db.withdrawals.aggregate(wd_withdrawals_pipeline).to_list(1)
    wd_total = wd_withdrawals_result[0]["total"] if wd_withdrawals_result else 0
    
    total_withdrawals = tx_total + wd_total
    
    # Get pending counts (always current)
    pending_withdrawals = await db.transactions.count_documents({"type": "withdrawal", "status": "pending"})
    pending_deposits = await db.deposits.count_documents({"status": "pending"})
    
    # Active users in period
    if date_filter:
        active_users = await db.trades.distinct("user_id", trades_filter)
    else:
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        active_users = await db.trades.distinct("user_id", {"created_at": {"$gte": today}})
    
    # Calculate platform profit (deposits - withdrawals)
    platform_profit = total_deposits - total_withdrawals
    
    return {
        "total_users": total_users,
        "total_trades": total_trades,
        "total_volume": total_volume,
        "total_deposits": total_deposits,
        "total_withdrawals": total_withdrawals,
        "platform_profit": platform_profit,
        "pending_withdrawals": pending_withdrawals,
        "pending_deposits": pending_deposits,
        "active_users_today": len(active_users),
        "period": period
    }

@api_router.get("/admin/top-traders")
async def admin_get_top_traders(
    authorization: Optional[str] = Header(None), 
    request: Request = None,
    period: str = "all",  # 24h, 7d, 30d, 90d, all
    limit: int = 10
):
    """Get top traders by trading volume for admin dashboard"""
    user = await get_current_user(authorization, request)
    
    # Calculate date filter based on period
    match_filter: dict = {"status": {"$in": ["won", "lost"]}}
    now = datetime.now(timezone.utc)
    
    if period == "24h":
        match_filter["created_at"] = {"$gte": now - timedelta(hours=24)}
    elif period == "7d":
        match_filter["created_at"] = {"$gte": now - timedelta(days=7)}
    elif period == "30d":
        match_filter["created_at"] = {"$gte": now - timedelta(days=30)}
    elif period == "90d":
        match_filter["created_at"] = {"$gte": now - timedelta(days=90)}
    # 'all' = no date filter
    
    # Aggregate trades to get volume and stats per user
    pipeline = [
        {"$match": match_filter},
        {
            "$group": {
                "_id": "$user_id",
                "total_volume": {"$sum": "$amount"},
                "total_trades": {"$sum": 1},
                "won_trades": {"$sum": {"$cond": [{"$eq": ["$status", "won"]}, 1, 0]}},
                "total_profit": {"$sum": "$profit_loss"},
            }
        },
        {"$sort": {"total_volume": -1}},  # Sort by volume descending
        {"$limit": limit}
    ]
    
    results = await db.trades.aggregate(pipeline).to_list(limit)
    
    # Fetch user details
    top_traders = []
    for i, result in enumerate(results):
        user_doc = await db.users.find_one(
            {"user_id": result["_id"]},
            {"_id": 0, "user_id": 1, "email": 1, "name": 1, "full_name": 1, "account_id": 1}
        )
        
        if user_doc:
            win_rate = (result["won_trades"] / result["total_trades"] * 100) if result["total_trades"] > 0 else 0
            top_traders.append({
                "rank": i + 1,
                "user_id": result["_id"],
                "email": user_doc.get("email", "N/A"),
                "name": user_doc.get("name") or user_doc.get("full_name") or "Unnamed",
                "account_id": user_doc.get("account_id", "N/A"),
                "total_volume": round(result["total_volume"], 2),
                "total_trades": result["total_trades"],
                "win_rate": round(win_rate, 1),
                "total_profit": round(result["total_profit"], 2),
            })
    
    return {"top_traders": top_traders, "period": period}

@api_router.get("/admin/users")
async def admin_get_users(
    authorization: Optional[str] = Header(None), 
    request: Request = None,
    search: Optional[str] = None,
    limit: int = 500
):
    """Get all users for admin with optional search"""
    user = await get_current_user(authorization, request)
    
    # Build search query
    query = {}
    if search and search.strip():
        search_term = search.strip()
        # Search by email, name, account_id, or user_id
        query["$or"] = [
            {"email": {"$regex": search_term, "$options": "i"}},
            {"name": {"$regex": search_term, "$options": "i"}},
            {"full_name": {"$regex": search_term, "$options": "i"}},
            {"account_id": {"$regex": search_term, "$options": "i"}},
            {"user_id": {"$regex": search_term, "$options": "i"}}
        ]
        # Also try exact match for account_id (numeric)
        if search_term.isdigit():
            query["$or"].append({"account_id": search_term})
            query["$or"].append({"account_id": int(search_term)})
    
    users = await db.users.find(query).sort("created_at", -1).limit(limit).to_list(limit)
    
    # Get KYC status for all users
    user_ids = [u.get("user_id") for u in users]
    kyc_submissions = await db.kyc_submissions.find(
        {"user_id": {"$in": user_ids}},
        sort=[("created_at", -1)]
    ).to_list(None)
    
    # Create a map of user_id -> kyc_status (most recent submission)
    kyc_status_map = {}
    for kyc in kyc_submissions:
        uid = kyc.get("user_id")
        if uid not in kyc_status_map:  # Only take the latest
            kyc_status_map[uid] = kyc.get("status", "not_submitted")
    
    return {
        "users": [
            {
                "user_id": u.get("user_id"),
                "email": u.get("email"),
                "name": u.get("name") or u.get("full_name"),
                "account_id": u.get("account_id"),
                "real_balance": u.get("real_balance", 0),
                "demo_balance": u.get("demo_balance", 10000),
                "bonus_balance": u.get("bonus_balance", 0),
                "is_verified": u.get("is_verified", False),
                "is_banned": u.get("is_banned", False),
                "is_deleted": u.get("is_deleted", False),
                "is_admin": u.get("is_admin", False),
                "country": u.get("country"),
                "country_flag": u.get("country_flag"),
                "created_at": str(u.get("created_at", "")),
                "kyc_status": kyc_status_map.get(u.get("user_id"), "not_submitted"),
                "kyc_verified": u.get("kyc_verified", False)
            }
            for u in users
        ]
    }

@api_router.get("/admin/trades")
async def admin_get_trades(
    limit: int = 50,
    status: Optional[str] = None,
    account_type: Optional[str] = None,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get all trades for admin with optional status and account_type filter"""
    user = await get_current_user(authorization, request)
    
    # Build query with optional filters
    query = {}
    if status == "active":
        query["status"] = "pending"
    elif status:
        query["status"] = status
    
    # Filter by account type (real/demo)
    if account_type:
        query["account_type"] = account_type
    
    trades = await db.trades.find(query).sort("created_at", -1).limit(limit).to_list(limit)
    
    # Get user names for all trades
    user_ids = list(set([t.get("user_id") for t in trades if t.get("user_id")]))
    users_data = await db.users.find({"user_id": {"$in": user_ids}}).to_list(None)
    user_names = {u.get("user_id"): u.get("name") or u.get("email", "Unknown") for u in users_data}
    
    # Calculate volume stats
    call_volume = sum(t.get("amount", 0) for t in trades if t.get("trade_type") == "call" or t.get("direction") == "up")
    put_volume = sum(t.get("amount", 0) for t in trades if t.get("trade_type") == "put" or t.get("direction") == "down")
    
    return {
        "trades": [
            {
                "trade_id": t.get("trade_id"),
                "user_id": t.get("user_id"),
                "user_name": user_names.get(t.get("user_id"), "Unknown User"),
                "asset": t.get("asset"),
                "amount": t.get("amount"),
                "direction": t.get("direction") or ("up" if t.get("trade_type") == "call" else "down"),
                "trade_type": t.get("trade_type"),
                "status": t.get("status"),
                "profit_loss": t.get("profit_loss", 0),
                "entry_price": t.get("entry_price"),
                "exit_price": t.get("exit_price"),
                "payout_percentage": t.get("payout_percentage", 95),
                "duration": t.get("duration", 60),
                "account_type": t.get("account_type", "demo"),
                "predetermined_outcome": t.get("predetermined_outcome"),
                "expires_at": str(t.get("expires_at", "")) if t.get("expires_at") else None,
                "created_at": str(t.get("created_at", ""))
            }
            for t in trades
        ],
        "stats": {
            "total_trades": len(trades),
            "call_volume": call_volume,
            "put_volume": put_volume
        }
    }

@api_router.get("/admin/deposits")
async def admin_get_deposits(authorization: Optional[str] = Header(None), request: Request = None):
    """Get all deposits for admin"""
    user = await get_current_user(authorization, request)
    
    # Auto-expire pending deposits older than 12 minutes
    twelve_minutes_ago = datetime.now(timezone.utc) - timedelta(minutes=12)
    await db.deposits.update_many(
        {
            "status": "pending",
            "created_at": {"$lt": twelve_minutes_ago}
        },
        {
            "$set": {
                "status": "expired",
                "expired_at": datetime.now(timezone.utc)
            }
        }
    )
    
    deposits = await db.deposits.find({}).sort("created_at", -1).limit(100).to_list(100)
    
    return {
        "deposits": [
            {
                "_id": str(d.get("_id")),
                "user_id": d.get("user_id"),
                "amount_usd": d.get("amount_usd") or d.get("amount", 0),
                "status": d.get("status"),
                "payment_type": d.get("payment_type", "crypto"),
                "created_at": str(d.get("created_at", ""))
            }
            for d in deposits
        ]
    }

@api_router.post("/admin/users/{user_id}/balance")
async def admin_update_user_balance(
    user_id: str,
    balance_type: str = "real",
    amount: float = 0,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Update user balance (admin only)"""
    admin = await get_current_user(authorization, request)
    
    # Get request body
    try:
        body = await request.json()
        balance_type = body.get("balance_type", "real")
        amount = body.get("amount", 0)
    except:
        pass
    
    # Update balance field
    field_map = {
        "real": "real_balance",
        "demo": "demo_balance",
        "bonus": "bonus_balance"
    }
    
    field = field_map.get(balance_type, "real_balance")
    
    result = await db.users.update_one(
        {"user_id": user_id},
        {"$set": {field: float(amount)}}
    )
    
    if result.modified_count > 0:
        return {"success": True, "message": f"Updated {balance_type} balance to ${amount}"}
    else:
        raise HTTPException(status_code=404, detail="User not found")



# ============= ADVANCED ADMIN ANALYTICS =============

@api_router.get("/admin/analytics")
async def get_admin_analytics(
    period: str = "week",  # week, month, year
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get analytics data with time filters"""
    user = await get_current_user(authorization, request)
    
    now = datetime.now(timezone.utc)
    
    # Calculate date range based on period
    if period == "week":
        start_date = now - timedelta(days=7)
        group_format = "%Y-%m-%d"
        labels = [(now - timedelta(days=i)).strftime("%a") for i in range(6, -1, -1)]
    elif period == "month":
        start_date = now - timedelta(days=30)
        group_format = "%Y-%m-%d"
        labels = [(now - timedelta(days=i)).strftime("%d %b") for i in range(29, -1, -3)][::-1]
    else:  # year
        start_date = now - timedelta(days=365)
        group_format = "%Y-%m"
        labels = [(now - timedelta(days=i*30)).strftime("%b") for i in range(11, -1, -1)]
    
    # Get deposits by date
    deposits_pipeline = [
        {"$match": {"created_at": {"$gte": start_date}, "status": "completed"}},
        {"$group": {
            "_id": {"$dateToString": {"format": group_format, "date": "$created_at"}},
            "total": {"$sum": "$amount_usd"}
        }},
        {"$sort": {"_id": 1}}
    ]
    deposits_data = await db.deposits.aggregate(deposits_pipeline).to_list(100)
    
    # Get withdrawals by date
    withdrawals_pipeline = [
        {"$match": {"created_at": {"$gte": start_date}, "status": "completed"}},
        {"$group": {
            "_id": {"$dateToString": {"format": group_format, "date": "$created_at"}},
            "total": {"$sum": "$amount"}
        }},
        {"$sort": {"_id": 1}}
    ]
    withdrawals_data = await db.withdrawals.aggregate(withdrawals_pipeline).to_list(100)
    
    # Get profit/loss by date (platform profit = user losses)
    trades_pipeline = [
        {"$match": {"created_at": {"$gte": start_date}, "status": {"$in": ["won", "lost"]}}},
        {"$group": {
            "_id": {"$dateToString": {"format": group_format, "date": "$created_at"}},
            "platform_profit": {"$sum": {"$cond": [{"$eq": ["$status", "lost"]}, "$amount", {"$multiply": ["$profit_loss", -1]}]}}
        }},
        {"$sort": {"_id": 1}}
    ]
    profit_data = await db.trades.aggregate(trades_pipeline).to_list(100)
    
    # Calculate totals
    total_deposits = sum(d["total"] for d in deposits_data) if deposits_data else 0
    total_withdrawals = sum(w["total"] for w in withdrawals_data) if withdrawals_data else 0
    total_profit = sum(p["platform_profit"] for p in profit_data) if profit_data else 0
    
    return {
        "period": period,
        "labels": labels,
        "deposits": {
            "data": [d["total"] for d in deposits_data],
            "dates": [d["_id"] for d in deposits_data],
            "total": total_deposits
        },
        "withdrawals": {
            "data": [w["total"] for w in withdrawals_data],
            "dates": [w["_id"] for w in withdrawals_data],
            "total": total_withdrawals
        },
        "profit_loss": {
            "data": [p["platform_profit"] for p in profit_data],
            "dates": [p["_id"] for p in profit_data],
            "total": total_profit
        },
        "summary": {
            "net_revenue": total_deposits - total_withdrawals,
            "total_deposits": total_deposits,
            "total_withdrawals": total_withdrawals,
            "platform_profit": total_profit
        }
    }

# ============= MANUAL DEPOSIT SYSTEM =============

@api_router.post("/admin/manual-deposit")
async def create_manual_deposit(
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Admin manually adds deposit to user account"""
    admin = await get_current_user(authorization, request)
    
    body = await request.json()
    user_id = body.get("user_id")
    amount = float(body.get("amount", 0))
    balance_type = body.get("balance_type", "real")  # real, demo, bonus
    note = body.get("note", "Manual deposit by admin")
    
    if not user_id or amount <= 0:
        raise HTTPException(status_code=400, detail="Invalid user_id or amount")
    
    # Find user
    user = await db.users.find_one({"user_id": user_id})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Map balance type to field
    field_map = {
        "real": "real_balance",
        "demo": "demo_balance",
        "bonus": "bonus_balance"
    }
    field = field_map.get(balance_type, "real_balance")
    current_balance = user.get(field, 0)
    new_balance = current_balance + amount
    
    # Update user balance
    await db.users.update_one(
        {"user_id": user_id},
        {"$set": {field: new_balance}}
    )
    
    # Create deposit record
    deposit_id = f"manual_{uuid.uuid4().hex[:12]}"
    await db.deposits.insert_one({
        "deposit_id": deposit_id,
        "user_id": user_id,
        "amount_usd": amount,
        "balance_type": balance_type,
        "status": "completed",
        "payment_type": "manual",
        "note": note,
        "admin_id": admin.user_id,
        "created_at": datetime.now(timezone.utc)
    })
    
    return {
        "success": True,
        "message": f"Added ${amount} to {balance_type} balance",
        "deposit_id": deposit_id,
        "new_balance": new_balance
    }

# ============= ASSET MANAGEMENT =============

@api_router.get("/admin/assets")
async def get_admin_assets(authorization: Optional[str] = Header(None), request: Request = None):
    """Get all assets for admin management"""
    user = await get_current_user(authorization, request)
    
    assets = await db.assets.find({}).to_list(100)
    
    return {
        "assets": [
            {
                "asset_id": a.get("asset_id"),
                "symbol": a.get("symbol"),
                "name": a.get("name"),
                "category": a.get("category"),
                "payout_percentage": a.get("payout_percentage", 80),
                "is_active": a.get("is_active", True),
                "is_otc": a.get("is_otc", False),
                "min_amount": a.get("min_amount", 1),
                "max_amount": a.get("max_amount", 10000),
                "created_at": str(a.get("created_at", ""))
            }
            for a in assets
        ]
    }

@api_router.post("/admin/assets")
async def create_asset(authorization: Optional[str] = Header(None), request: Request = None):
    """Create new trading asset (including OTC)"""
    user = await get_current_user(authorization, request)
    
    body = await request.json()
    
    asset_id = f"asset_{uuid.uuid4().hex[:8]}"
    symbol = body.get("symbol", "").upper()
    name = body.get("name", symbol)
    category = body.get("category", "forex")  # forex, crypto, stocks, otc
    payout_percentage = float(body.get("payout_percentage", 80))
    is_otc = body.get("is_otc", False)
    min_amount = float(body.get("min_amount", 1))
    max_amount = float(body.get("max_amount", 10000))
    
    if not symbol:
        raise HTTPException(status_code=400, detail="Symbol is required")
    
    # Check if asset already exists
    existing = await db.assets.find_one({"symbol": symbol})
    if existing:
        raise HTTPException(status_code=400, detail="Asset with this symbol already exists")
    
    asset = {
        "asset_id": asset_id,
        "symbol": symbol,
        "name": name,
        "category": category,
        "payout_percentage": payout_percentage,
        "is_active": True,
        "is_otc": is_otc,
        "min_amount": min_amount,
        "max_amount": max_amount,
        "created_at": datetime.now(timezone.utc)
    }
    
    await db.assets.insert_one(asset)
    
    return {"success": True, "asset": asset}

@api_router.put("/admin/assets/{asset_id}")
async def update_asset(
    asset_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Update asset settings"""
    user = await get_current_user(authorization, request)
    
    body = await request.json()
    
    update_fields = {}
    if "name" in body:
        update_fields["name"] = body["name"]
    if "payout_percentage" in body:
        update_fields["payout_percentage"] = float(body["payout_percentage"])
    if "is_active" in body:
        update_fields["is_active"] = bool(body["is_active"])
    if "is_otc" in body:
        update_fields["is_otc"] = bool(body["is_otc"])
    if "min_amount" in body:
        update_fields["min_amount"] = float(body["min_amount"])
    if "max_amount" in body:
        update_fields["max_amount"] = float(body["max_amount"])
    if "category" in body:
        update_fields["category"] = body["category"]
    
    result = await db.assets.update_one(
        {"asset_id": asset_id},
        {"$set": update_fields}
    )
    
    if result.modified_count > 0:
        return {"success": True, "message": "Asset updated"}
    else:
        raise HTTPException(status_code=404, detail="Asset not found")

@api_router.post("/admin/assets/{asset_id}/toggle")
async def toggle_asset(
    asset_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Toggle asset on/off"""
    user = await get_current_user(authorization, request)
    
    asset = await db.assets.find_one({"asset_id": asset_id})
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")
    
    new_status = not asset.get("is_active", True)
    
    await db.assets.update_one(
        {"asset_id": asset_id},
        {"$set": {"is_active": new_status}}
    )
    
    return {"success": True, "is_active": new_status}

@api_router.delete("/admin/assets/{asset_id}")
async def delete_asset(
    asset_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Delete an asset"""
    user = await get_current_user(authorization, request)
    
    result = await db.assets.delete_one({"asset_id": asset_id})
    
    if result.deleted_count > 0:
        return {"success": True, "message": "Asset deleted"}
    else:
        raise HTTPException(status_code=404, detail="Asset not found")

# ============= WITHDRAWAL MANAGEMENT =============

@api_router.get("/admin/withdrawals")
async def get_admin_withdrawals(
    status: str = None,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get all withdrawals for admin - from both transactions and withdrawals collections"""
    user = await get_current_user(authorization, request)
    
    # Query from transactions collection where type is withdrawal
    tx_query = {"type": "withdrawal"}
    if status:
        tx_query["status"] = status
    
    tx_withdrawals = await db.transactions.find(tx_query).sort("created_at", -1).limit(100).to_list(100)
    
    # Also query from withdrawals collection (TarsPay E-Wallet withdrawals)
    wd_query = {}
    if status:
        wd_query["status"] = status
    wd_withdrawals = await db.withdrawals.find(wd_query).sort("created_at", -1).limit(100).to_list(100)
    
    # Merge and deduplicate by order_id/transaction_id
    seen_ids = set()
    result = []
    
    # Process transactions collection
    for w in tx_withdrawals:
        tx_id = w.get("transaction_id") or str(w.get("_id"))
        if tx_id in seen_ids:
            continue
        seen_ids.add(tx_id)
        
        user_info = await db.users.find_one({"user_id": w.get("user_id")})
        result.append({
            "withdrawal_id": tx_id,
            "order_id": w.get("order_id") or tx_id,
            "transaction_id": tx_id,
            "user_id": w.get("user_id"),
            "user_email": user_info.get("email") if user_info else "Unknown",
            "user_name": user_info.get("name") or user_info.get("full_name") if user_info else "Unknown",
            "amount": w.get("amount", 0),
            "amount_usd": w.get("amount", 0),
            "net_amount": w.get("net_amount", w.get("amount", 0)),
            "payment_type": w.get("payment_type", "usdt"),
            "method": "USDT_TRC20" if w.get("payment_type") == "nowpayments" else w.get("currency", "USDT"),
            "wallet_address": w.get("crypto_address", ""),
            "crypto_address": w.get("crypto_address", ""),
            "status": w.get("status", "pending"),
            "requires_admin_approval": w.get("requires_admin_approval", False),
            "created_at": str(w.get("created_at", "")),
            "completed_at": str(w.get("completed_at", "")) if w.get("completed_at") else None
        })
    
    # Process withdrawals collection (TarsPay)
    for w in wd_withdrawals:
        order_id = w.get("order_id") or w.get("transaction_id") or str(w.get("_id"))
        if order_id in seen_ids:
            continue
        seen_ids.add(order_id)
        
        user_info = await db.users.find_one({"user_id": w.get("user_id")})
        result.append({
            "withdrawal_id": order_id,
            "order_id": order_id,
            "transaction_id": w.get("transaction_id") or order_id,
            "user_id": w.get("user_id"),
            "user_email": user_info.get("email") if user_info else "Unknown",
            "user_name": user_info.get("name") or user_info.get("full_name") if user_info else "Unknown",
            "amount": w.get("amount_usd", w.get("amount", 0)),
            "amount_usd": w.get("amount_usd", w.get("amount", 0)),
            "amount_bdt": w.get("amount_bdt", 0),
            "net_amount_bdt": w.get("net_amount_bdt", 0),
            "payment_type": w.get("payment_type", "tarspay"),
            "channel": w.get("channel", ""),
            "channel_name": w.get("channel_name", "E-Wallet"),
            "wallet_id": w.get("wallet_id", ""),
            "method": w.get("channel_name", "E-Wallet"),
            "wallet_address": w.get("wallet_id", ""),
            "crypto_address": w.get("crypto_address", ""),
            "status": w.get("status", "pending"),
            "requires_admin_approval": w.get("requires_admin_approval", False),
            "created_at": str(w.get("created_at", "")),
            "completed_at": str(w.get("completed_at", "")) if w.get("completed_at") else None
        })
    
    # Sort by created_at descending
    result.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    
    return {"withdrawals": result[:200]}

@api_router.post("/admin/withdrawals/{withdrawal_id}/approve")
async def approve_withdrawal(
    withdrawal_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Approve a withdrawal request and process via NOWPayments/TarsPay"""
    admin = await get_current_user(authorization, request)
    
    # Check both withdrawals and transactions collections
    withdrawal = await db.withdrawals.find_one({
        "$or": [
            {"withdrawal_id": withdrawal_id},
            {"order_id": withdrawal_id},
            {"transaction_id": withdrawal_id},
            {"_id": withdrawal_id}
        ]
    })
    
    # Also check transactions collection for legacy withdrawals
    if not withdrawal:
        withdrawal = await db.transactions.find_one({
            "type": "withdrawal",
            "$or": [
                {"transaction_id": withdrawal_id},
                {"_id": withdrawal_id}
            ]
        })
    
    if not withdrawal:
        raise HTTPException(status_code=404, detail="Withdrawal not found")
    
    current_status = withdrawal.get("status")
    if current_status not in ["pending", "pending_approval"]:
        raise HTTPException(status_code=400, detail=f"Withdrawal is not pending (current status: {current_status})")
    
    user_id = withdrawal.get("user_id")
    payment_type = withdrawal.get("payment_type", "")
    
    # Get callback URLs
    integration_proxy = os.environ.get("INTEGRATION_PROXY_URL", "")
    host = request.headers.get("host", "localhost")
    scheme = "https" if any(x in host for x in ["preview.emergentagent.com", "preview.emergentcf.cloud", "emergent.host"]) else request.url.scheme
    base_url = f"{scheme}://{host}"
    
    # Handle based on payment type
    if payment_type == "nowpayments":
        # Process USDT TRC20 withdrawal via NOWPayments
        crypto_address = withdrawal.get("crypto_address")
        net_amount = withdrawal.get("net_amount", 0)
        transaction_id = withdrawal.get("transaction_id") or withdrawal.get("order_id")
        
        callback_url = f"{integration_proxy}/api/nowpayments/withdrawal/callback" if integration_proxy else f"{base_url}/api/nowpayments/withdrawal/callback"
        
        print(f"[Admin] Approving NOWPayments withdrawal: {transaction_id}, amount={net_amount}")
        payout_result = await create_usdt_payout(
            address=crypto_address,
            amount=net_amount,
            external_id=transaction_id,
            callback_url=callback_url
        )
        
        if payout_result.get("success"):
            payout_data = payout_result.get("data", {})
            payout_id = payout_data.get("id") or payout_data.get("payout_id")
            np_status = payout_data.get("status", "waiting")
            new_status = "processing" if np_status in ["waiting", "confirming", "sending"] else ("completed" if np_status == "finished" else "pending")
            
            # Update in both collections
            update_data = {
                "status": new_status,
                "nowpayments_payout_id": payout_id,
                "approved_by": admin.user_id,
                "approved_at": datetime.now(timezone.utc)
            }
            await db.withdrawals.update_one(
                {"$or": [{"order_id": transaction_id}, {"transaction_id": transaction_id}]},
                {"$set": update_data}
            )
            await db.transactions.update_one(
                {"transaction_id": transaction_id},
                {"$set": update_data}
            )
            
            # Notify user
            await create_notification(
                user_id,
                "✅ Withdrawal Approved",
                f"Your USDT TRC20 withdrawal has been approved and is being processed.",
                "success"
            )
            
            return {"success": True, "message": "Withdrawal approved and sent to NOWPayments", "payout_id": payout_id}
        else:
            error = payout_result.get("error", "NOWPayments API error")
            print(f"[Admin] NOWPayments error: {error}")
            return {"success": False, "error": error}
    
    elif payment_type == "tarspay":
        # Process E-Wallet withdrawal via TarsPay
        order_id = withdrawal.get("order_id")
        net_amount_bdt = withdrawal.get("net_amount_bdt", 0)
        wallet_id = withdrawal.get("wallet_id")
        way_code = withdrawal.get("way_code")
        channel_name = withdrawal.get("channel_name")
        
        notify_url = f"{integration_proxy}/api/tarspay/withdrawal/callback" if integration_proxy else f"{base_url}/api/tarspay/withdrawal/callback"
        
        print(f"[Admin] Approving TarsPay withdrawal: {order_id}, amount={net_amount_bdt} BDT")
        result = await tarspay_service.create_withdrawal(
            order_id=order_id,
            amount_bdt=net_amount_bdt,
            wallet_id=wallet_id,
            way_code=way_code,
            notify_url=notify_url
        )
        
        if result.get("success"):
            await db.withdrawals.update_one(
                {"order_id": order_id},
                {
                    "$set": {
                        "status": "pending",
                        "payment_id": result.get("payment_id"),
                        "approved_by": admin.user_id,
                        "approved_at": datetime.now(timezone.utc)
                    }
                }
            )
            
            # Notify user
            await create_notification(
                user_id,
                "✅ Withdrawal Approved",
                f"Your withdrawal of ৳{net_amount_bdt} to {channel_name} has been approved and is being processed.",
                "success"
            )
            
            return {"success": True, "message": f"Withdrawal approved and sent to {channel_name}"}
        else:
            error = result.get("error", "TarsPay API error")
            print(f"[Admin] TarsPay error: {error}")
            return {"success": False, "error": error}
    
    else:
        # Legacy withdrawal - just mark as completed
        collection = db.withdrawals if withdrawal.get("order_id") else db.transactions
        id_field = "order_id" if withdrawal.get("order_id") else "transaction_id"
        id_value = withdrawal.get("order_id") or withdrawal.get("transaction_id")
        
        await collection.update_one(
            {id_field: id_value},
            {
                "$set": {
                    "status": "completed",
                    "approved_by": admin.user_id,
                    "approved_at": datetime.now(timezone.utc)
                }
            }
        )
        
        await create_notification(
            user_id,
            "✅ Withdrawal Approved",
            f"Your withdrawal has been approved.",
            "success"
        )
        
        return {"success": True, "message": "Withdrawal approved"}

@api_router.post("/admin/withdrawals/{withdrawal_id}/reject")
async def reject_withdrawal(
    withdrawal_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Reject a withdrawal request and refund balance"""
    admin = await get_current_user(authorization, request)
    
    body = await request.json()
    reason = body.get("reason", "Rejected by admin")
    
    withdrawal = await db.withdrawals.find_one({
        "$or": [
            {"withdrawal_id": withdrawal_id},
            {"_id": withdrawal_id}
        ]
    })
    
    # Also check transactions collection for legacy withdrawals
    if not withdrawal:
        withdrawal = await db.transactions.find_one({
            "type": "withdrawal",
            "$or": [
                {"transaction_id": withdrawal_id},
                {"_id": withdrawal_id}
            ]
        })
        if withdrawal:
            if withdrawal.get("status") != "pending":
                raise HTTPException(status_code=400, detail="Withdrawal is not pending")
            
            # Refund the amount to user
            await db.users.update_one(
                {"user_id": withdrawal["user_id"]},
                {"$inc": {"real_balance": withdrawal.get("amount", 0)}}
            )
            
            await db.transactions.update_one(
                {"_id": withdrawal["_id"]},
                {
                    "$set": {
                        "status": "rejected",
                        "rejected_by": admin.user_id,
                        "rejected_at": datetime.now(timezone.utc),
                        "rejection_reason": reason
                    }
                }
            )
            return {"success": True, "message": "Withdrawal rejected and balance refunded"}
    
    if not withdrawal:
        raise HTTPException(status_code=404, detail="Withdrawal not found")
    
    if withdrawal.get("status") != "pending":
        raise HTTPException(status_code=400, detail="Withdrawal is not pending")
    
    # Refund the amount to user
    await db.users.update_one(
        {"user_id": withdrawal["user_id"]},
        {"$inc": {"real_balance": withdrawal.get("amount", 0)}}
    )
    
    await db.withdrawals.update_one(
        {"_id": withdrawal["_id"]},
        {
            "$set": {
                "status": "rejected",
                "rejected_by": admin.user_id,
                "rejected_at": datetime.now(timezone.utc),
                "rejection_reason": reason
            }
        }
    )
    
    return {"success": True, "message": "Withdrawal rejected and balance refunded"}


@api_router.get("/admin/withdrawals/{user_id}/user-stats")
async def get_withdrawal_user_stats(
    user_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get user stats for withdrawal review"""
    admin = await get_current_user(authorization, request)
    
    # Get user info
    user = await db.users.find_one({"user_id": user_id})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Calculate total deposits
    deposits = await db.transactions.find({
        "user_id": user_id,
        "type": "deposit",
        "status": "completed"
    }).to_list(1000)
    total_deposit = sum(d.get("amount", 0) for d in deposits)
    
    # Calculate total withdrawals (completed)
    withdrawals = await db.transactions.find({
        "user_id": user_id,
        "type": "withdrawal",
        "status": "completed"
    }).to_list(1000)
    total_withdraw = sum(w.get("amount", 0) for w in withdrawals)
    
    # Calculate trading stats
    trades = await db.trades.find({"user_id": user_id}).to_list(10000)
    total_trades = len(trades)
    won_trades = sum(1 for t in trades if t.get("result") == "win")
    lost_trades = sum(1 for t in trades if t.get("result") == "loss")
    
    # Calculate profit from trades
    total_profit = 0
    for trade in trades:
        if trade.get("result") == "win":
            payout = trade.get("payout_rate", 85)
            total_profit += trade.get("amount", 0) * (payout / 100)
        elif trade.get("result") == "loss":
            total_profit -= trade.get("amount", 0)
    
    # Profit rate
    profit_rate = (won_trades / total_trades * 100) if total_trades > 0 else 0
    
    # Current balance (real_balance already includes bonus, don't double-count)
    total_balance = user.get("real_balance", 0)
    
    return {
        "user_id": user_id,
        "email": user.get("email", ""),
        "name": user.get("name") or user.get("full_name", ""),
        "total_deposit": total_deposit,
        "total_withdraw": total_withdraw,
        "total_profit": total_profit,
        "profit_rate": round(profit_rate, 2),
        "total_balance": total_balance,
        "real_balance": user.get("real_balance", 0),
        "bonus_balance": user.get("bonus_balance", 0),
        "total_trades": total_trades,
        "won_trades": won_trades,
        "lost_trades": lost_trades,
        "kyc_verified": user.get("kyc_verified", False),
        "created_at": str(user.get("created_at", ""))
    }


@api_router.post("/admin/withdrawals/{withdrawal_id}/lock")
async def lock_withdrawal(
    withdrawal_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Lock a withdrawal request and require additional KYC"""
    admin = await get_current_user(authorization, request)
    
    body = await request.json()
    kyc_requirement = body.get("kyc_requirement", "Bank Statement")
    lock_reason = body.get("reason", "Additional verification required")
    
    # Find the withdrawal in transactions collection
    withdrawal = await db.transactions.find_one({
        "type": "withdrawal",
        "$or": [
            {"transaction_id": withdrawal_id},
            {"_id": withdrawal_id}
        ]
    })
    
    if not withdrawal:
        # Also check legacy withdrawals collection
        withdrawal = await db.withdrawals.find_one({
            "$or": [
                {"withdrawal_id": withdrawal_id},
                {"_id": withdrawal_id}
            ]
        })
        if withdrawal:
            await db.withdrawals.update_one(
                {"_id": withdrawal["_id"]},
                {
                    "$set": {
                        "status": "locked",
                        "locked_by": admin.user_id,
                        "locked_at": datetime.now(timezone.utc),
                        "lock_reason": lock_reason,
                        "kyc_requirement": kyc_requirement,
                        "kyc_submitted": False
                    }
                }
            )
            return {"success": True, "message": f"Withdrawal locked. User must submit: {kyc_requirement}"}
    
    if not withdrawal:
        raise HTTPException(status_code=404, detail="Withdrawal not found")
    
    if withdrawal.get("status") not in ["pending", "locked"]:
        raise HTTPException(status_code=400, detail="Cannot lock this withdrawal")
    
    await db.transactions.update_one(
        {"_id": withdrawal["_id"]},
        {
            "$set": {
                "status": "locked",
                "locked_by": admin.user_id,
                "locked_at": datetime.now(timezone.utc),
                "lock_reason": lock_reason,
                "kyc_requirement": kyc_requirement,
                "kyc_submitted": False
            }
        }
    )
    
    return {"success": True, "message": f"Withdrawal locked. User must submit: {kyc_requirement}"}


@api_router.post("/admin/withdrawals/{withdrawal_id}/unlock")
async def unlock_withdrawal(
    withdrawal_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Unlock a locked withdrawal"""
    admin = await get_current_user(authorization, request)
    
    # Find the withdrawal in transactions collection
    withdrawal = await db.transactions.find_one({
        "type": "withdrawal",
        "$or": [
            {"transaction_id": withdrawal_id},
            {"_id": withdrawal_id}
        ]
    })
    
    if not withdrawal:
        raise HTTPException(status_code=404, detail="Withdrawal not found")
    
    if withdrawal.get("status") != "locked":
        raise HTTPException(status_code=400, detail="Withdrawal is not locked")
    
    await db.transactions.update_one(
        {"_id": withdrawal["_id"]},
        {
            "$set": {
                "status": "pending",
                "unlocked_by": admin.user_id,
                "unlocked_at": datetime.now(timezone.utc)
            },
            "$unset": {
                "locked_by": "",
                "locked_at": "",
                "lock_reason": "",
                "kyc_requirement": "",
                "kyc_submitted": "",
                "kyc_document_url": ""
            }
        }
    )
    
    return {"success": True, "message": "Withdrawal unlocked and moved back to pending"}


@api_router.post("/admin/withdrawals/{withdrawal_id}/approve-kyc")
async def approve_kyc_document(
    withdrawal_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Approve KYC document and move withdrawal back to pending for final approval"""
    admin = await get_current_user(authorization, request)
    
    # Find the withdrawal in transactions collection
    withdrawal = await db.transactions.find_one({
        "type": "withdrawal",
        "$or": [
            {"transaction_id": withdrawal_id},
            {"_id": withdrawal_id}
        ]
    })
    
    if not withdrawal:
        raise HTTPException(status_code=404, detail="Withdrawal not found")
    
    if withdrawal.get("status") != "locked":
        raise HTTPException(status_code=400, detail="Withdrawal is not locked")
    
    # Move to pending with KYC approved flag
    await db.transactions.update_one(
        {"_id": withdrawal["_id"]},
        {
            "$set": {
                "status": "pending",
                "kyc_approved": True,
                "kyc_approved_by": admin.user_id,
                "kyc_approved_at": datetime.now(timezone.utc)
            },
            "$unset": {
                "locked_by": "",
                "locked_at": "",
                "lock_reason": ""
            }
        }
    )
    
    return {"success": True, "message": "KYC approved. Withdrawal moved to pending for final approval."}


@api_router.post("/withdraw/upload-kyc/{transaction_id}")
async def upload_kyc_document(
    transaction_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Upload KYC document for a locked withdrawal"""
    user = await get_current_user(authorization, request)
    
    body = await request.json()
    document_url = body.get("document_url", "")
    document_type = body.get("document_type", "")
    
    if not document_url:
        raise HTTPException(status_code=400, detail="Document URL is required")
    
    # Find the withdrawal
    withdrawal = await db.transactions.find_one({
        "transaction_id": transaction_id,
        "user_id": user.user_id,
        "type": "withdrawal",
        "status": "locked"
    })
    
    if not withdrawal:
        raise HTTPException(status_code=404, detail="Locked withdrawal not found")
    
    # Update with KYC document
    await db.transactions.update_one(
        {"_id": withdrawal["_id"]},
        {
            "$set": {
                "kyc_submitted": True,
                "kyc_document_url": document_url,
                "kyc_document_type": document_type,
                "kyc_submitted_at": datetime.now(timezone.utc)
            }
        }
    )
    
    return {"success": True, "message": "KYC document uploaded successfully. Awaiting admin review."}


@api_router.get("/withdraw/check-locked")
async def check_locked_withdrawal(
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Check if user has any locked withdrawal"""
    user = await get_current_user(authorization, request)
    
    locked_withdrawal = await db.transactions.find_one({
        "user_id": user.user_id,
        "type": "withdrawal",
        "status": "locked"
    })
    
    if locked_withdrawal:
        return {
            "has_locked": True,
            "transaction_id": locked_withdrawal.get("transaction_id"),
            "amount": locked_withdrawal.get("amount"),
            "kyc_requirement": locked_withdrawal.get("kyc_requirement", "Bank Statement"),
            "kyc_submitted": locked_withdrawal.get("kyc_submitted", False)
        }
    
    return {"has_locked": False}


@api_router.get("/admin/kyc-submissions")
async def get_kyc_submissions(
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get all withdrawals with KYC submissions"""
    admin = await get_current_user(authorization, request)
    
    submissions = await db.transactions.find({
        "type": "withdrawal",
        "status": "locked",
        "kyc_submitted": True
    }).to_list(100)
    
    result = []
    for sub in submissions:
        user = await db.users.find_one({"user_id": sub.get("user_id")})
        
        # Get user's verified profile info
        verified_name = ""
        verified_id = ""
        kyc_verified = False
        
        if user:
            kyc_verified = user.get("kyc_verified", False)
            # Check for ID verification data
            verified_name = user.get("kyc_full_name") or user.get("verified_name") or user.get("name") or user.get("full_name", "")
            verified_id = user.get("kyc_id_number") or user.get("id_number") or user.get("verified_id", "")
        
        result.append({
            "transaction_id": sub.get("transaction_id"),
            "user_id": sub.get("user_id"),
            "user_email": user.get("email") if user else "Unknown",
            "user_name": user.get("name") or user.get("full_name", "") if user else "",
            "verified_name": verified_name,
            "verified_id": verified_id,
            "kyc_verified": kyc_verified,
            "amount": sub.get("amount"),
            "wallet_address": sub.get("wallet_address", ""),
            "kyc_requirement": sub.get("kyc_requirement"),
            "kyc_document_url": sub.get("kyc_document_url"),
            "kyc_document_type": sub.get("kyc_document_type"),
            "kyc_submitted_at": str(sub.get("kyc_submitted_at", "")),
            "created_at": str(sub.get("created_at", ""))
        })
    
    return {"submissions": result, "count": len(result)}


# ============= GOD MODE CONTROL SYSTEM =============

@api_router.get("/admin/god-mode/status")
async def get_god_mode_status(authorization: Optional[str] = Header(None), request: Request = None):
    """Get current God Mode settings"""
    user = await get_current_user(authorization, request)
    
    # Get or create platform settings
    settings = await db.platform_settings.find_one({"_id": "god_mode"})
    if not settings:
        settings = {
            "_id": "god_mode",
            "trading_enabled": True,
            "withdrawals_enabled": True,
            "deposits_enabled": True,
            "global_payout_modifier": 100,  # percentage (100 = normal)
            "global_win_rate_modifier": 100,  # percentage
            "maintenance_mode": False,
            "emergency_message": "",
            "updated_at": datetime.now(timezone.utc),
            "updated_by": None
        }
        await db.platform_settings.insert_one(settings)
    
    return {
        "trading_enabled": settings.get("trading_enabled", True),
        "withdrawals_enabled": settings.get("withdrawals_enabled", True),
        "deposits_enabled": settings.get("deposits_enabled", True),
        "global_payout_modifier": settings.get("global_payout_modifier", 100),
        "global_win_rate_modifier": settings.get("global_win_rate_modifier", 100),
        "maintenance_mode": settings.get("maintenance_mode", False),
        "emergency_message": settings.get("emergency_message", ""),
        "updated_at": str(settings.get("updated_at", "")),
        "updated_by": settings.get("updated_by"),
        # AI Automation settings
        "ai_enabled": settings.get("ai_enabled", True),
        "ai_strategy": settings.get("ai_strategy", "balanced"),  # conservative, balanced, aggressive
        "ai_win_rate": settings.get("ai_win_rate", 45),  # 0-100 for real balance
        "demo_strategy": settings.get("demo_strategy", "encouraging"),  # realistic, encouraging, generous, vip
        "demo_win_rate": settings.get("demo_win_rate", 70),  # 0-100 for demo balance
        "ai_market_trend": settings.get("ai_market_trend", "sideways")  # bullish, sideways, bearish
    }

@api_router.post("/admin/ai/toggle")
async def toggle_ai_system(authorization: Optional[str] = Header(None), request: Request = None):
    """Toggle AI trading system on/off"""
    user = await get_current_user(authorization, request)
    
    body = await request.json()
    enabled = body.get("enabled", True)
    
    await db.platform_settings.update_one(
        {"_id": "god_mode"},
        {
            "$set": {
                "ai_enabled": enabled,
                "updated_at": datetime.now(timezone.utc),
                "updated_by": user.user_id
            }
        },
        upsert=True
    )
    
    return {"success": True, "ai_enabled": enabled}

@api_router.post("/admin/ai/strategy")
async def set_ai_strategy(authorization: Optional[str] = Header(None), request: Request = None):
    """Set AI trading strategy preset"""
    user = await get_current_user(authorization, request)
    
    body = await request.json()
    strategy = body.get("strategy", "balanced")  # conservative, balanced, aggressive
    
    # Set corresponding win rate based on strategy
    strategy_win_rates = {
        "conservative": 35,
        "balanced": 45,
        "aggressive": 55
    }
    win_rate = strategy_win_rates.get(strategy, 45)
    
    await db.platform_settings.update_one(
        {"_id": "god_mode"},
        {
            "$set": {
                "ai_strategy": strategy,
                "ai_win_rate": win_rate,
                "updated_at": datetime.now(timezone.utc),
                "updated_by": user.user_id
            }
        },
        upsert=True
    )
    
    return {"success": True, "ai_strategy": strategy, "ai_win_rate": win_rate}

@api_router.post("/admin/ai/win-rate")
async def set_ai_win_rate(authorization: Optional[str] = Header(None), request: Request = None):
    """Set AI win rate control"""
    user = await get_current_user(authorization, request)
    
    body = await request.json()
    win_rate = body.get("win_rate", 45)
    win_rate = max(0, min(100, win_rate))  # Clamp between 0-100
    
    await db.platform_settings.update_one(
        {"_id": "god_mode"},
        {
            "$set": {
                "ai_win_rate": win_rate,
                "updated_at": datetime.now(timezone.utc),
                "updated_by": user.user_id
            }
        },
        upsert=True
    )
    
    return {"success": True, "ai_win_rate": win_rate}

@api_router.post("/admin/ai/demo-win-rate")
async def set_demo_win_rate(authorization: Optional[str] = Header(None), request: Request = None):
    """Set Demo Balance AI win rate control"""
    user = await get_current_user(authorization, request)
    
    body = await request.json()
    win_rate = body.get("win_rate", 65)
    win_rate = max(0, min(100, win_rate))  # Clamp between 0-100
    
    await db.platform_settings.update_one(
        {"_id": "god_mode"},
        {
            "$set": {
                "demo_win_rate": win_rate,
                "updated_at": datetime.now(timezone.utc),
                "updated_by": user.user_id
            }
        },
        upsert=True
    )
    
    return {"success": True, "demo_win_rate": win_rate}

@api_router.post("/admin/ai/demo-strategy")
async def set_demo_strategy(authorization: Optional[str] = Header(None), request: Request = None):
    """Set Demo trading strategy preset - controls demo account win rate"""
    user = await get_current_user(authorization, request)
    
    body = await request.json()
    strategy = body.get("strategy", "encouraging")  # realistic, encouraging, generous
    
    # Demo strategies focus on encouraging users to feel confident
    demo_strategy_win_rates = {
        "realistic": 55,      # Slightly above 50% - feels realistic
        "encouraging": 70,    # 70% wins - builds confidence
        "generous": 85,       # 85% wins - very positive experience
        "vip": 95             # 95% wins - almost always wins (for special users)
    }
    win_rate = demo_strategy_win_rates.get(strategy, 70)
    
    await db.platform_settings.update_one(
        {"_id": "god_mode"},
        {
            "$set": {
                "demo_strategy": strategy,
                "demo_win_rate": win_rate,
                "updated_at": datetime.now(timezone.utc),
                "updated_by": user.user_id
            }
        },
        upsert=True
    )
    
    return {"success": True, "demo_strategy": strategy, "demo_win_rate": win_rate}

@api_router.post("/admin/ai/market-trend")
async def set_ai_market_trend(authorization: Optional[str] = Header(None), request: Request = None):
    """Set AI market trend simulation"""
    user = await get_current_user(authorization, request)
    
    body = await request.json()
    trend = body.get("trend", "sideways")  # bullish, sideways, bearish
    
    await db.platform_settings.update_one(
        {"_id": "god_mode"},
        {
            "$set": {
                "ai_market_trend": trend,
                "updated_at": datetime.now(timezone.utc),
                "updated_by": user.user_id
            }
        },
        upsert=True
    )
    
    return {"success": True, "ai_market_trend": trend}

@api_router.post("/admin/god-mode/kill-switch")
async def toggle_kill_switch(authorization: Optional[str] = Header(None), request: Request = None):
    """Toggle trading kill switch - instantly disable/enable all trading"""
    user = await get_current_user(authorization, request)
    
    body = await request.json()
    enabled = body.get("enabled", False)
    
    await db.platform_settings.update_one(
        {"_id": "god_mode"},
        {
            "$set": {
                "trading_enabled": enabled,
                "updated_at": datetime.now(timezone.utc),
                "updated_by": user.user_id
            }
        },
        upsert=True
    )
    
    # Log the action
    await db.admin_logs.insert_one({
        "action": "kill_switch",
        "admin_id": user.user_id,
        "details": {"trading_enabled": enabled},
        "timestamp": datetime.now(timezone.utc)
    })
    
    return {"success": True, "trading_enabled": enabled}

@api_router.post("/admin/god-mode/freeze-withdrawals")
async def freeze_withdrawals(authorization: Optional[str] = Header(None), request: Request = None):
    """Freeze/unfreeze all withdrawals"""
    user = await get_current_user(authorization, request)
    
    body = await request.json()
    enabled = body.get("enabled", True)
    
    await db.platform_settings.update_one(
        {"_id": "god_mode"},
        {
            "$set": {
                "withdrawals_enabled": enabled,
                "updated_at": datetime.now(timezone.utc),
                "updated_by": user.user_id
            }
        },
        upsert=True
    )
    
    await db.admin_logs.insert_one({
        "action": "freeze_withdrawals",
        "admin_id": user.user_id,
        "details": {"withdrawals_enabled": enabled},
        "timestamp": datetime.now(timezone.utc)
    })
    
    return {"success": True, "withdrawals_enabled": enabled}

@api_router.post("/admin/god-mode/global-payout")
async def set_global_payout(authorization: Optional[str] = Header(None), request: Request = None):
    """Set global payout modifier (affects all trades)"""
    user = await get_current_user(authorization, request)
    
    body = await request.json()
    modifier = float(body.get("modifier", 100))  # 0-200%
    
    if modifier < 0 or modifier > 200:
        raise HTTPException(status_code=400, detail="Modifier must be between 0 and 200")
    
    await db.platform_settings.update_one(
        {"_id": "god_mode"},
        {
            "$set": {
                "global_payout_modifier": modifier,
                "updated_at": datetime.now(timezone.utc),
                "updated_by": user.user_id
            }
        },
        upsert=True
    )
    
    await db.admin_logs.insert_one({
        "action": "global_payout_change",
        "admin_id": user.user_id,
        "details": {"modifier": modifier},
        "timestamp": datetime.now(timezone.utc)
    })
    
    return {"success": True, "global_payout_modifier": modifier}

@api_router.post("/admin/god-mode/global-win-rate")
async def set_global_win_rate(authorization: Optional[str] = Header(None), request: Request = None):
    """Set global win rate modifier"""
    user = await get_current_user(authorization, request)
    
    body = await request.json()
    modifier = float(body.get("modifier", 100))  # 0-200%
    
    if modifier < 0 or modifier > 200:
        raise HTTPException(status_code=400, detail="Modifier must be between 0 and 200")
    
    await db.platform_settings.update_one(
        {"_id": "god_mode"},
        {
            "$set": {
                "global_win_rate_modifier": modifier,
                "updated_at": datetime.now(timezone.utc),
                "updated_by": user.user_id
            }
        },
        upsert=True
    )
    
    await db.admin_logs.insert_one({
        "action": "global_win_rate_change",
        "admin_id": user.user_id,
        "details": {"modifier": modifier},
        "timestamp": datetime.now(timezone.utc)
    })
    
    return {"success": True, "global_win_rate_modifier": modifier}

@api_router.post("/admin/god-mode/maintenance")
async def toggle_maintenance(authorization: Optional[str] = Header(None), request: Request = None):
    """Toggle maintenance mode"""
    user = await get_current_user(authorization, request)
    
    body = await request.json()
    enabled = body.get("enabled", False)
    message = body.get("message", "Platform is under maintenance. Please try again later.")
    
    await db.platform_settings.update_one(
        {"_id": "god_mode"},
        {
            "$set": {
                "maintenance_mode": enabled,
                "emergency_message": message,
                "updated_at": datetime.now(timezone.utc),
                "updated_by": user.user_id
            }
        },
        upsert=True
    )
    
    return {"success": True, "maintenance_mode": enabled}

# ============= PROMO CODE / DEPOSIT BONUS SYSTEM =============

@api_router.get("/admin/promo-codes")
async def get_promo_codes(authorization: Optional[str] = Header(None), request: Request = None):
    """Get all promo codes"""
    user = await get_current_user(authorization, request)
    
    promo_codes = await db.promo_codes.find({}).sort("created_at", -1).to_list(100)
    
    result = []
    for pc in promo_codes:
        result.append({
            "code": pc.get("code"),
            "bonus_type": pc.get("bonus_type", "percentage"),  # percentage or fixed
            "bonus_value": pc.get("bonus_value", 0),
            "min_deposit": pc.get("min_deposit", 0),
            "max_bonus": pc.get("max_bonus", 0),
            "usage_limit": pc.get("usage_limit", 0),  # 0 = unlimited
            "usage_count": pc.get("usage_count", 0),
            "is_active": pc.get("is_active", True),
            "expires_at": str(pc.get("expires_at", "")) if pc.get("expires_at") else None,
            "created_at": str(pc.get("created_at", "")),
            "created_by": pc.get("created_by", "")
        })
    
    return {"promo_codes": result, "count": len(result)}


@api_router.post("/admin/promo-codes")
async def create_promo_code(authorization: Optional[str] = Header(None), request: Request = None):
    """Create a new promo code"""
    user = await get_current_user(authorization, request)
    
    body = await request.json()
    code = body.get("code", "").upper().strip()
    bonus_type = body.get("bonus_type", "percentage")  # percentage or fixed
    bonus_value = float(body.get("bonus_value", 0))
    min_deposit = float(body.get("min_deposit", 0))
    max_bonus = float(body.get("max_bonus", 0))
    usage_limit = int(body.get("usage_limit", 0))  # 0 = unlimited
    expires_days = int(body.get("expires_days", 0))  # 0 = never expires
    
    if not code:
        raise HTTPException(status_code=400, detail="Promo code is required")
    
    if bonus_value <= 0:
        raise HTTPException(status_code=400, detail="Bonus value must be greater than 0")
    
    # Check if code already exists
    existing = await db.promo_codes.find_one({"code": code})
    if existing:
        raise HTTPException(status_code=400, detail="Promo code already exists")
    
    expires_at = None
    if expires_days > 0:
        expires_at = datetime.now(timezone.utc) + timedelta(days=expires_days)
    
    promo_doc = {
        "code": code,
        "bonus_type": bonus_type,
        "bonus_value": bonus_value,
        "min_deposit": min_deposit,
        "max_bonus": max_bonus,
        "usage_limit": usage_limit,
        "usage_count": 0,
        "is_active": True,
        "expires_at": expires_at,
        "created_at": datetime.now(timezone.utc),
        "created_by": user.user_id
    }
    
    await db.promo_codes.insert_one(promo_doc)
    
    return {
        "success": True,
        "message": f"Promo code '{code}' created successfully",
        "promo_code": {
            "code": code,
            "bonus_type": bonus_type,
            "bonus_value": bonus_value,
            "min_deposit": min_deposit,
            "max_bonus": max_bonus
        }
    }


@api_router.post("/admin/promo-codes/{code}/toggle")
async def toggle_promo_code(code: str, authorization: Optional[str] = Header(None), request: Request = None):
    """Enable/Disable a promo code"""
    user = await get_current_user(authorization, request)
    
    promo = await db.promo_codes.find_one({"code": code.upper()})
    if not promo:
        raise HTTPException(status_code=404, detail="Promo code not found")
    
    new_status = not promo.get("is_active", True)
    
    await db.promo_codes.update_one(
        {"code": code.upper()},
        {"$set": {"is_active": new_status}}
    )
    
    return {"success": True, "is_active": new_status}


@api_router.delete("/admin/promo-codes/{code}")
async def delete_promo_code(code: str, authorization: Optional[str] = Header(None), request: Request = None):
    """Delete a promo code"""
    user = await get_current_user(authorization, request)
    
    result = await db.promo_codes.delete_one({"code": code.upper()})
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Promo code not found")
    
    return {"success": True, "message": f"Promo code '{code}' deleted"}


@api_router.post("/promo-codes/validate")
async def validate_promo_code(authorization: Optional[str] = Header(None), request: Request = None):
    """Validate a promo code for user deposit"""
    user = await get_current_user(authorization, request)
    
    body = await request.json()
    code = body.get("code", "").upper().strip()
    deposit_amount = float(body.get("deposit_amount", 0))
    
    if not code:
        raise HTTPException(status_code=400, detail="Promo code is required")
    
    promo = await db.promo_codes.find_one({"code": code})
    
    if not promo:
        raise HTTPException(status_code=404, detail="Invalid promo code")
    
    if not promo.get("is_active", False):
        raise HTTPException(status_code=400, detail="This promo code is no longer active")
    
    # Check expiry
    if promo.get("expires_at"):
        expires_at = promo["expires_at"]
        # Make sure both are timezone aware
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) > expires_at:
            raise HTTPException(status_code=400, detail="This promo code has expired")
    
    # Check usage limit
    if promo.get("usage_limit", 0) > 0:
        if promo.get("usage_count", 0) >= promo["usage_limit"]:
            raise HTTPException(status_code=400, detail="This promo code has reached its usage limit")
    
    # Check minimum deposit
    min_deposit = promo.get("min_deposit", 0)
    if deposit_amount < min_deposit:
        raise HTTPException(status_code=400, detail=f"Minimum deposit for this code is ${min_deposit}")
    
    # Check if user already used this code
    already_used = await db.promo_usage.find_one({
        "user_id": user.user_id,
        "promo_code": code
    })
    if already_used:
        raise HTTPException(status_code=400, detail="You have already used this promo code")
    
    # Calculate bonus
    bonus_type = promo.get("bonus_type", "percentage")
    bonus_value = promo.get("bonus_value", 0)
    max_bonus = promo.get("max_bonus", 0)
    
    if bonus_type == "percentage":
        calculated_bonus = deposit_amount * (bonus_value / 100)
        if max_bonus > 0 and calculated_bonus > max_bonus:
            calculated_bonus = max_bonus
    else:  # fixed
        calculated_bonus = bonus_value
    
    return {
        "valid": True,
        "code": code,
        "bonus_type": bonus_type,
        "bonus_value": bonus_value,
        "calculated_bonus": round(calculated_bonus, 2),
        "deposit_amount": deposit_amount,
        "total_credit": round(deposit_amount + calculated_bonus, 2),
        "message": f"You will receive ${round(calculated_bonus, 2)} bonus!"
    }


@api_router.post("/promo-codes/apply")
async def apply_promo_code(authorization: Optional[str] = Header(None), request: Request = None):
    """Apply a promo code after successful deposit"""
    user = await get_current_user(authorization, request)
    
    body = await request.json()
    code = body.get("code", "").upper().strip()
    deposit_amount = float(body.get("deposit_amount", 0))
    transaction_id = body.get("transaction_id", "")
    
    if not code:
        raise HTTPException(status_code=400, detail="Promo code is required")
    
    promo = await db.promo_codes.find_one({"code": code})
    
    if not promo or not promo.get("is_active", False):
        raise HTTPException(status_code=400, detail="Invalid or inactive promo code")
    
    # Calculate bonus
    bonus_type = promo.get("bonus_type", "percentage")
    bonus_value = promo.get("bonus_value", 0)
    max_bonus = promo.get("max_bonus", 0)
    
    if bonus_type == "percentage":
        calculated_bonus = deposit_amount * (bonus_value / 100)
        if max_bonus > 0 and calculated_bonus > max_bonus:
            calculated_bonus = max_bonus
    else:
        calculated_bonus = bonus_value
    
    calculated_bonus = round(calculated_bonus, 2)
    
    # Add bonus to user's balance
    await db.users.update_one(
        {"user_id": user.user_id},
        {"$inc": {"real_balance": calculated_bonus, "bonus_balance": calculated_bonus}}
    )
    
    # Record promo usage
    await db.promo_usage.insert_one({
        "user_id": user.user_id,
        "promo_code": code,
        "deposit_amount": deposit_amount,
        "bonus_amount": calculated_bonus,
        "transaction_id": transaction_id,
        "applied_at": datetime.now(timezone.utc)
    })
    
    # Increment usage count
    await db.promo_codes.update_one(
        {"code": code},
        {"$inc": {"usage_count": 1}}
    )
    
    return {
        "success": True,
        "bonus_applied": calculated_bonus,
        "message": f"Bonus of ${calculated_bonus} has been added to your account!"
    }


@api_router.get("/admin/promo-codes/usage")
async def get_promo_usage(authorization: Optional[str] = Header(None), request: Request = None):
    """Get promo code usage history"""
    user = await get_current_user(authorization, request)
    
    usage_records = await db.promo_usage.find({}).sort("applied_at", -1).to_list(100)
    
    result = []
    for record in usage_records:
        # Get user info
        user_doc = await db.users.find_one({"user_id": record.get("user_id")})
        result.append({
            "user_id": record.get("user_id"),
            "user_email": user_doc.get("email") if user_doc else "Unknown",
            "promo_code": record.get("promo_code"),
            "deposit_amount": record.get("deposit_amount"),
            "bonus_amount": record.get("bonus_amount"),
            "applied_at": str(record.get("applied_at", ""))
        })
    
    return {"usage": result, "count": len(result)}


# ============= ADMIN ASSET CONTROL =============

@api_router.get("/admin/assets")
async def get_admin_assets(authorization: Optional[str] = Header(None), request: Request = None):
    """Get all assets for admin (including inactive)"""
    user = await get_current_user(authorization, request)
    
    assets = await db.assets.find({}, {"_id": 0}).to_list(200)
    return {"assets": assets}

@api_router.post("/admin/assets/{asset_id}/toggle")
async def toggle_asset_status(asset_id: str, authorization: Optional[str] = Header(None), request: Request = None):
    """Enable/Disable an asset"""
    user = await get_current_user(authorization, request)
    
    body = await request.json()
    is_active = body.get("is_active", True)
    
    # Try to update by asset_id first
    result = await db.assets.update_one(
        {"asset_id": asset_id},
        {"$set": {"is_active": is_active}}
    )
    
    if result.modified_count == 0:
        # Try by symbol
        result = await db.assets.update_one(
            {"symbol": asset_id},
            {"$set": {"is_active": is_active}}
        )
        
        if result.modified_count == 0:
            # Asset doesn't exist, create it with is_active status
            # This handles hardcoded frontend assets
            await db.assets.insert_one({
                "asset_id": asset_id,
                "symbol": asset_id,
                "name": asset_id + " OTC",
                "category": "forex",
                "payout_percentage": 85.0,
                "is_active": is_active
            })
    
    return {"success": True, "is_active": is_active}

@api_router.post("/admin/assets/{asset_id}/payout")
async def update_asset_payout(asset_id: str, authorization: Optional[str] = Header(None), request: Request = None):
    """Update asset payout percentage"""
    user = await get_current_user(authorization, request)
    
    body = await request.json()
    payout = body.get("payout_percentage", 85.0)
    
    result = await db.assets.update_one(
        {"asset_id": asset_id},
        {"$set": {"payout_percentage": payout}}
    )
    
    if result.modified_count == 0:
        result = await db.assets.update_one(
            {"symbol": asset_id},
            {"$set": {"payout_percentage": payout}}
        )
    
    return {"success": True, "payout_percentage": payout}

@api_router.post("/admin/global-payout")
async def set_global_payout(authorization: Optional[str] = Header(None), request: Request = None):
    """Set global payout for all assets"""
    user = await get_current_user(authorization, request)
    
    body = await request.json()
    payout = body.get("payout_percentage", 85.0)
    
    await db.assets.update_many(
        {},
        {"$set": {"payout_percentage": payout}}
    )
    
    return {"success": True, "global_payout": payout}

@api_router.post("/admin/trading/toggle")
async def toggle_global_trading(authorization: Optional[str] = Header(None), request: Request = None):
    """Toggle global trading on/off"""
    user = await get_current_user(authorization, request)
    
    body = await request.json()
    enabled = body.get("enabled", True)
    
    await db.platform_settings.update_one(
        {"_id": "god_mode"},
        {
            "$set": {
                "trading_enabled": enabled,
                "updated_at": datetime.now(timezone.utc),
                "updated_by": user.user_id
            }
        },
        upsert=True
    )
    
    return {"success": True, "trading_enabled": enabled}

# ============= TRADE ENGINE CONTROL =============

@api_router.get("/admin/trades/live")
async def get_live_trades(
    limit: int = 50,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get live/active trades for monitoring"""
    user = await get_current_user(authorization, request)
    
    # Get active (pending) trades
    active_trades = await db.trades.find(
        {"status": "active"}
    ).sort("created_at", -1).limit(limit).to_list(limit)
    
    # Get recent completed trades
    recent_trades = await db.trades.find(
        {"status": {"$in": ["won", "lost", "cancelled"]}}
    ).sort("created_at", -1).limit(limit).to_list(limit)
    
    # Get user info for trades
    result_active = []
    for t in active_trades:
        user_info = await db.users.find_one({"user_id": t.get("user_id")})
        result_active.append({
            "trade_id": t.get("trade_id"),
            "user_id": t.get("user_id"),
            "user_email": user_info.get("email") if user_info else "Unknown",
            "asset": t.get("asset"),
            "amount": t.get("amount"),
            "direction": t.get("direction"),
            "entry_price": t.get("entry_price"),
            "payout_percentage": t.get("payout_percentage", 80),
            "expiry_time": str(t.get("expiry_time", "")),
            "created_at": str(t.get("created_at", "")),
            "account_type": t.get("account_type", "demo"),
            "status": "active"
        })
    
    result_recent = []
    for t in recent_trades:
        user_info = await db.users.find_one({"user_id": t.get("user_id")})
        result_recent.append({
            "trade_id": t.get("trade_id"),
            "user_id": t.get("user_id"),
            "user_email": user_info.get("email") if user_info else "Unknown",
            "asset": t.get("asset"),
            "amount": t.get("amount"),
            "direction": t.get("direction"),
            "entry_price": t.get("entry_price"),
            "exit_price": t.get("exit_price"),
            "profit_loss": t.get("profit_loss", 0),
            "status": t.get("status"),
            "created_at": str(t.get("created_at", "")),
            "account_type": t.get("account_type", "demo")
        })
    
    return {
        "active_trades": result_active,
        "recent_trades": result_recent,
        "active_count": len(result_active)
    }

@api_router.post("/admin/trades/{trade_id}/override")
async def override_trade_result(
    trade_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Override trade result (force win/lose)"""
    admin = await get_current_user(authorization, request)
    
    body = await request.json()
    forced_result = body.get("result")  # "win" or "lose"
    
    if forced_result not in ["win", "lose"]:
        raise HTTPException(status_code=400, detail="Result must be 'win' or 'lose'")
    
    trade = await db.trades.find_one({"trade_id": trade_id})
    if not trade:
        raise HTTPException(status_code=404, detail="Trade not found")
    
    # Get user
    user = await db.users.find_one({"user_id": trade["user_id"]})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    amount = trade.get("amount", 0)
    payout_percentage = trade.get("payout_percentage", 80)
    account_type = trade.get("account_type", "demo")
    balance_field = "demo_balance" if account_type == "demo" else "real_balance"
    
    # If trade was already settled, reverse the previous result first
    if trade.get("status") in ["won", "lost"]:
        old_profit_loss = trade.get("profit_loss", 0)
        await db.users.update_one(
            {"user_id": trade["user_id"]},
            {"$inc": {balance_field: -old_profit_loss}}
        )
    
    # Apply new result
    if forced_result == "win":
        profit = amount * (payout_percentage / 100)
        new_status = "won"
        profit_loss = profit
        # Return original amount + profit
        await db.users.update_one(
            {"user_id": trade["user_id"]},
            {"$inc": {balance_field: amount + profit}}
        )
    else:
        new_status = "lost"
        profit_loss = -amount
        # Don't return anything (amount already deducted)
    
    # Update trade
    await db.trades.update_one(
        {"trade_id": trade_id},
        {
            "$set": {
                "status": new_status,
                "profit_loss": profit_loss,
                "admin_override": True,
                "overridden_by": admin.user_id,
                "overridden_at": datetime.now(timezone.utc)
            }
        }
    )
    
    # Log the action
    await db.admin_logs.insert_one({
        "action": "trade_override",
        "admin_id": admin.user_id,
        "trade_id": trade_id,
        "details": {
            "forced_result": forced_result,
            "user_id": trade["user_id"],
            "amount": amount
        },
        "timestamp": datetime.now(timezone.utc)
    })
    
    return {"success": True, "new_status": new_status, "profit_loss": profit_loss}

@api_router.post("/admin/trades/{trade_id}/cancel")
async def cancel_trade(
    trade_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Cancel a trade and refund the user"""
    admin = await get_current_user(authorization, request)
    
    trade = await db.trades.find_one({"trade_id": trade_id})
    if not trade:
        raise HTTPException(status_code=404, detail="Trade not found")
    
    if trade.get("status") == "cancelled":
        raise HTTPException(status_code=400, detail="Trade already cancelled")
    
    amount = trade.get("amount", 0)
    account_type = trade.get("account_type", "demo")
    balance_field = "demo_balance" if account_type == "demo" else "real_balance"
    
    # If trade was settled, need to reverse
    if trade.get("status") in ["won", "lost"]:
        old_profit_loss = trade.get("profit_loss", 0)
        # Reverse previous settlement
        await db.users.update_one(
            {"user_id": trade["user_id"]},
            {"$inc": {balance_field: -old_profit_loss}}
        )
    
    # Refund original amount
    await db.users.update_one(
        {"user_id": trade["user_id"]},
        {"$inc": {balance_field: amount}}
    )
    
    # Update trade status
    await db.trades.update_one(
        {"trade_id": trade_id},
        {
            "$set": {
                "status": "cancelled",
                "profit_loss": 0,
                "cancelled_by": admin.user_id,
                "cancelled_at": datetime.now(timezone.utc)
            }
        }
    )
    
    await db.admin_logs.insert_one({
        "action": "trade_cancelled",
        "admin_id": admin.user_id,
        "trade_id": trade_id,
        "details": {"amount_refunded": amount},
        "timestamp": datetime.now(timezone.utc)
    })
    
    return {"success": True, "message": f"Trade cancelled, ${amount} refunded"}

# ============= USER RISK CONTROL =============

@api_router.get("/admin/users/{user_id}/risk-profile")
async def get_user_risk_profile(
    user_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get detailed user risk profile"""
    admin = await get_current_user(authorization, request)
    
    user = await db.users.find_one({"user_id": user_id})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Calculate trading stats
    trades = await db.trades.find({"user_id": user_id}).to_list(1000)
    total_trades = len(trades)
    won_trades = len([t for t in trades if t.get("status") == "won"])
    lost_trades = len([t for t in trades if t.get("status") == "lost"])
    total_volume = sum(t.get("amount", 0) for t in trades)
    total_profit = sum(t.get("profit_loss", 0) for t in trades if t.get("status") in ["won", "lost"])
    
    win_rate = (won_trades / total_trades * 100) if total_trades > 0 else 0
    
    # Calculate deposits/withdrawals
    deposits = await db.deposits.find({"user_id": user_id, "status": "completed"}).to_list(100)
    withdrawals = await db.withdrawals.find({"user_id": user_id, "status": "completed"}).to_list(100)
    total_deposited = sum(d.get("amount_usd", 0) for d in deposits)
    total_withdrawn = sum(w.get("amount", 0) for w in withdrawals)
    
    # Calculate AI risk score (simplified)
    risk_score = 50  # Base score
    if total_profit > total_deposited:
        risk_score += min(30, (total_profit - total_deposited) / 100)  # Profitable user = higher risk
    if win_rate > 60:
        risk_score += 10  # High win rate
    if total_volume > 10000:
        risk_score += 10  # High volume
    risk_score = min(100, max(0, risk_score))
    
    return {
        "user_id": user_id,
        "email": user.get("email"),
        "name": user.get("name") or user.get("full_name"),
        "balances": {
            "real": user.get("real_balance", 0),
            "demo": user.get("demo_balance", 10000),
            "bonus": user.get("bonus_balance", 0)
        },
        "trading_stats": {
            "total_trades": total_trades,
            "won_trades": won_trades,
            "lost_trades": lost_trades,
            "win_rate": round(win_rate, 2),
            "total_volume": total_volume,
            "total_profit": total_profit
        },
        "financial_stats": {
            "total_deposited": total_deposited,
            "total_withdrawn": total_withdrawn,
            "net_deposit": total_deposited - total_withdrawn
        },
        "risk_controls": {
            "win_rate_modifier": user.get("win_rate_modifier", 100),
            "payout_modifier": user.get("payout_modifier", 100),
            "max_trade_amount": user.get("max_trade_amount"),
            "is_shadow_banned": user.get("is_shadow_banned", False),
            "is_flagged": user.get("is_flagged", False),
            "risk_level": user.get("risk_level", "normal"),
            "notes": user.get("admin_notes", "")
        },
        "ai_risk_score": round(risk_score, 1),
        "created_at": str(user.get("created_at", ""))
    }

@api_router.post("/admin/users/{user_id}/win-rate")
async def set_user_win_rate(
    user_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Set user-specific win rate modifier (hidden from user)"""
    admin = await get_current_user(authorization, request)
    
    body = await request.json()
    modifier = float(body.get("modifier", 100))  # 0-200%
    
    if modifier < 0 or modifier > 200:
        raise HTTPException(status_code=400, detail="Modifier must be between 0 and 200")
    
    result = await db.users.update_one(
        {"user_id": user_id},
        {"$set": {"win_rate_modifier": modifier}}
    )
    
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    
    await db.admin_logs.insert_one({
        "action": "user_win_rate_change",
        "admin_id": admin.user_id,
        "target_user_id": user_id,
        "details": {"modifier": modifier},
        "timestamp": datetime.now(timezone.utc)
    })
    
    return {"success": True, "win_rate_modifier": modifier}

@api_router.post("/admin/users/{user_id}/payout")
async def set_user_payout(
    user_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Set user-specific payout modifier"""
    admin = await get_current_user(authorization, request)
    
    body = await request.json()
    modifier = float(body.get("modifier", 100))  # 0-200%
    
    result = await db.users.update_one(
        {"user_id": user_id},
        {"$set": {"payout_modifier": modifier}}
    )
    
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    
    await db.admin_logs.insert_one({
        "action": "user_payout_change",
        "admin_id": admin.user_id,
        "target_user_id": user_id,
        "details": {"modifier": modifier},
        "timestamp": datetime.now(timezone.utc)
    })
    
    return {"success": True, "payout_modifier": modifier}

@api_router.post("/admin/users/{user_id}/ban")
async def ban_user(
    user_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Ban or unban user - they will see suspension message on login"""
    admin = await get_current_user(authorization, request)
    
    body = await request.json()
    banned = body.get("banned", True)
    reason = body.get("reason", "This account is suspended for violation of company rules")
    
    result = await db.users.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "is_banned": banned,
                "ban_reason": reason if banned else None,
                "banned_at": datetime.now(timezone.utc) if banned else None,
                "banned_by": admin.user_id if banned else None
            }
        }
    )
    
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    
    return {"success": True, "banned": banned, "message": f"User {'banned' if banned else 'unbanned'} successfully"}

@api_router.post("/admin/users/{user_id}/delete")
async def delete_user(
    user_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Soft delete user - they will see deleted message on login"""
    admin = await get_current_user(authorization, request)
    
    body = await request.json()
    reason = body.get("reason", "This account has been deleted by the owner")
    
    result = await db.users.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "is_deleted": True,
                "delete_reason": reason,
                "deleted_at": datetime.now(timezone.utc),
                "deleted_by": admin.user_id
            }
        }
    )
    
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    
    return {"success": True, "message": "User deleted successfully"}

@api_router.get("/admin/users/{user_id}/trades")
async def get_user_trades(
    user_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None,
    limit: int = 50
):
    """Get trade history for a specific user"""
    admin = await get_current_user(authorization, request)
    
    trades = await db.trades.find({"user_id": user_id}).sort("created_at", -1).limit(limit).to_list(limit)
    
    return {
        "trades": [
            {
                "trade_id": t.get("trade_id"),
                "asset": t.get("asset"),
                "trade_type": t.get("trade_type"),
                "amount": t.get("amount"),
                "status": t.get("status"),
                "profit_loss": t.get("profit_loss"),
                "created_at": str(t.get("created_at")),
                "account_type": t.get("account_type")
            }
            for t in trades
        ],
        "total": len(trades)
    }

@api_router.post("/admin/users/{user_id}/shadow-ban")
async def shadow_ban_user(
    user_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Shadow ban user (they don't know they're banned)"""
    admin = await get_current_user(authorization, request)
    
    body = await request.json()
    banned = body.get("banned", True)
    reason = body.get("reason", "")
    
    result = await db.users.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "is_shadow_banned": banned,
                "shadow_ban_reason": reason,
                "shadow_banned_at": datetime.now(timezone.utc) if banned else None,
                "shadow_banned_by": admin.user_id if banned else None
            }
        }
    )
    
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    
    await db.admin_logs.insert_one({
        "action": "shadow_ban",
        "admin_id": admin.user_id,
        "target_user_id": user_id,
        "details": {"banned": banned, "reason": reason},
        "timestamp": datetime.now(timezone.utc)
    })
    
    return {"success": True, "is_shadow_banned": banned}

@api_router.post("/admin/users/{user_id}/flag")
async def flag_user(
    user_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Flag user for review"""
    admin = await get_current_user(authorization, request)
    
    body = await request.json()
    flagged = body.get("flagged", True)
    reason = body.get("reason", "")
    
    result = await db.users.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "is_flagged": flagged,
                "flag_reason": reason,
                "flagged_at": datetime.now(timezone.utc) if flagged else None,
                "flagged_by": admin.user_id if flagged else None
            }
        }
    )
    
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    
    return {"success": True, "is_flagged": flagged}

@api_router.post("/admin/users/{user_id}/risk-level")
async def set_user_risk_level(
    user_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Set user risk level (normal, low, medium, high, critical)"""
    admin = await get_current_user(authorization, request)
    
    body = await request.json()
    risk_level = body.get("level", "normal")
    
    if risk_level not in ["normal", "low", "medium", "high", "critical"]:
        raise HTTPException(status_code=400, detail="Invalid risk level")
    
    result = await db.users.update_one(
        {"user_id": user_id},
        {"$set": {"risk_level": risk_level}}
    )
    
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    
    return {"success": True, "risk_level": risk_level}

@api_router.post("/admin/users/{user_id}/max-trade")
async def set_user_max_trade(
    user_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Set maximum trade amount for user"""
    admin = await get_current_user(authorization, request)
    
    body = await request.json()
    max_amount = body.get("max_amount")  # None = no limit
    
    result = await db.users.update_one(
        {"user_id": user_id},
        {"$set": {"max_trade_amount": max_amount}}
    )
    
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    
    return {"success": True, "max_trade_amount": max_amount}

@api_router.post("/admin/users/{user_id}/notes")
async def add_admin_notes(
    user_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Add admin notes to user profile"""
    admin = await get_current_user(authorization, request)
    
    body = await request.json()
    notes = body.get("notes", "")
    
    result = await db.users.update_one(
        {"user_id": user_id},
        {"$set": {"admin_notes": notes}}
    )
    
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    
    return {"success": True}

# ============= ADMIN LOGS =============

@api_router.get("/admin/logs")
async def get_admin_logs(
    limit: int = 100,
    action: str = None,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get admin activity logs"""
    user = await get_current_user(authorization, request)
    
    query = {}
    if action:
        query["action"] = action
    
    logs = await db.admin_logs.find(query).sort("timestamp", -1).limit(limit).to_list(limit)
    
    return {
        "logs": [
            {
                "action": log.get("action"),
                "admin_id": log.get("admin_id"),
                "details": log.get("details"),
                "timestamp": str(log.get("timestamp", ""))
            }
            for log in logs
        ]
    }

# ============= PLATFORM STATS (REAL-TIME) =============

@api_router.get("/admin/platform/live-stats")
async def get_live_platform_stats(authorization: Optional[str] = Header(None), request: Request = None):
    """Get real-time platform statistics"""
    user = await get_current_user(authorization, request)
    
    now = datetime.now(timezone.utc)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Active trades
    active_trades = await db.trades.count_documents({"status": "active"})
    
    # Today's stats
    today_trades = await db.trades.find({"created_at": {"$gte": today}}).to_list(10000)
    today_volume = sum(t.get("amount", 0) for t in today_trades)
    today_profit = sum(t.get("profit_loss", 0) for t in today_trades if t.get("status") in ["won", "lost"])
    platform_profit_today = -today_profit  # Platform profit = user losses
    
    today_deposits = await db.deposits.find({"created_at": {"$gte": today}, "status": "completed"}).to_list(1000)
    today_deposit_total = sum(d.get("amount_usd", 0) for d in today_deposits)
    
    today_withdrawals = await db.withdrawals.find({"created_at": {"$gte": today}, "status": "completed"}).to_list(1000)
    today_withdrawal_total = sum(w.get("amount", 0) for w in today_withdrawals)
    
    # Pending counts
    pending_withdrawals = await db.withdrawals.count_documents({"status": "pending"})
    pending_deposits = await db.deposits.count_documents({"status": "pending"})
    
    # Active users (traded in last hour)
    hour_ago = now - timedelta(hours=1)
    active_users = len(await db.trades.distinct("user_id", {"created_at": {"$gte": hour_ago}}))
    
    # God mode status
    god_mode = await db.platform_settings.find_one({"_id": "god_mode"})
    
    return {
        "live": {
            "active_trades": active_trades,
            "active_users": active_users,
            "pending_withdrawals": pending_withdrawals,
            "pending_deposits": pending_deposits
        },
        "today": {
            "total_trades": len(today_trades),
            "total_volume": today_volume,
            "platform_profit": platform_profit_today,
            "total_deposits": today_deposit_total,
            "total_withdrawals": today_withdrawal_total,
            "net_flow": today_deposit_total - today_withdrawal_total
        },
        "god_mode": {
            "trading_enabled": god_mode.get("trading_enabled", True) if god_mode else True,
            "withdrawals_enabled": god_mode.get("withdrawals_enabled", True) if god_mode else True,
            "global_payout": god_mode.get("global_payout_modifier", 100) if god_mode else 100,
            "global_win_rate": god_mode.get("global_win_rate_modifier", 100) if god_mode else 100
        },
        "timestamp": str(now)
    }



# ============= ROLE HIERARCHY SYSTEM =============

ROLE_PERMISSIONS = {
    "super_admin": ["*"],  # Full access
    "financial_admin": ["deposits", "withdrawals", "transactions", "manual_deposit", "payouts"],
    "risk_manager": ["users", "risk_controls", "trades", "fraud_detection"],
    "support_agent": ["users_view", "tickets", "basic_info"],
    "auditor": ["view_only", "logs", "reports"],
    "affiliate_manager": ["affiliates", "commissions", "payouts"]
}

@api_router.get("/admin/roles")
async def get_roles(authorization: Optional[str] = Header(None), request: Request = None):
    """Get all available roles and permissions"""
    user = await get_current_user(authorization, request)
    return {"roles": ROLE_PERMISSIONS}

@api_router.post("/admin/users/{user_id}/role")
async def set_user_role(
    user_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Assign role to user"""
    admin = await get_current_user(authorization, request)
    
    body = await request.json()
    role = body.get("role", "user")
    
    if role not in ROLE_PERMISSIONS and role != "user":
        raise HTTPException(status_code=400, detail="Invalid role")
    
    await db.users.update_one(
        {"user_id": user_id},
        {"$set": {"role": role, "is_admin": role != "user"}}
    )
    
    await db.admin_logs.insert_one({
        "action": "role_assigned",
        "admin_id": admin.user_id,
        "target_user_id": user_id,
        "details": {"role": role},
        "timestamp": datetime.now(timezone.utc)
    })
    
    return {"success": True, "role": role}

# ============= AFFILIATE MANAGEMENT SYSTEM =============

def generate_affiliate_code():
    """Generate unique affiliate code"""
    chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return ''.join(random.choices(chars, k=8))

def generate_referral_link(affiliate_code: str):
    """Generate referral link"""
    base_url = "https://bynix.io"
    return f"{base_url}/ref/{affiliate_code}"

@api_router.post("/affiliates/register")
async def register_affiliate(authorization: Optional[str] = Header(None), request: Request = None):
    """Register as affiliate"""
    user = await get_current_user(authorization, request)
    
    # Check if already affiliate
    existing = await db.affiliates.find_one({"user_id": user.user_id})
    if existing:
        raise HTTPException(status_code=400, detail="Already registered as affiliate")
    
    body = await request.json()
    
    affiliate_code = generate_affiliate_code()
    while await db.affiliates.find_one({"affiliate_code": affiliate_code}):
        affiliate_code = generate_affiliate_code()
    
    affiliate = {
        "affiliate_id": f"aff_{uuid.uuid4().hex[:12]}",
        "user_id": user.user_id,
        "email": user.email,
        "affiliate_code": affiliate_code,
        "referral_link": generate_referral_link(affiliate_code),
        "status": "pending",  # pending, active, suspended
        "commission_type": "revenue_share",  # cpa, revenue_share, hybrid
        "commission_rate": 25,  # percentage
        "cpa_amount": 10,  # fixed CPA amount
        "tier": 1,  # affiliate tier level
        "payment_info": {
            "method": body.get("payment_method", "crypto"),
            "wallet_address": body.get("wallet_address", ""),
            "bank_details": body.get("bank_details", {})
        },
        "stats": {
            "total_clicks": 0,
            "total_signups": 0,
            "total_deposits": 0,
            "total_traders": 0,
            "total_volume": 0,
            "total_earnings": 0,
            "pending_earnings": 0,
            "paid_earnings": 0
        },
        "created_at": datetime.now(timezone.utc),
        "approved_at": None,
        "manager_id": None
    }
    
    await db.affiliates.insert_one(affiliate)
    
    return {
        "success": True,
        "affiliate_code": affiliate_code,
        "referral_link": affiliate["referral_link"],
        "status": "pending"
    }

@api_router.get("/affiliates/me")
async def get_my_affiliate_profile(authorization: Optional[str] = Header(None), request: Request = None):
    """Get current user's affiliate profile"""
    user = await get_current_user(authorization, request)
    
    affiliate = await db.affiliates.find_one({"user_id": user.user_id})
    if not affiliate:
        return {"is_affiliate": False}
    
    # Get referrals
    referrals = await db.referrals.find({"affiliate_id": affiliate["affiliate_id"]}).to_list(1000)
    
    # Get commissions
    commissions = await db.commissions.find({"affiliate_id": affiliate["affiliate_id"]}).sort("created_at", -1).limit(50).to_list(50)
    
    return {
        "is_affiliate": True,
        "affiliate_id": affiliate["affiliate_id"],
        "affiliate_code": affiliate["affiliate_code"],
        "referral_link": affiliate["referral_link"],
        "status": affiliate["status"],
        "commission_type": affiliate["commission_type"],
        "commission_rate": affiliate["commission_rate"],
        "cpa_amount": affiliate.get("cpa_amount", 10),
        "tier": affiliate.get("tier", 1),
        "stats": affiliate["stats"],
        "payment_info": affiliate.get("payment_info", {}),
        "referrals_count": len(referrals),
        "recent_commissions": [
            {
                "commission_id": c.get("commission_id"),
                "amount": c.get("amount"),
                "type": c.get("type"),
                "status": c.get("status"),
                "created_at": str(c.get("created_at", ""))
            }
            for c in commissions
        ]
    }

@api_router.get("/affiliates/referrals")
async def get_affiliate_referrals(authorization: Optional[str] = Header(None), request: Request = None):
    """Get affiliate's referrals"""
    user = await get_current_user(authorization, request)
    
    affiliate = await db.affiliates.find_one({"user_id": user.user_id})
    if not affiliate:
        raise HTTPException(status_code=404, detail="Not an affiliate")
    
    referrals = await db.referrals.find({"affiliate_id": affiliate["affiliate_id"]}).sort("created_at", -1).to_list(100)
    
    result = []
    for ref in referrals:
        referred_user = await db.users.find_one({"user_id": ref.get("referred_user_id")})
        if referred_user:
            # Get user's trading stats
            trades = await db.trades.find({"user_id": ref.get("referred_user_id"), "account_type": "real"}).to_list(1000)
            total_volume = sum(t.get("amount", 0) for t in trades)
            total_loss = sum(t.get("profit_loss", 0) for t in trades if t.get("status") == "lost")
            
            result.append({
                "referral_id": ref.get("referral_id"),
                "user_email": referred_user.get("email", "")[:3] + "***",  # Masked
                "signup_date": str(ref.get("created_at", "")),
                "first_deposit": ref.get("first_deposit", 0),
                "total_deposits": ref.get("total_deposits", 0),
                "total_volume": total_volume,
                "total_commission": ref.get("total_commission", 0),
                "status": "active" if trades else "inactive"
            })
    
    return {"referrals": result}

@api_router.post("/affiliates/track-click")
async def track_affiliate_click(request: Request):
    """Track affiliate link click (public endpoint)"""
    body = await request.json()
    affiliate_code = body.get("code")
    
    if not affiliate_code:
        return {"success": False}
    
    # Find affiliate by ref_code or link code
    affiliate = await db.affiliates.find_one({"ref_code": affiliate_code})
    link = None
    
    if not affiliate:
        # Try to find via affiliate_links
        link = await db.affiliate_links.find_one({"code": affiliate_code})
        if link:
            affiliate = await db.affiliates.find_one({"affiliate_id": link.get("affiliate_id")})
    
    if not affiliate:
        return {"success": False}
    
    # Record click
    await db.affiliate_clicks.insert_one({
        "affiliate_id": affiliate["affiliate_id"],
        "affiliate_code": affiliate_code,
        "link_id": link.get("link_id") if link else None,
        "ip": request.client.host if request.client else "unknown",
        "user_agent": request.headers.get("user-agent", ""),
        "timestamp": datetime.now(timezone.utc)
    })
    
    # Update affiliate total_clicks
    await db.affiliates.update_one(
        {"affiliate_id": affiliate["affiliate_id"]},
        {"$inc": {"total_clicks": 1}}
    )
    
    # Update link clicks if specific link was used
    if link:
        await db.affiliate_links.update_one(
            {"link_id": link.get("link_id")},
            {"$inc": {"clicks": 1}}
        )
    
    return {"success": True, "code": affiliate_code}

@api_router.post("/affiliates/process-signup")
async def process_affiliate_signup(
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Process signup from affiliate referral (internal use)"""
    body = await request.json()
    affiliate_code = body.get("affiliate_code")
    new_user_id = body.get("user_id")
    
    if not affiliate_code or not new_user_id:
        return {"success": False}
    
    affiliate = await db.affiliates.find_one({"affiliate_code": affiliate_code, "status": "active"})
    if not affiliate:
        return {"success": False}
    
    # Check if already referred
    existing = await db.referrals.find_one({"referred_user_id": new_user_id})
    if existing:
        return {"success": False, "message": "User already referred"}
    
    # Create referral record
    referral = {
        "referral_id": f"ref_{uuid.uuid4().hex[:12]}",
        "affiliate_id": affiliate["affiliate_id"],
        "affiliate_code": affiliate_code,
        "referred_user_id": new_user_id,
        "first_deposit": 0,
        "total_deposits": 0,
        "total_commission": 0,
        "created_at": datetime.now(timezone.utc)
    }
    
    await db.referrals.insert_one(referral)
    
    # Update affiliate stats
    await db.affiliates.update_one(
        {"affiliate_id": affiliate["affiliate_id"]},
        {"$inc": {"stats.total_signups": 1}}
    )
    
    # Mark user as referred
    await db.users.update_one(
        {"user_id": new_user_id},
        {"$set": {"referred_by": affiliate["affiliate_id"], "referral_code": affiliate_code}}
    )
    
    return {"success": True}

@api_router.post("/affiliates/process-deposit")
async def process_affiliate_deposit(
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Process deposit commission for affiliate (called after deposit confirmed)"""
    body = await request.json()
    user_id = body.get("user_id")
    deposit_amount = float(body.get("amount", 0))
    
    # Find referral
    referral = await db.referrals.find_one({"referred_user_id": user_id})
    if not referral:
        return {"success": False, "message": "User not referred"}
    
    affiliate = await db.affiliates.find_one({"affiliate_id": referral["affiliate_id"]})
    if not affiliate or affiliate["status"] != "active":
        return {"success": False}
    
    commission_amount = 0
    commission_type = ""
    
    # Calculate commission based on type
    if affiliate["commission_type"] == "cpa":
        # CPA: One-time payment on first deposit
        if referral.get("first_deposit", 0) == 0:
            commission_amount = affiliate.get("cpa_amount", 10)
            commission_type = "cpa"
    elif affiliate["commission_type"] == "revenue_share":
        # Revenue share: percentage of deposit
        commission_amount = deposit_amount * (affiliate["commission_rate"] / 100)
        commission_type = "revenue_share"
    else:  # hybrid
        if referral.get("first_deposit", 0) == 0:
            commission_amount = affiliate.get("cpa_amount", 10)
            commission_type = "cpa"
        commission_amount += deposit_amount * (affiliate["commission_rate"] / 100)
        commission_type = "hybrid"
    
    if commission_amount > 0:
        # Create commission record
        commission = {
            "commission_id": f"comm_{uuid.uuid4().hex[:12]}",
            "affiliate_id": affiliate["affiliate_id"],
            "referral_id": referral["referral_id"],
            "referred_user_id": user_id,
            "amount": commission_amount,
            "type": commission_type,
            "source_amount": deposit_amount,
            "status": "pending",
            "created_at": datetime.now(timezone.utc)
        }
        
        await db.commissions.insert_one(commission)
        
        # Update affiliate stats
        await db.affiliates.update_one(
            {"affiliate_id": affiliate["affiliate_id"]},
            {
                "$inc": {
                    "stats.total_deposits": deposit_amount,
                    "stats.total_earnings": commission_amount,
                    "stats.pending_earnings": commission_amount
                }
            }
        )
        
        # Update referral
        update_fields = {"$inc": {"total_deposits": deposit_amount, "total_commission": commission_amount}}
        if referral.get("first_deposit", 0) == 0:
            update_fields["$set"] = {"first_deposit": deposit_amount}
        await db.referrals.update_one({"referral_id": referral["referral_id"]}, update_fields)
    
    return {"success": True, "commission": commission_amount}

@api_router.post("/affiliates/withdraw")
async def request_affiliate_withdrawal(
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Request withdrawal of affiliate earnings"""
    user = await get_current_user(authorization, request)
    
    affiliate = await db.affiliates.find_one({"user_id": user.user_id})
    if not affiliate:
        raise HTTPException(status_code=404, detail="Not an affiliate")
    
    body = await request.json()
    amount = float(body.get("amount", 0))
    
    pending = affiliate["stats"].get("pending_earnings", 0)
    if amount > pending:
        raise HTTPException(status_code=400, detail="Insufficient pending earnings")
    
    min_payout = 50  # Minimum payout threshold
    if amount < min_payout:
        raise HTTPException(status_code=400, detail=f"Minimum payout is ${min_payout}")
    
    # Create payout request
    payout = {
        "payout_id": f"payout_{uuid.uuid4().hex[:12]}",
        "affiliate_id": affiliate["affiliate_id"],
        "user_id": user.user_id,
        "amount": amount,
        "payment_method": affiliate.get("payment_info", {}).get("method", "crypto"),
        "wallet_address": affiliate.get("payment_info", {}).get("wallet_address", ""),
        "status": "pending",
        "created_at": datetime.now(timezone.utc)
    }
    
    await db.affiliate_payouts.insert_one(payout)
    
    # Move from pending to processing
    await db.affiliates.update_one(
        {"affiliate_id": affiliate["affiliate_id"]},
        {"$inc": {"stats.pending_earnings": -amount}}
    )
    
    return {"success": True, "payout_id": payout["payout_id"]}

# ============= ADMIN AFFILIATE MANAGEMENT =============

@api_router.get("/admin/affiliates")
async def admin_get_affiliates(
    status: str = None,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get all affiliates (admin)"""
    user = await get_current_user(authorization, request)
    
    query = {}
    if status:
        query["status"] = status
    
    affiliates = await db.affiliates.find(query).sort("created_at", -1).to_list(500)
    
    return {
        "affiliates": [
            {
                "affiliate_id": a.get("affiliate_id"),
                "user_id": a.get("user_id"),
                "email": a.get("email"),
                "affiliate_code": a.get("affiliate_code"),
                "referral_link": a.get("referral_link"),
                "status": a.get("status"),
                "commission_type": a.get("commission_type"),
                "commission_rate": a.get("commission_rate"),
                "cpa_amount": a.get("cpa_amount", 10),
                "tier": a.get("tier", 1),
                "stats": a.get("stats", {}),
                "created_at": str(a.get("created_at", ""))
            }
            for a in affiliates
        ]
    }

@api_router.post("/admin/affiliates/{affiliate_id}/approve")
async def admin_approve_affiliate(
    affiliate_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Approve affiliate application"""
    admin = await get_current_user(authorization, request)
    
    result = await db.affiliates.update_one(
        {"affiliate_id": affiliate_id},
        {
            "$set": {
                "status": "active",
                "approved_at": datetime.now(timezone.utc),
                "approved_by": admin.user_id
            }
        }
    )
    
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Affiliate not found")
    
    return {"success": True, "status": "active"}

@api_router.post("/admin/affiliates/{affiliate_id}/suspend")
async def admin_suspend_affiliate(
    affiliate_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Suspend affiliate"""
    admin = await get_current_user(authorization, request)
    
    body = await request.json()
    reason = body.get("reason", "")
    
    await db.affiliates.update_one(
        {"affiliate_id": affiliate_id},
        {
            "$set": {
                "status": "suspended",
                "suspended_at": datetime.now(timezone.utc),
                "suspended_by": admin.user_id,
                "suspension_reason": reason
            }
        }
    )
    
    return {"success": True, "status": "suspended"}

@api_router.post("/admin/affiliates/{affiliate_id}/commission")
async def admin_set_affiliate_commission(
    affiliate_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Set affiliate commission settings"""
    admin = await get_current_user(authorization, request)
    
    body = await request.json()
    
    update = {}
    if "commission_type" in body:
        update["commission_type"] = body["commission_type"]
    if "commission_rate" in body:
        update["commission_rate"] = float(body["commission_rate"])
    if "cpa_amount" in body:
        update["cpa_amount"] = float(body["cpa_amount"])
    if "tier" in body:
        update["tier"] = int(body["tier"])
    
    if update:
        await db.affiliates.update_one(
            {"affiliate_id": affiliate_id},
            {"$set": update}
        )
    
    return {"success": True}

@api_router.post("/admin/affiliates/payouts/{payout_id}/approve")
async def admin_approve_payout(
    payout_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Approve affiliate payout"""
    admin = await get_current_user(authorization, request)
    
    payout = await db.affiliate_payouts.find_one({"payout_id": payout_id})
    if not payout:
        raise HTTPException(status_code=404, detail="Payout not found")
    
    if payout["status"] != "pending":
        raise HTTPException(status_code=400, detail="Payout not pending")
    
    await db.affiliate_payouts.update_one(
        {"payout_id": payout_id},
        {
            "$set": {
                "status": "completed",
                "approved_by": admin.user_id,
                "approved_at": datetime.now(timezone.utc)
            }
        }
    )
    
    # Update affiliate stats
    await db.affiliates.update_one(
        {"affiliate_id": payout["affiliate_id"]},
        {"$inc": {"stats.paid_earnings": payout["amount"]}}
    )
    
    return {"success": True}

@api_router.post("/admin/affiliates/payouts/{payout_id}/reject")
async def admin_reject_payout(
    payout_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Reject affiliate payout"""
    admin = await get_current_user(authorization, request)
    
    body = await request.json()
    reason = body.get("reason", "")
    
    payout = await db.affiliate_payouts.find_one({"payout_id": payout_id})
    if not payout:
        raise HTTPException(status_code=404, detail="Payout not found")
    
    await db.affiliate_payouts.update_one(
        {"payout_id": payout_id},
        {
            "$set": {
                "status": "rejected",
                "rejected_by": admin.user_id,
                "rejected_at": datetime.now(timezone.utc),
                "rejection_reason": reason
            }
        }
    )
    
    # Return to pending earnings
    await db.affiliates.update_one(
        {"affiliate_id": payout["affiliate_id"]},
        {"$inc": {"stats.pending_earnings": payout["amount"]}}
    )
    
    return {"success": True}

@api_router.get("/admin/affiliates/stats")
async def admin_get_affiliate_stats(authorization: Optional[str] = Header(None), request: Request = None):
    """Get overall affiliate program stats"""
    user = await get_current_user(authorization, request)
    
    total_affiliates = await db.affiliates.count_documents({})
    # Check both is_active and status fields for backwards compatibility
    active_affiliates = await db.affiliates.count_documents({
        "$or": [
            {"is_active": True},
            {"status": "active"}
        ]
    })
    pending_affiliates = await db.affiliates.count_documents({
        "$or": [
            {"is_active": False},
            {"status": "pending"}
        ]
    })
    
    total_referrals = await db.affiliate_referrals.count_documents({})
    
    # Calculate total commissions paid
    pipeline = [
        {"$match": {"status": "paid"}},
        {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
    ]
    paid_result = await db.affiliate_commissions.aggregate(pipeline).to_list(1)
    total_paid = paid_result[0]["total"] if paid_result else 0
    
    # Pending payouts amount - from affiliate_withdrawals collection
    pending_pipeline = [
        {"$match": {"status": "pending"}},
        {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
    ]
    pending_result = await db.affiliate_withdrawals.aggregate(pending_pipeline).to_list(1)
    pending_payouts_amount = pending_result[0]["total"] if pending_result else 0
    
    # Count pending requests
    pending_count = await db.affiliate_withdrawals.count_documents({"status": "pending"})
    
    # Get commission settings
    settings = await db.affiliate_settings.find_one({"type": "commission"})
    if not settings:
        settings = {
            "revenue_share": 50,
            "turnover_commission": 2,
            "cpa_amount": 50
        }
    
    return {
        "total_affiliates": total_affiliates,
        "active_affiliates": active_affiliates,
        "pending_affiliates": pending_affiliates,
        "total_referrals": total_referrals,
        "total_paid": total_paid,
        "pending_payouts": pending_payouts_amount,
        "pending_payout_count": pending_count,
        "commission_settings": {
            "revenue_share": settings.get("revenue_share", 50),
            "turnover_commission": settings.get("turnover_commission", 2),
            "cpa_amount": settings.get("cpa_amount", 50)
        }
    }

@api_router.put("/admin/affiliates/settings")
async def admin_update_affiliate_settings(
    settings: dict,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Update affiliate commission settings"""
    user = await get_current_user(authorization, request)
    
    await db.affiliate_settings.update_one(
        {"type": "commission"},
        {"$set": {
            "type": "commission",
            "revenue_share": settings.get("revenue_share", 50),
            "turnover_commission": settings.get("turnover_commission", 2),
            "cpa_amount": settings.get("cpa_amount", 50),
            "updated_at": datetime.now(timezone.utc),
            "updated_by": user.user_id
        }},
        upsert=True
    )
    
    return {"message": "Settings updated successfully"}

@api_router.post("/admin/affiliates/create")
async def admin_create_affiliate(
    data: dict,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Create a new affiliate partner"""
    user = await get_current_user(authorization, request)
    
    # Generate unique affiliate code
    import secrets
    affiliate_code = f"BNX{secrets.token_hex(4).upper()}"
    affiliate_id = f"aff_{secrets.token_hex(8)}"
    
    affiliate_doc = {
        "affiliate_id": affiliate_id,
        "name": data.get("name", ""),
        "email": data.get("email", ""),
        "phone": data.get("phone", ""),
        "company": data.get("company", ""),
        "ref_code": affiliate_code,
        "status": "active",
        "commission_rate": data.get("commission_rate", 50),
        "turnover_rate": data.get("turnover_rate", 2),
        "cpa_amount": data.get("cpa_amount", 50),
        "total_referrals": 0,
        "total_ftds": 0,
        "total_deposits": 0,
        "total_earnings": 0,
        "pending_earnings": 0,
        "paid_earnings": 0,
        "created_at": datetime.now(timezone.utc),
        "created_by": user.user_id
    }
    
    await db.affiliates.insert_one(affiliate_doc)
    
    return {
        "message": "Affiliate created successfully",
        "affiliate": {
            "affiliate_id": affiliate_id,
            "ref_code": affiliate_code,
            "name": data.get("name"),
            "email": data.get("email")
        }
    }

@api_router.delete("/admin/affiliates/{affiliate_id}")
async def admin_delete_affiliate(
    affiliate_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Delete an affiliate"""
    user = await get_current_user(authorization, request)
    
    result = await db.affiliates.delete_one({"affiliate_id": affiliate_id})
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Affiliate not found")
    
    return {"message": "Affiliate deleted successfully"}

@api_router.put("/admin/affiliates/{affiliate_id}")
async def admin_update_affiliate(
    affiliate_id: str,
    data: dict,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Update affiliate details"""
    user = await get_current_user(authorization, request)
    
    update_data = {
        "updated_at": datetime.now(timezone.utc),
        "updated_by": user.user_id
    }
    
    if "name" in data:
        update_data["name"] = data["name"]
    if "email" in data:
        update_data["email"] = data["email"]
    if "phone" in data:
        update_data["phone"] = data["phone"]
    if "company" in data:
        update_data["company"] = data["company"]
    if "commission_rate" in data:
        update_data["commission_rate"] = data["commission_rate"]
    if "turnover_rate" in data:
        update_data["turnover_rate"] = data["turnover_rate"]
    if "cpa_amount" in data:
        update_data["cpa_amount"] = data["cpa_amount"]
    if "status" in data:
        update_data["status"] = data["status"]
    
    result = await db.affiliates.update_one(
        {"affiliate_id": affiliate_id},
        {"$set": update_data}
    )
    
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Affiliate not found")
    
    return {"message": "Affiliate updated successfully"}

# ============= COMPREHENSIVE AFFILIATE MANAGEMENT SYSTEM =============

@api_router.get("/admin/affiliates/list")
async def admin_get_affiliates_list(
    search: str = None,
    status: str = None,
    sort_by: str = "total_earnings",
    sort_order: str = "desc",
    limit: int = 50,
    offset: int = 0,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get all affiliates with search and filters"""
    user = await get_current_user(authorization, request)
    
    query = {}
    
    if search:
        query["$or"] = [
            {"name": {"$regex": search, "$options": "i"}},
            {"email": {"$regex": search, "$options": "i"}},
            {"ref_code": {"$regex": search, "$options": "i"}},
            {"affiliate_id": {"$regex": search, "$options": "i"}}
        ]
    
    if status:
        query["status"] = status
    
    # Sort direction
    sort_dir = -1 if sort_order == "desc" else 1
    
    affiliates = await db.affiliates.find(query).sort(sort_by, sort_dir).skip(offset).limit(limit).to_list(length=limit)
    total = await db.affiliates.count_documents(query)
    
    # Process affiliates
    result = []
    for aff in affiliates:
        aff["_id"] = str(aff["_id"])
        # Calculate fraud score - returns dict with {score, level, alerts}
        fraud_data = await calculate_affiliate_fraud_score(aff.get("affiliate_id"))
        aff["fraud_score"] = fraud_data.get("score", 0) if isinstance(fraud_data, dict) else 0
        aff["fraud_level"] = fraud_data.get("level", "low") if isinstance(fraud_data, dict) else "low"
        aff["fraud_alerts"] = fraud_data.get("alerts", []) if isinstance(fraud_data, dict) else []
        result.append(aff)
    
    return {
        "affiliates": result,
        "total": total,
        "limit": limit,
        "offset": offset
    }

@api_router.get("/admin/affiliates/top")
async def admin_get_top_affiliates(
    limit: int = 10,
    period: str = "all",  # all, month, week
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get top performing affiliates"""
    user = await get_current_user(authorization, request)
    
    # Date filter
    date_filter = {}
    if period == "month":
        date_filter = {"created_at": {"$gte": datetime.now(timezone.utc) - timedelta(days=30)}}
    elif period == "week":
        date_filter = {"created_at": {"$gte": datetime.now(timezone.utc) - timedelta(days=7)}}
    
    # Get top by earnings
    top_by_earnings = await db.affiliates.find(date_filter).sort("total_earnings", -1).limit(limit).to_list(length=limit)
    
    # Get top by referrals
    top_by_referrals = await db.affiliates.find(date_filter).sort("total_referrals", -1).limit(limit).to_list(length=limit)
    
    # Get top by FTDs
    top_by_ftds = await db.affiliates.find(date_filter).sort("total_ftds", -1).limit(limit).to_list(length=limit)
    
    return {
        "top_by_earnings": [{**a, "_id": str(a["_id"])} for a in top_by_earnings],
        "top_by_referrals": [{**a, "_id": str(a["_id"])} for a in top_by_referrals],
        "top_by_ftds": [{**a, "_id": str(a["_id"])} for a in top_by_ftds]
    }

@api_router.get("/admin/affiliates/{affiliate_id}/profile")
async def admin_get_affiliate_profile(
    affiliate_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get detailed affiliate profile with referred clients"""
    user = await get_current_user(authorization, request)
    
    affiliate = await db.affiliates.find_one({"affiliate_id": affiliate_id})
    if not affiliate:
        raise HTTPException(status_code=404, detail="Affiliate not found")
    
    affiliate["_id"] = str(affiliate["_id"])
    
    # Get referred clients
    referred_users = await db.users.find(
        {"referred_by": affiliate.get("ref_code")}
    ).to_list(length=500)
    
    # Calculate commission for each client
    clients_with_commission = []
    total_commission = 0
    
    for ref_user in referred_users:
        # Get trades for this user
        user_trades = await db.trades.find({
            "user_id": ref_user["user_id"],
            "status": {"$in": ["won", "lost"]}
        }).to_list(length=1000)
        
        user_commission = 0
        user_volume = 0
        
        for trade in user_trades:
            user_volume += abs(trade.get("amount", 0))
            # Commission from losses (revenue share)
            if trade.get("status") == "lost":
                user_commission += abs(trade.get("profit", 0)) * (affiliate.get("commission_rate", 50) / 100)
            # Turnover commission
            user_commission += abs(trade.get("amount", 0)) * (affiliate.get("turnover_rate", 2) / 100)
        
        # Check if FTD (first time deposit)
        first_deposit = await db.deposits.find_one({
            "user_id": ref_user["user_id"],
            "status": "completed"
        }, sort=[("created_at", 1)])
        
        is_ftd = first_deposit is not None
        if is_ftd:
            user_commission += affiliate.get("cpa_amount", 50)
        
        total_commission += user_commission
        
        clients_with_commission.append({
            "user_id": ref_user["user_id"],
            "email": ref_user.get("email", ""),
            "name": ref_user.get("name", ref_user.get("nickname", "")),
            "registered_at": ref_user.get("created_at"),
            "is_ftd": is_ftd,
            "total_volume": round(user_volume, 2),
            "commission_earned": round(user_commission, 2),
            "country": ref_user.get("country", "Unknown"),
            "country_flag": ref_user.get("country_flag", "🌍")
        })
    
    # Calculate fraud score
    fraud_score = await calculate_affiliate_fraud_score(affiliate_id)
    
    # Get fraud alerts
    fraud_alerts = await db.affiliate_fraud_alerts.find(
        {"affiliate_id": affiliate_id}
    ).sort("created_at", -1).limit(20).to_list(length=20)
    
    # Get payout history
    payouts = await db.affiliate_payouts.find(
        {"affiliate_id": affiliate_id}
    ).sort("created_at", -1).limit(50).to_list(length=50)
    
    return {
        "affiliate": affiliate,
        "referred_clients": clients_with_commission,
        "total_clients": len(clients_with_commission),
        "total_commission_earned": round(total_commission, 2),
        "fraud_score": fraud_score,
        "fraud_alerts": [{**a, "_id": str(a["_id"])} for a in fraud_alerts],
        "payouts": [{**p, "_id": str(p["_id"])} for p in payouts]
    }

async def calculate_affiliate_fraud_score(affiliate_id: str) -> dict:
    """Calculate fraud score for an affiliate"""
    score = 0
    alerts = []
    
    affiliate = await db.affiliates.find_one({"affiliate_id": affiliate_id})
    if not affiliate:
        return {"score": 0, "level": "low", "alerts": []}
    
    ref_code = affiliate.get("ref_code")
    
    # Get referred users
    referred_users = await db.users.find({"referred_by": ref_code}).to_list(length=500)
    
    if not referred_users:
        return {"score": 0, "level": "low", "alerts": []}
    
    # 1. Self-referral detection (same email domain, IP)
    affiliate_email = affiliate.get("email", "")
    affiliate_domain = affiliate_email.split("@")[-1] if "@" in affiliate_email else ""
    
    same_domain_count = 0
    for ref_user in referred_users:
        user_email = ref_user.get("email", "")
        user_domain = user_email.split("@")[-1] if "@" in user_email else ""
        if user_domain == affiliate_domain and affiliate_domain:
            same_domain_count += 1
    
    if same_domain_count > 3:
        score += 25
        alerts.append({
            "type": "self_referral",
            "message": f"{same_domain_count} referrals with same email domain",
            "severity": "high"
        })
    
    # 2. Same IP tracking
    user_ips = [u.get("registration_ip", u.get("last_ip")) for u in referred_users if u.get("registration_ip") or u.get("last_ip")]
    ip_counts = {}
    for ip in user_ips:
        if ip:
            ip_counts[ip] = ip_counts.get(ip, 0) + 1
    
    duplicate_ips = [ip for ip, count in ip_counts.items() if count > 2]
    if duplicate_ips:
        score += 20
        alerts.append({
            "type": "same_ip",
            "message": f"{len(duplicate_ips)} IPs used by multiple referrals",
            "severity": "medium"
        })
    
    # 3. Fake volume filtering (very quick trades, same amounts)
    suspicious_trades = 0
    for ref_user in referred_users[:20]:  # Check first 20
        trades = await db.trades.find({
            "user_id": ref_user["user_id"],
            "status": {"$in": ["won", "lost"]}
        }).limit(50).to_list(length=50)
        
        # Check for suspicious patterns
        trade_amounts = [t.get("amount", 0) for t in trades]
        if trade_amounts:
            # All same amount
            if len(set(trade_amounts)) == 1 and len(trade_amounts) > 5:
                suspicious_trades += 1
    
    if suspicious_trades > 3:
        score += 30
        alerts.append({
            "type": "fake_volume",
            "message": f"{suspicious_trades} users with suspicious trading patterns",
            "severity": "high"
        })
    
    # 4. Abnormal trading pattern (all losses for commission farming)
    high_loss_users = 0
    for ref_user in referred_users[:20]:
        trades = await db.trades.find({
            "user_id": ref_user["user_id"],
            "status": {"$in": ["won", "lost"]}
        }).to_list(length=100)
        
        if len(trades) > 5:
            losses = sum(1 for t in trades if t.get("status") == "lost")
            if losses / len(trades) > 0.9:  # 90%+ losses
                high_loss_users += 1
    
    if high_loss_users > 2:
        score += 25
        alerts.append({
            "type": "abnormal_pattern",
            "message": f"{high_loss_users} users with 90%+ loss rate",
            "severity": "high"
        })
    
    # Determine level
    if score >= 60:
        level = "critical"
    elif score >= 40:
        level = "high"
    elif score >= 20:
        level = "medium"
    else:
        level = "low"
    
    return {
        "score": min(score, 100),
        "level": level,
        "alerts": alerts
    }

@api_router.post("/admin/affiliates/{affiliate_id}/commission-adjustment")
async def admin_adjust_affiliate_commission(
    affiliate_id: str,
    data: dict,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Manually adjust affiliate commission"""
    user = await get_current_user(authorization, request)
    
    affiliate = await db.affiliates.find_one({"affiliate_id": affiliate_id})
    if not affiliate:
        # Try by ref_code
        affiliate = await db.affiliates.find_one({"ref_code": affiliate_id})
    if not affiliate:
        raise HTTPException(status_code=404, detail="Affiliate not found")
    
    adjustment_type = data.get("type", "add")  # add/credit, subtract/debit, set
    amount = float(data.get("amount", 0))
    reason = data.get("reason", "")
    client_id = data.get("client_id")  # Optional - specific client adjustment
    
    # Normalize type names
    if adjustment_type in ["credit", "add"]:
        adjustment_type = "add"
    elif adjustment_type in ["debit", "subtract"]:
        adjustment_type = "subtract"
    
    current_balance = affiliate.get("balance", affiliate.get("pending_earnings", 0))
    current_earnings = affiliate.get("total_earnings", 0)
    
    if adjustment_type == "add":
        new_balance = current_balance + amount
        new_earnings = current_earnings + amount
    elif adjustment_type == "subtract":
        new_balance = max(0, current_balance - amount)
        new_earnings = current_earnings  # Don't reduce total earnings for debits
    else:  # set
        new_balance = amount
        new_earnings = current_earnings
    
    # Update affiliate
    await db.affiliates.update_one(
        {"_id": affiliate["_id"]},
        {
            "$set": {
                "balance": new_balance,
                "pending_earnings": new_balance,
                "total_earnings": new_earnings
            }
        }
    )
    
    # Log adjustment
    adjustment_doc = {
        "affiliate_id": affiliate_id,
        "type": adjustment_type,
        "amount": amount,
        "reason": reason,
        "client_id": client_id,
        "previous_balance": current_balance,
        "new_balance": new_balance,
        "adjusted_by": user.user_id,
        "created_at": datetime.now(timezone.utc)
    }
    await db.affiliate_adjustments.insert_one(adjustment_doc)
    
    return {
        "message": "Commission adjusted successfully",
        "previous_balance": current_balance,
        "new_balance": new_balance
    }

@api_router.post("/admin/affiliates/{affiliate_id}/change-password")
async def admin_change_affiliate_password(
    affiliate_id: str,
    data: dict,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Admin change affiliate password"""
    user = await get_current_user(authorization, request)
    
    new_password = data.get("new_password", "")
    if len(new_password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters")
    
    affiliate = await db.affiliates.find_one({"affiliate_id": affiliate_id})
    if not affiliate:
        affiliate = await db.affiliates.find_one({"ref_code": affiliate_id})
    if not affiliate:
        raise HTTPException(status_code=404, detail="Affiliate not found")
    
    # Hash new password
    import hashlib
    hashed_password = hashlib.sha256(new_password.encode()).hexdigest()
    
    # Update affiliate password
    await db.affiliates.update_one(
        {"_id": affiliate["_id"]},
        {"$set": {"password": hashed_password, "password_updated_at": datetime.now(timezone.utc)}}
    )
    
    return {"message": "Password changed successfully"}

# ============= AFFILIATE PAYOUT MANAGEMENT =============

@api_router.get("/admin/affiliates/payouts")
async def admin_get_affiliate_payouts(
    status: str = None,
    affiliate_id: str = None,
    limit: int = 50,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get all affiliate payout requests"""
    user = await get_current_user(authorization, request)
    
    query = {}
    if status:
        query["status"] = status
    if affiliate_id:
        query["affiliate_id"] = affiliate_id
    
    # Fetch from affiliate_withdrawals collection (where actual withdrawal requests are stored)
    payouts = await db.affiliate_withdrawals.find(query).sort("created_at", -1).limit(limit).to_list(length=limit)
    
    result = []
    for payout in payouts:
        payout["_id"] = str(payout["_id"])
        # Get affiliate info
        affiliate = await db.affiliates.find_one({"affiliate_id": payout.get("affiliate_id")})
        if affiliate:
            payout["affiliate_name"] = affiliate.get("name", "")
            payout["affiliate_email"] = affiliate.get("email", "")
            payout["affiliate_ref_code"] = affiliate.get("ref_code", "")
        result.append(payout)
    
    # Get payout settings
    settings = await db.affiliate_settings.find_one({"type": "payout"})
    if not settings:
        settings = {
            "hold_period_days": 7,
            "min_payout": 50,
            "negative_balance_carryover": True
        }
    
    return {
        "payouts": result,
        "settings": settings
    }

@api_router.post("/admin/affiliates/payouts/process")
async def admin_process_affiliate_payout(
    data: dict,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Process/approve affiliate payout"""
    user = await get_current_user(authorization, request)
    
    payout_id = data.get("payout_id")
    action = data.get("action")  # approve, reject, hold
    notes = data.get("notes", "")
    
    # Try to find in affiliate_withdrawals first (where actual requests are)
    payout = await db.affiliate_withdrawals.find_one({"_id": ObjectId(payout_id)})
    if not payout:
        # Fallback to affiliate_payouts for backward compatibility
        payout = await db.affiliate_payouts.find_one({"_id": ObjectId(payout_id)})
    if not payout:
        raise HTTPException(status_code=404, detail="Payout not found")
    
    new_status = "approved" if action == "approve" else "rejected" if action == "reject" else "on_hold"
    collection_name = "affiliate_withdrawals" if await db.affiliate_withdrawals.find_one({"_id": ObjectId(payout_id)}) else "affiliate_payouts"
    
    await db[collection_name].update_one(
        {"_id": ObjectId(payout_id)},
        {
            "$set": {
                "status": new_status,
                "processed_by": user.user_id,
                "processed_at": datetime.now(timezone.utc),
                "admin_notes": notes
            }
        }
    )
    
    # If approved, deduct from balance (money is being sent out)
    if action == "approve":
        await db.affiliates.update_one(
            {"affiliate_id": payout.get("affiliate_id")},
            {
                "$inc": {
                    "paid_earnings": payout.get("amount", 0),
                    "balance": -payout.get("amount", 0),
                    "pending_earnings": -payout.get("amount", 0)
                }
            }
        )
    
    # If rejected, return the amount back to affiliate's available balance
    # The amount was already deducted from balance when withdrawal was requested
    # So we need to add it back
    if action == "reject":
        await db.affiliates.update_one(
            {"affiliate_id": payout.get("affiliate_id")},
            {
                "$inc": {
                    "balance": payout.get("amount", 0),  # Return amount to balance
                    "pending_earnings": -payout.get("amount", 0)  # Remove from pending
                }
            }
        )
    
    return {"message": f"Payout {new_status}"}

@api_router.put("/admin/affiliates/payout-settings")
async def admin_update_payout_settings(
    data: dict,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Update affiliate payout settings"""
    user = await get_current_user(authorization, request)
    
    await db.affiliate_settings.update_one(
        {"type": "payout"},
        {
            "$set": {
                "type": "payout",
                "hold_period_days": data.get("hold_period_days", 7),
                "min_payout": data.get("min_payout", 50),
                "negative_balance_carryover": data.get("negative_balance_carryover", True),
                "updated_at": datetime.now(timezone.utc),
                "updated_by": user.user_id
            }
        },
        upsert=True
    )
    
    return {"message": "Payout settings updated"}

@api_router.get("/admin/affiliates/withdrawal/{withdrawal_id}/details")
async def admin_get_withdrawal_details(
    withdrawal_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get detailed withdrawal info including commission breakdown"""
    user = await get_current_user(authorization, request)
    
    # Get withdrawal
    withdrawal = await db.affiliate_withdrawals.find_one({"_id": ObjectId(withdrawal_id)})
    if not withdrawal:
        raise HTTPException(status_code=404, detail="Withdrawal not found")
    
    withdrawal["_id"] = str(withdrawal["_id"])
    
    # Get affiliate details
    affiliate = await db.affiliates.find_one({"affiliate_id": withdrawal.get("affiliate_id")})
    affiliate_info = {}
    if affiliate:
        # Calculate fraud score
        fraud_score_raw = affiliate.get("fraud_score", {})
        if isinstance(fraud_score_raw, dict):
            total_score = sum(fraud_score_raw.values()) if fraud_score_raw else 0
            max_possible = len(fraud_score_raw) * 10 if fraud_score_raw else 100
            fraud_percentage = min(100, int((total_score / max_possible) * 100)) if max_possible > 0 else 0
        else:
            fraud_percentage = int(fraud_score_raw) if fraud_score_raw else 0
        
        affiliate_info = {
            "name": affiliate.get("name", "Unknown"),
            "email": affiliate.get("email", ""),
            "ref_code": affiliate.get("ref_code", ""),
            "level": affiliate.get("level", "starter"),
            "total_earnings": affiliate.get("total_earnings", 0),
            "paid_earnings": affiliate.get("paid_earnings", 0),
            "balance": affiliate.get("balance", 0),
            "pending_earnings": affiliate.get("pending_earnings", 0),
            "total_referrals": affiliate.get("total_referrals", 0),
            "total_ftds": affiliate.get("total_ftds", 0),
            "fraud_score": fraud_percentage,
            "fraud_details": fraud_score_raw if isinstance(fraud_score_raw, dict) else {},
            "is_active": affiliate.get("is_active", True),
            "created_at": affiliate.get("created_at", "").isoformat() if affiliate.get("created_at") else None,
            "wallets": affiliate.get("wallets", []),
        }
    
    # Get commission breakdown for this affiliate (last 30 days or related to this withdrawal)
    commission_query = {"affiliate_id": withdrawal.get("affiliate_id")}
    commissions = await db.affiliate_commissions.find(commission_query).sort("created_at", -1).limit(50).to_list(50)
    
    commission_breakdown = []
    for comm in commissions:
        # Get source user info
        source_user = await db.users.find_one({"user_id": comm.get("source_user_id")})
        commission_breakdown.append({
            "commission_id": str(comm.get("_id")),
            "amount": comm.get("amount", 0),
            "type": comm.get("type", "revenue_share"),  # revenue_share, cpa, turnover
            "source_user": {
                "user_id": comm.get("source_user_id"),
                "email": source_user.get("email", "Unknown") if source_user else "Unknown",
                "name": source_user.get("name", "User") if source_user else "User",
            },
            "trade_id": comm.get("trade_id"),
            "trade_amount": comm.get("trade_amount", 0),
            "created_at": comm.get("created_at", "").isoformat() if comm.get("created_at") else None,
        })
    
    # Get referrals list for this affiliate
    referrals = await db.referrals.find({"affiliate_id": withdrawal.get("affiliate_id")}).sort("created_at", -1).limit(50).to_list(50)
    
    referral_list = []
    for ref in referrals:
        ref_user = await db.users.find_one({"user_id": ref.get("referred_user_id")})
        referral_list.append({
            "referral_id": ref.get("referral_id"),
            "user_id": ref.get("referred_user_id"),
            "email": ref_user.get("email", "Unknown") if ref_user else "Unknown",
            "name": ref_user.get("name", "User") if ref_user else "User",
            "is_ftd": ref.get("is_ftd", False),
            "total_deposited": ref.get("total_deposited", 0),
            "total_traded": ref.get("total_traded", 0),
            "commission_earned": ref.get("commission_earned", 0),
            "created_at": ref.get("created_at", "").isoformat() if ref.get("created_at") else None,
        })
    
    # Calculate summary stats (Revenue Share and Turnover only - no CPA)
    total_revenue_share = sum(c.get("amount", 0) for c in commissions if c.get("type") == "revenue_share")
    total_turnover_commission = sum(c.get("amount", 0) for c in commissions if c.get("type") == "turnover")
    
    return {
        "withdrawal": withdrawal,
        "affiliate": affiliate_info,
        "commission_breakdown": commission_breakdown,
        "referrals": referral_list,
        "summary": {
            "total_revenue_share": total_revenue_share,
            "total_turnover_commission": total_turnover_commission,
            "total_referrals": len(referral_list),
            "total_ftds": len([r for r in referral_list if r.get("is_ftd")]),
        }
    }

# ============= AFFILIATE SUPPORT CHAT =============

@api_router.get("/admin/affiliates/support-chats")
async def admin_get_affiliate_support_chats(
    status: str = None,
    affiliate_id: str = None,
    limit: int = 50,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get affiliate support chat conversations"""
    user = await get_current_user(authorization, request)
    
    query = {}
    if status:
        query["status"] = status
    if affiliate_id:
        query["affiliate_id"] = affiliate_id
    
    chats = await db.affiliate_support_chats.find(query).sort("last_message_at", -1).limit(limit).to_list(length=limit)
    
    result = []
    for chat in chats:
        chat["_id"] = str(chat["_id"])
        # Get unread count
        unread = await db.affiliate_chat_messages.count_documents({
            "chat_id": chat.get("chat_id"),
            "sender_type": "affiliate",
            "read": False
        })
        chat["unread_count"] = unread
        
        # Get affiliate info
        affiliate = await db.affiliates.find_one({"affiliate_id": chat.get("affiliate_id")})
        if affiliate:
            chat["affiliate_name"] = affiliate.get("name", "")
            chat["affiliate_email"] = affiliate.get("email", "")
        
        result.append(chat)
    
    return {"chats": result}

@api_router.get("/admin/affiliates/support-chats/{chat_id}/messages")
async def admin_get_chat_messages(
    chat_id: str,
    limit: int = 100,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get messages for a specific chat"""
    user = await get_current_user(authorization, request)
    
    messages = await db.affiliate_chat_messages.find(
        {"chat_id": chat_id}
    ).sort("created_at", 1).limit(limit).to_list(length=limit)
    
    # Mark as read
    await db.affiliate_chat_messages.update_many(
        {"chat_id": chat_id, "sender_type": "affiliate"},
        {"$set": {"read": True}}
    )
    
    return {"messages": [{**m, "_id": str(m["_id"])} for m in messages]}

@api_router.post("/admin/affiliates/support-chats/{chat_id}/reply")
async def admin_reply_to_chat(
    chat_id: str,
    data: dict,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Reply to affiliate support chat"""
    user = await get_current_user(authorization, request)
    
    message = data.get("message", "")
    
    msg_doc = {
        "chat_id": chat_id,
        "message": message,
        "sender_type": "admin",
        "sender_id": user.user_id,
        "created_at": datetime.now(timezone.utc),
        "read": True
    }
    
    await db.affiliate_chat_messages.insert_one(msg_doc)
    
    # Update chat last message
    await db.affiliate_support_chats.update_one(
        {"chat_id": chat_id},
        {
            "$set": {
                "last_message": message,
                "last_message_at": datetime.now(timezone.utc),
                "status": "active"
            }
        }
    )
    
    return {"message": "Reply sent"}

# ============= AFFILIATE FRAUD ALERTS =============

@api_router.get("/admin/affiliates/fraud-alerts")
async def admin_get_fraud_alerts(
    severity: str = None,
    affiliate_id: str = None,
    resolved: bool = None,
    limit: int = 50,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get all fraud alerts"""
    user = await get_current_user(authorization, request)
    
    query = {}
    if severity:
        query["severity"] = severity
    if affiliate_id:
        query["affiliate_id"] = affiliate_id
    if resolved is not None:
        query["resolved"] = resolved
    
    alerts = await db.affiliate_fraud_alerts.find(query).sort("created_at", -1).limit(limit).to_list(length=limit)
    
    result = []
    for alert in alerts:
        alert["_id"] = str(alert["_id"])
        affiliate = await db.affiliates.find_one({"affiliate_id": alert.get("affiliate_id")})
        if affiliate:
            alert["affiliate_name"] = affiliate.get("name", "")
        result.append(alert)
    
    return {"alerts": result}

@api_router.post("/admin/affiliates/fraud-alerts/{alert_id}/resolve")
async def admin_resolve_fraud_alert(
    alert_id: str,
    data: dict,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Resolve a fraud alert"""
    user = await get_current_user(authorization, request)
    
    action = data.get("action", "dismiss")  # dismiss, ban_affiliate, deduct_commission
    notes = data.get("notes", "")
    
    await db.affiliate_fraud_alerts.update_one(
        {"_id": ObjectId(alert_id)},
        {
            "$set": {
                "resolved": True,
                "resolution_action": action,
                "resolution_notes": notes,
                "resolved_by": user.user_id,
                "resolved_at": datetime.now(timezone.utc)
            }
        }
    )
    
    alert = await db.affiliate_fraud_alerts.find_one({"_id": ObjectId(alert_id)})
    
    # Take action
    if action == "ban_affiliate" and alert:
        await db.affiliates.update_one(
            {"affiliate_id": alert.get("affiliate_id")},
            {"$set": {"status": "banned"}}
        )
    
    return {"message": "Alert resolved"}

# ============= ADVANCED USER MANAGEMENT SYSTEM =============

@api_router.get("/admin/users/detailed")
async def get_detailed_users(
    search: str = None,
    status: str = None,
    country: str = None,
    min_balance: float = None,
    max_balance: float = None,
    profit_status: str = None,  # profitable, losing, neutral
    segment: str = None,  # vip, high_risk, new, inactive
    sort_by: str = "real_balance",  # Default sort by balance
    sort_order: str = "desc",
    limit: int = 100,
    offset: int = 0,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get users with advanced filtering - sorted by balance by default"""
    admin = await get_current_user(authorization, request)
    
    # Build query
    query = {}
    
    if search:
        query["$or"] = [
            {"email": {"$regex": search, "$options": "i"}},
            {"name": {"$regex": search, "$options": "i"}},
            {"full_name": {"$regex": search, "$options": "i"}},
            {"account_id": {"$regex": search, "$options": "i"}},
            {"user_id": {"$regex": search, "$options": "i"}}
        ]
    
    if status:
        query["account_status"] = status
    
    if country:
        query["country"] = country
    
    if min_balance is not None:
        query["real_balance"] = {"$gte": min_balance}
    
    if max_balance is not None:
        if "real_balance" in query:
            query["real_balance"]["$lte"] = max_balance
        else:
            query["real_balance"] = {"$lte": max_balance}
    
    # Sorting
    sort_dir = -1 if sort_order == "desc" else 1
    
    users = await db.users.find(query).sort(sort_by, sort_dir).skip(offset).limit(limit).to_list(limit)
    total_count = await db.users.count_documents(query)
    
    result = []
    for u in users:
        # Get trading stats
        trades = await db.trades.find({"user_id": u.get("user_id"), "account_type": "real"}).to_list(1000)
        total_trades = len(trades)
        won_trades = len([t for t in trades if t.get("status") == "won"])
        lost_trades = len([t for t in trades if t.get("status") == "lost"])
        total_profit = sum(t.get("profit_loss", 0) for t in trades if t.get("status") in ["won", "lost"])
        win_rate = (won_trades / total_trades * 100) if total_trades > 0 else 0
        
        # Get deposit/withdrawal totals
        deposits = await db.deposits.find({"user_id": u.get("user_id"), "status": "completed"}).to_list(100)
        withdrawals = await db.withdrawals.find({"user_id": u.get("user_id"), "status": "completed"}).to_list(100)
        total_deposited = sum(d.get("amount_usd", 0) for d in deposits)
        total_withdrawn = sum(w.get("amount", 0) for w in withdrawals)
        
        # Filter by profit status
        if profit_status:
            if profit_status == "profitable" and total_profit <= 0:
                continue
            if profit_status == "losing" and total_profit >= 0:
                continue
        
        # Filter by segment
        if segment:
            if segment == "vip" and u.get("tier") != "vip":
                continue
            if segment == "high_risk" and u.get("risk_level") not in ["high", "critical"]:
                continue
            if segment == "new":
                created = u.get("created_at")
                if created and (datetime.now(timezone.utc) - created).days > 7:
                    continue
        
        result.append({
            "user_id": u.get("user_id"),
            "email": u.get("email"),
            "name": u.get("name") or u.get("full_name"),
            "account_id": u.get("account_id"),
            "country": u.get("country", "Unknown"),
            "country_flag": u.get("country_flag", "🌍"),
            "phone": u.get("phone"),
            "account_status": u.get("account_status", "active"),
            "is_verified": u.get("is_verified", False),
            "kyc_status": u.get("kyc_status", "pending"),
            "tier": u.get("tier", "standard"),
            "role": u.get("role", "user"),
            "balances": {
                "real": u.get("real_balance", 0),
                "demo": u.get("demo_balance", 10000),
                "bonus": u.get("bonus_balance", 0),
                "locked": u.get("locked_balance", 0)
            },
            "trading_stats": {
                "total_trades": total_trades,
                "won_trades": won_trades,
                "lost_trades": lost_trades,
                "win_rate": round(win_rate, 1),
                "net_profit": round(total_profit, 2)
            },
            "financial_stats": {
                "total_deposited": total_deposited,
                "total_withdrawn": total_withdrawn,
                "net_deposit": total_deposited - total_withdrawn
            },
            "risk_level": u.get("risk_level", "normal"),
            "risk_score": u.get("risk_score", 0),
            "is_shadow_banned": u.get("is_shadow_banned", False),
            "is_flagged": u.get("is_flagged", False),
            "last_login": str(u.get("last_login", "")),
            "created_at": str(u.get("created_at", ""))
        })
    
    return {
        "users": result,
        "total": total_count,
        "limit": limit,
        "offset": offset
    }

@api_router.get("/admin/users/{user_id}/full-profile")
async def get_user_full_profile(
    user_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get complete user profile with all details"""
    admin = await get_current_user(authorization, request)
    
    user = await db.users.find_one({"user_id": user_id})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Trading data
    trades = await db.trades.find({"user_id": user_id}).sort("created_at", -1).to_list(500)
    real_trades = [t for t in trades if t.get("account_type") == "real"]
    
    total_trades = len(real_trades)
    won_trades = len([t for t in real_trades if t.get("status") == "won"])
    lost_trades = len([t for t in real_trades if t.get("status") == "lost"])
    total_profit = sum(t.get("profit_loss", 0) for t in real_trades if t.get("status") in ["won", "lost"])
    total_volume = sum(t.get("amount", 0) for t in real_trades)
    avg_trade_size = total_volume / total_trades if total_trades > 0 else 0
    win_rate = (won_trades / total_trades * 100) if total_trades > 0 else 0
    
    # Financial data
    deposits = await db.deposits.find({"user_id": user_id}).sort("created_at", -1).to_list(100)
    withdrawals = await db.withdrawals.find({"user_id": user_id}).sort("created_at", -1).to_list(100)
    
    total_deposited = sum(d.get("amount_usd", 0) for d in deposits if d.get("status") == "completed")
    total_withdrawn = sum(w.get("amount", 0) for w in withdrawals if w.get("status") == "completed")
    pending_withdrawals = sum(w.get("amount", 0) for w in withdrawals if w.get("status") == "pending")
    
    # Activity logs
    user_logs = await db.user_activity_logs.find({"user_id": user_id}).sort("timestamp", -1).limit(50).to_list(50)
    
    # Admin notes
    admin_notes = await db.admin_notes.find({"user_id": user_id}).sort("created_at", -1).to_list(20)
    
    # Calculate risk score
    risk_score = 30  # Base
    if total_profit > total_deposited * 0.5:
        risk_score += 20  # Highly profitable
    if win_rate > 65:
        risk_score += 15  # High win rate
    if total_volume > 50000:
        risk_score += 10  # High volume
    if user.get("is_flagged"):
        risk_score += 20
    risk_score = min(100, max(0, risk_score))
    
    # Recent trades for chart
    recent_trades = [
        {
            "trade_id": t.get("trade_id"),
            "asset": t.get("asset"),
            "amount": t.get("amount"),
            "direction": t.get("direction"),
            "status": t.get("status"),
            "profit_loss": t.get("profit_loss", 0),
            "created_at": str(t.get("created_at", ""))
        }
        for t in trades[:20]
    ]
    
    # Recent deposits
    recent_deposits = [
        {
            "amount": d.get("amount_usd"),
            "method": d.get("payment_type", "crypto"),
            "status": d.get("status"),
            "created_at": str(d.get("created_at", ""))
        }
        for d in deposits[:10]
    ]
    
    # Recent withdrawals
    recent_withdrawals = [
        {
            "amount": w.get("amount"),
            "method": w.get("method", "crypto"),
            "status": w.get("status"),
            "wallet": w.get("wallet_address", "")[:20] + "..." if w.get("wallet_address") else "",
            "created_at": str(w.get("created_at", ""))
        }
        for w in withdrawals[:10]
    ]
    
    return {
        "user_id": user.get("user_id"),
        "account_id": user.get("account_id"),
        "email": user.get("email"),
        "name": user.get("name") or user.get("full_name"),
        "phone": user.get("phone"),
        "country": user.get("country", "Unknown"),
        "country_flag": user.get("country_flag", "🌍"),
        "timezone": user.get("timezone", "UTC"),
        "registration_ip": user.get("registration_ip"),
        "last_ip": user.get("last_ip"),
        "account_status": user.get("account_status", "active"),
        "is_verified": user.get("is_verified", False),
        "kyc_status": user.get("kyc_status", "pending"),
        "kyc_documents": user.get("kyc_documents", []),
        "tier": user.get("tier", "standard"),
        "role": user.get("role", "user"),
        "two_factor_enabled": user.get("two_factor_enabled", False),
        "balances": {
            "real": user.get("real_balance", 0),
            "demo": user.get("demo_balance", 10000),
            "bonus": user.get("bonus_balance", 0),
            "locked": user.get("locked_balance", 0)
        },
        "trading_summary": {
            "total_trades": total_trades,
            "won_trades": won_trades,
            "lost_trades": lost_trades,
            "win_rate": round(win_rate, 1),
            "total_volume": round(total_volume, 2),
            "avg_trade_size": round(avg_trade_size, 2),
            "net_profit": round(total_profit, 2)
        },
        "financial_summary": {
            "total_deposited": total_deposited,
            "total_withdrawn": total_withdrawn,
            "pending_withdrawals": pending_withdrawals,
            "net_deposit": total_deposited - total_withdrawn
        },
        "risk_profile": {
            "risk_score": risk_score,
            "risk_level": user.get("risk_level", "normal"),
            "is_flagged": user.get("is_flagged", False),
            "is_shadow_banned": user.get("is_shadow_banned", False),
            "flag_reason": user.get("flag_reason"),
            "win_rate_modifier": user.get("win_rate_modifier", 100),
            "payout_modifier": user.get("payout_modifier", 100),
            "max_trade_amount": user.get("max_trade_amount"),
            "withdrawal_locked": user.get("withdrawal_locked", False)
        },
        "admin_notes": [
            {
                "note": n.get("note"),
                "admin_id": n.get("admin_id"),
                "created_at": str(n.get("created_at", ""))
            }
            for n in admin_notes
        ],
        "recent_trades": recent_trades,
        "recent_deposits": recent_deposits,
        "recent_withdrawals": recent_withdrawals,
        "activity_logs": [
            {
                "action": l.get("action"),
                "details": l.get("details"),
                "ip": l.get("ip"),
                "timestamp": str(l.get("timestamp", ""))
            }
            for l in user_logs[:20]
        ],
        "last_login": str(user.get("last_login", "")),
        "created_at": str(user.get("created_at", ""))
    }

@api_router.post("/admin/users/{user_id}/status")
async def update_user_status(
    user_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Update user account status (active/suspended/banned)"""
    admin = await get_current_user(authorization, request)
    
    body = await request.json()
    status = body.get("status")  # active, suspended, restricted, banned
    reason = body.get("reason", "")
    
    if status not in ["active", "suspended", "restricted", "banned"]:
        raise HTTPException(status_code=400, detail="Invalid status")
    
    await db.users.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "account_status": status,
                "status_reason": reason,
                "status_updated_at": datetime.now(timezone.utc),
                "status_updated_by": admin.user_id
            }
        }
    )
    
    await db.admin_logs.insert_one({
        "action": "user_status_change",
        "admin_id": admin.user_id,
        "target_user_id": user_id,
        "details": {"status": status, "reason": reason},
        "timestamp": datetime.now(timezone.utc)
    })
    
    return {"success": True, "status": status}

@api_router.post("/admin/users/{user_id}/adjust-balance")
async def admin_adjust_balance(
    user_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Adjust user balance (add/remove)"""
    admin = await get_current_user(authorization, request)
    
    body = await request.json()
    amount = float(body.get("amount", 0))
    balance_type = body.get("balance_type", "real")  # real, demo, bonus
    operation = body.get("operation", "add")  # add, remove
    reason = body.get("reason", "Admin adjustment")
    
    field_map = {"real": "real_balance", "demo": "demo_balance", "bonus": "bonus_balance"}
    field = field_map.get(balance_type, "real_balance")
    
    if operation == "remove":
        amount = -abs(amount)
    else:
        amount = abs(amount)
    
    result = await db.users.update_one(
        {"user_id": user_id},
        {"$inc": {field: amount}}
    )
    
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Get admin id safely
    admin_id = admin.get("user_id") if isinstance(admin, dict) else getattr(admin, "user_id", "unknown")
    
    # Log the adjustment
    await db.balance_adjustments.insert_one({
        "adjustment_id": f"adj_{uuid.uuid4().hex[:12]}",
        "user_id": user_id,
        "admin_id": admin_id,
        "amount": amount,
        "balance_type": balance_type,
        "reason": reason,
        "created_at": datetime.now(timezone.utc)
    })
    
    await db.admin_logs.insert_one({
        "action": "balance_adjustment",
        "admin_id": admin_id,
        "target_user_id": user_id,
        "details": {"amount": amount, "balance_type": balance_type, "reason": reason},
        "timestamp": datetime.now(timezone.utc)
    })
    
    # Get updated balance
    updated_user = await db.users.find_one({"user_id": user_id})
    new_balance = updated_user.get(field, 0) if updated_user else 0
    
    return {"success": True, "amount_adjusted": amount, "new_balance": new_balance}

@api_router.post("/admin/users/{user_id}/lock-withdrawals")
async def lock_user_withdrawals(
    user_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Lock/unlock user withdrawals"""
    admin = await get_current_user(authorization, request)
    
    body = await request.json()
    locked = body.get("locked", True)
    reason = body.get("reason", "")
    
    await db.users.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "withdrawal_locked": locked,
                "withdrawal_lock_reason": reason,
                "withdrawal_locked_at": datetime.now(timezone.utc) if locked else None,
                "withdrawal_locked_by": admin.user_id if locked else None
            }
        }
    )
    
    await db.admin_logs.insert_one({
        "action": "withdrawal_lock",
        "admin_id": admin.user_id,
        "target_user_id": user_id,
        "details": {"locked": locked, "reason": reason},
        "timestamp": datetime.now(timezone.utc)
    })
    
    return {"success": True, "withdrawal_locked": locked}

@api_router.post("/admin/users/{user_id}/tier")
async def set_user_tier(
    user_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Set user tier (standard/vip/premium)"""
    admin = await get_current_user(authorization, request)
    
    body = await request.json()
    tier = body.get("tier", "standard")
    
    if tier not in ["standard", "vip", "premium"]:
        raise HTTPException(status_code=400, detail="Invalid tier")
    
    await db.users.update_one(
        {"user_id": user_id},
        {"$set": {"tier": tier}}
    )
    
    await db.admin_logs.insert_one({
        "action": "tier_change",
        "admin_id": admin.user_id,
        "target_user_id": user_id,
        "details": {"tier": tier},
        "timestamp": datetime.now(timezone.utc)
    })
    
    return {"success": True, "tier": tier}

@api_router.post("/admin/users/{user_id}/notes")
async def add_user_note(
    user_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Add admin note to user"""
    admin = await get_current_user(authorization, request)
    
    body = await request.json()
    note = body.get("note", "")
    
    if not note:
        raise HTTPException(status_code=400, detail="Note cannot be empty")
    
    await db.admin_notes.insert_one({
        "note_id": f"note_{uuid.uuid4().hex[:12]}",
        "user_id": user_id,
        "admin_id": admin.user_id,
        "note": note,
        "created_at": datetime.now(timezone.utc)
    })
    
    return {"success": True}

@api_router.post("/admin/users/{user_id}/kyc/verify")
async def verify_user_kyc(
    user_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Verify or reject user KYC"""
    admin = await get_current_user(authorization, request)
    
    body = await request.json()
    status = body.get("status")  # verified, rejected
    reason = body.get("reason", "")
    
    if status not in ["verified", "rejected"]:
        raise HTTPException(status_code=400, detail="Invalid status")
    
    await db.users.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "kyc_status": status,
                "kyc_verified_at": datetime.now(timezone.utc) if status == "verified" else None,
                "kyc_rejected_reason": reason if status == "rejected" else None,
                "kyc_verified_by": admin.user_id
            }
        }
    )
    
    await db.admin_logs.insert_one({
        "action": "kyc_verification",
        "admin_id": admin.user_id,
        "target_user_id": user_id,
        "details": {"status": status, "reason": reason},
        "timestamp": datetime.now(timezone.utc)
    })
    
    return {"success": True, "kyc_status": status}

@api_router.post("/admin/users/{user_id}/force-logout")
async def force_logout_user(
    user_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Force logout user from all sessions"""
    admin = await get_current_user(authorization, request)
    
    # Invalidate all user sessions
    await db.user_sessions.delete_many({"user_id": user_id})
    
    # Update user record
    await db.users.update_one(
        {"user_id": user_id},
        {"$set": {"force_logout_at": datetime.now(timezone.utc)}}
    )
    
    await db.admin_logs.insert_one({
        "action": "force_logout",
        "admin_id": admin.user_id,
        "target_user_id": user_id,
        "timestamp": datetime.now(timezone.utc)
    })
    
    return {"success": True, "message": "User logged out from all sessions"}

@api_router.get("/admin/users/{user_id}/trades")
async def get_user_trades(
    user_id: str,
    limit: int = 100,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get user's trade history"""
    admin = await get_current_user(authorization, request)
    
    trades = await db.trades.find({"user_id": user_id}).sort("created_at", -1).limit(limit).to_list(limit)
    
    return {
        "trades": [
            {
                "trade_id": t.get("trade_id"),
                "asset": t.get("asset"),
                "amount": t.get("amount"),
                "direction": t.get("direction"),
                "status": t.get("status"),
                "entry_price": t.get("entry_price"),
                "exit_price": t.get("exit_price"),
                "profit_loss": t.get("profit_loss", 0),
                "payout_percentage": t.get("payout_percentage"),
                "account_type": t.get("account_type"),
                "admin_override": t.get("admin_override", False),
                "created_at": str(t.get("created_at", ""))
            }
            for t in trades
        ]
    }

@api_router.get("/admin/users/{user_id}/transactions")
async def get_user_transactions(
    user_id: str,
    limit: int = 100,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get user's transaction history"""
    admin = await get_current_user(authorization, request)
    
    deposits = await db.deposits.find({"user_id": user_id}).sort("created_at", -1).limit(limit).to_list(limit)
    withdrawals = await db.withdrawals.find({"user_id": user_id}).sort("created_at", -1).limit(limit).to_list(limit)
    adjustments = await db.balance_adjustments.find({"user_id": user_id}).sort("created_at", -1).limit(limit).to_list(limit)
    
    return {
        "deposits": [
            {
                "amount": d.get("amount_usd"),
                "method": d.get("payment_type"),
                "status": d.get("status"),
                "transaction_id": d.get("transaction_id") or d.get("deposit_id"),
                "created_at": str(d.get("created_at", ""))
            }
            for d in deposits
        ],
        "withdrawals": [
            {
                "amount": w.get("amount"),
                "method": w.get("method"),
                "status": w.get("status"),
                "wallet_address": w.get("wallet_address"),
                "created_at": str(w.get("created_at", ""))
            }
            for w in withdrawals
        ],
        "adjustments": [
            {
                "amount": a.get("amount"),
                "balance_type": a.get("balance_type"),
                "reason": a.get("reason"),
                "admin_id": a.get("admin_id"),
                "created_at": str(a.get("created_at", ""))
            }
            for a in adjustments
        ]
    }

@api_router.post("/admin/users/bulk-action")
async def bulk_user_action(
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Perform bulk action on multiple users"""
    admin = await get_current_user(authorization, request)
    
    body = await request.json()
    user_ids = body.get("user_ids", [])
    action = body.get("action")  # suspend, activate, flag, unlag
    reason = body.get("reason", "Bulk action")
    
    if not user_ids:
        raise HTTPException(status_code=400, detail="No users selected")
    
    update = {}
    if action == "suspend":
        update = {"account_status": "suspended", "status_reason": reason}
    elif action == "activate":
        update = {"account_status": "active", "status_reason": ""}
    elif action == "flag":
        update = {"is_flagged": True, "flag_reason": reason}
    elif action == "unflag":
        update = {"is_flagged": False, "flag_reason": ""}
    else:
        raise HTTPException(status_code=400, detail="Invalid action")
    
    result = await db.users.update_many(
        {"user_id": {"$in": user_ids}},
        {"$set": update}
    )
    
    await db.admin_logs.insert_one({
        "action": f"bulk_{action}",
        "admin_id": admin.user_id,
        "details": {"user_ids": user_ids, "reason": reason, "count": result.modified_count},
        "timestamp": datetime.now(timezone.utc)
    })
    
    return {"success": True, "affected_count": result.modified_count}

@api_router.get("/admin/users/segments")
async def get_user_segments(authorization: Optional[str] = Header(None), request: Request = None):
    """Get user segment counts"""
    admin = await get_current_user(authorization, request)
    
    total = await db.users.count_documents({})
    active = await db.users.count_documents({"account_status": "active"})
    suspended = await db.users.count_documents({"account_status": "suspended"})
    vip = await db.users.count_documents({"tier": "vip"})
    verified = await db.users.count_documents({"is_verified": True})
    flagged = await db.users.count_documents({"is_flagged": True})
    shadow_banned = await db.users.count_documents({"is_shadow_banned": True})
    
    # New users (last 7 days)
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    new_users = await db.users.count_documents({"created_at": {"$gte": week_ago}})
    
    return {
        "total": total,
        "active": active,
        "suspended": suspended,
        "vip": vip,
        "verified": verified,
        "flagged": flagged,
        "shadow_banned": shadow_banned,
        "new_users_7d": new_users
    }

# ============= AUTOMATION ENGINE =============

class AutomationRule(BaseModel):
    name: str
    description: Optional[str] = None
    trigger_type: str  # profit_threshold, win_streak, deposit_amount, withdrawal_request, loss_streak
    trigger_value: float
    trigger_operator: str = "gte"  # gte, lte, eq, gt, lt
    action_type: str  # adjust_payout, adjust_winrate, flag_user, suspend_user, shadow_ban, send_alert, lock_withdrawals
    action_value: Optional[float] = None
    priority: int = 0
    is_active: bool = True
    target_segment: Optional[str] = None  # all, vip, new_users, high_rollers

@api_router.get("/admin/automation/rules")
async def get_automation_rules(
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get all automation rules"""
    admin = await get_current_user(authorization, request)
    
    rules = await db.automation_rules.find().sort("priority", -1).to_list(100)
    
    result = []
    for rule in rules:
        result.append({
            "rule_id": rule.get("rule_id"),
            "name": rule.get("name"),
            "description": rule.get("description"),
            "trigger_type": rule.get("trigger_type"),
            "trigger_value": rule.get("trigger_value"),
            "trigger_operator": rule.get("trigger_operator", "gte"),
            "action_type": rule.get("action_type"),
            "action_value": rule.get("action_value"),
            "priority": rule.get("priority", 0),
            "is_active": rule.get("is_active", True),
            "target_segment": rule.get("target_segment", "all"),
            "executions": rule.get("executions", 0),
            "last_executed": rule.get("last_executed"),
            "created_at": rule.get("created_at")
        })
    
    return {"rules": result}

@api_router.post("/admin/automation/rules")
async def create_automation_rule(
    rule: AutomationRule,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Create new automation rule"""
    admin = await get_current_user(authorization, request)
    
    rule_data = {
        "rule_id": str(uuid.uuid4()),
        "name": rule.name,
        "description": rule.description,
        "trigger_type": rule.trigger_type,
        "trigger_value": rule.trigger_value,
        "trigger_operator": rule.trigger_operator,
        "action_type": rule.action_type,
        "action_value": rule.action_value,
        "priority": rule.priority,
        "is_active": rule.is_active,
        "target_segment": rule.target_segment or "all",
        "executions": 0,
        "created_by": admin.get("user_id"),
        "created_at": datetime.now(timezone.utc)
    }
    
    await db.automation_rules.insert_one(rule_data)
    
    # Log action
    await db.admin_logs.insert_one({
        "log_id": str(uuid.uuid4()),
        "admin_id": admin.get("user_id"),
        "action": "create_automation_rule",
        "details": {"rule_name": rule.name, "trigger": rule.trigger_type, "action": rule.action_type},
        "timestamp": datetime.now(timezone.utc)
    })
    
    return {"success": True, "rule_id": rule_data["rule_id"], "message": "Rule created"}

@api_router.put("/admin/automation/rules/{rule_id}")
async def update_automation_rule(
    rule_id: str,
    rule: AutomationRule,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Update automation rule"""
    admin = await get_current_user(authorization, request)
    
    await db.automation_rules.update_one(
        {"rule_id": rule_id},
        {"$set": {
            "name": rule.name,
            "description": rule.description,
            "trigger_type": rule.trigger_type,
            "trigger_value": rule.trigger_value,
            "trigger_operator": rule.trigger_operator,
            "action_type": rule.action_type,
            "action_value": rule.action_value,
            "priority": rule.priority,
            "is_active": rule.is_active,
            "target_segment": rule.target_segment,
            "updated_at": datetime.now(timezone.utc)
        }}
    )
    
    return {"success": True, "message": "Rule updated"}

@api_router.delete("/admin/automation/rules/{rule_id}")
async def delete_automation_rule(
    rule_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Delete automation rule"""
    admin = await get_current_user(authorization, request)
    
    await db.automation_rules.delete_one({"rule_id": rule_id})
    
    return {"success": True, "message": "Rule deleted"}

@api_router.post("/admin/automation/rules/{rule_id}/toggle")
async def toggle_automation_rule(
    rule_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Toggle rule active status"""
    admin = await get_current_user(authorization, request)
    
    rule = await db.automation_rules.find_one({"rule_id": rule_id})
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")
    
    new_status = not rule.get("is_active", True)
    await db.automation_rules.update_one(
        {"rule_id": rule_id},
        {"$set": {"is_active": new_status}}
    )
    
    return {"success": True, "is_active": new_status}

@api_router.post("/admin/automation/execute")
async def execute_automation_rules(
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Manually execute all active automation rules"""
    admin = await get_current_user(authorization, request)
    
    rules = await db.automation_rules.find({"is_active": True}).sort("priority", -1).to_list(100)
    
    results = []
    for rule in rules:
        affected = await execute_single_rule(rule)
        results.append({
            "rule_id": rule.get("rule_id"),
            "name": rule.get("name"),
            "affected_users": affected
        })
        
        # Update execution count
        await db.automation_rules.update_one(
            {"rule_id": rule.get("rule_id")},
            {"$inc": {"executions": 1}, "$set": {"last_executed": datetime.now(timezone.utc)}}
        )
    
    return {"success": True, "results": results}

async def execute_single_rule(rule: dict) -> int:
    """Execute a single automation rule and return affected users count"""
    trigger_type = rule.get("trigger_type")
    trigger_value = rule.get("trigger_value", 0)
    trigger_op = rule.get("trigger_operator", "gte")
    action_type = rule.get("action_type")
    action_value = rule.get("action_value")
    target_segment = rule.get("target_segment", "all")
    
    # Build query based on trigger
    query = {}
    
    if target_segment == "vip":
        query["tier"] = "vip"
    elif target_segment == "new_users":
        week_ago = datetime.now(timezone.utc) - timedelta(days=7)
        query["created_at"] = {"$gte": week_ago}
    
    # Get users to check
    users = await db.users.find(query).to_list(1000)
    
    affected = 0
    for user in users:
        should_apply = False
        
        # Check trigger condition
        if trigger_type == "profit_threshold":
            trades = await db.trades.find({"user_id": user.get("user_id"), "status": {"$in": ["won", "lost"]}}).to_list(1000)
            total_profit = sum(t.get("profit_loss", 0) for t in trades)
            should_apply = compare_values(total_profit, trigger_value, trigger_op)
            
        elif trigger_type == "win_streak":
            trades = await db.trades.find({"user_id": user.get("user_id")}).sort("created_at", -1).to_list(int(trigger_value) + 5)
            streak = 0
            for t in trades:
                if t.get("status") == "won":
                    streak += 1
                else:
                    break
            should_apply = compare_values(streak, trigger_value, trigger_op)
            
        elif trigger_type == "deposit_amount":
            deposits = await db.deposits.find({"user_id": user.get("user_id"), "status": "completed"}).to_list(100)
            total_deposited = sum(d.get("amount_usd", 0) for d in deposits)
            should_apply = compare_values(total_deposited, trigger_value, trigger_op)
            
        elif trigger_type == "loss_streak":
            trades = await db.trades.find({"user_id": user.get("user_id")}).sort("created_at", -1).to_list(int(trigger_value) + 5)
            streak = 0
            for t in trades:
                if t.get("status") == "lost":
                    streak += 1
                else:
                    break
            should_apply = compare_values(streak, trigger_value, trigger_op)
        
        # Apply action if condition met
        if should_apply:
            affected += 1
            await apply_automation_action(user.get("user_id"), action_type, action_value, rule.get("rule_id"))
    
    return affected

def compare_values(actual: float, target: float, operator: str) -> bool:
    """Compare values based on operator"""
    if operator == "gte":
        return actual >= target
    elif operator == "lte":
        return actual <= target
    elif operator == "gt":
        return actual > target
    elif operator == "lt":
        return actual < target
    elif operator == "eq":
        return actual == target
    return False

async def apply_automation_action(user_id: str, action_type: str, action_value: Optional[float], rule_id: str):
    """Apply automation action to a user"""
    update = {}
    
    if action_type == "adjust_payout":
        update["payout_modifier"] = action_value or 80
    elif action_type == "adjust_winrate":
        update["win_rate_modifier"] = action_value or 40
    elif action_type == "flag_user":
        update["is_flagged"] = True
    elif action_type == "suspend_user":
        update["account_status"] = "suspended"
    elif action_type == "shadow_ban":
        update["is_shadow_banned"] = True
    elif action_type == "lock_withdrawals":
        update["withdrawal_locked"] = True
    
    if update:
        update["last_automation_rule"] = rule_id
        update["last_automation_at"] = datetime.now(timezone.utc)
        await db.users.update_one({"user_id": user_id}, {"$set": update})

@api_router.get("/admin/automation/logs")
async def get_automation_logs(
    limit: int = 50,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get automation execution logs"""
    admin = await get_current_user(authorization, request)
    
    logs = await db.admin_logs.find(
        {"action": {"$regex": "automation"}}
    ).sort("timestamp", -1).limit(limit).to_list(limit)
    
    return {"logs": logs}

# ============= TRENDING ASSETS =============

@api_router.get("/market/trending")
async def get_public_trending_assets(
    limit: int = 10,
    days: int = 7
):
    """Get trending assets for users (public endpoint)"""
    # Get trades from last N days
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    
    # Aggregate trades by asset
    pipeline = [
        {"$match": {"created_at": {"$gte": cutoff}}},
        {"$group": {
            "_id": "$asset",
            "trade_count": {"$sum": 1},
            "total_volume": {"$sum": "$amount"},
            "win_count": {"$sum": {"$cond": [{"$eq": ["$status", "won"]}, 1, 0]}},
            "loss_count": {"$sum": {"$cond": [{"$eq": ["$status", "lost"]}, 1, 0]}},
            "unique_traders": {"$addToSet": "$user_id"}
        }},
        {"$project": {
            "asset": "$_id",
            "trade_count": 1,
            "total_volume": 1,
            "win_count": 1,
            "loss_count": 1,
            "unique_traders": {"$size": "$unique_traders"},
            "win_rate": {
                "$cond": [
                    {"$eq": [{"$add": ["$win_count", "$loss_count"]}, 0]},
                    0,
                    {"$multiply": [{"$divide": ["$win_count", {"$add": ["$win_count", "$loss_count"]}]}, 100]}
                ]
            }
        }},
        {"$sort": {"trade_count": -1}},
        {"$limit": limit}
    ]
    
    trending = await db.trades.aggregate(pipeline).to_list(limit)
    
    result = []
    for item in trending:
        asset_name = item.get("asset", "")
        # Try multiple symbol formats for matching
        clean_symbol = asset_name.replace(" OTC", "").replace("/", "")
        symbol_with_slash = asset_name.replace(" OTC", "")
        
        asset_doc = await db.assets.find_one({
            "$or": [
                {"symbol": asset_name},
                {"symbol": symbol_with_slash},
                {"name": asset_name},
                {"symbol": {"$regex": f"^{clean_symbol[:3]}", "$options": "i"}}
            ]
        })
        
        # Determine category from asset name if not found in DB
        category = "forex"  # default
        if asset_doc:
            category = asset_doc.get("category", "forex")
        elif "BTC" in asset_name or "ETH" in asset_name or "SOL" in asset_name or "ADA" in asset_name or "XRP" in asset_name or "DOGE" in asset_name:
            category = "crypto"
        elif "GOLD" in asset_name or "SILVER" in asset_name or "OIL" in asset_name:
            category = "commodities"
        elif "AAPL" in asset_name or "GOOGL" in asset_name or "MSFT" in asset_name or "TSLA" in asset_name:
            category = "stocks"
        
        result.append({
            "asset": item.get("asset"),
            "name": asset_doc.get("name") if asset_doc else item.get("asset"),
            "category": category,
            "payout": asset_doc.get("payout_percentage", 80) if asset_doc else 80,
            "trade_count": item.get("trade_count", 0),
            "unique_traders": item.get("unique_traders", 0),
            "win_rate": round(item.get("win_rate", 0), 1)
        })
    
    return {"trending": result}

@api_router.get("/admin/market/trending")
async def get_trending_assets(
    limit: int = 10,
    days: int = 7,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get trending assets based on trade volume"""
    try:
        admin = await get_current_user(authorization, request)
    except:
        pass  # Allow public access for this endpoint
    
    # Get trades from last N days
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    
    # Aggregate trades by asset
    pipeline = [
        {"$match": {"created_at": {"$gte": cutoff}}},
        {"$group": {
            "_id": "$asset",
            "trade_count": {"$sum": 1},
            "total_volume": {"$sum": "$amount"},
            "total_profit": {"$sum": "$profit_loss"},
            "win_count": {"$sum": {"$cond": [{"$eq": ["$status", "won"]}, 1, 0]}},
            "loss_count": {"$sum": {"$cond": [{"$eq": ["$status", "lost"]}, 1, 0]}},
            "unique_traders": {"$addToSet": "$user_id"}
        }},
        {"$project": {
            "asset": "$_id",
            "trade_count": 1,
            "total_volume": 1,
            "total_profit": 1,
            "win_count": 1,
            "loss_count": 1,
            "unique_traders": {"$size": "$unique_traders"},
            "win_rate": {
                "$cond": [
                    {"$eq": [{"$add": ["$win_count", "$loss_count"]}, 0]},
                    0,
                    {"$multiply": [{"$divide": ["$win_count", {"$add": ["$win_count", "$loss_count"]}]}, 100]}
                ]
            }
        }},
        {"$sort": {"trade_count": -1}},
        {"$limit": limit}
    ]
    
    trending = await db.trades.aggregate(pipeline).to_list(limit)
    
    # Get asset details
    result = []
    for item in trending:
        asset_doc = await db.assets.find_one({"symbol": item.get("asset")})
        result.append({
            "asset": item.get("asset"),
            "name": asset_doc.get("name") if asset_doc else item.get("asset"),
            "category": asset_doc.get("category") if asset_doc else "unknown",
            "trade_count": item.get("trade_count", 0),
            "total_volume": round(item.get("total_volume", 0), 2),
            "unique_traders": item.get("unique_traders", 0),
            "win_rate": round(item.get("win_rate", 0), 1),
            "total_profit": round(item.get("total_profit", 0), 2),
            "trend_score": item.get("trade_count", 0) * item.get("unique_traders", 0)  # Simple scoring
        })
    
    # Sort by trend_score
    result.sort(key=lambda x: x["trend_score"], reverse=True)
    
    return {"trending": result, "period_days": days}

# ============= MARKET & PRICE MANIPULATION =============

class PriceInjection(BaseModel):
    asset: str
    price: float
    duration_seconds: int = 60  # How long to maintain this price
    affect_all_users: bool = True
    target_user_ids: Optional[List[str]] = None

class CandleEdit(BaseModel):
    asset: str
    timestamp: datetime
    open_price: Optional[float] = None
    close_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None

class PriceSpike(BaseModel):
    asset: str
    direction: str  # up, down
    percentage: float  # How much to move (e.g., 5 for 5%)
    duration_ms: int = 1000  # How fast to move

# Store active price manipulations in memory
active_price_injections = {}
shadow_prices = {}  # user_id -> {asset: price}

@api_router.get("/admin/market/status")
async def get_market_manipulation_status(
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get current market manipulation status"""
    admin = await get_current_user(authorization, request)
    
    # Get stored manipulations from DB
    manipulations = await db.price_manipulations.find({"is_active": True}).to_list(50)
    
    # Get shadow prices
    shadows = await db.shadow_prices.find({"is_active": True}).to_list(100)
    
    return {
        "active_injections": [{
            "asset": m.get("asset"),
            "injected_price": m.get("price"),
            "original_price": m.get("original_price"),
            "expires_at": m.get("expires_at"),
            "created_by": m.get("created_by")
        } for m in manipulations],
        "shadow_prices": [{
            "user_id": s.get("user_id"),
            "user_email": s.get("user_email"),
            "asset": s.get("asset"),
            "shadow_price": s.get("price"),
            "market_price": s.get("market_price")
        } for s in shadows],
        "total_active_manipulations": len(manipulations),
        "total_shadow_users": len(set(s.get("user_id") for s in shadows))
    }

@api_router.post("/admin/market/inject-price")
async def inject_price(
    injection: PriceInjection,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Inject a specific price for an asset"""
    admin = await get_current_user(authorization, request)
    
    # Get current price
    current_price = await get_asset_price(injection.asset)
    
    # Store manipulation
    manipulation_data = {
        "manipulation_id": str(uuid.uuid4()),
        "asset": injection.asset,
        "price": injection.price,
        "original_price": current_price,
        "duration_seconds": injection.duration_seconds,
        "affect_all_users": injection.affect_all_users,
        "target_user_ids": injection.target_user_ids,
        "is_active": True,
        "created_by": admin.get("user_id"),
        "created_at": datetime.now(timezone.utc),
        "expires_at": datetime.now(timezone.utc) + timedelta(seconds=injection.duration_seconds)
    }
    
    await db.price_manipulations.insert_one(manipulation_data)
    
    # Update global price cache
    active_price_injections[injection.asset] = {
        "price": injection.price,
        "expires_at": manipulation_data["expires_at"]
    }
    
    # Log
    await db.admin_logs.insert_one({
        "log_id": str(uuid.uuid4()),
        "admin_id": admin.get("user_id"),
        "action": "price_injection",
        "details": {
            "asset": injection.asset,
            "original_price": current_price,
            "injected_price": injection.price,
            "duration": injection.duration_seconds
        },
        "timestamp": datetime.now(timezone.utc)
    })
    
    return {"success": True, "message": f"Price injected: {injection.asset} = ${injection.price}"}

@api_router.post("/admin/market/clear-injection")
async def clear_price_injection(
    asset: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Clear price injection for an asset"""
    admin = await get_current_user(authorization, request)
    
    await db.price_manipulations.update_many(
        {"asset": asset, "is_active": True},
        {"$set": {"is_active": False, "cleared_at": datetime.now(timezone.utc)}}
    )
    
    if asset in active_price_injections:
        del active_price_injections[asset]
    
    return {"success": True, "message": f"Price injection cleared for {asset}"}

@api_router.post("/admin/market/edit-candle")
async def edit_candle(
    edit: CandleEdit,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Edit historical candle data"""
    admin = await get_current_user(authorization, request)
    
    update = {}
    if edit.open_price is not None:
        update["open"] = edit.open_price
    if edit.close_price is not None:
        update["close"] = edit.close_price
    if edit.high_price is not None:
        update["high"] = edit.high_price
    if edit.low_price is not None:
        update["low"] = edit.low_price
    
    if update:
        update["edited_by"] = admin.get("user_id")
        update["edited_at"] = datetime.now(timezone.utc)
        
        # Store candle edit
        await db.candle_edits.insert_one({
            "edit_id": str(uuid.uuid4()),
            "asset": edit.asset,
            "timestamp": edit.timestamp,
            "changes": update,
            "created_by": admin.get("user_id"),
            "created_at": datetime.now(timezone.utc)
        })
    
    return {"success": True, "message": "Candle edited"}

@api_router.post("/admin/market/price-spike")
async def trigger_price_spike(
    spike: PriceSpike,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Trigger an instant price spike/drop"""
    admin = await get_current_user(authorization, request)
    
    current_price = await get_asset_price(spike.asset)
    
    if spike.direction == "up":
        new_price = current_price * (1 + spike.percentage / 100)
    else:
        new_price = current_price * (1 - spike.percentage / 100)
    
    # Store spike data
    await db.price_spikes.insert_one({
        "spike_id": str(uuid.uuid4()),
        "asset": spike.asset,
        "direction": spike.direction,
        "percentage": spike.percentage,
        "original_price": current_price,
        "spike_price": new_price,
        "duration_ms": spike.duration_ms,
        "created_by": admin.get("user_id"),
        "created_at": datetime.now(timezone.utc)
    })
    
    # Temporarily inject the spike price
    active_price_injections[spike.asset] = {
        "price": new_price,
        "expires_at": datetime.now(timezone.utc) + timedelta(milliseconds=spike.duration_ms)
    }
    
    return {
        "success": True,
        "message": f"Price spike triggered: {spike.asset} {spike.direction} {spike.percentage}%",
        "original_price": current_price,
        "spike_price": new_price
    }

@api_router.post("/admin/market/shadow-price")
async def set_shadow_price(
    user_id: str,
    asset: str,
    price: float,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Set a shadow price for a specific user (they see different price)"""
    admin = await get_current_user(authorization, request)
    
    user = await db.users.find_one({"user_id": user_id})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    market_price = await get_asset_price(asset)
    
    # Store shadow price
    await db.shadow_prices.update_one(
        {"user_id": user_id, "asset": asset},
        {"$set": {
            "user_id": user_id,
            "user_email": user.get("email"),
            "asset": asset,
            "price": price,
            "market_price": market_price,
            "is_active": True,
            "created_by": admin.get("user_id"),
            "updated_at": datetime.now(timezone.utc)
        }},
        upsert=True
    )
    
    return {"success": True, "message": f"Shadow price set for {user.get('email')}: {asset} = ${price}"}

@api_router.delete("/admin/market/shadow-price")
async def remove_shadow_price(
    user_id: str,
    asset: str,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Remove shadow price for a user"""
    admin = await get_current_user(authorization, request)
    
    await db.shadow_prices.delete_one({"user_id": user_id, "asset": asset})
    
    return {"success": True, "message": "Shadow price removed"}

@api_router.get("/admin/market/history")
async def get_manipulation_history(
    limit: int = 50,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get history of all market manipulations"""
    admin = await get_current_user(authorization, request)
    
    injections = await db.price_manipulations.find().sort("created_at", -1).limit(limit).to_list(limit)
    spikes = await db.price_spikes.find().sort("created_at", -1).limit(limit).to_list(limit)
    edits = await db.candle_edits.find().sort("created_at", -1).limit(limit).to_list(limit)
    
    return {
        "injections": injections,
        "spikes": spikes,
        "candle_edits": edits
    }

async def get_asset_price(asset: str) -> float:
    """Get current price for an asset (check for active injections first)"""
    # Check for active injection
    if asset in active_price_injections:
        injection = active_price_injections[asset]
        if datetime.now(timezone.utc) < injection["expires_at"]:
            return injection["price"]
        else:
            del active_price_injections[asset]
    
    # Get from DB or generate
    price_data = await db.asset_prices.find_one({"symbol": asset})
    if price_data:
        return price_data.get("current_price", 1.0)
    
    # Default prices for common assets
    default_prices = {
        "BTCUSD": 65000,
        "ETHUSD": 3500,
        "EURUSD": 1.08,
        "GBPUSD": 1.26,
        "USDJPY": 150,
        "GOLD": 2350,
        "AAPL": 175,
        "GOOGL": 140
    }
    return default_prices.get(asset, 100)

async def get_user_price(user_id: str, asset: str) -> float:
    """Get price for a specific user (checks shadow prices)"""
    # Check for shadow price
    shadow = await db.shadow_prices.find_one({"user_id": user_id, "asset": asset, "is_active": True})
    if shadow:
        return shadow.get("price")
    
    # Return normal price
    return await get_asset_price(asset)

# ============= AFFILIATE SYSTEM =============

# Affiliate Models
class AffiliateCreate(BaseModel):
    email: EmailStr
    password: str
    name: str
    telegram: Optional[str] = None
    
class AffiliateLogin(BaseModel):
    email: EmailStr
    password: str

class AffiliateWithdrawalRequest(BaseModel):
    amount: float
    wallet_address: str
    payment_method: str = "USDT"

class AffiliateLinkCreate(BaseModel):
    name: str
    campaign: Optional[str] = None
    program: Optional[str] = "revenue_sharing"  # revenue_sharing or turnover_sharing
    comment: Optional[str] = None

# ============= COMMISSION STRUCTURE =============
# Revenue Share: Affiliate earns % of user's trading LOSSES
# Turnover: Affiliate earns % of user's total trading VOLUME (win or lose)

AFFILIATE_LEVELS = {
    1: {"name": "Starter", "min_ftds": 0, "revenue_share": 50, "turnover_share": 2.0},
    2: {"name": "Advanced", "min_ftds": 15, "revenue_share": 55, "turnover_share": 2.5},
    3: {"name": "Professional", "min_ftds": 50, "revenue_share": 60, "turnover_share": 3.0},
    4: {"name": "Expert", "min_ftds": 100, "revenue_share": 65, "turnover_share": 3.5},
    5: {"name": "Master", "min_ftds": 200, "revenue_share": 70, "turnover_share": 4.0},
    6: {"name": "Guru", "min_ftds": 400, "revenue_share": 75, "turnover_share": 4.5},
    7: {"name": "Legend", "min_ftds": 700, "revenue_share": 85, "turnover_share": 5.5},
}

def get_affiliate_level(total_ftds: int) -> dict:
    """Get affiliate level based on FTD count"""
    level = 1
    for lvl, data in AFFILIATE_LEVELS.items():
        if total_ftds >= data["min_ftds"]:
            level = lvl
    return {"level": level, **AFFILIATE_LEVELS[level]}

async def calculate_commission_with_cap(
    db_instance, 
    affiliate_id: str, 
    source_user_id: str,
    affiliate_level: dict, 
    trade_amount: float, 
    trade_result: str, 
    program_type: str = "revenue_sharing"
) -> float:
    """
    Calculate commission based on trade with TURNOVER CAP
    
    Revenue Share Model:
    - Affiliate earns commission ONLY when referred user LOSES
    - Commission = User's Loss × (Revenue Share % / 100)
    - NO CAP - unlimited earnings from losses
    
    Turnover Model:
    - Affiliate earns commission on EVERY trade regardless of win/loss
    - Commission = Trade Volume × (Turnover % / 100)
    - CAP: Maximum 50% of user's TOTAL DEPOSIT
    - Once cap reached, no more commission from that user
    """
    if program_type == "revenue_sharing":
        # Revenue Share: Only earn on user LOSSES - NO CAP
        if trade_result == "lost":
            return trade_amount * (affiliate_level["revenue_share"] / 100)
        return 0.0
    else:
        # Turnover Model with 50% DEPOSIT CAP
        
        # Get user's total deposits
        referral = await db_instance.referrals.find_one({
            "referred_user_id": source_user_id,
            "affiliate_id": affiliate_id
        })
        
        if not referral:
            # Check affiliate_referrals as fallback
            referral = await db_instance.affiliate_referrals.find_one({
                "user_id": source_user_id,
                "affiliate_id": affiliate_id
            })
        
        total_deposited = referral.get("total_deposited", 0) if referral else 0
        commission_earned_from_user = referral.get("commission_earned", 0) if referral else 0
        
        # Calculate max commission cap (50% of total deposits)
        max_commission_cap = total_deposited * 0.50
        
        # Calculate potential commission
        potential_commission = trade_amount * (affiliate_level["turnover_share"] / 100)
        
        # Check if already at cap
        if commission_earned_from_user >= max_commission_cap:
            return 0.0  # Already reached cap, no more commission
        
        # Check if this commission would exceed the cap
        remaining_cap = max_commission_cap - commission_earned_from_user
        
        if potential_commission > remaining_cap:
            # Only give what's remaining until cap
            return remaining_cap
        
        return potential_commission


def calculate_commission(affiliate_id: str, affiliate_level: dict, trade_amount: float, trade_result: str, program_type: str = "revenue_sharing") -> float:
    """
    Simple commission calculation without cap check (for backward compatibility)
    Use calculate_commission_with_cap for proper turnover cap handling
    
    Revenue Share model:
    - User LOSES → Affiliate gets POSITIVE commission (% of loss)
    - User WINS → Affiliate gets NEGATIVE commission (deducted from earnings)
    
    Turnover model:
    - Always POSITIVE based on trade volume (regardless of win/loss)
    """
    if program_type == "revenue_sharing":
        if trade_result == "lost":
            # User lost - affiliate earns positive commission
            return trade_amount * (affiliate_level["revenue_share"] / 100)
        else:
            # User won - affiliate gets negative commission (deducted)
            return -(trade_amount * (affiliate_level["revenue_share"] / 100))
    else:
        # Turnover model - always positive based on volume
        return trade_amount * (affiliate_level["turnover_share"] / 100)


# Affiliate Registration
@api_router.post("/affiliate/register")
async def affiliate_register(affiliate: AffiliateCreate):
    # Check if email already exists
    existing = await db.affiliates.find_one({"email": affiliate.email})
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    affiliate_id = str(uuid.uuid4())
    ref_code = f"BYN{random.randint(10000, 99999)}"
    
    affiliate_doc = {
        "affiliate_id": affiliate_id,
        "email": affiliate.email,
        "password_hash": hash_password(affiliate.password),
        "name": affiliate.name,
        "telegram": affiliate.telegram,
        "ref_code": ref_code,
        "level": 1,
        "balance": 0.0,
        "hold_balance": 0.0,  # Commission goes here first, released on Monday 6AM SGT
        "total_earnings": 0.0,
        "total_deposits": 0,
        "total_ftds": 0,
        "total_clicks": 0,
        "total_registrations": 0,
        "is_active": True,
        "last_payout_date": None,
        "created_at": datetime.now(timezone.utc)
    }
    
    await db.affiliates.insert_one(affiliate_doc)
    
    # Create default affiliate link
    default_link = {
        "link_id": str(uuid.uuid4()),
        "affiliate_id": affiliate_id,
        "name": "Default Link",
        "code": ref_code,
        "clicks": 0,
        "registrations": 0,
        "ftds": 0,
        "deposits": 0.0,
        "created_at": datetime.now(timezone.utc)
    }
    await db.affiliate_links.insert_one(default_link)
    
    # Generate token
    token = create_access_token({"sub": affiliate_id, "type": "affiliate"})
    
    return {
        "success": True,
        "token": token,
        "affiliate": {
            "affiliate_id": affiliate_id,
            "email": affiliate.email,
            "name": affiliate.name,
            "ref_code": ref_code,
            "level": 1
        }
    }

# Affiliate Login
@api_router.post("/affiliate/login")
async def affiliate_login(credentials: AffiliateLogin):
    affiliate = await db.affiliates.find_one({"email": credentials.email})
    if not affiliate:
        raise HTTPException(status_code=401, detail="Invalid email or password")
    
    if not verify_password(credentials.password, affiliate.get("password_hash", "")):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    
    if not affiliate.get("is_active", True):
        raise HTTPException(status_code=403, detail="Account is suspended")
    
    token = create_access_token({"sub": affiliate["affiliate_id"], "type": "affiliate"})
    
    return {
        "success": True,
        "token": token,
        "affiliate": {
            "affiliate_id": affiliate["affiliate_id"],
            "email": affiliate["email"],
            "name": affiliate["name"],
            "ref_code": affiliate["ref_code"],
            "level": affiliate.get("level", 1)
        }
    }

# Update Affiliate Profile (Name)
class AffiliateUpdateProfile(BaseModel):
    name: str

@api_router.put("/affiliate/profile")
async def update_affiliate_profile(
    update_data: AffiliateUpdateProfile,
    authorization: str = Header(None)
):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    token = authorization.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        affiliate_id = payload.get("sub")
        token_type = payload.get("type")
        
        if token_type != "affiliate":
            raise HTTPException(status_code=403, detail="Invalid token type")
        
        # Update affiliate name in database
        result = await db.affiliates.update_one(
            {"affiliate_id": affiliate_id},
            {"$set": {"name": update_data.name}}
        )
        
        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Affiliate not found")
        
        # Get updated affiliate
        affiliate = await db.affiliates.find_one({"affiliate_id": affiliate_id})
        
        return {
            "success": True,
            "message": "Profile updated successfully",
            "affiliate": {
                "affiliate_id": affiliate["affiliate_id"],
                "email": affiliate["email"],
                "name": affiliate["name"],
                "ref_code": affiliate.get("ref_code"),
                "level": affiliate.get("level", 1)
            }
        }
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

# Update Affiliate Settings (Notifications, Withdrawal Address)
class AffiliateSettingsUpdate(BaseModel):
    email_notifications: Optional[bool] = None
    push_notifications: Optional[bool] = None
    usdt_trc20_address: Optional[str] = None

@api_router.put("/affiliate/settings")
async def update_affiliate_settings(
    settings: AffiliateSettingsUpdate,
    authorization: str = Header(None)
):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    token = authorization.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        affiliate_id = payload.get("sub")
        
        # Build update object
        update_data = {}
        if settings.email_notifications is not None:
            update_data["email_notifications"] = settings.email_notifications
        if settings.push_notifications is not None:
            update_data["push_notifications"] = settings.push_notifications
        if settings.usdt_trc20_address is not None:
            # Validate TRC20 address format (should start with T)
            if settings.usdt_trc20_address and not settings.usdt_trc20_address.startswith('T'):
                raise HTTPException(status_code=400, detail="Invalid TRC20 address format")
            update_data["usdt_trc20_address"] = settings.usdt_trc20_address
        
        if update_data:
            await db.affiliates.update_one(
                {"affiliate_id": affiliate_id},
                {"$set": update_data}
            )
        
        # Get updated settings
        affiliate = await db.affiliates.find_one({"affiliate_id": affiliate_id})
        
        return {
            "success": True,
            "message": "Settings updated successfully",
            "settings": {
                "email_notifications": affiliate.get("email_notifications", True),
                "push_notifications": affiliate.get("push_notifications", True),
                "usdt_trc20_address": affiliate.get("usdt_trc20_address", "")
            }
        }
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

# Get Affiliate Settings
@api_router.get("/affiliate/settings")
async def get_affiliate_settings(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    token = authorization.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        affiliate_id = payload.get("sub")
        
        affiliate = await db.affiliates.find_one({"affiliate_id": affiliate_id})
        if not affiliate:
            raise HTTPException(status_code=404, detail="Affiliate not found")
        
        return {
            "success": True,
            "settings": {
                "email_notifications": affiliate.get("email_notifications", True),
                "push_notifications": affiliate.get("push_notifications", True),
                "usdt_trc20_address": affiliate.get("usdt_trc20_address", "")
            }
        }
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

# Change Affiliate Password
class AffiliateChangePassword(BaseModel):
    current_password: str
    new_password: str

@api_router.put("/affiliate/change-password")
async def change_affiliate_password(
    data: AffiliateChangePassword,
    authorization: str = Header(None)
):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    token = authorization.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        affiliate_id = payload.get("sub")
        
        affiliate = await db.affiliates.find_one({"affiliate_id": affiliate_id})
        if not affiliate:
            raise HTTPException(status_code=404, detail="Affiliate not found")
        
        # Verify current password
        if not verify_password(data.current_password, affiliate.get("password_hash", "")):
            raise HTTPException(status_code=400, detail="Current password is incorrect")
        
        # Hash and save new password
        new_hash = get_password_hash(data.new_password)
        await db.affiliates.update_one(
            {"affiliate_id": affiliate_id},
            {"$set": {"password_hash": new_hash}}
        )
        
        return {"success": True, "message": "Password changed successfully"}
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

# Get current affiliate
@api_router.get("/affiliate/me")
async def get_affiliate_me(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    token = authorization.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        affiliate_id = payload.get("sub")
        token_type = payload.get("type")
        
        if token_type != "affiliate":
            raise HTTPException(status_code=401, detail="Invalid token type")
        
        affiliate = await db.affiliates.find_one({"affiliate_id": affiliate_id})
        if not affiliate:
            raise HTTPException(status_code=404, detail="Affiliate not found")
        
        # Calculate level based on deposits
        total_ftds = affiliate.get("total_ftds", 0)
        level = 1
        if total_ftds >= 700: level = 7
        elif total_ftds >= 400: level = 6
        elif total_ftds >= 200: level = 5
        elif total_ftds >= 100: level = 4
        elif total_ftds >= 50: level = 3
        elif total_ftds >= 15: level = 2
        
        # Update level if changed
        if level != affiliate.get("level", 1):
            await db.affiliates.update_one(
                {"affiliate_id": affiliate_id},
                {"$set": {"level": level}}
            )
        
        return {
            "affiliate_id": affiliate["affiliate_id"],
            "email": affiliate["email"],
            "name": affiliate["name"],
            "ref_code": affiliate["ref_code"],
            "telegram": affiliate.get("telegram"),
            "level": level,
            "balance": affiliate.get("balance", 0),
            "total_earnings": affiliate.get("total_earnings", 0),
            "total_deposits": affiliate.get("total_deposits", 0),
            "total_ftds": affiliate.get("total_ftds", 0),
            "total_clicks": affiliate.get("total_clicks", 0),
            "total_registrations": affiliate.get("total_registrations", 0),
            "created_at": affiliate.get("created_at")
        }
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

# Get affiliate dashboard stats
@api_router.get("/affiliate/dashboard")
async def get_affiliate_dashboard(authorization: str = Header(None), days: int = 7):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    token = authorization.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        affiliate_id = payload.get("sub")
        
        affiliate = await db.affiliates.find_one({"affiliate_id": affiliate_id})
        if not affiliate:
            raise HTTPException(status_code=404, detail="Affiliate not found")
        
        # Get stats for date range
        start_date = datetime.now(timezone.utc) - timedelta(days=days)
        
        # Get affiliate links
        links = await db.affiliate_links.find({"affiliate_id": affiliate_id}).to_list(100)
        
        # Calculate period stats from referrals
        period_stats = {
            "deposits": 0,
            "ftds": 0,
            "clicks": 0,
            "registrations": 0,
            "earnings": 0
        }
        
        # Get referrals in period
        referrals = await db.affiliate_referrals.find({
            "affiliate_id": affiliate_id,
            "created_at": {"$gte": start_date}
        }).to_list(1000)
        
        for ref in referrals:
            period_stats["registrations"] += 1
            if ref.get("has_deposited"):
                period_stats["ftds"] += 1
            period_stats["deposits"] += ref.get("total_deposits", 0)
            period_stats["earnings"] += ref.get("commission_earned", 0)
        
        # Get clicks in period
        clicks = await db.affiliate_clicks.count_documents({
            "affiliate_id": affiliate_id,
            "created_at": {"$gte": start_date}
        })
        period_stats["clicks"] = clicks
        
        # Calculate level
        total_ftds = affiliate.get("total_ftds", 0)
        level = 1
        level_name = "Starter"
        revenue_share = 50
        turnover_share = 2.0
        
        levels_data = [
            {"level": 1, "name": "Starter", "min_ftds": 0, "revenue": 50, "turnover": 2.0},
            {"level": 2, "name": "Advanced", "min_ftds": 15, "revenue": 55, "turnover": 2.5},
            {"level": 3, "name": "Professional", "min_ftds": 50, "revenue": 60, "turnover": 3.0},
            {"level": 4, "name": "Expert", "min_ftds": 100, "revenue": 65, "turnover": 3.5},
            {"level": 5, "name": "Master", "min_ftds": 200, "revenue": 70, "turnover": 4.0},
            {"level": 6, "name": "Guru", "min_ftds": 400, "revenue": 75, "turnover": 4.5},
            {"level": 7, "name": "Legend", "min_ftds": 700, "revenue": 85, "turnover": 5.5},
        ]
        
        for l in levels_data:
            if total_ftds >= l["min_ftds"]:
                level = l["level"]
                level_name = l["name"]
                revenue_share = l["revenue"]
                turnover_share = l["turnover"]
        
        return {
            "affiliate": {
                "affiliate_id": affiliate["affiliate_id"],
                "name": affiliate["name"],
                "email": affiliate["email"],
                "ref_code": affiliate["ref_code"],
                "level": level,
                "level_name": level_name,
                "revenue_share": revenue_share,
                "turnover_share": turnover_share,
                "balance": affiliate.get("balance", 0),
                "hold_balance": affiliate.get("hold_balance", 0),
                "hold_balance_revenue": affiliate.get("hold_balance_revenue", 0),
                "hold_balance_turnover": affiliate.get("hold_balance_turnover", 0),
                "total_earnings": affiliate.get("total_earnings", 0),
                "total_ftds": total_ftds
            },
            "period_stats": period_stats,
            "levels": levels_data,
            "links_count": len(links)
        }
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

# Get affiliate statistics with chart data
@api_router.get("/affiliate/statistics")
async def get_affiliate_statistics(authorization: str = Header(None), days: int = 7):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    token = authorization.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        affiliate_id = payload.get("sub")
        
        start_date = datetime.now(timezone.utc) - timedelta(days=days)
        
        # Get daily stats
        daily_stats = []
        for i in range(days):
            day_start = datetime.now(timezone.utc) - timedelta(days=days-1-i)
            day_start = day_start.replace(hour=0, minute=0, second=0, microsecond=0)
            day_end = day_start + timedelta(days=1)
            
            # Count clicks for this day
            clicks = await db.affiliate_clicks.count_documents({
                "affiliate_id": affiliate_id,
                "created_at": {"$gte": day_start, "$lt": day_end}
            })
            
            # Count registrations
            registrations = await db.affiliate_referrals.count_documents({
                "affiliate_id": affiliate_id,
                "created_at": {"$gte": day_start, "$lt": day_end}
            })
            
            # Count FTDs
            ftds = await db.affiliate_referrals.count_documents({
                "affiliate_id": affiliate_id,
                "has_deposited": True,
                "first_deposit_at": {"$gte": day_start, "$lt": day_end}
            })
            
            daily_stats.append({
                "date": day_start.strftime("%Y-%m-%d"),
                "clicks": clicks,
                "registrations": registrations,
                "ftds": ftds
            })
        
        # Get totals for period
        total_clicks = await db.affiliate_clicks.count_documents({
            "affiliate_id": affiliate_id,
            "created_at": {"$gte": start_date}
        })
        
        total_registrations = await db.affiliate_referrals.count_documents({
            "affiliate_id": affiliate_id,
            "created_at": {"$gte": start_date}
        })
        
        total_ftds = await db.affiliate_referrals.count_documents({
            "affiliate_id": affiliate_id,
            "has_deposited": True,
            "first_deposit_at": {"$gte": start_date}
        })
        
        # Sum deposits
        pipeline = [
            {"$match": {"affiliate_id": affiliate_id, "created_at": {"$gte": start_date}}},
            {"$group": {"_id": None, "total": {"$sum": "$total_deposits"}}}
        ]
        deposits_result = await db.affiliate_referrals.aggregate(pipeline).to_list(1)
        total_deposits = deposits_result[0]["total"] if deposits_result else 0
        
        # Get traders list (referred users)
        traders = []
        referrals_raw = await db.referrals.find({
            "affiliate_id": affiliate_id
        }).sort("created_at", -1).limit(100).to_list(100)
        
        # Get all referrals to assign sequential display IDs
        all_referrals = await db.referrals.find({
            "affiliate_id": affiliate_id
        }).sort("created_at", 1).to_list(1000)
        
        # Create a mapping of user_id to display_id (starting from 10000001)
        user_display_ids = {}
        for idx, ref in enumerate(all_referrals):
            user_display_ids[ref.get("referred_user_id")] = f"1000{str(idx + 1).zfill(4)}"
        
        for ref in referrals_raw:
            # Get user details
            user = await db.users.find_one({"user_id": ref.get("referred_user_id")})
            
            # Get country and flag from user record
            user_country = "Unknown"
            user_flag = "🌍"
            link_code = ""
            
            if user:
                user_country = user.get("country") or user.get("country_name") or "Unknown"
                user_flag = user.get("country_flag") or user.get("flag") or "🌍"
                link_code = user.get("referred_by") or ""
            
            # Get the link that was used (from affiliate_links)
            link_used = None
            if link_code:
                link_used = await db.affiliate_links.find_one({
                    "affiliate_id": affiliate_id,
                    "code": link_code
                })
            
            # Calculate commission cap info for turnover model
            affiliate = await db.affiliates.find_one({"affiliate_id": affiliate_id})
            commission_type = affiliate.get("commission_type", "revenue_share") if affiliate else "revenue_share"
            
            total_deposited = ref.get("total_deposited", 0)
            commission_earned = ref.get("commission_earned", 0)
            
            # Generate display ID (10000xxx format)
            raw_user_id = ref.get("referred_user_id", "")
            display_id = user_display_ids.get(raw_user_id, raw_user_id)
            
            # If user has a custom display_id stored, use that
            if user and user.get("display_id"):
                display_id = user.get("display_id")
            
            trader_data = {
                "id": display_id,  # Show display ID (10000xxx format)
                "user_id": raw_user_id,  # Keep original for reference
                "display_id": display_id,
                "email": ref.get("user_email", "Unknown"),
                "date": ref.get("created_at").strftime("%Y-%m-%d") if ref.get("created_at") else "",
                "type": "Turnover" if commission_type == "turnover" else "Revenue",
                "linkId": link_code or affiliate.get("ref_code", "") if affiliate else "",  # Use ref code if no specific link
                "country": user_country,
                "flag": user_flag,
                "is_ftd": ref.get("is_ftd", False),
                "total_deposited": total_deposited,
                "total_traded": ref.get("total_traded", 0),
                "commission": commission_earned,
                # Live user balance data
                "balance": user.get("real_balance", 0) if user else 0,
                "demo_balance": user.get("demo_balance", 0) if user else 0,
                "bonus_balance": user.get("bonus_balance", 0) if user else 0,
                "deposits_count": user.get("total_deposits_count", 0) if user else 0,
                "deposits_sum": user.get("total_deposits", 0) if user else total_deposited,
                "withdrawals": user.get("total_withdrawals", 0) if user else 0,
                "bonuses": user.get("bonus_balance", 0) if user else 0,
            }
            
            # Add cap info for turnover model
            if commission_type == "turnover" and total_deposited > 0:
                max_cap = total_deposited * 0.5
                cap_remaining = max(0, max_cap - commission_earned)
                cap_percentage = (commission_earned / max_cap * 100) if max_cap > 0 else 0
                trader_data["commission_cap"] = max_cap
                trader_data["cap_remaining"] = cap_remaining
                trader_data["cap_percentage"] = cap_percentage
                trader_data["cap_reached"] = commission_earned >= max_cap
            
            traders.append(trader_data)
        
        # Get countries breakdown
        countries = {}
        for t in traders:
            country = t.get("country", "Unknown")
            if country not in countries:
                countries[country] = {"country": country, "flag": t.get("flag", "🌍"), "traders": 0, "ftds": 0, "deposits": 0}
            countries[country]["traders"] += 1
            if t.get("is_ftd"):
                countries[country]["ftds"] += 1
            countries[country]["deposits"] += t.get("total_deposited", 0)
        
        return {
            "period": {"days": days, "start": start_date.isoformat()},
            "totals": {
                "clicks": total_clicks,
                "registrations": total_registrations,
                "ftds": total_ftds,
                "deposits": total_deposits
            },
            "daily_stats": daily_stats,
            "traders": traders,
            "countries": list(countries.values())
        }
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

# Get affiliate links
@api_router.get("/affiliate/links")
async def get_affiliate_links(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    token = authorization.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        affiliate_id = payload.get("sub")
        
        links_cursor = await db.affiliate_links.find({"affiliate_id": affiliate_id}).to_list(100)
        
        # Convert ObjectId to string for JSON serialization
        links = []
        for link in links_cursor:
            link_dict = {
                "link_id": str(link.get("link_id", "")),
                "affiliate_id": str(link.get("affiliate_id", "")),
                "name": link.get("name", "Default Link"),
                "code": link.get("code", ""),
                "campaign": link.get("campaign"),
                "clicks": link.get("clicks", 0),
                "registrations": link.get("registrations", 0),
                "ftds": link.get("ftds", 0),
                "deposits": link.get("deposits", 0.0),
                "created_at": link.get("created_at")
            }
            links.append(link_dict)
        
        return {"links": links}
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

# Create affiliate link
@api_router.post("/affiliate/links")
async def create_affiliate_link(link: AffiliateLinkCreate, authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    token = authorization.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        affiliate_id = payload.get("sub")
        
        # Generate unique code
        code = f"BYN{random.randint(10000, 99999)}"
        
        link_doc = {
            "link_id": str(uuid.uuid4()),
            "affiliate_id": affiliate_id,
            "name": link.name,
            "campaign": link.campaign,
            "program": link.program or "revenue_sharing",
            "comment": link.comment,
            "code": code,
            "clicks": 0,
            "registrations": 0,
            "ftds": 0,
            "deposits": 0.0,
            "created_at": datetime.now(timezone.utc)
        }
        
        await db.affiliate_links.insert_one(link_doc)
        
        # Return serializable response
        return {
            "success": True, 
            "link": {
                "link_id": link_doc["link_id"],
                "affiliate_id": link_doc["affiliate_id"],
                "name": link_doc["name"],
                "code": link_doc["code"],
                "campaign": link_doc["campaign"],
                "program": link_doc["program"],
                "clicks": 0,
                "registrations": 0,
                "ftds": 0,
                "deposits": 0.0
            }
        }
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

# Delete affiliate link
@api_router.delete("/affiliate/links/{link_code}")
async def delete_affiliate_link(link_code: str, authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    token = authorization.replace("Bearer ", "")
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        affiliate_id = payload.get("sub")  # Changed from affiliate_id to sub
        
        if not affiliate_id:
            raise HTTPException(status_code=401, detail="Invalid affiliate token")
        
        # Find and delete the link
        result = await db.affiliate_links.delete_one({
            "code": link_code,
            "affiliate_id": affiliate_id
        })
        
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Link not found")
        
        return {"success": True, "message": "Link deleted"}
        
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

# Track affiliate click
@api_router.post("/affiliate/track/click")
async def track_affiliate_click(ref_code: str):
    # Find the link
    link = await db.affiliate_links.find_one({"code": ref_code})
    if not link:
        return {"success": False, "error": "Invalid ref code"}
    
    # Record click
    click_doc = {
        "click_id": str(uuid.uuid4()),
        "affiliate_id": link["affiliate_id"],
        "link_id": link["link_id"],
        "ref_code": ref_code,
        "created_at": datetime.now(timezone.utc)
    }
    await db.affiliate_clicks.insert_one(click_doc)
    
    # Update link stats
    await db.affiliate_links.update_one(
        {"link_id": link["link_id"]},
        {"$inc": {"clicks": 1}}
    )
    
    # Update affiliate stats
    await db.affiliates.update_one(
        {"affiliate_id": link["affiliate_id"]},
        {"$inc": {"total_clicks": 1}}
    )
    
    return {"success": True}

# Get affiliate referrals
@api_router.get("/affiliate/referrals")
async def get_affiliate_referrals(authorization: str = Header(None), limit: int = 50):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    token = authorization.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        affiliate_id = payload.get("sub")
        
        # Get affiliate info for commission type
        affiliate = await db.affiliates.find_one({"affiliate_id": affiliate_id})
        commission_type = affiliate.get("commission_type", "revenue_share") if affiliate else "revenue_share"
        
        # Get referrals from referrals collection (more complete data)
        referrals_raw = await db.referrals.find(
            {"affiliate_id": affiliate_id}
        ).sort("created_at", -1).limit(limit).to_list(limit)
        
        referrals = []
        for ref in referrals_raw:
            ref_data = {
                "referral_id": ref.get("referral_id"),
                "user_id": ref.get("referred_user_id"),
                "email": ref.get("user_email", "Unknown"),
                "is_ftd": ref.get("is_ftd", False),
                "total_deposited": ref.get("total_deposited", 0),
                "total_traded": ref.get("total_traded", 0),
                "commission_earned": ref.get("commission_earned", 0),
                "created_at": ref.get("created_at"),
            }
            
            # For Turnover model - add cap info
            if commission_type == "turnover":
                total_deposited = ref.get("total_deposited", 0)
                max_cap = total_deposited * 0.5  # 50% cap
                commission_earned = ref.get("commission_earned", 0)
                
                ref_data["commission_cap"] = max_cap
                ref_data["cap_remaining"] = max(0, max_cap - commission_earned)
                ref_data["cap_percentage_used"] = (commission_earned / max_cap * 100) if max_cap > 0 else 0
                ref_data["cap_reached"] = commission_earned >= max_cap
            
            referrals.append(ref_data)
        
        return {
            "referrals": referrals,
            "commission_type": commission_type,
            "has_turnover_cap": commission_type == "turnover"
        }
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

# Request withdrawal
@api_router.post("/affiliate/withdrawal")
async def request_affiliate_withdrawal(request: AffiliateWithdrawalRequest, authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    token = authorization.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        affiliate_id = payload.get("sub")
        
        affiliate = await db.affiliates.find_one({"affiliate_id": affiliate_id})
        if not affiliate:
            raise HTTPException(status_code=404, detail="Affiliate not found")
        
        if affiliate.get("balance", 0) < request.amount:
            raise HTTPException(status_code=400, detail="Insufficient balance")
        
        if request.amount < 50:
            raise HTTPException(status_code=400, detail="Minimum withdrawal is $50")
        
        withdrawal_doc = {
            "withdrawal_id": str(uuid.uuid4()),
            "affiliate_id": affiliate_id,
            "amount": request.amount,
            "wallet_address": request.wallet_address,
            "payment_method": request.payment_method,
            "status": "pending",
            "created_at": datetime.now(timezone.utc)
        }
        
        await db.affiliate_withdrawals.insert_one(withdrawal_doc)
        
        # Deduct from balance
        await db.affiliates.update_one(
            {"affiliate_id": affiliate_id},
            {"$inc": {"balance": -request.amount}}
        )
        
        # Return without _id field
        return {"success": True, "withdrawal": {
            "withdrawal_id": withdrawal_doc["withdrawal_id"],
            "amount": withdrawal_doc["amount"],
            "status": withdrawal_doc["status"]
        }}
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

# Get withdrawal history
@api_router.get("/affiliate/withdrawals")
async def get_affiliate_withdrawals(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    token = authorization.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        affiliate_id = payload.get("sub")
        
        withdrawals = await db.affiliate_withdrawals.find(
            {"affiliate_id": affiliate_id}
        ).sort("created_at", -1).to_list(100)
        
        # Serialize properly - remove ObjectId
        result = []
        for w in withdrawals:
            result.append({
                "withdrawal_id": w.get("withdrawal_id") or str(w.get("_id")),
                "amount": w.get("amount"),
                "wallet_address": w.get("wallet_address"),
                "payment_method": w.get("payment_method"),
                "status": w.get("status"),
                "created_at": str(w.get("created_at", ""))
            })
        
        return {"withdrawals": result}
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

# Get promo materials
@api_router.get("/affiliate/promo-materials")
async def get_promo_materials():
    materials = await db.promo_materials.find({"is_active": True}).to_list(100)
    
    # Default materials if none exist
    if not materials:
        materials = [
            {
                "material_id": "1",
                "type": "banner",
                "name": "Main Banner 728x90",
                "size": "728x90",
                "preview_url": "https://via.placeholder.com/728x90/00E55A/000000?text=BYNIX+Trade+Now",
                "category": "banners"
            },
            {
                "material_id": "2",
                "type": "banner",
                "name": "Square Banner 300x250",
                "size": "300x250",
                "preview_url": "https://via.placeholder.com/300x250/00E55A/000000?text=BYNIX",
                "category": "banners"
            },
            {
                "material_id": "3",
                "type": "landing",
                "name": "Main Landing Page",
                "preview_url": "/affiliate",
                "category": "landings"
            }
        ]
    
    return {"materials": materials}

# Get TOP 10 affiliates
@api_router.get("/affiliate/top10")
async def get_top10_affiliates():
    top_affiliates = await db.affiliates.find(
        {"is_active": True}
    ).sort("total_earnings", -1).limit(10).to_list(10)
    
    # Mask names for privacy
    result = []
    for i, aff in enumerate(top_affiliates):
        name = aff.get("name", "Anonymous")
        masked_name = name[0] + "***" + name[-1] if len(name) > 2 else "***"
        result.append({
            "rank": i + 1,
            "name": masked_name,
            "level": aff.get("level", 1),
            "total_earnings": aff.get("total_earnings", 0),
            "total_ftds": aff.get("total_ftds", 0)
        })
    
    return {"top_affiliates": result}

# Update affiliate profile
@api_router.put("/affiliate/profile")
async def update_affiliate_profile(authorization: str = Header(None), name: str = None, telegram: str = None):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    token = authorization.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        affiliate_id = payload.get("sub")
        
        update_data = {}
        if name: update_data["name"] = name
        if telegram: update_data["telegram"] = telegram
        
        if update_data:
            await db.affiliates.update_one(
                {"affiliate_id": affiliate_id},
                {"$set": update_data}
            )
        
        return {"success": True}
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

# ============== HOLD BALANCE SYSTEM ==============

# Get affiliate hold balance info
@api_router.get("/affiliate/balance-info")
async def get_affiliate_balance_info(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    token = authorization.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        affiliate_id = payload.get("sub")
        
        affiliate = await db.affiliates.find_one({"affiliate_id": affiliate_id})
        if not affiliate:
            raise HTTPException(status_code=404, detail="Affiliate not found")
        
        # Calculate next payout date (next Monday 6 AM Singapore time)
        import pytz
        singapore_tz = pytz.timezone('Asia/Singapore')
        now_sgt = datetime.now(singapore_tz)
        
        # Find next Monday
        days_until_monday = (7 - now_sgt.weekday()) % 7
        if days_until_monday == 0 and now_sgt.hour >= 6:
            days_until_monday = 7  # If it's Monday after 6 AM, next payout is next week
        
        next_monday = now_sgt + timedelta(days=days_until_monday)
        next_payout = next_monday.replace(hour=6, minute=0, second=0, microsecond=0)
        
        return {
            "available_balance": affiliate.get("balance", 0),
            "hold_balance": affiliate.get("hold_balance", 0),
            "hold_balance_revenue": affiliate.get("hold_balance_revenue", 0),
            "hold_balance_turnover": affiliate.get("hold_balance_turnover", 0),
            "total_earnings": affiliate.get("total_earnings", 0),
            "last_payout_date": affiliate.get("last_payout_date"),
            "next_payout_date": next_payout.isoformat(),
            "commission_rate": affiliate.get("commission_rate", 50),
            "turnover_rate": affiliate.get("turnover_rate", 2)
        }
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

# Admin: Get all affiliates with hold balances
@api_router.get("/admin/affiliate-hold-balances")
async def admin_get_affiliate_hold_balances(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    token = authorization.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
        
        user = await db.users.find_one({"user_id": user_id})
        if not user or not user.get("is_admin"):
            raise HTTPException(status_code=403, detail="Admin access required")
        
        affiliates = await db.affiliates.find().to_list(1000)
        
        result = []
        for aff in affiliates:
            result.append({
                "affiliate_id": aff.get("affiliate_id"),
                "email": aff.get("email"),
                "name": aff.get("name"),
                "ref_code": aff.get("ref_code"),
                "available_balance": aff.get("balance", 0),
                "hold_balance": aff.get("hold_balance", 0),
                "total_earnings": aff.get("total_earnings", 0),
                "total_referrals": aff.get("total_referrals", 0),
                "last_payout_date": aff.get("last_payout_date"),
                "is_active": aff.get("is_active", True)
            })
        
        return {"affiliates": result}
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

# Admin: Adjust affiliate hold balance
@api_router.post("/admin/affiliate-hold-balance/adjust")
async def admin_adjust_affiliate_hold_balance(
    affiliate_id: str = Body(...),
    amount: float = Body(...),
    action: str = Body(...),  # "add", "subtract", "release" (transfer to available)
    note: str = Body(None),
    authorization: str = Header(None)
):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    token = authorization.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
        
        user = await db.users.find_one({"user_id": user_id})
        if not user or not user.get("is_admin"):
            raise HTTPException(status_code=403, detail="Admin access required")
        
        affiliate = await db.affiliates.find_one({"affiliate_id": affiliate_id})
        if not affiliate:
            raise HTTPException(status_code=404, detail="Affiliate not found")
        
        current_hold = affiliate.get("hold_balance", 0)
        current_available = affiliate.get("balance", 0)
        
        if action == "add":
            new_hold = current_hold + amount
            await db.affiliates.update_one(
                {"affiliate_id": affiliate_id},
                {"$set": {"hold_balance": new_hold}}
            )
        elif action == "subtract":
            new_hold = max(0, current_hold - amount)
            await db.affiliates.update_one(
                {"affiliate_id": affiliate_id},
                {"$set": {"hold_balance": new_hold}}
            )
        elif action == "release":
            # Transfer from hold to available
            transfer_amount = min(amount, current_hold)
            new_hold = current_hold - transfer_amount
            new_available = current_available + transfer_amount
            await db.affiliates.update_one(
                {"affiliate_id": affiliate_id},
                {"$set": {
                    "hold_balance": new_hold,
                    "balance": new_available,
                    "last_payout_date": datetime.now(timezone.utc)
                }}
            )
        else:
            raise HTTPException(status_code=400, detail="Invalid action")
        
        # Log the adjustment
        adjustment_log = {
            "log_id": f"adj_{uuid.uuid4().hex[:12]}",
            "affiliate_id": affiliate_id,
            "action": action,
            "amount": amount,
            "note": note,
            "admin_id": user_id,
            "created_at": datetime.now(timezone.utc)
        }
        await db.affiliate_balance_adjustments.insert_one(adjustment_log)
        
        # Get updated affiliate
        updated_aff = await db.affiliates.find_one({"affiliate_id": affiliate_id})
        
        return {
            "success": True,
            "available_balance": updated_aff.get("balance", 0),
            "hold_balance": updated_aff.get("hold_balance", 0)
        }
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

# Admin: Release all hold balances (for manual Monday payout)
@api_router.post("/admin/affiliate-hold-balance/release-all")
async def admin_release_all_hold_balances(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    token = authorization.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
        
        user = await db.users.find_one({"user_id": user_id})
        if not user or not user.get("is_admin"):
            raise HTTPException(status_code=403, detail="Admin access required")
        
        # Get all affiliates with hold balance > 0
        affiliates = await db.affiliates.find({"hold_balance": {"$gt": 0}}).to_list(1000)
        
        released_count = 0
        total_released = 0
        
        for aff in affiliates:
            hold_balance = aff.get("hold_balance", 0)
            if hold_balance > 0:
                new_available = aff.get("balance", 0) + hold_balance
                await db.affiliates.update_one(
                    {"affiliate_id": aff.get("affiliate_id")},
                    {"$set": {
                        "balance": new_available,
                        "hold_balance": 0,
                        "hold_balance_revenue": 0,
                        "hold_balance_turnover": 0,
                        "last_payout_date": datetime.now(timezone.utc)
                    }}
                )
                released_count += 1
                total_released += hold_balance
        
        return {
            "success": True,
            "affiliates_released": released_count,
            "total_amount_released": total_released
        }
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

# Background task to auto-release hold balances on Monday 6 AM SGT
async def check_monday_payout():
    """Background task that runs every hour to check if it's Monday 6 AM SGT"""
    import pytz
    while True:
        try:
            singapore_tz = pytz.timezone('Asia/Singapore')
            now_sgt = datetime.now(singapore_tz)
            
            # Check if it's Monday between 6:00 and 6:59 AM SGT
            if now_sgt.weekday() == 0 and now_sgt.hour == 6:
                print(f"[PAYOUT] Monday 6 AM SGT - Processing auto payout...")
                
                # Check if we already processed today
                today_start = now_sgt.replace(hour=0, minute=0, second=0, microsecond=0)
                last_auto_payout = await db.system_settings.find_one({"key": "last_auto_payout"})
                
                if last_auto_payout:
                    last_payout_date = last_auto_payout.get("value")
                    if last_payout_date and last_payout_date.date() == now_sgt.date():
                        print("[PAYOUT] Already processed today, skipping...")
                        await asyncio.sleep(3600)  # Wait 1 hour
                        continue
                
                # Get all affiliates with hold balance > 0
                affiliates = await db.affiliates.find({"hold_balance": {"$gt": 0}}).to_list(1000)
                
                released_count = 0
                total_released = 0
                
                for aff in affiliates:
                    hold_balance = aff.get("hold_balance", 0)
                    if hold_balance > 0:
                        new_available = aff.get("balance", 0) + hold_balance
                        await db.affiliates.update_one(
                            {"affiliate_id": aff.get("affiliate_id")},
                            {"$set": {
                                "balance": new_available,
                                "hold_balance": 0,
                                "hold_balance_revenue": 0,
                                "hold_balance_turnover": 0,
                                "last_payout_date": datetime.now(timezone.utc)
                            }}
                        )
                        released_count += 1
                        total_released += hold_balance
                        print(f"[PAYOUT] Released ${hold_balance:.2f} to {aff.get('email')}")
                
                # Update last auto payout time
                await db.system_settings.update_one(
                    {"key": "last_auto_payout"},
                    {"$set": {"value": datetime.now(timezone.utc)}},
                    upsert=True
                )
                
                print(f"[PAYOUT] Completed - Released ${total_released:.2f} to {released_count} affiliates")
            
        except Exception as e:
            print(f"[PAYOUT] Error in auto payout check: {e}")
        
        await asyncio.sleep(3600)  # Check every hour


# ============= MARKETING API ENDPOINTS =============

class PushNotificationCreate(BaseModel):
    title: str
    body: str
    image_url: Optional[str] = None
    cta_text: Optional[str] = None
    cta_url: Optional[str] = None
    target_audience: str = "all_users"  # all_users, all_affiliates, custom
    target_ids: Optional[List[str]] = None
    target_filter: Optional[Dict] = None  # {"country": "BD", "is_active": True, etc}
    schedule_at: Optional[str] = None  # ISO datetime string for scheduling

class EmailCampaignCreate(BaseModel):
    subject: str
    html_body: str
    plain_body: Optional[str] = None
    template: Optional[str] = "promotional"
    image_url: Optional[str] = None
    cta_text: Optional[str] = None
    cta_url: Optional[str] = None
    target_audience: str = "all_users"
    target_ids: Optional[List[str]] = None
    target_filter: Optional[Dict] = None
    schedule_at: Optional[str] = None


@api_router.post("/admin/marketing/push-notifications")
async def create_push_notification(
    notification: PushNotificationCreate,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Create and send push notification"""
    try:
        admin = await get_current_user(authorization, request)
    except HTTPException:
        raise HTTPException(status_code=401, detail="Admin authentication required")
    
    notification_id = f"notif_{uuid.uuid4().hex[:12]}"
    
    # Get target user IDs based on audience
    target_user_ids = []
    
    if notification.target_audience == "all_users":
        users = await db.users.find({}, {"user_id": 1}).to_list(10000)
        target_user_ids = [u["user_id"] for u in users]
    elif notification.target_audience == "all_affiliates":
        affiliates = await db.affiliates.find({}, {"user_id": 1}).to_list(10000)
        target_user_ids = [a["user_id"] for a in affiliates]
    elif notification.target_audience == "custom" and notification.target_ids:
        target_user_ids = notification.target_ids
    elif notification.target_filter:
        # Apply filter to users
        query = {}
        if notification.target_filter.get("country"):
            query["country"] = notification.target_filter["country"]
        if notification.target_filter.get("is_verified"):
            query["kyc_status"] = "verified"
        if notification.target_filter.get("has_balance"):
            query["real_balance"] = {"$gt": 0}
        users = await db.users.find(query, {"user_id": 1}).to_list(10000)
        target_user_ids = [u["user_id"] for u in users]
    
    # Store notification record
    notification_record = {
        "notification_id": notification_id,
        "type": "push",
        "title": notification.title,
        "body": notification.body,
        "image_url": notification.image_url,
        "cta_text": notification.cta_text,
        "cta_url": notification.cta_url,
        "target_audience": notification.target_audience,
        "target_count": len(target_user_ids),
        "sent_count": 0,
        "delivered_count": 0,
        "click_count": 0,
        "status": "scheduled" if notification.schedule_at else "sending",
        "schedule_at": notification.schedule_at,
        "created_by": admin.user_id,
        "created_at": datetime.now(timezone.utc)
    }
    await db.marketing_notifications.insert_one(notification_record)
    
    # Send if not scheduled
    if not notification.schedule_at:
        result = await marketing_service.send_bulk_in_app_notifications(
            db=db,
            user_ids=target_user_ids,
            title=notification.title,
            message=notification.body,
            notification_type="marketing"
        )
        
        # Update record
        await db.marketing_notifications.update_one(
            {"notification_id": notification_id},
            {
                "$set": {
                    "status": "sent",
                    "sent_count": result["sent"],
                    "sent_at": datetime.now(timezone.utc)
                }
            }
        )
    
    return {
        "success": True,
        "notification_id": notification_id,
        "target_count": len(target_user_ids),
        "status": "scheduled" if notification.schedule_at else "sent"
    }


@api_router.get("/admin/marketing/push-notifications")
async def get_push_notifications(
    limit: int = 50,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get list of push notifications"""
    try:
        admin = await get_current_user(authorization, request)
    except HTTPException:
        raise HTTPException(status_code=401, detail="Admin authentication required")
    
    notifications = await db.marketing_notifications.find(
        {"type": "push"}
    ).sort("created_at", -1).limit(limit).to_list(limit)
    
    result = []
    for n in notifications:
        result.append({
            "notification_id": n.get("notification_id"),
            "title": n.get("title"),
            "body": n.get("body"),
            "image_url": n.get("image_url"),
            "target_audience": n.get("target_audience"),
            "target_count": n.get("target_count", 0),
            "sent_count": n.get("sent_count", 0),
            "delivered_count": n.get("delivered_count", 0),
            "click_count": n.get("click_count", 0),
            "status": n.get("status"),
            "schedule_at": n.get("schedule_at"),
            "created_at": str(n.get("created_at", ""))
        })
    
    return {"success": True, "notifications": result}


@api_router.post("/admin/marketing/email-campaigns")
async def create_email_campaign(
    campaign: EmailCampaignCreate,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Create and send email campaign"""
    try:
        admin = await get_current_user(authorization, request)
    except HTTPException:
        raise HTTPException(status_code=401, detail="Admin authentication required")
    
    campaign_id = f"camp_{uuid.uuid4().hex[:12]}"
    
    # Get target emails based on audience
    target_emails = []
    target_user_ids = []
    
    if campaign.target_audience == "all_users":
        users = await db.users.find({}, {"user_id": 1, "email": 1}).to_list(10000)
        target_emails = [u["email"] for u in users if u.get("email")]
        target_user_ids = [u["user_id"] for u in users]
    elif campaign.target_audience == "all_affiliates":
        affiliates = await db.affiliates.find({}, {"user_id": 1}).to_list(10000)
        aff_user_ids = [a["user_id"] for a in affiliates]
        users = await db.users.find({"user_id": {"$in": aff_user_ids}}, {"user_id": 1, "email": 1}).to_list(10000)
        target_emails = [u["email"] for u in users if u.get("email")]
        target_user_ids = [u["user_id"] for u in users]
    elif campaign.target_audience == "custom" and campaign.target_ids:
        users = await db.users.find({"user_id": {"$in": campaign.target_ids}}, {"user_id": 1, "email": 1}).to_list(10000)
        target_emails = [u["email"] for u in users if u.get("email")]
        target_user_ids = campaign.target_ids
    elif campaign.target_filter:
        query = {}
        if campaign.target_filter.get("verified_only"):
            query["kyc_status"] = "verified"
        if campaign.target_filter.get("has_deposits"):
            # Users who have deposited
            depositors = await db.deposits.distinct("user_id", {"status": "completed"})
            query["user_id"] = {"$in": depositors}
        users = await db.users.find(query, {"user_id": 1, "email": 1}).to_list(10000)
        target_emails = [u["email"] for u in users if u.get("email")]
        target_user_ids = [u["user_id"] for u in users]
    
    # Build email body with template
    html_body = campaign.html_body
    if campaign.template and campaign.template in EMAIL_TEMPLATES:
        content = f"<h2>{campaign.subject}</h2>{campaign.html_body}"
        if campaign.cta_text and campaign.cta_url:
            content += f'<p style="text-align:center;"><a href="{campaign.cta_url}" class="cta-btn">{campaign.cta_text}</a></p>'
        if campaign.image_url:
            content = f'<div class="image-container"><img src="{campaign.image_url}" alt=""/></div>' + content
        html_body = EMAIL_TEMPLATES[campaign.template].replace("{{CONTENT}}", content)
        html_body = html_body.replace("{{UNSUBSCRIBE_URL}}", "https://bynix.io/unsubscribe")
    
    # Store campaign record
    campaign_record = {
        "campaign_id": campaign_id,
        "type": "email",
        "subject": campaign.subject,
        "html_body": html_body,
        "plain_body": campaign.plain_body,
        "template": campaign.template,
        "image_url": campaign.image_url,
        "cta_text": campaign.cta_text,
        "cta_url": campaign.cta_url,
        "target_audience": campaign.target_audience,
        "target_count": len(target_emails),
        "sent_count": 0,
        "open_count": 0,
        "click_count": 0,
        "bounce_count": 0,
        "status": "scheduled" if campaign.schedule_at else "sending",
        "schedule_at": campaign.schedule_at,
        "created_by": admin.user_id,
        "created_at": datetime.now(timezone.utc)
    }
    await db.marketing_campaigns.insert_one(campaign_record)
    
    # Send if not scheduled
    if not campaign.schedule_at:
        # Determine account type based on audience
        account_type = "affiliate" if campaign.target_audience == "all_affiliates" else "user"
        
        result = await marketing_service.send_bulk_emails(
            recipients=target_emails,
            subject=campaign.subject,
            html_body=html_body,
            plain_body=campaign.plain_body,
            campaign_id=campaign_id,
            account_type=account_type
        )
        
        # Update record
        await db.marketing_campaigns.update_one(
            {"campaign_id": campaign_id},
            {
                "$set": {
                    "status": "sent",
                    "sent_count": result["sent"],
                    "failed_count": result["failed"],
                    "sent_at": datetime.now(timezone.utc)
                }
            }
        )
        
        # Also send in-app notification
        await marketing_service.send_bulk_in_app_notifications(
            db=db,
            user_ids=target_user_ids,
            title=f"📧 {campaign.subject}",
            message="Check your email for an important update from Bynix!",
            notification_type="email_campaign"
        )
    
    return {
        "success": True,
        "campaign_id": campaign_id,
        "target_count": len(target_emails),
        "status": "scheduled" if campaign.schedule_at else "sent"
    }


@api_router.get("/admin/marketing/email-campaigns")
async def get_email_campaigns(
    limit: int = 50,
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get list of email campaigns"""
    try:
        admin = await get_current_user(authorization, request)
    except HTTPException:
        raise HTTPException(status_code=401, detail="Admin authentication required")
    
    campaigns = await db.marketing_campaigns.find(
        {"type": "email"}
    ).sort("created_at", -1).limit(limit).to_list(limit)
    
    result = []
    for c in campaigns:
        result.append({
            "campaign_id": c.get("campaign_id"),
            "subject": c.get("subject"),
            "template": c.get("template"),
            "target_audience": c.get("target_audience"),
            "target_count": c.get("target_count", 0),
            "sent_count": c.get("sent_count", 0),
            "open_count": c.get("open_count", 0),
            "click_count": c.get("click_count", 0),
            "bounce_count": c.get("bounce_count", 0),
            "status": c.get("status"),
            "schedule_at": c.get("schedule_at"),
            "created_at": str(c.get("created_at", ""))
        })
    
    return {"success": True, "campaigns": result}


@api_router.get("/admin/marketing/stats")
async def get_marketing_stats(
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get marketing analytics overview"""
    try:
        admin = await get_current_user(authorization, request)
    except HTTPException:
        raise HTTPException(status_code=401, detail="Admin authentication required")
    
    # Notification stats
    total_notifications = await db.marketing_notifications.count_documents({})
    total_notif_sent = await db.marketing_notifications.aggregate([
        {"$group": {"_id": None, "total": {"$sum": "$sent_count"}}}
    ]).to_list(1)
    
    # Campaign stats
    total_campaigns = await db.marketing_campaigns.count_documents({})
    total_emails_sent = await db.marketing_campaigns.aggregate([
        {"$group": {"_id": None, "total": {"$sum": "$sent_count"}}}
    ]).to_list(1)
    total_opens = await db.marketing_campaigns.aggregate([
        {"$group": {"_id": None, "total": {"$sum": "$open_count"}}}
    ]).to_list(1)
    
    return {
        "success": True,
        "notifications": {
            "total": total_notifications,
            "sent": total_notif_sent[0]["total"] if total_notif_sent else 0
        },
        "campaigns": {
            "total": total_campaigns,
            "emails_sent": total_emails_sent[0]["total"] if total_emails_sent else 0,
            "opens": total_opens[0]["total"] if total_opens else 0
        }
    }


@api_router.get("/admin/marketing/audience-stats")
async def get_audience_stats(
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get audience statistics for targeting"""
    try:
        admin = await get_current_user(authorization, request)
    except HTTPException:
        raise HTTPException(status_code=401, detail="Admin authentication required")
    
    total_users = await db.users.count_documents({})
    verified_users = await db.users.count_documents({"kyc_status": "verified"})
    users_with_balance = await db.users.count_documents({"real_balance": {"$gt": 0}})
    total_affiliates = await db.affiliates.count_documents({})
    
    # Country breakdown
    country_stats = await db.users.aggregate([
        {"$group": {"_id": "$country", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]).to_list(10)
    
    return {
        "success": True,
        "all_users": total_users,
        "verified_users": verified_users,
        "users_with_balance": users_with_balance,
        "affiliates": total_affiliates,
        "countries": [{"country": c["_id"] or "Unknown", "count": c["count"]} for c in country_stats]
    }


@api_router.get("/admin/marketing/email-status")
async def get_email_status(
    authorization: Optional[str] = Header(None),
    request: Request = None
):
    """Get email configuration status"""
    try:
        admin = await get_current_user(authorization, request)
    except HTTPException:
        raise HTTPException(status_code=401, detail="Admin authentication required")
    
    return {
        "success": True,
        "status": marketing_service.get_email_status()
    }


# Start background task on app startup
@app.on_event("startup")
async def start_background_tasks():
    asyncio.create_task(check_monday_payout())
    print("[SYSTEM] Monday payout scheduler started")

# Include router - MUST be at the end of file after ALL route definitions
app.include_router(api_router)
