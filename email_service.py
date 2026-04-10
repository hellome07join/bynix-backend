"""
Email Service for OTP Verification
Using Namecheap PrivateEmail SMTP
"""

import os
import random
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from typing import Optional, Dict
from dotenv import load_dotenv

load_dotenv()

# SMTP Configuration
SMTP_SERVER = "mail.privateemail.com"
SMTP_PORT = 587
SMTP_EMAIL = os.getenv("SMTP_EMAIL", "noreply@bynix.io")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "$88442211$aA")

# OTP Storage (in production, use Redis or database)
otp_storage: Dict[str, Dict] = {}

def generate_otp() -> str:
    """Generate 6-digit OTP"""
    return str(random.randint(100000, 999999))

def store_otp(email: str, otp: str, expires_minutes: int = 10) -> None:
    """Store OTP with expiration time"""
    otp_storage[email.lower()] = {
        "otp": otp,
        "expires_at": datetime.now() + timedelta(minutes=expires_minutes),
        "attempts": 0
    }

def verify_otp(email: str, otp: str) -> tuple[bool, str]:
    """
    Verify OTP for given email
    Returns: (success, message)
    """
    email_lower = email.lower()
    
    if email_lower not in otp_storage:
        return False, "OTP not found. Please request a new one."
    
    stored = otp_storage[email_lower]
    
    # Check expiration
    if datetime.now() > stored["expires_at"]:
        del otp_storage[email_lower]
        return False, "OTP expired. Please request a new one."
    
    # Check attempts (max 5)
    if stored["attempts"] >= 5:
        del otp_storage[email_lower]
        return False, "Too many attempts. Please request a new OTP."
    
    # Increment attempts
    stored["attempts"] += 1
    
    # Verify OTP
    if stored["otp"] == otp:
        del otp_storage[email_lower]
        return True, "Email verified successfully!"
    
    remaining = 5 - stored["attempts"]
    return False, f"Invalid OTP. {remaining} attempts remaining."

def send_otp_email(to_email: str, otp: str) -> tuple[bool, str]:
    """
    Send OTP email using SMTP
    Returns: (success, message)
    """
    try:
        # Create message
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"🔐 Bynix Verification Code: {otp}"
        msg["From"] = f"Bynix <{SMTP_EMAIL}>"
        msg["To"] = to_email
        
        # Plain text version
        text = f"""
Bynix Email Verification

Your verification code is: {otp}

This code will expire in 10 minutes.

If you didn't request this code, please ignore this email.

- Bynix Team
"""
        
        # HTML version
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin: 0; padding: 0; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #0A0A0A;">
    <table role="presentation" style="width: 100%; border-collapse: collapse;">
        <tr>
            <td align="center" style="padding: 40px 0;">
                <table role="presentation" style="width: 100%; max-width: 500px; border-collapse: collapse; background-color: #0F1428; border-radius: 16px; overflow: hidden;">
                    <!-- Header with Logo -->
                    <tr>
                        <td style="padding: 30px 40px; text-align: center; background: linear-gradient(135deg, #0D2818 0%, #0F1428 100%);">
                            <img src="https://customer-assets.emergentagent.com/job_bynix-markets/artifacts/zzctjkxm_IMG_3541.jpeg" alt="Bynix" style="width: 120px; height: 120px; border-radius: 12px; margin-bottom: 15px;" />
                            <p style="margin: 10px 0 0; color: #888; font-size: 14px;">Email Verification</p>
                        </td>
                    </tr>
                    
                    <!-- Content -->
                    <tr>
                        <td style="padding: 40px;">
                            <p style="margin: 0 0 20px; color: #FFFFFF; font-size: 16px; line-height: 1.5;">
                                Your verification code is:
                            </p>
                            
                            <!-- OTP Box -->
                            <div style="background: linear-gradient(135deg, #0D2818 0%, rgba(0, 229, 90, 0.1) 100%); border: 2px solid #00E55A; border-radius: 12px; padding: 25px; text-align: center; margin: 20px 0;">
                                <span style="font-size: 36px; font-weight: 700; color: #00E55A; letter-spacing: 8px;">{otp}</span>
                            </div>
                            
                            <p style="margin: 20px 0 0; color: #888; font-size: 14px; line-height: 1.5;">
                                ⏱️ This code will expire in <strong style="color: #FFD700;">10 minutes</strong>
                            </p>
                            
                            <p style="margin: 15px 0 0; color: #666; font-size: 13px; line-height: 1.5;">
                                If you didn't request this code, please ignore this email.
                            </p>
                        </td>
                    </tr>
                    
                    <!-- Footer -->
                    <tr>
                        <td style="padding: 20px 40px; background-color: rgba(255, 255, 255, 0.02); border-top: 1px solid rgba(255, 255, 255, 0.1);">
                            <p style="margin: 0; color: #666; font-size: 12px; text-align: center;">
                                © 2024 Bynix. All rights reserved.
                            </p>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>
</body>
</html>
"""
        
        # Attach parts
        part1 = MIMEText(text, "plain")
        part2 = MIMEText(html, "html")
        msg.attach(part1)
        msg.attach(part2)
        
        # Send email
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_EMAIL, SMTP_PASSWORD)
            server.sendmail(SMTP_EMAIL, to_email, msg.as_string())
        
        print(f"OTP email sent to {to_email}")
        return True, "OTP sent successfully!"
        
    except smtplib.SMTPAuthenticationError as e:
        print(f"SMTP Auth Error: {e}")
        return False, "Email authentication failed. Please contact support."
    except smtplib.SMTPException as e:
        print(f"SMTP Error: {e}")
        return False, f"Failed to send email: {str(e)}"
    except Exception as e:
        print(f"Email Error: {e}")
        return False, f"Email service error: {str(e)}"

def send_verification_otp(email: str) -> tuple[bool, str]:
    """
    Generate and send OTP to email
    Returns: (success, message)
    """
    otp = generate_otp()
    store_otp(email, otp)
    return send_otp_email(email, otp)

def resend_otp(email: str) -> tuple[bool, str]:
    """
    Resend OTP (with rate limiting)
    """
    email_lower = email.lower()
    
    # Check if OTP was recently sent (within 1 minute)
    if email_lower in otp_storage:
        stored = otp_storage[email_lower]
        time_since_created = datetime.now() - (stored["expires_at"] - timedelta(minutes=10))
        if time_since_created < timedelta(minutes=1):
            return False, "Please wait 1 minute before requesting a new OTP."
    
    return send_verification_otp(email)
