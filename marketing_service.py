"""
Marketing Service - Push Notifications & Email Campaigns
Supports separate email accounts for Users and Affiliates
"""

import os
import httpx
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv
from pathlib import Path

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# SMTP Configuration
SMTP_HOST = os.environ.get("SMTP_HOST", "mail.privateemail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))

# Affiliate Email Configuration
AFFILIATE_EMAIL = os.environ.get("AFFILIATE_EMAIL", "")
AFFILIATE_EMAIL_PASSWORD = os.environ.get("AFFILIATE_EMAIL_PASSWORD", "")
AFFILIATE_EMAIL_FROM_NAME = os.environ.get("AFFILIATE_EMAIL_FROM_NAME", "Bynix Affiliates")

# User Email Configuration
USER_EMAIL = os.environ.get("USER_EMAIL", "")
USER_EMAIL_PASSWORD = os.environ.get("USER_EMAIL_PASSWORD", "")
USER_EMAIL_FROM_NAME = os.environ.get("USER_EMAIL_FROM_NAME", "Bynix Trading")


class MarketingService:
    """Service for handling push notifications and email campaigns"""
    
    def __init__(self):
        self.smtp_host = SMTP_HOST
        self.smtp_port = SMTP_PORT
        
        # Email accounts
        self.affiliate_email = AFFILIATE_EMAIL
        self.affiliate_password = AFFILIATE_EMAIL_PASSWORD
        self.affiliate_from_name = AFFILIATE_EMAIL_FROM_NAME
        
        self.user_email = USER_EMAIL
        self.user_password = USER_EMAIL_PASSWORD
        self.user_from_name = USER_EMAIL_FROM_NAME
    
    def _get_email_account(self, account_type: str = "user") -> Dict[str, str]:
        """Get email credentials based on account type"""
        if account_type == "affiliate":
            return {
                "email": self.affiliate_email,
                "password": self.affiliate_password,
                "from_name": self.affiliate_from_name
            }
        else:
            # Default to user email, fallback to affiliate if user not configured
            if self.user_email and self.user_password:
                return {
                    "email": self.user_email,
                    "password": self.user_password,
                    "from_name": self.user_from_name
                }
            elif self.affiliate_email and self.affiliate_password:
                # Fallback to affiliate email
                return {
                    "email": self.affiliate_email,
                    "password": self.affiliate_password,
                    "from_name": self.affiliate_from_name
                }
            else:
                return {"email": "", "password": "", "from_name": "Bynix"}
    
    async def send_email(
        self,
        to_email: str,
        subject: str,
        html_body: str,
        plain_body: Optional[str] = None,
        attachments: Optional[List[Dict]] = None,
        tracking_id: Optional[str] = None,
        account_type: str = "user"  # "user" or "affiliate"
    ) -> Dict[str, Any]:
        """
        Send an email to a single recipient
        
        Args:
            to_email: Recipient email address
            subject: Email subject
            html_body: HTML email body
            plain_body: Plain text fallback (optional)
            attachments: List of {"filename": "", "data": bytes, "content_type": ""} (optional)
            tracking_id: Campaign tracking ID (optional)
            account_type: "user" for user emails, "affiliate" for affiliate emails
        
        Returns:
            {"success": bool, "message": str}
        """
        try:
            # Get email account based on type
            account = self._get_email_account(account_type)
            
            if not account["email"] or not account["password"]:
                print(f"[Marketing] No {account_type} email configured, using mock mode")
                return {"success": True, "message": f"Email sent (mock mode - no {account_type} email configured)"}
            
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = f"{account['from_name']} <{account['email']}>"
            msg['To'] = to_email
            
            # Add tracking pixel if tracking_id provided
            if tracking_id:
                html_body += f'<img src="https://bynix.io/api/marketing/track/open/{tracking_id}" width="1" height="1" style="display:none;" />'
            
            # Add plain text and HTML parts
            if plain_body:
                msg.attach(MIMEText(plain_body, 'plain'))
            msg.attach(MIMEText(html_body, 'html'))
            
            # Add attachments if any
            if attachments:
                for attachment in attachments:
                    if attachment.get("content_type", "").startswith("image/"):
                        img = MIMEImage(attachment["data"])
                        img.add_header('Content-Disposition', 'attachment', filename=attachment.get("filename", "image.png"))
                        msg.attach(img)
            
            # Send email via SMTP
            print(f"[Marketing] Sending email from {account['email']} to {to_email}")
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(account["email"], account["password"])
                server.send_message(msg)
            
            print(f"[Marketing] Email sent successfully to {to_email}")
            return {"success": True, "message": "Email sent successfully"}
                
        except Exception as e:
            print(f"[Marketing] Email send error: {e}")
            import traceback
            traceback.print_exc()
            return {"success": False, "message": str(e)}
    
    async def send_bulk_emails(
        self,
        recipients: List[str],
        subject: str,
        html_body: str,
        plain_body: Optional[str] = None,
        campaign_id: Optional[str] = None,
        account_type: str = "user"
    ) -> Dict[str, Any]:
        """
        Send emails to multiple recipients
        
        Returns:
            {"success": bool, "sent": int, "failed": int, "errors": List[str]}
        """
        sent = 0
        failed = 0
        errors = []
        
        for email in recipients:
            result = await self.send_email(
                to_email=email,
                subject=subject,
                html_body=html_body,
                plain_body=plain_body,
                tracking_id=f"{campaign_id}_{email}" if campaign_id else None,
                account_type=account_type
            )
            
            if result["success"]:
                sent += 1
            else:
                failed += 1
                errors.append(f"{email}: {result['message']}")
        
        return {
            "success": failed == 0,
            "sent": sent,
            "failed": failed,
            "errors": errors[:10]  # Limit error list
        }
    
    async def create_push_notification(
        self,
        title: str,
        body: str,
        image_url: Optional[str] = None,
        cta_text: Optional[str] = None,
        cta_url: Optional[str] = None,
        data: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Create a push notification payload
        
        Returns notification data structure
        """
        notification = {
            "title": title,
            "body": body,
            "icon": "/icon.png",
            "badge": "/badge.png",
        }
        
        if image_url:
            notification["image"] = image_url
        
        if cta_text and cta_url:
            notification["actions"] = [
                {
                    "action": "open_url",
                    "title": cta_text,
                    "url": cta_url
                }
            ]
        
        if data:
            notification["data"] = data
        
        return notification
    
    async def send_in_app_notification(
        self,
        db,
        user_id: str,
        title: str,
        message: str,
        notification_type: str = "marketing"
    ) -> bool:
        """
        Send in-app notification (stored in database)
        """
        try:
            from datetime import datetime, timezone
            
            notification = {
                "notification_id": f"notif_{datetime.now().timestamp()}_{user_id[:8]}",
                "user_id": user_id,
                "title": title,
                "message": message,
                "type": notification_type,
                "read": False,
                "created_at": datetime.now(timezone.utc)
            }
            
            await db.notifications.insert_one(notification)
            return True
        except Exception as e:
            print(f"[Marketing] In-app notification error: {e}")
            return False
    
    async def send_bulk_in_app_notifications(
        self,
        db,
        user_ids: List[str],
        title: str,
        message: str,
        notification_type: str = "marketing"
    ) -> Dict[str, int]:
        """
        Send in-app notifications to multiple users
        """
        sent = 0
        failed = 0
        
        for user_id in user_ids:
            success = await self.send_in_app_notification(
                db, user_id, title, message, notification_type
            )
            if success:
                sent += 1
            else:
                failed += 1
        
        return {"sent": sent, "failed": failed}
    
    def get_email_status(self) -> Dict[str, Any]:
        """Get email configuration status"""
        return {
            "affiliate_email": {
                "configured": bool(self.affiliate_email and self.affiliate_password),
                "email": self.affiliate_email if self.affiliate_email else "Not configured"
            },
            "user_email": {
                "configured": bool(self.user_email and self.user_password),
                "email": self.user_email if self.user_email else "Not configured (will use affiliate email)"
            },
            "smtp_host": self.smtp_host,
            "smtp_port": self.smtp_port
        }


# Email Templates
EMAIL_TEMPLATES = {
    "promotional": """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body { font-family: 'Segoe UI', Arial, sans-serif; margin: 0; padding: 0; background: #f4f4f4; }
        .container { max-width: 600px; margin: 0 auto; background: #ffffff; }
        .header { background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%); padding: 30px; text-align: center; }
        .header img { max-width: 120px; border-radius: 12px; }
        .header h1 { color: #00E55A; margin: 10px 0 0; font-size: 24px; }
        .content { padding: 30px; }
        .content h2 { color: #1a1a2e; margin-top: 0; }
        .content p { color: #666; line-height: 1.6; }
        .cta-btn { display: inline-block; background: #00E55A; color: #000 !important; padding: 14px 30px; text-decoration: none; border-radius: 8px; font-weight: bold; margin: 20px 0; }
        .image-container { text-align: center; margin: 20px 0; }
        .image-container img { max-width: 100%; border-radius: 8px; }
        .footer { background: #1a1a2e; color: #888; padding: 20px; text-align: center; font-size: 12px; }
        .footer a { color: #00E55A; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <img src="https://customer-assets.emergentagent.com/job_bynix-markets/artifacts/zzctjkxm_IMG_3541.jpeg" alt="Bynix" />
        </div>
        <div class="content">
            {{CONTENT}}
        </div>
        <div class="footer">
            <p>© 2025 Bynix Trading. All rights reserved.</p>
            <p><a href="{{UNSUBSCRIBE_URL}}">Unsubscribe</a> | <a href="https://bynix.io">Visit Website</a></p>
        </div>
    </div>
</body>
</html>
""",
    "notification": """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body { font-family: Arial, sans-serif; background: #f5f5f5; padding: 20px; }
        .card { max-width: 500px; margin: 0 auto; background: #fff; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .card-header { background: #1a1a2e; color: #00E55A; padding: 20px; text-align: center; }
        .card-header img { max-width: 100px; border-radius: 10px; }
        .card-body { padding: 25px; }
        .card-body h2 { color: #333; margin-top: 0; }
        .card-body p { color: #666; }
        .btn { display: inline-block; background: #00E55A; color: #000; padding: 12px 25px; text-decoration: none; border-radius: 6px; font-weight: bold; }
    </style>
</head>
<body>
    <div class="card">
        <div class="card-header">
            <img src="https://customer-assets.emergentagent.com/job_bynix-markets/artifacts/zzctjkxm_IMG_3541.jpeg" alt="Bynix" />
        </div>
        <div class="card-body">
            {{CONTENT}}
        </div>
    </div>
</body>
</html>
"""
}


# Singleton instance
marketing_service = MarketingService()
