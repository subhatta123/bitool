import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from typing import Dict, List, Any, Optional
import logging
from django.conf import settings
from django.core.mail import send_mail
from django.template.loader import render_to_string
import logging

# Initialize logger early for module-level usage
logger = logging.getLogger(__name__)

# Optional import for HTML to image conversion
try:
    from html2image import Html2Image
    HTML2IMAGE_AVAILABLE = True
except ImportError:
    Html2Image = None
    HTML2IMAGE_AVAILABLE = False
    logger.warning("html2image not available - chart export to image will be disabled")

import os
import tempfile
from celery import shared_task
from core.models import LLMConfig

class EmailService:
    """
    Comprehensive email service for dashboard sharing and notifications
    """
    
    def __init__(self):
        if HTML2IMAGE_AVAILABLE:
            self.hti = Html2Image(output_path=tempfile.gettempdir())
        else:
            self.hti = None
    
    def get_email_config(self) -> Dict[str, Any]:
        """Get email configuration from database or Django settings"""
        try:
            # Try to get from EmailConfig model first
            from core.models import EmailConfig
            email_config = EmailConfig.get_active_config()
            if email_config:
                email_settings = email_config.get_email_settings_dict()
                return {
                    "SMTP_SERVER": email_settings.get('smtp_host'),
                    "SMTP_PORT": email_settings.get('smtp_port', 587),
                    "SMTP_USERNAME": email_settings.get('smtp_user'),
                    "SMTP_PASSWORD": email_settings.get('smtp_password'),
                    "SENDER_EMAIL": email_settings.get('sender_email'),
                    "SENDER_NAME": email_settings.get('sender_name', 'ConvaBI System'),
                    "USE_TLS": email_settings.get('use_tls', True),
                    "USE_SSL": email_settings.get('use_ssl', False),
                    "TIMEOUT": email_settings.get('timeout', 30),
                    "FAIL_SILENTLY": email_settings.get('fail_silently', False)
                }
        except Exception as e:
            logger.warning(f"Failed to get email config from database: {e}")
        
        # Fallback to Django settings
        return {
            "SMTP_SERVER": getattr(settings, 'EMAIL_HOST', None),
            "SMTP_PORT": getattr(settings, 'EMAIL_PORT', 587),
            "SMTP_USERNAME": getattr(settings, 'EMAIL_HOST_USER', None),
            "SMTP_PASSWORD": getattr(settings, 'EMAIL_HOST_PASSWORD', None),
            "SENDER_EMAIL": getattr(settings, 'DEFAULT_FROM_EMAIL', None),
            "SENDER_NAME": getattr(settings, 'DEFAULT_FROM_EMAIL', 'ConvaBI System'),
            "USE_TLS": getattr(settings, 'EMAIL_USE_TLS', True),
            "USE_SSL": getattr(settings, 'EMAIL_USE_SSL', False),
            "TIMEOUT": 30,
            "FAIL_SILENTLY": False
        }
    
    def send_dashboard_email(self, recipient_email: str, subject: str, body: str, 
                           attachments: List[Dict[str, Any]] = None, 
                           schedule_info: Dict[str, Any] = None) -> bool:
        """
        Send dashboard email with attachments
        
        Args:
            recipient_email: Email address of recipient
            subject: Email subject
            body: Email body (HTML)
            attachments: List of attachment dicts with content, filename, type
            schedule_info: Optional scheduling information
        
        Returns:
            Boolean indicating success
        """
        email_cfg = self.get_email_config()
        
        smtp_server_host = email_cfg.get("SMTP_SERVER")
        smtp_user = email_cfg.get("SMTP_USERNAME") or ""
        smtp_pass = email_cfg.get("SMTP_PASSWORD") or ""
        sender = email_cfg.get("SENDER_EMAIL")
        
        if not all([smtp_user, smtp_pass, sender, smtp_server_host]):
            error_msg = "Email server credentials are not configured."
            logger.warning(f"Email not configured for {recipient_email}: {error_msg}")
            
            # For development/testing: create a simple file-based email log instead
            try:
                import os
                from django.conf import settings
                log_dir = os.path.join(settings.BASE_DIR, 'logs')
                os.makedirs(log_dir, exist_ok=True)
                
                log_file = os.path.join(log_dir, 'email_log.txt')
                with open(log_file, 'a', encoding='utf-8') as f:
                    from datetime import datetime
                    f.write(f"\n{'='*50}\n")
                    f.write(f"Email Log Entry - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"To: {recipient_email}\n")
                    f.write(f"Subject: {subject}\n")
                    f.write(f"Attachments: {len(attachments) if attachments else 0}\n")
                    f.write(f"Schedule Info: {schedule_info}\n")
                    f.write(f"Body Preview: {body[:200]}...\n")
                    f.write(f"{'='*50}\n")
                
                logger.info(f"Email logged to file for {recipient_email} (SMTP not configured)")
                return True  # Return True for development mode
                
            except Exception as log_error:
                logger.error(f"Failed to log email: {log_error}")
                return False
        
        # Create email message
        msg = MIMEMultipart('mixed')
        sender_name = email_cfg.get("SENDER_NAME", "ConvaBI System")
        msg['From'] = f"{sender_name} <{sender}>"
        msg['To'] = recipient_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'html'))
        
        # Add attachments
        if attachments:
            for attachment in attachments:
                attachment_content = attachment.get('content')
                attachment_filename = attachment.get('filename')
                attachment_type = attachment.get('type')
                
                if not all([attachment_content, attachment_filename, attachment_type]):
                    continue
                
                part = None
                if attachment_type == 'html':
                    part = MIMEBase('text', 'html')
                    part.set_payload(attachment_content.encode('utf-8'))
                    encoders.encode_base64(part)
                elif attachment_type == 'pdf':
                    part = MIMEBase('application', 'pdf')
                    part.set_payload(attachment_content)
                    encoders.encode_base64(part)
                elif attachment_type == 'png':
                    part = MIMEBase('image', 'png')
                    part.set_payload(attachment_content)
                    encoders.encode_base64(part)
                
                if part:
                    part.add_header('Content-Disposition', 
                                  f'attachment; filename="{attachment_filename}.{attachment_type}"')
                    msg.attach(part)
        
        # Send email
        try:
            port = int(email_cfg.get("SMTP_PORT", 587))
            timeout = email_cfg.get("TIMEOUT", 30)
            server = smtplib.SMTP(smtp_server_host, port, timeout=timeout)
            if email_cfg.get('USE_TLS'):
                server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(sender, recipient_email, msg.as_string())
            server.quit()
            
            if not (schedule_info and schedule_info.get('is_scheduled_job')):
                logger.info(f"Dashboard email sent to {recipient_email}")
            return True
            
        except Exception as e:
            error_msg = f"Failed to send email: {e}"
            if schedule_info and schedule_info.get('is_scheduled_job'):
                logger.error(error_msg)
            else:
                logger.error(error_msg)
            return False
    
    def generate_dashboard_image(self, dashboard_html_content: str, 
                               dashboard_name: str = "dashboard") -> Optional[bytes]:
        """
        Generate PNG image from dashboard HTML content
        
        Args:
            dashboard_html_content: HTML content to convert
            dashboard_name: Name for the output file
            
        Returns:
            Image bytes if successful, None if failed
        """
        if not dashboard_html_content:
            logger.warning("No HTML content provided for image generation")
            return None
        
        if not HTML2IMAGE_AVAILABLE:
            logger.warning("html2image not available - cannot generate dashboard image")
            return None
        
        try:
            # Create temporary HTML file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False) as temp_file:
                temp_file.write(dashboard_html_content)
                temp_html_path = temp_file.name
            
            # Generate image
            output_filename = f"{dashboard_name}.png"
            self.hti.screenshot(html_file=temp_html_path, save_as=output_filename, size=(1200, 900))
            
            image_path = os.path.join(tempfile.gettempdir(), output_filename)
            
            if os.path.exists(image_path):
                with open(image_path, 'rb') as f:
                    image_bytes = f.read()
                
                # Clean up files
                os.unlink(temp_html_path)
                os.unlink(image_path)
                
                logger.info(f"Successfully generated image: {output_filename}")
                return image_bytes
            else:
                logger.error(f"Image generation failed: file not found at {image_path}")
                return None
                
        except Exception as e:
            logger.error(f"Image generation failed: {e}")
            return None
    
    def generate_dashboard_html(self, dashboard_items: List[Dict[str, Any]], 
                              dashboard_name: str = "Dashboard") -> str:
        """
        Generate HTML content for dashboard export
        
        Args:
            dashboard_items: List of dashboard items
            dashboard_name: Name of the dashboard
            
        Returns:
            HTML string
        """
        try:
            return render_to_string('emails/dashboard_export.html', {
                'dashboard_name': dashboard_name,
                'dashboard_items': dashboard_items,
            })
        except Exception as e:
            logger.error(f"Failed to generate dashboard HTML: {e}")
            return f"<h1>{dashboard_name}</h1><p>Dashboard content could not be generated.</p>"
    
    def schedule_dashboard_email(self, dashboard_id: str, recipient_email: str, 
                               schedule_type: str, schedule_params: Dict[str, Any]) -> bool:
        """
        Schedule recurring dashboard emails using Celery
        
        Args:
            dashboard_id: ID of dashboard to send
            recipient_email: Email recipient
            schedule_type: Type of schedule (daily, weekly, monthly)
            schedule_params: Additional scheduling parameters
            
        Returns:
            Boolean indicating success
        """
        try:
            from django_celery_beat.models import PeriodicTask, CrontabSchedule
            import json
            
            # Create crontab schedule based on type
            if schedule_type == 'daily':
                schedule, _ = CrontabSchedule.objects.get_or_create(
                    minute=schedule_params.get('minute', 0),
                    hour=schedule_params.get('hour', 9),
                    day_of_week='*',
                    day_of_month='*',
                    month_of_year='*'
                )
            elif schedule_type == 'weekly':
                schedule, _ = CrontabSchedule.objects.get_or_create(
                    minute=schedule_params.get('minute', 0),
                    hour=schedule_params.get('hour', 9),
                    day_of_week=schedule_params.get('day_of_week', 1),
                    day_of_month='*',
                    month_of_year='*'
                )
            elif schedule_type == 'monthly':
                schedule, _ = CrontabSchedule.objects.get_or_create(
                    minute=schedule_params.get('minute', 0),
                    hour=schedule_params.get('hour', 9),
                    day_of_week='*',
                    day_of_month=schedule_params.get('day_of_month', 1),
                    month_of_year='*'
                )
            else:
                return False
            
            # Create periodic task
            task_name = f"dashboard_email_{dashboard_id}_{recipient_email}"
            PeriodicTask.objects.update_or_create(
                name=task_name,
                defaults={
                    'crontab': schedule,
                    'task': 'services.email_service.send_scheduled_dashboard_email',
                    'args': json.dumps([dashboard_id, recipient_email]),
                    'enabled': True
                }
            )
            
            logger.info(f"Scheduled dashboard email: {task_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to schedule dashboard email: {e}")
            return False


@shared_task
def send_scheduled_dashboard_email(dashboard_id: str, recipient_email: str):
    """
    Celery task for sending scheduled dashboard emails
    """
    from dashboards.models import Dashboard
    
    try:
        dashboard = Dashboard.objects.get(id=dashboard_id)
        email_service = EmailService()
        
        # Generate dashboard HTML
        dashboard_items = list(dashboard.dashboard_items.all().values())
        dashboard_html = email_service.generate_dashboard_html(dashboard_items, dashboard.name)
        
        # Generate attachments
        attachments = []
        
        # HTML attachment
        attachments.append({
            'content': dashboard_html,
            'filename': dashboard.name.replace(' ', '_'),
            'type': 'html'
        })
        
        # Image attachment
        image_bytes = email_service.generate_dashboard_image(dashboard_html, dashboard.name)
        if image_bytes:
            attachments.append({
                'content': image_bytes,
                'filename': dashboard.name.replace(' ', '_'),
                'type': 'png'
            })
        
        # Send email
        subject = f"Scheduled Dashboard: {dashboard.name}"
        body = f"<p>Your scheduled dashboard <strong>{dashboard.name}</strong> is attached.</p>"
        
        success = email_service.send_dashboard_email(
            recipient_email=recipient_email,
            subject=subject,
            body=body,
            attachments=attachments,
            schedule_info={'is_scheduled_job': True}
        )
        
        logger.info(f"Scheduled email sent successfully: {success}")
        return success
        
    except Exception as e:
        logger.error(f"Failed to send scheduled dashboard email: {e}")
        return False


@shared_task
def send_dashboard_share_notification(dashboard_id: str, recipient_email: str, 
                                    sender_name: str):
    """
    Celery task for sending dashboard share notifications
    """
    try:
        from dashboards.models import Dashboard
        
        dashboard = Dashboard.objects.get(id=dashboard_id)
        email_service = EmailService()
        
        subject = f"{sender_name} shared a dashboard with you: {dashboard.name}"
        body = render_to_string('emails/dashboard_shared.html', {
            'dashboard_name': dashboard.name,
            'sender_name': sender_name,
            'dashboard_url': f"/dashboards/{dashboard_id}/",
        })
        
        success = email_service.send_dashboard_email(
            recipient_email=recipient_email,
            subject=subject,
            body=body
        )
        
        logger.info(f"Share notification sent: {success}")
        return success
        
    except Exception as e:
        logger.error(f"Failed to send dashboard share notification: {e}")
        return False 