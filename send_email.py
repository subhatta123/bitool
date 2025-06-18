import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import streamlit as st
import datetime
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
import pdfkit
import os
from html2image import Html2Image

PDFKIT_AVAILABLE = True
PDFKIT_IMPORT_ERROR = ""
try:
    import pdfkit
except ImportError as e:
    PDFKIT_AVAILABLE = False
    PDFKIT_IMPORT_ERROR = str(e)
except OSError as e:
    PDFKIT_AVAILABLE = False
    PDFKIT_IMPORT_ERROR = str(e) + "\nEnsure wkhtmltopdf is installed and in your system PATH."

WEASYPRINT_AVAILABLE = False
WEASYPRINT_IMPORT_ERROR = "WeasyPrint has been disabled to resolve library loading issues."
HTML_CLASS = None

_scheduler_instance = None

if 'scheduler' not in st.session_state:
    st.session_state.scheduler = BackgroundScheduler(daemon=True)
    _scheduler_instance = st.session_state.scheduler
    st.session_state.scheduler.start()
    def shutdown_scheduler():
        global _scheduler_instance
        if _scheduler_instance and _scheduler_instance.running:
            _scheduler_instance.shutdown()
    atexit.register(shutdown_scheduler)
elif not st.session_state.scheduler.running:
    try:
        st.session_state.scheduler.start(paused=False)
        _scheduler_instance = st.session_state.scheduler
    except Exception as e:
        st.session_state.scheduler = BackgroundScheduler(daemon=True)
        _scheduler_instance = st.session_state.scheduler
        st.session_state.scheduler.start()

scheduler = st.session_state.scheduler

def get_email_config():
    default_config = {
        "SMTP_SERVER": None, "SMTP_PORT": 587, "SMTP_USERNAME": None,
        "SMTP_PASSWORD": None, "SENDER_EMAIL": None,
        "USE_TLS": True, "USE_SSL": False
    }
    try:
        import database
        if hasattr(st.session_state, 'db_configured_successfully') and st.session_state.db_configured_successfully:
            conn = database.get_db_connection()
            if conn:
                try:
                    cursor = conn.cursor()
                    cursor.execute("SELECT smtp_server, smtp_port, smtp_username, smtp_password, use_tls, from_email FROM email_settings WHERE is_active = TRUE ORDER BY id DESC LIMIT 1")
                    settings = cursor.fetchone()
                    if settings and settings[0]:
                        return {
                            "SMTP_SERVER": settings[0], "SMTP_PORT": int(settings[1] or 587),
                            "SMTP_USERNAME": settings[2], "SMTP_PASSWORD": settings[3],
                            "SENDER_EMAIL": settings[5], "USE_TLS": settings[4] if settings[4] is not None else True,
                            "USE_SSL": False
                        }
                finally:
                    conn.close()
    except Exception:
        pass

    secrets_smtp_server = st.secrets.get('SMTP_SERVER')
    if secrets_smtp_server:
        return {
            "SMTP_SERVER": secrets_smtp_server,
            "SMTP_PORT": st.secrets.get('SMTP_PORT', 587),
            "SMTP_USERNAME": st.secrets.get('SMTP_USERNAME'),
            "SMTP_PASSWORD": st.secrets.get("SMTP_PASSWORD"),
            "SENDER_EMAIL": st.secrets.get("SENDER_EMAIL"),
            "USE_TLS": st.secrets.get("USE_TLS", True),
            "USE_SSL": st.secrets.get("USE_SSL", False)
        }
    return default_config

def send_dashboard_email(recipient_email, subject, body, attachments, schedule_info=None, resolved_email_cfg=None):
    """
    Sends an email with multiple attachments.
    attachments should be a list of dicts: [{'content': ..., 'filename': ..., 'type': ...}]
    """
    email_cfg = resolved_email_cfg if resolved_email_cfg else get_email_config()
    
    smtp_server_host = email_cfg.get("SMTP_SERVER")
    smtp_user = email_cfg.get("SMTP_USERNAME") or ""
    smtp_pass = email_cfg.get("SMTP_PASSWORD") or ""
    sender = email_cfg.get("SENDER_EMAIL")

    if not smtp_user or not smtp_pass or not sender or not smtp_server_host:
        err_msg = "Email server credentials are not configured."
        if schedule_info and schedule_info.get('is_scheduled_job'):
            print(f"ERROR for {recipient_email}: {err_msg}")
        else:
            st.error(err_msg)
        return False

    msg = MIMEMultipart('mixed')
    msg['From'] = sender
    msg['To'] = recipient_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'html'))

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
                part.add_header('Content-Disposition', f'attachment; filename="{attachment_filename}.{attachment_type}"')
                msg.attach(part)
        
    try:
        port = int(email_cfg.get("SMTP_PORT", 587))
        server = smtplib.SMTP(smtp_server_host, port)
        if email_cfg.get('USE_TLS'):
            server.starttls()
        server.login(smtp_user, smtp_pass)
        server.sendmail(sender, recipient_email, msg.as_string())
        server.quit()
        if not (schedule_info and schedule_info.get('is_scheduled_job')):
            st.toast(f"Dashboard email sent to {recipient_email}!", icon="📧")
        return True
    except Exception as e:
        err_msg = f"An unexpected error occurred while sending email: {e}"
        if schedule_info and schedule_info.get('is_scheduled_job'):
            print(err_msg)
        else:
            st.error(err_msg)
        return False

def show_send_email_ui(dashboard_html_content=None, dashboard_name=None, all_dashboards=None):
    st.subheader("Send Dashboard via Email")
    
    if all_dashboards is None:
        all_dashboards = []

    selected_dashboard_name = dashboard_name
    selected_dashboard_html = dashboard_html_content
    
    if len(all_dashboards) > 1:
        dashboard_options = [dash[0] for dash in all_dashboards]
        default_index = dashboard_options.index(dashboard_name) if dashboard_name in dashboard_options else 0
        selected_dashboard_name = st.selectbox("Choose dashboard:", dashboard_options, index=default_index, key="email_dashboard_select")
        
        if selected_dashboard_name != dashboard_name:
            for dash_name_iter, _, _ in all_dashboards:
                if dash_name_iter == selected_dashboard_name:
                    try:
                        import app
                        items = app.load_user_dashboard(st.session_state.logged_in_user, selected_dashboard_name)
                        selected_dashboard_html = app.generate_dashboard_html(items)
                    except Exception as e:
                        st.error(f"Error loading dashboard '{selected_dashboard_name}': {e}")
                        selected_dashboard_html = "Could not load dashboard content."
                    break
    elif dashboard_name:
        st.info(f"📊 Sharing dashboard: **{dashboard_name}**")
    else:
        st.warning("No dashboard to share.")
        return

    recipient = st.text_input("Recipient Email:", key="email_recipient_input") 
    subject = st.text_input("Subject:", value=f"Your Dashboard: {selected_dashboard_name}", key="email_subject_input")
    
    include_html = st.checkbox("Include HTML attachment", value=True, key="include_html")
    include_image = st.checkbox("Include Image attachment", value=True, key="include_image_v6",
                                 help="Static PNG image of the dashboard")

    body_template = "<p>Hi,</p><p>Please find your dashboard attached.</p>"
    body_html_content = st.text_area("Email Body:", value=body_template, height=150, key="email_body_content")

    if st.button("Send Email", key="send_email_final_button"):
        if not recipient:
            st.error("Recipient email is required.")
            return

        if not include_html and not include_image:
            st.warning("Please select at least one attachment format.")
            return

        safe_filename = "".join(c for c in (selected_dashboard_name or "dashboard") if c.isalnum() or c in (' ', '_')).rstrip()
        
        attachments = []
        if include_image:
            with st.spinner("Generating Image..."):
                image_bytes = generate_dashboard_image(selected_dashboard_html, safe_filename)
            
            if image_bytes:
                attachments.append({
                    'content': image_bytes,
                    'filename': safe_filename,
                    'type': 'png'
                })
            else:
                st.error("Failed to generate image attachment.")

        if include_html:
            attachments.append({
                'content': selected_dashboard_html,
                'filename': safe_filename,
                'type': 'html'
            })
        
        if not attachments:
            st.error("Failed to prepare any attachments. Email not sent.")
            return

        send_dashboard_email(recipient, subject, body_html_content, attachments)

def generate_dashboard_image(dashboard_html_content, dashboard_name="dashboard"):
    """
    Generate PNG image from dashboard HTML content using html2image.
    Returns image bytes if successful, None if failed.
    """
    if not dashboard_html_content:
        print("[IMAGE] No HTML content provided for image generation")
        return None
    
    try:
        hti = Html2Image(output_path='temp_images')
        
        # Create a temporary HTML file to render
        temp_html_path = 'temp_dashboard.html'
        with open(temp_html_path, 'w', encoding='utf-8') as f:
            f.write(dashboard_html_content)

        # Generate the image
        output_filename = f"{dashboard_name}.png"
        hti.screenshot(html_file=temp_html_path, save_as=output_filename, size=(1200, 900))
        
        image_path = os.path.join('temp_images', output_filename)
        
        if os.path.exists(image_path):
            with open(image_path, 'rb') as f:
                image_bytes = f.read()
            
            # Clean up generated files
            os.remove(temp_html_path)
            os.remove(image_path)
            
            print(f"[IMAGE] Successfully generated image: {image_path}")
            return image_bytes
        else:
            print(f"[IMAGE] Image generation failed: file not found at {image_path}")
            return None

    except Exception as e:
        print(f"[IMAGE] html2image generation failed: {e}")
        st.error(f"Image generation failed: {e}")
        return None