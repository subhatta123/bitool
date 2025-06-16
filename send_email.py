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

def send_dashboard_email(recipient_email, subject, body, attachment_content, attachment_filename, attachment_type='html', schedule_info=None, resolved_email_cfg=None):
    email_cfg = resolved_email_cfg if resolved_email_cfg else get_email_config()
    
    smtp_server_host = email_cfg.get("SMTP_SERVER")
    smtp_user = email_cfg.get("SMTP_USERNAME")
    sender = email_cfg.get("SENDER_EMAIL")

    if not smtp_user or not email_cfg.get("SMTP_PASSWORD") or not sender or not smtp_server_host:
        err_msg = "Email server credentials are not configured."
        if schedule_info and schedule_info.get('is_scheduled_job'):
            print(f"ERROR for {recipient_email}: {err_msg}")
        else:
            st.error(err_msg)
        return False

    msg = MIMEMultipart('alternative')
    msg['From'] = sender
    msg['To'] = recipient_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'html'))

    if attachment_content and attachment_filename and attachment_type:
        part = MIMEBase('application', "octet-stream")
        if attachment_type == 'html':
            part.set_payload(attachment_content.encode('utf-8'))
        elif attachment_type == 'pdf':
            part.set_payload(attachment_content)
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{attachment_filename}.{attachment_type}"')
        msg.attach(part)
        
    try:
        port = int(email_cfg.get("SMTP_PORT", 587))
        server = smtplib.SMTP(smtp_server_host, port)
        if email_cfg.get('USE_TLS'):
            server.starttls()
        server.login(smtp_user, email_cfg.get("SMTP_PASSWORD"))
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
                    break
    elif dashboard_name:
        st.info(f"📊 Sharing dashboard: **{dashboard_name}**")
    else:
        st.warning("No dashboard to share.")
        return

    recipient = st.text_input("Recipient Email:", key="email_recipient_input") 
    subject = st.text_input("Subject:", value=f"Your Dashboard: {selected_dashboard_name}", key="email_subject_input")
    
    include_html = st.checkbox("Include HTML attachment", value=True, key="include_html")
    include_pdf = st.checkbox("Include PDF attachment", value=True, key="include_pdf", disabled=not PDFKIT_AVAILABLE, help=f"PDF generation unavailable: {PDFKIT_IMPORT_ERROR}" if not PDFKIT_AVAILABLE else "")

    body_template = "<p>Hi,</p><p>Please find your dashboard attached.</p>"
    body_html_content = st.text_area("Email Body:", value=body_template, height=150, key="email_body_content")

    if st.button("Send Email", key="send_email_final_button"):
        if not recipient:
            st.error("Recipient email is required.")
            return

        safe_filename = "".join(c for c in (selected_dashboard_name or "dashboard") if c.isalnum() or c in (' ', '_')).rstrip()
        
        if include_html:
            send_dashboard_email(recipient, subject, body_html_content, selected_dashboard_html, safe_filename, 'html')
        
        if include_pdf:
            if PDFKIT_AVAILABLE:
                with st.spinner("Generating PDF..."):
                    pdf_bytes = generate_dashboard_pdf(selected_dashboard_html, safe_filename)
                if pdf_bytes:
                    send_dashboard_email(recipient, subject, body_html_content, pdf_bytes, safe_filename, 'pdf')
                else:
                    st.error("Failed to generate PDF.")
            else:
                st.warning("PDF generation is not available.")

def generate_dashboard_pdf(html_content, dashboard_name="dashboard"):
    if not PDFKIT_AVAILABLE:
        return None
    try:
        options = {'page-size': 'A4', 'margin-top': '0.75in', 'margin-right': '0.75in', 'margin-bottom': '0.75in', 'margin-left': '0.75in', 'encoding': "UTF-8"}
        return pdfkit.from_string(html_content, False, options=options)
    except Exception as e:
        print(f"[PDF] pdfkit generation failed: {e}")
        return None