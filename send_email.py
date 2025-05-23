import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import streamlit as st # For error/success messages
import datetime # For time/date inputs
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import atexit
# from pytz import utc # APScheduler uses system timezone by default if not specified or if pytz not used. For simplicity, we'll rely on system time for now.

# Try to import pdfkit, but don't make it a hard requirement for the module to load
PDFKIT_AVAILABLE = False
PDFKIT_IMPORT_ERROR = ""
try:
    import pdfkit
    PDFKIT_AVAILABLE = True
except ImportError as e_pdfkit:
    PDFKIT_IMPORT_ERROR = str(e_pdfkit)
    print(f"[send_email.py] pdfkit library not found. PDF generation will not be available. Error: {e_pdfkit}")
except OSError as e_wkhtml:
    PDFKIT_IMPORT_ERROR = str(e_wkhtml) + "\nEnsure wkhtmltopdf is installed and in your system PATH." 
    print(f"[send_email.py] Error importing pdfkit, likely due to wkhtmltopdf not found. Error: {e_wkhtml}")

# --- Initialize Scheduler ---
_scheduler_instance = None # Global-like reference for atexit

if 'scheduler' not in st.session_state:
    st.session_state.scheduler = BackgroundScheduler(daemon=True)
    _scheduler_instance = st.session_state.scheduler # Keep a direct reference
    st.session_state.scheduler.start()
    
    # Use the direct reference for atexit
    def shutdown_scheduler():
        global _scheduler_instance
        if _scheduler_instance and _scheduler_instance.running:
            _scheduler_instance.shutdown()
            print("APScheduler shutdown successfully via atexit.")
        elif _scheduler_instance:
            print("APScheduler was not running at exit or already shutdown.")
        else:
            print("APScheduler instance not found for shutdown at exit.")
    atexit.register(shutdown_scheduler)
    print("APScheduler started and registered for shutdown.")
elif not st.session_state.scheduler.running:
    # If scheduler is in session_state but not running (e.g. after a script rerun where it was shutdown),
    # re-start it. This might happen if atexit didn't run properly on a previous fast exit.
    try:
        st.session_state.scheduler.start(paused=False)
        print("APScheduler re-started.")
        # Re-assign _scheduler_instance if it was lost somehow (though it shouldn't be if scheduler is in session_state)
        _scheduler_instance = st.session_state.scheduler
        # Re-register atexit if it was somehow cleared or not effective (defensive)
        # Note: atexit usually calls functions only once. Re-registering might not be harmful but also might not be strictly needed
        # if the initial registration holds. Let's keep it simple and assume initial registration is fine unless issues persist.
    except Exception as e:
        print(f"Error re-starting APScheduler: {e}")
        # Fallback: create a new one if restart fails badly
        st.session_state.scheduler = BackgroundScheduler(daemon=True)
        _scheduler_instance = st.session_state.scheduler
        st.session_state.scheduler.start()
        print("APScheduler re-initialized and started after error.")

scheduler = st.session_state.scheduler # Main scheduler reference for the module

# --- Configuration Priority ---
# 1. User-defined session configuration (from email_config_ui.py)
# 2. Streamlit secrets (secrets.toml)
# 3. Environment variables (less common for Streamlit Cloud but good practice)

def get_email_config():
    """
    Retrieves email configuration, prioritizing session state,
    then Streamlit secrets, then environment variables.
    """
    session_cfg = st.session_state.get('email_config', {})
    if session_cfg.get('smtp_server') and session_cfg.get('smtp_username') and session_cfg.get('sender_email'):
        # Basic check to see if session config seems populated enough
        st.toast("Using email configuration from current session.", icon="⚙️")
        return {
            "SMTP_SERVER": session_cfg.get('smtp_server'),
            "SMTP_PORT": int(session_cfg.get('smtp_port', 587)), # Ensure port is int
            "SMTP_USERNAME": session_cfg.get('smtp_username'),
            "SMTP_PASSWORD": session_cfg.get('smtp_password'),
            "SENDER_EMAIL": session_cfg.get('sender_email')
        }

    # Fallback to Streamlit secrets if session config is not sufficiently populated
    secrets_smtp_server = st.secrets.get('SMTP_SERVER')
    secrets_smtp_username = st.secrets.get('SMTP_USERNAME')
    secrets_sender_email = st.secrets.get('SENDER_EMAIL')

    if secrets_smtp_server and secrets_smtp_username and secrets_sender_email:
        st.toast("Using email configuration from Streamlit secrets.", icon="🔒")
        return {
            "SMTP_SERVER": secrets_smtp_server,
            "SMTP_PORT": st.secrets.get('SMTP_PORT', 587), # Default if not in secrets
            "SMTP_USERNAME": secrets_smtp_username,
            "SMTP_PASSWORD": st.secrets.get("SMTP_PASSWORD", ""),
            "SENDER_EMAIL": secrets_sender_email
        }
    
    st.toast("Email configuration not fully found in session or secrets. Attempting to send might fail.", icon="⚠️")
    # Return defaults or empty if nothing is found, leading to error in send_dashboard_email
    return {
        "SMTP_SERVER": None,
        "SMTP_PORT": 587,
        "SMTP_USERNAME": None,
        "SMTP_PASSWORD": None,
        "SENDER_EMAIL": None
    }

# --- Configuration (Consider moving to a config file or environment variables for production) ---
# SMTP_SERVER = st.secrets.get('SMTP_SERVER', 'smtp.gmail.com')
# SMTP_PORT = st.secrets.get('SMTP_PORT', 587)
# # IMPORTANT: Use App Passwords if using Gmail and 2FA is enabled.
# # Store credentials securely, e.g., using Streamlit secrets or environment variables.
# SMTP_USERNAME = st.secrets.get("SMTP_USERNAME", "") 
# SMTP_PASSWORD = st.secrets.get("SMTP_PASSWORD", "")
# SENDER_EMAIL = st.secrets.get("SENDER_EMAIL", "")

def send_dashboard_email(recipient_email, subject, body, attachment_content, attachment_filename, attachment_type='html', schedule_info=None, resolved_email_cfg=None):
    """Sends an email with the dashboard as an attachment.

    Args:
        recipient_email (str): The email address of the recipient.
        subject (str): The subject of the email.
        body (str): The HTML body content of the email.
        attachment_content (str or bytes): The content of the attachment (e.g., HTML string or PDF bytes).
        attachment_filename (str): The name of the attachment file (e.g., "dashboard.html" or "dashboard.pdf").
        attachment_type (str): 'html' or 'pdf'.
        schedule_info (dict, optional): Information about the schedule if provided.
        resolved_email_cfg (dict, optional): Pre-resolved email configuration to bypass get_email_config().

    Returns:
        bool: True if email was sent successfully, False otherwise.
    """
    email_cfg = resolved_email_cfg if resolved_email_cfg else get_email_config()
    
    smtp_server_host = email_cfg.get("SMTP_SERVER")
    smtp_server_port = email_cfg.get("SMTP_PORT")
    smtp_user = email_cfg.get("SMTP_USERNAME")
    smtp_pass = email_cfg.get("SMTP_PASSWORD")
    sender = email_cfg.get("SENDER_EMAIL")

    if not smtp_user or not smtp_pass or not sender or not smtp_server_host:
        # If called by scheduler, this error should print to console.
        # If called directly, it will use st.error (though get_email_config would have already shown a toast).
        err_msg = "Email server credentials are not configured or are incomplete."
        if schedule_info and schedule_info.get('is_scheduled_job', False):
            print(f"ERROR for {recipient_email}: {err_msg} Check SMTP_SERVER, USERNAME, PASSWORD, SENDER_EMAIL.")
        else:
            st.error(err_msg + " Please set them in the Email Configuration section, or via Streamlit secrets / environment variables.")
        return False

    msg = MIMEMultipart('alternative')
    msg['From'] = sender
    msg['To'] = recipient_email
    msg['Subject'] = subject

    # Augment body if schedule_info is present (for now, just to show it's captured)
    full_body = body
    if schedule_info:
        schedule_str = f"<p><small><i>This email was requested with the following schedule: {schedule_info.get('frequency')}" 
        if schedule_info.get('time'): schedule_str += f" at {schedule_info.get('time')}"
        if schedule_info.get('day_of_week'): schedule_str += f" on {schedule_info.get('day_of_week')}"
        if schedule_info.get('minute_of_hour') is not None: schedule_str += f" at minute {schedule_info.get('minute_of_hour')}"
        schedule_str += ".</i></small></p>"
        full_body = body + schedule_str

    msg.attach(MIMEText(full_body, 'html'))

    # Prepare the attachment
    if attachment_content and attachment_filename:
        if attachment_type == 'html':
            part = MIMEBase('application', "octet-stream")
            part.set_payload(attachment_content.encode('utf-8'))
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename="{attachment_filename}.html"')
            msg.attach(part)
        elif attachment_type == 'pdf':
            part = MIMEBase('application', "pdf")
            part.set_payload(attachment_content)
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename="{attachment_filename}.pdf"')
            msg.attach(part)
        else:
            # If called by scheduler, this error should print to console.
            # If called from UI, this should be handled gracefully there.
            print(f"Error: Unknown attachment type '{attachment_type}' requested for email to {recipient_email}.")
            st.error(f"Error: Unknown attachment type '{attachment_type}'. Could not attach file.")
            # Optionally log this with more detail using the logging module
            log_email_event(f"Failed to send email to {recipient_email}: Unknown attachment type '{attachment_type}'")
            return False # Indicate failure

    try:
        server = smtplib.SMTP(smtp_server_host, smtp_server_port)
        server.starttls() # Secure the connection
        server.login(smtp_user, smtp_pass)
        server.sendmail(sender, recipient_email, msg.as_string())
        server.quit()
        
        is_job = schedule_info.get('is_scheduled_job', False) if schedule_info else False

        if is_job:
            print(f"SUCCESS: Scheduled dashboard email sent to {recipient_email} as per {schedule_info.get('frequency')} schedule.")
        else:
            toast_message = f"Dashboard email sent to {recipient_email}!"
            # The original logic for adjusting toast_message if schedule_info was present but not 'Send Now'
            # might need re-evaluation if it was meant for non-scheduled immediate sends with schedule info.
            # For now, simplifying: direct sends show direct message, scheduled sends print to console.
            if schedule_info and schedule_info.get('frequency') != 'Send Now' and not is_job:
                 toast_message = f"Email for {recipient_email} (intended schedule: {schedule_info.get('frequency')}) sent now."
            st.toast(toast_message, icon="📧")
        return True
    except smtplib.SMTPAuthenticationError as e:
        err_msg = f"SMTP Authentication Error: Could not log in. Check credentials/settings. Details: {e}"
        if schedule_info and schedule_info.get('is_scheduled_job', False):
            print(f"ERROR sending scheduled email to {recipient_email}: {err_msg}")
        else:
            st.error(err_msg)
        return False
    except smtplib.SMTPConnectError as e:
        err_msg = f"SMTP Connect Error: Could not connect to {smtp_server_host}:{smtp_server_port}. Details: {e}"
        if schedule_info and schedule_info.get('is_scheduled_job', False):
            print(f"ERROR sending scheduled email to {recipient_email}: {err_msg}")
        else:
            st.error(err_msg)
        return False
    except smtplib.SMTPServerDisconnected as e:
        err_msg = f"SMTP Server Disconnected. Details: {e}"
        if schedule_info and schedule_info.get('is_scheduled_job', False):
            print(f"ERROR sending scheduled email to {recipient_email}: {err_msg}")
        else:
            st.error(err_msg)
        return False
    except smtplib.SMTPException as e:
        err_msg = f"SMTP Error. Details: {e}"
        if schedule_info and schedule_info.get('is_scheduled_job', False):
            print(f"ERROR sending scheduled email to {recipient_email}: {err_msg}")
        else:
            st.error(err_msg)
        return False
    except Exception as e:
        err_msg = f"An unexpected error occurred while sending the email. Details: {e}"
        if schedule_info and schedule_info.get('is_scheduled_job', False):
            print(f"ERROR sending scheduled email to {recipient_email}: {err_msg}")
        else:
            st.error(err_msg)
        return False

# --- UI for Sending Email (Can be called from app.py or used for testing) ---
def show_send_email_ui(dashboard_html_content_for_html_email, dashboard_name):
    """Displays UI elements for sending the email and calls send_dashboard_email.
    The attachment will always be HTML.
    
    Args:
        dashboard_html_content_for_html_email (str): The HTML content for direct HTML attachment.
        dashboard_name (str): Name of the dashboard for subject and filename.
    """
    st.subheader("Send Dashboard via Email")
    recipient = st.text_input("Recipient Email:", key="email_recipient_input_v3") 
    subject = st.text_input("Subject:", value=f"Your Dashboard: {dashboard_name}", key="email_subject_input_v3")
    
    # PDF option and radio button removed. Attachment is always HTML.
    # st.markdown("Attachment Type: HTML") # Optionally inform the user explicitly

    body_template = f"""\\
    <html>
        <body>
            <p>Please find your dashboard, '{dashboard_name}', attached.</p>
            <p>This email was generated by the DB Chat application.</p>
        </body>
    </html>
    """
    body_html_content = st.text_area("Email Body:", value=body_template, height=200, key="email_body_content_v3")

    # --- Scheduling Options ---
    st.markdown("##### Scheduling Options (Optional)")
    schedule_frequency = st.selectbox(
        "Frequency:",
        ["Send Now", "Hourly", "Daily", "Weekly"],
        index=0, # Default to "Send Now"
        key="email_schedule_freq_v3"
    )

    schedule_time_input = None
    schedule_day_of_week_input = None
    schedule_minute_of_hour_input = None

    if schedule_frequency == "Daily":
        schedule_time_input = st.time_input("Time to send daily:", datetime.time(9, 0), key="email_schedule_daily_time_v3")
    elif schedule_frequency == "Weekly":
        schedule_day_of_week_input = st.selectbox(
            "Day of the week:",
            ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
            key="email_schedule_weekly_day_v3"
        )
        schedule_time_input = st.time_input("Time to send weekly:", datetime.time(9, 0), key="email_schedule_weekly_time_v3")
    elif schedule_frequency == "Hourly":
        schedule_minute_of_hour_input = st.number_input("Minute of the hour to send (0-59):", min_value=0, max_value=59, value=0, step=1, key="email_schedule_hourly_minute_v3")

    # Retrieve current email config for display or info
    current_email_cfg = get_email_config()
    sender_display = current_email_cfg.get("SENDER_EMAIL", "Not Configured")
    smtp_display = current_email_cfg.get("SMTP_SERVER", "Not Configured")
    st.caption(f"Sending from: {sender_display} via {smtp_display}")
    if sender_display == "Not Configured" or smtp_display == "Not Configured":
        st.warning("Email sending is not fully configured. Please check Email Settings.", icon="⚙️")


    if st.button("Send Email", key="send_email_final_button_v3"):
        if not recipient:
            st.error("Recipient email cannot be empty.")
            return
        if not subject:
            st.error("Subject cannot be empty.")
            return
        if not body_html_content: #Though it has a default, user might clear it
            st.error("Email body cannot be empty.")
            return
        if sender_display == "Not Configured" or smtp_display == "Not Configured":
            st.error("Cannot send email. Email sending is not configured.")
            return

        attachment_content_to_send = None
        # Sanitize dashboard_name for use as a filename
        safe_dashboard_name = "".join(c if c.isalnum() or c in (' ', '_', '-') else '_' for c in dashboard_name)
        attachment_filename_base = safe_dashboard_name.replace(" ", "_").replace("/", "-")
        
        # Attachment type is now always HTML
        actual_attachment_type_for_send_func = 'html'
        attachment_content_to_send = dashboard_html_content_for_html_email

        # --- Prepare schedule_info dictionary ---
        schedule_info_for_send = {
            'frequency': schedule_frequency,
            'time': schedule_time_input.strftime('%H:%M') if schedule_time_input else None,
            'day_of_week': schedule_day_of_week_input,
            'minute_of_hour': schedule_minute_of_hour_input
            # 'is_scheduled_job' will be set by the scheduling function if used
        }
        # If "Send Now", clear specific time/day/minute details not relevant for immediate send
        if schedule_frequency == "Send Now":
            schedule_info_for_send['time'] = None
            schedule_info_for_send['day_of_week'] = None
            schedule_info_for_send['minute_of_hour'] = None

        # Ensure content is set (should be already by `attachment_content_to_send = dashboard_html_content_for_html_email`)
        if not attachment_content_to_send:
            st.error("Dashboard content is missing. Cannot send email.") # Should not happen
            return

        send_dashboard_email(
            recipient_email=recipient, 
            subject=subject, 
            body=body_html_content, 
            attachment_content=attachment_content_to_send,
            attachment_filename=attachment_filename_base, # Extension is added in send_dashboard_email
            attachment_type=actual_attachment_type_for_send_func,
            schedule_info=schedule_info_for_send,
            resolved_email_cfg=current_email_cfg # Pass the already fetched config
        )

if __name__ == '__main__':
    # --- Test Section (Only runs when send_email.py is executed directly) ---
    st.set_page_config(layout="wide")
    st.title("Test Email Sending Module")

    # Create dummy dashboard content for testing
    mock_dashboard_name = "My Test Dashboard"
    mock_dashboard_html = f"""\
    <!DOCTYPE html><html><head><title>{mock_dashboard_name}</title></head>
    <body><h1>{mock_dashboard_name}</h1><p>This is a test dashboard.</p></body></html>
    """

    st.markdown("### Test Email Sending Functionality")
    st.markdown("Configure your SMTP settings in `streamlit/secrets.toml`, as environment variables, or use the 'Configure Email SMTP Settings' section in the app.")
    st.markdown("Example `secrets.toml`:")
    st.code("""SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USERNAME = "your_email@gmail.com"
SMTP_PASSWORD = "your_gmail_app_password" # Use an App Password if 2FA is enabled
SENDER_EMAIL = "your_email@gmail.com""" , language="toml")
    
    st.warning("Ensure you have configured your email credentials in Streamlit secrets (e.g., `.streamlit/secrets.toml`) or as environment variables.")

    # Removed generate_pdf_callback from the test call as it's no longer a parameter
    show_send_email_ui(mock_dashboard_html, mock_dashboard_name)

    st.markdown("---")
    st.subheader("Direct function call test (for debugging - requires configured credentials):")
    test_recipient = st.text_input("Test Recipient Email:", key="direct_test_recipient")
    if st.button("Send Test Email Directly"):
        if test_recipient:
            email_cfg_test = get_email_config() # Use the getter for direct test too
            if email_cfg_test.get("SMTP_USERNAME") and email_cfg_test.get("SMTP_PASSWORD") and email_cfg_test.get("SENDER_EMAIL"):
                send_dashboard_email(
                    recipient_email=test_recipient,
                    subject="Direct Test Email from DBChat",
                    body="<p>This is a direct test email with an HTML attachment.</p>",
                    attachment_content=mock_dashboard_html,
                    attachment_filename="test_dashboard.html",
                    attachment_type='html',
                    resolved_email_cfg=email_cfg_test # Pass resolved config for direct test too
                )
            else:
                st.error("SMTP credentials not found via session or secrets. Cannot run direct test. Please configure them.")
        else:
            st.warning("Please enter a test recipient email.") 