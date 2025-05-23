import streamlit as st

def show_email_config_ui():
    """Displays UI elements for configuring email settings."""
    st.subheader("Configure Email SMTP Settings")

    st.write("Enter your SMTP server details below. These settings will be used for the current session.")
    st.write("Alternatively, configure them via Streamlit secrets or environment variables for persistent settings.")

    # Load existing session state values if they exist, otherwise use placeholders or defaults
    current_config = st.session_state.get('email_config', {})

    smtp_server = st.text_input(
        "SMTP Server:",
        value=current_config.get('smtp_server', 'smtp.gmail.com'),
        key="config_smtp_server"
    )
    smtp_port = st.number_input(
        "SMTP Port:",
        min_value=1,
        max_value=65535,
        value=current_config.get('smtp_port', 587),
        key="config_smtp_port"
    )
    smtp_username = st.text_input(
        "SMTP Username (e.g., your_email@example.com):",
        value=current_config.get('smtp_username', ''),
        key="config_smtp_username"
    )
    smtp_password = st.text_input(
        "SMTP Password/App Password:",
        type="password",
        value=current_config.get('smtp_password', ''),
        key="config_smtp_password"
    )
    sender_email = st.text_input(
        "Sender Email Address (should match username or be authorized by the server):",
        value=current_config.get('sender_email', current_config.get('smtp_username', '')), # Default to username if available
        key="config_sender_email"
    )

    if st.button("Save/Apply Email Configuration for this Session", key="save_email_config_button"):
        if not smtp_server:
            st.warning("SMTP Server cannot be empty.")
        elif not smtp_username:
            st.warning("SMTP Username cannot be empty.")
        # Password can be empty for some non-authenticating servers, though rare.
        # Sender email is crucial.
        elif not sender_email:
            st.warning("Sender Email cannot be empty.")
        elif "@" not in smtp_username and smtp_username: # Simple check if username looks like an email
             st.info("The SMTP Username usually is an email address.")
        elif "@" not in sender_email or "." not in sender_email.split("@")[-1]:
            st.warning("Please enter a valid Sender Email address.")
        else:
            config_data = {
                "smtp_server": smtp_server,
                "smtp_port": int(smtp_port),
                "smtp_username": smtp_username,
                "smtp_password": smtp_password,
                "sender_email": sender_email,
            }
            st.session_state['email_config'] = config_data
            st.success("Email configuration saved for the current session!")
            st.info("The application will now attempt to use these settings for sending emails.")

    st.markdown("---")
    st.caption("Note: These settings are stored in the session and will be lost if you close the browser tab or restart the application. For persistent storage, use Streamlit secrets or environment variables.")

if __name__ == '__main__':
    st.set_page_config(layout="centered")
    st.title("Email Configuration Module Test")
    
    if 'email_config' not in st.session_state:
        st.session_state['email_config'] = {} # Initialize if not present

    show_email_config_ui()

    st.subheader("Current Session Configuration (for testing):")
    if st.session_state.get('email_config'):
        st.json(st.session_state['email_config'])
    else:
        st.write("No configuration saved in session yet.") 