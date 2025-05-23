import streamlit as st
from email_config_ui import show_email_config_ui # Assuming email_config_ui.py is in the root directory

# Initialize session state for email_config if not already present
# This is crucial if the user navigates directly to this page
if 'email_config' not in st.session_state:
    st.session_state.email_config = {}

st.set_page_config(layout="centered", page_title="Email Settings")

st.title("⚙️ Email SMTP Configuration")
st.caption("Configure the SMTP settings for sending emails from the application.")

show_email_config_ui()

st.markdown("---")
st.subheader("Current Session Configuration (for reference):")
if st.session_state.get('email_config') and any(st.session_state.email_config.values()): # Check if not empty
    st.json(st.session_state.email_config)
else:
    st.info("No custom email configuration has been saved in the current session. The application will attempt to use settings from Streamlit secrets or environment variables if available.") 