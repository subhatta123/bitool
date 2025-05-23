import streamlit as st
from llm_config_ui import show_llm_config_ui # Assuming llm_config_ui.py is in the root directory

# Initialize session state for llm_config if not already present
# This is crucial if the user navigates directly to this page
if 'llm_config' not in st.session_state:
    st.session_state.llm_config = {}

st.set_page_config(layout="centered", page_title="LLM Settings")

st.title("🧠 LLM Provider Configuration")
st.caption("Choose and configure your Large Language Model provider for the application.")

show_llm_config_ui()

st.markdown("---")
st.subheader("Current Session LLM Configuration (for reference):")
if st.session_state.get('llm_config') and any(st.session_state.llm_config.values()):
    # For display, mask the API key
    display_config = st.session_state.llm_config.copy()
    if display_config.get("api_key"):
        key = display_config["api_key"]
        display_config["api_key"] = "********" + key[-4:] if len(key) > 8 else "********"
    st.json(display_config)
else:
    st.info(
        "No custom LLM configuration has been saved in the current session. "
        "The application may attempt to use settings from Streamlit secrets or environment variables if available and implemented."
    ) 