import streamlit as st

def show_llm_config_ui():
    """Displays UI elements for configuring LLM settings."""
    st.subheader("Configure Large Language Model (LLM) Provider")

    st.write(
        "Select your LLM provider and enter the required details. "
        "These settings will be used for the current session."
    )
    st.caption(
        "For persistent settings, consider using Streamlit secrets or environment variables, "
        "and adapt your application logic to prioritize session state, then secrets/env vars."
    )

    # Load existing session state values if they exist
    current_config = st.session_state.get('llm_config', {})
    default_provider = current_config.get('provider', "OpenAI")

    llm_provider = st.radio(
        "Select LLM Provider:",
        ("OpenAI", "Local LLM (OpenAI-Compatible API)"),
        index=["OpenAI", "Local LLM (OpenAI-Compatible API)"].index(default_provider),
        key="llm_provider_select",
        horizontal=True,
    )

    config_data = {"provider": llm_provider}
    api_key_display = current_config.get('api_key', '')
    base_url_display = current_config.get('base_url', '')
    
    # Mask API key if it's already set, for display purposes
    # if api_key_display:
    #     api_key_display = "********" + api_key_display[-4:] if len(api_key_display) > 8 else "********"


    if llm_provider == "OpenAI":
        st.markdown("##### OpenAI Configuration")
        openai_api_key = st.text_input(
            "OpenAI API Key:",
            value=current_config.get('api_key', '') if default_provider == "OpenAI" else '', # Clear if switching provider
            type="password",
            key="openai_api_key_input",
            help="Your secret API key for OpenAI services."
        )
        config_data["api_key"] = openai_api_key
        config_data["base_url"] = None # Ensure base_url is cleared for OpenAI

    elif llm_provider == "Local LLM (OpenAI-Compatible API)":
        st.markdown("##### Local LLM Configuration")
        local_base_url = st.text_input(
            "API Base URL:",
            value=current_config.get('base_url', 'http://localhost:11434/v1') if default_provider == "Local LLM (OpenAI-Compatible API)" else 'http://localhost:11434/v1',
            key="local_base_url_input",
            help="The base URL of your local LLM's OpenAI-compatible API (e.g., for Ollama, LM Studio)."
        )
        local_api_key = st.text_input(
            "API Key (optional):",
            value=current_config.get('api_key', '') if default_provider == "Local LLM (OpenAI-Compatible API)" else '', # Clear if switching provider
            type="password",
            key="local_api_key_input",
            help="API key if your local LLM requires one (leave blank if not needed)."
        )
        config_data["base_url"] = local_base_url
        config_data["api_key"] = local_api_key if local_api_key else None


    if st.button("Save/Apply LLM Configuration for this Session", key="save_llm_config_button"):
        valid_config = True
        if llm_provider == "OpenAI":
            if not config_data.get("api_key"):
                st.warning("OpenAI API Key cannot be empty when OpenAI is selected.")
                valid_config = False
            elif not config_data.get("api_key", "").startswith("sk-"):
                st.info("OpenAI API keys usually start with 'sk-'. Please double-check.")
                # Not a strict failure, just info

        elif llm_provider == "Local LLM (OpenAI-Compatible API)":
            if not config_data.get("base_url"):
                st.warning("Local LLM API Base URL cannot be empty when Local LLM is selected.")
                valid_config = False
            elif not (config_data.get("base_url","").startswith("http://") or config_data.get("base_url","").startswith("https://")):
                st.warning("Please enter a valid URL for the Local LLM API Base URL (e.g., http://localhost:11434/v1).")
                valid_config = False
        
        if valid_config:
            st.session_state['llm_config'] = config_data
            st.success(f"LLM configuration for '{llm_provider}' saved for the current session!")
            st.info("The application will now attempt to use these settings for LLM interactions.")
            # Force a rerun to update the displayed current_config if it's shown on the same page
            # st.experimental_rerun() 
        else:
            st.error("Configuration not saved. Please address the warnings.")


    st.markdown("---")
    st.caption(
        "Note: These settings are stored in the session and will be lost if you close the browser tab "
        "or restart the application. For persistent storage, integrate with Streamlit secrets or environment variables."
    )

if __name__ == '__main__':
    st.set_page_config(layout="centered", page_title="LLM Configuration Test")
    st.title("LLM Configuration Module Test")
    
    if 'llm_config' not in st.session_state:
        st.session_state['llm_config'] = {} # Initialize if not present

    show_llm_config_ui()

    st.subheader("Current Session LLM Configuration (for testing):")
    if st.session_state.get('llm_config'):
        # For display, mask the API key
        display_config = st.session_state.llm_config.copy()
        if display_config.get("api_key"):
            key = display_config["api_key"]
            display_config["api_key"] = "********" + key[-4:] if len(key) > 8 else "********"
        st.json(display_config)
    else:
        st.write("No LLM configuration saved in session yet.") 