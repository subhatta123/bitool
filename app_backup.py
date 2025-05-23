import streamlit as st
import pandas as pd
import pyodbc # Added for SQL Server
from sqlalchemy import create_engine # For potential future use or other DBs
import openai # Added for OpenAI integration
import json # To help parse LLM responses if they include JSON and for user data
from werkzeug.security import generate_password_hash, check_password_hash # For password hashing
import os # For checking if users.json exists
import sqlite3 # Added for direct SQLite usage with CSVs
import plotly.express as px # Added for advanced visualizations
import copy # For deep copying data for dashboard items
import base64 # Added for HTML download link
import time # Added for simulating delays if needed
from io import BytesIO # For handling byte streams, e.g., for PDF export
from openai import OpenAI # Assuming use of the OpenAI library

# Attempt to import Playwright for Image generation
PLAYWRIGHT_AVAILABLE_APP = False  # Force disable due to Windows asyncio NotImplementedError
PLAYWRIGHT_IMPORT_ERROR = "Disabled to avoid Windows asyncio NotImplementedError"
try:
    # Importing is attempted but flag remains False to prevent usage
    from playwright.sync_api import sync_playwright
    # We're not setting PLAYWRIGHT_AVAILABLE_APP to True to avoid the error
    print("[app.py] Playwright found but disabled to avoid Windows asyncio NotImplementedError")
except ImportError as e_pw:
    PLAYWRIGHT_IMPORT_ERROR = str(e_pw)
    # This warning will appear if playwright is not installed. Users should pip install playwright & playwright install
    st.sidebar.warning(f"Playwright library not found. Image generation for email disabled. Error: {e_pw}", icon="📷")
    print(f"[app.py] Playwright not found: {e_pw}. Run 'pip install playwright' and 'playwright install'.")

# --- Custom Modules ---
# from send_email import show_send_email_ui # Import the UI function - Commented out old way
import send_email # New way: Import the module
import database # Import the new database module

# --- Page Setup (MUST BE THE FIRST STREAMLIT COMMAND) ---
st.set_page_config(
    layout="wide", 
    page_title="DBChat - Query Your Data with AI",
    initial_sidebar_state="expanded", # Keep sidebar open by default
)

# --- Initialize Database (after page config) ---
# database.init_db() # Create tables if they don't exist - THIS WILL BE MOVED AND HANDLED DIFFERENTLY

# --- Custom CSS (after page config) ---
def load_custom_css():
    css = """
    <style>
        /* --- Global Styles --- */
        body {
            color: #333333; /* Dark gray text for better readability on light background */
        }

        /* --- Streamlit Specific Overrides --- */
        .stApp {
            background-color: #F0F2F6; /* Light grayish-blue background */
        }

        /* --- Login Page Styling --- */
        .login-container {
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            padding-top: 2rem; /* Add some space from the top */
        }
        .login-box {
            background-color: #FFFFFF;
            padding: 2rem 2.5rem 2.5rem 2.5rem; /* N S E W */
            border-radius: 0.5rem;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
            width: 100%;
            max-width: 420px; /* Control max width of the login box */
        }
        .login-box h1 {
            font-size: 1.8rem; /* Slightly smaller than default H1 for balance */
            text-align: center;
            color: #1F2937;
            margin-bottom: 1.5rem;
        }
        .login-box .stButton button {
            width: 100%; /* Make login button full width */
            margin-top: 1rem; /* Add space above the button */
        }
        .login-box .stTextInput input {
             height: 2.8rem; /* Make input fields a bit taller */
        }

        /* --- Sidebar Styling --- */
        div[data-testid="stSidebar"] {
            background-color: #FFFFFF; /* White sidebar */
            border-right: 1px solid #D1D5DB; /* Light gray border */
        }
        div[data-testid="stSidebar"] .stButton button {
            background-color: #7C3AED; /* Vibrant purple for buttons */
            color: #FFFFFF;
            border-radius: 0.375rem;
            transition: background-color 0.3s ease;
        }
        div[data-testid="stSidebar"] .stButton button:hover {
            background-color: #6D28D9; /* Darker purple on hover */
        }
        div[data-testid="stSidebar"] h2, 
        div[data-testid="stSidebar"] h3,
        div[data-testid="stSidebar"] label,
        div[data-testid="stSidebar"] .stMarkdown {
            color: #4B5563; /* Cool gray for sidebar text */
        }
         div[data-testid="stSidebar"] .stExpander header {
            font-size: 1.0rem;
            color: #A855F7; /* Lighter purple for expander headers */
            font-weight: 600;
        }
        div[data-testid="stSidebar"] .stExpander header span { 
            color: #A855F7 !important; 
        }


        /* --- Main Content Styling --- */
        h1, h2, h3, h4, h5, h6 {
            color: #1F2937; /* Very dark gray (almost black) for headers */
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        }
        
        .stButton button {
            background-color: #8B5CF6; /* Purple for primary actions */
            color: #FFFFFF;
            border: none;
            border-radius: 0.375rem;
            padding: 0.5rem 1rem;
            font-weight: 600;
            transition: background-color 0.3s ease;
        }
        .stButton button:hover {
            background-color: #7C3AED; /* Darker purple on hover */
        }
        .stButton button:disabled {
            background-color: #D1D5DB; /* Light gray for disabled */
            color: #6B7280; /* Medium gray text for disabled */
        }

        /* Styling for text inputs, selectbox, etc. */
        div[data-testid="stTextInput"] input, 
        div[data-testid="stSelectbox"] div[data-baseweb="select"] > div,
        div[data-testid="stTextArea"] textarea {
            background-color: #FFFFFF; /* White background for inputs */
            color: #1F2937; /* Dark text in inputs */
            border: 1px solid #D1D5DB; /* Light gray border */
            border-radius: 0.375rem;
        }
        div[data-testid="stTextInput"] label,
        div[data-testid="stSelectbox"] label,
        div[data-testid="stTextArea"] label {
            color: #374151; /* Dark gray for form labels */
        }


        /* Style for KPI Metrics */
        div[data-testid="stMetric"] {
            background-color: #FFFFFF; /* White card background */
            border: 1px solid #E5E7EB; /* Very light gray border */
            border-radius: 0.5rem;
            padding: 1.5rem;
            text-align: left;
            color: #1F2937; /* Dark text for KPIs */
            box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1), 0 1px 2px 0 rgba(0, 0, 0, 0.06); /* Subtle shadow */
        }
        div[data-testid="stMetricLabel"] {
            font-size: 0.9rem;
            color: #6B7280; /* Medium gray for label */
            margin-bottom: 0.3rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }
        div[data-testid="stMetricValue"] {
            font-size: 2.5rem;
            font-weight: 700;
            color: #7C3AED; /* Purple value */
            line-height: 1.1;
        }
        div[data-testid="stMetricDelta"] {
            font-size: 0.9rem;
            color: #10B981; /* Green for positive delta */
            /* Consider styles for negative delta: color: #EF4444; (Red) */
        }

        /* Styling for dashboard item containers (charts/tables below KPIs) */
        .stDataFrame, .stPlotlyChart {
            background-color: #FFFFFF; /* White background */
            padding: 1rem;
            border-radius: 0.5rem;
            border: 1px solid #E5E7EB; /* Very light gray border */
        }
        
        /* Expander styling */
        .stExpander header {
            font-size: 1.0rem;
            color: #7C3AED !important; /* Purple for expander headers */
            font-weight: 600;
        }
        .stExpander header span { 
            color: #7C3AED !important; 
        }
        .stExpander header svg { 
            fill: #7C3AED !important; /* Purple arrow */
        }
        .stExpander div[data-baseweb="card"]{
             background-color: #F9FAFB; /* Very light gray for expander content */
             border: 1px solid #E5E7EB; 
        }

        /* Table styling for st.dataframe */
        table {
            color: #374151; /* Dark gray text */
        }
        th {
            background-color: #F3F4F6; /* Light gray for table headers */
            color: #1F2937; /* Very dark gray for header text */
            font-weight: bold;
        }
        td, th {
            border: 1px solid #E5E7EB; /* Light gray borders */
        }

        /* --- Jazzy Accents --- */
        .stAlert { 
            border-radius: 0.375rem;
            border-left-width: 4px;
        }
        div[data-testid="stSuccess"] {
            background-color: #D1FAE5; 
            border-left-color: #10B981; 
            color: #065F46; 
        }
         div[data-testid="stSuccess"] svg { 
            fill: #065F46 !important; 
        }

        div[data-testid="stWarning"] {
            background-color: #FEF3C7;
            border-left-color: #F59E0B;
            color: #92400E;
        }
        div[data-testid="stWarning"] svg {
             fill: #92400E !important;
        }

        div[data-testid="stError"] {
            background-color: #FEE2E2;
            border-left-color: #EF4444;
            color: #991B1B;
        }
         div[data-testid="stError"] svg {
             fill: #991B1B !important;
        }

        div[data-testid="stInfo"] {
            background-color: #DBEAFE;
            border-left-color: #3B82F6;
            color: #1E40AF;
        }
        div[data-testid="stInfo"] svg {
            fill: #1E40AF !important;
        }


        /* --- Custom Scrollbars  --- */
        ::-webkit-scrollbar {
            width: 8px;
            height: 8px;
        }
        ::-webkit-scrollbar-track {
            background: #E5E7EB; /* Light gray track */
        }
        ::-webkit-scrollbar-thumb {
            background: #A78BFA; /* Lighter purple thumb */
            border-radius: 4px;
        }
        ::-webkit-scrollbar-thumb:hover {
            background: #8B5CF6; /* Purple thumb on hover */
        }

        /* Styling for in-place editable text inputs */
        .editable-title-input input[type="text"],
        .editable-kpi-label-input input[type="text"] {
            background-color: transparent !important;
            color: #1F2937 !important; /* Match header color */
            border: none !important;
            outline: none !important;
            box-shadow: none !important;
            padding-left: 0 !important; 
            font-size: 1.25rem; 
            font-weight: 600; 
            line-height: 1.5; 
            width: 100%;
        }
        .editable-kpi-label-input input[type="text"] {
            font-size: 0.9rem; 
            color: #6B7280 !important; /* Match KPI label color */
            text-transform: uppercase;
            letter-spacing: 0.05em;
            font-weight: normal; 
            margin-bottom: 0.3rem; 
        }

        .editable-title-input input[type="text"]:focus,
        .editable-kpi-label-input input[type="text"]:focus {
            background-color: #FFFFFF !important; 
            border: 1px solid #7C3AED !important; /* Purple border on focus */
            box-shadow: 0 0 0 2px rgba(124, 58, 237, 0.2) !important; /* Subtle purple glow */
        }

    </style>
    """
    st.markdown(css, unsafe_allow_html=True)

# --- Configuration ---
DEFAULT_ADMIN_USERNAME = "admin"
DEFAULT_ADMIN_PASSWORD = "admin123" # Change this in a real scenario!
MAX_DASHBOARD_VERSIONS = 3 # Maximum number of dashboard versions to keep per user

# --- Session State Initialization ---
if 'page' not in st.session_state:
    st.session_state.page = "login" # Default page is login
if 'logged_in_user' not in st.session_state:
    st.session_state.logged_in_user = None
if 'user_roles' not in st.session_state:
    st.session_state.user_roles = []
if 'db_configured_successfully' not in st.session_state: # New session state
    st.session_state.db_configured_successfully = False
if 'attempted_secrets_db_init' not in st.session_state: # New session state
    st.session_state.attempted_secrets_db_init = False

if 'connection_type' not in st.session_state:
    st.session_state.connection_type = None
if 'connected' not in st.session_state:
    st.session_state.connected = False
if 'data' not in st.session_state:
    st.session_state.data = None
if 'db_connection' not in st.session_state: # For actual DB connection objects
    st.session_state.db_connection = None
if 'db_engine' not in st.session_state: # For SQLAlchemy engine
    st.session_state.db_engine = None
if 'llm_api_key' not in st.session_state:
    st.session_state.llm_api_key = None
if 'data_schema' not in st.session_state:
    st.session_state.data_schema = None
if 'selected_table' not in st.session_state: # For focusing on a specific table
    st.session_state.selected_table = "All Tables / Auto-detect"

# Log session states
if 'log_data_schema_str' not in st.session_state:
    st.session_state.log_data_schema_str = None
if 'log_openai_prompt_str' not in st.session_state:
    st.session_state.log_openai_prompt_str = None
if 'log_generated_sql_str' not in st.session_state:
    st.session_state.log_generated_sql_str = None
if 'log_query_execution_details_str' not in st.session_state: # For results or errors
    st.session_state.log_query_execution_details_str = None
if 'results_df' not in st.session_state: # To store the results of the last query
    st.session_state.results_df = None
if 'dashboard_items' not in st.session_state: # To store dashboard charts
    st.session_state.dashboard_items = []
if 'current_dashboard_name' not in st.session_state: # To store the name of the currently active dashboard
    st.session_state.current_dashboard_name = None
if 'llm_client_instance' not in st.session_state: # New: To cache the LLM client
    st.session_state.llm_client_instance = None

# --- Initialize Session States ---
# Ensure these are at the very top, especially if other modules might try to access them on import.
if 'email_config' not in st.session_state:
    st.session_state.email_config = {}
if 'llm_config' not in st.session_state:
    st.session_state.llm_config = {}
if 'chat_history' not in st.session_state: # Example: for storing chat messages
    st.session_state.chat_history = []

# --- LLM Client Initialization ---
def get_llm_client():
    """
    Initializes and returns an LLM client based on configuration hierarchy:
    1. Database (app_llm_config table)
    2. Streamlit secrets (st.secrets.OPENAI_API_KEY or st.secrets.LOCAL_LLM_BASE_URL)
    Updates st.session_state.llm_api_key for compatibility if a key is found.
    Caches the client instance in st.session_state.llm_client_instance.
    Returns:
        An LLM client instance (e.g., OpenAI client) or None if configuration fails.
    """
    if st.session_state.llm_client_instance:
        return st.session_state.llm_client_instance

    client = None
    config_source_message = "LLM not configured."
    actual_api_key_to_store = None # To update st.session_state.llm_api_key

    # 1. Try to load from Database
    db_llm_config = database.load_llm_config()

    if db_llm_config:
        provider = db_llm_config.get('provider')
        api_key_from_db = db_llm_config.get('api_key') # Renamed for clarity
        base_url_from_db = db_llm_config.get('base_url') # Renamed for clarity
        # custom_model_name = db_llm_config.get('custom_model_name') # Placeholder for future use

        if provider == "OpenAI":
            if api_key_from_db:
                try:
                    client = OpenAI(api_key=api_key_from_db)
                    config_source_message = "LLM: OpenAI (from Database Settings)"
                    actual_api_key_to_store = api_key_from_db
                except Exception as e:
                    st.sidebar.error(f"Error with OpenAI (DB Settings): {str(e)}")
            else:
                st.sidebar.warning("OpenAI provider selected in DB, but API key is missing.")
        elif provider == "Local LLM (OpenAI-Compatible API)":
            if base_url_from_db:
                try:
                    # For local LLM, API key is optional. Pass None if not provided in DB.
                    client = OpenAI(base_url=base_url_from_db, api_key=api_key_from_db if api_key_from_db else None)
                    config_source_message = "LLM: Local (from Database Settings)"
                    if api_key_from_db: # Only store if an actual key was provided and potentially used
                        actual_api_key_to_store = api_key_from_db
                    # If only base_url, client is configured, but no specific API key to store for st.session_state.llm_api_key
                except Exception as e:
                    st.sidebar.error(f"Error with Local LLM (DB Settings): {str(e)}")
            else:
                st.sidebar.warning("Local LLM provider selected in DB, but Base URL is missing.")
        # Add other providers from DB here if necessary

    # 2. If no client from DB, try Streamlit secrets as a fallback
    if not client:
        openai_secret_key = st.secrets.get("OPENAI_API_KEY")
        local_llm_secret_base_url = st.secrets.get("LOCAL_LLM_BASE_URL")
        local_llm_secret_api_key = st.secrets.get("LOCAL_LLM_API_KEY")

        if openai_secret_key:
            try:
                client = OpenAI(api_key=openai_secret_key)
                config_source_message = "LLM: OpenAI (from Streamlit secrets)"
                actual_api_key_to_store = openai_secret_key
            except Exception as e:
                st.sidebar.error(f"Error with OpenAI (secrets): {str(e)}")
        elif local_llm_secret_base_url:
            try:
                client = OpenAI(base_url=local_llm_secret_base_url, api_key=local_llm_secret_api_key if local_llm_secret_api_key else None)
                config_source_message = "LLM: Local (from Streamlit secrets)"
                if local_llm_secret_api_key:
                    actual_api_key_to_store = local_llm_secret_api_key
            except Exception as e:
                st.sidebar.error(f"Error with Local LLM (secrets): {str(e)}")
    
    st.session_state.llm_client_instance = client
    st.session_state.llm_api_key = actual_api_key_to_store # This will be the actual key or None

    # Update sidebar status message (moved from original main() into here)
    # This message is now determined by the outcome of this function.
    if 'sidebar_llm_status_message' not in st.session_state:
        st.session_state.sidebar_llm_status_message = "LLM client: Not initialized."
    
    if client:
        st.session_state.sidebar_llm_status_message = config_source_message
    else:
        st.session_state.sidebar_llm_status_message = "LLM client: Not initialized. Please configure in LLM Settings."

    return client

llm_client = get_llm_client() # Initialize on script run

# --- Main Application UI ---
# Removed global st.title, st.info, chat history, and chat input from here.
# These will be part of show_main_chat_interface_content()

# --- Sidebar Information ---
st.sidebar.header("Status")
# Display the LLM status message that get_llm_client now sets
st.sidebar.caption(st.session_state.get('sidebar_llm_status_message', "LLM client: Not initialized."))



# Example of how you might show other modules (like send_email_ui)
# from send_email import show_send_email_ui # Assuming send_email.py is in root
# if st.checkbox("Show Email Sending UI (Test)"):
#     # This would need some dummy content or integration with your dashboard
#     show_send_email_ui("<html><body>Dummy Dashboard</body></html>", "Dummy Dashboard Name")

# --- User Data Management ---
def hash_password(password):
    """Hashes a password using Werkzeug."""
    return generate_password_hash(password)

def check_password(hashed_password, password):
    """Checks a password against a hashed version."""
    return check_password_hash(hashed_password, password)

# --- Image Generation Callback (New) ---
def generate_image_from_html(html_content):
    """Converts HTML content to PNG image bytes using Playwright or a fallback method."""
    if not PLAYWRIGHT_AVAILABLE_APP:
        st.warning(f"Playwright image generation is not available. Will attempt fallback method. Error: {PLAYWRIGHT_IMPORT_ERROR}")
        return generate_image_fallback(html_content)
    
    img_bytes = None
    try:
        with sync_playwright() as p:
            browser = None
            try:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                
                # Set a reasonable viewport size. This affects the screenshot dimensions.
                page.set_viewport_size({"width": 1200, "height": 900}) 
                
                page.set_content(html_content)
                
                # Wait for potential dynamic content or JS rendering
                page.wait_for_timeout(5000) # Wait 5 seconds
                
                img_bytes = page.screenshot(type='png', full_page=True)

            except NotImplementedError:
                # This specific exception happens on some Windows setups
                st.warning("Windows NotImplementedError encountered in Playwright. Using fallback method.")
                return generate_image_fallback(html_content)
            except Exception as e_img_gen:
                st.error(f"Error generating image with Playwright: {e_img_gen}")
                print(f"[App - Image Generation Error with Playwright] {e_img_gen}")
                return generate_image_fallback(html_content)
            finally:
                if browser:
                    browser.close()
    except Exception as outer_e:
        st.warning(f"Error setting up Playwright: {outer_e}. Using fallback method.")
        return generate_image_fallback(html_content)
        
    if img_bytes:
        return img_bytes
    else:
        st.warning("Playwright screenshot returned no data. Using fallback method.")
        return generate_image_fallback(html_content)

def generate_image_fallback(html_content):
    """Provides a fallback method for email attachment when Playwright fails.
    
    This function creates a simple HTML-only attachment instead of a PNG image.
    It effectively returns the HTML content directly, which will still work as an
    attachment in the email system.
    """
    st.info("Using HTML fallback instead of PNG image. The email will include an HTML attachment instead of an image.")
    
    # Return an empty image bytes - this will be detected in send_email.py and it will
    # fall back to sending HTML instead
    return None

# --- Application Defaults Initialization ---
def initialize_app_defaults():
    """Initializes application defaults, like ensuring the default admin user exists in the DB."""
    # Hash the default admin password once here
    default_admin_hashed_password = hash_password(DEFAULT_ADMIN_PASSWORD)
    default_admin_roles = ["admin", "superuser"]
    database.ensure_default_admin_user_in_db(DEFAULT_ADMIN_USERNAME, default_admin_hashed_password, default_admin_roles)
    # Future: Add other app default initializations here if needed

# --- Dashboard Save/Load Functions ---

def save_user_dashboard(username, dashboard_name, current_dashboard_items):
    """Saves the current dashboard state for the user to the database."""
    if not dashboard_name:
        st.error("Dashboard name cannot be empty when saving.")
        return

    # Fetch existing dashboard data to get current shared_with_users and older versions
    existing_dashboard_data = database.load_dashboard_from_db(username, dashboard_name)
    
    existing_versions_json = []
    shared_with_users_list = []

    if existing_dashboard_data and existing_dashboard_data['owner_username'] == username:
        # If dashboard exists and current user is the owner
        existing_versions_json = existing_dashboard_data.get("versions", [])
        shared_with_users_list = existing_dashboard_data.get("shared_with_users", [])
    elif existing_dashboard_data and existing_dashboard_data['owner_username'] != username:
        # This case should ideally be prevented by UI, but as a safeguard:
        st.error(f"User '{username}' is not the owner of dashboard '{dashboard_name}' and cannot save changes to its main versions.")
        return
    # If existing_dashboard_data is None, it's a new dashboard for this user, so lists remain empty.

    # Ensure existing_versions_json is a list (it should be from DB if exists)
    if not isinstance(existing_versions_json, list):
        st.warning(f"Versions for dashboard '{dashboard_name}' for user '{username}' has an unexpected format. Resetting its versions.")
        existing_versions_json = []

    serializable_new_version = []
    for item_original in current_dashboard_items:
        item = copy.deepcopy(item_original)
        if isinstance(item.get("data_snapshot"), pd.DataFrame):
            df_for_serialization = item["data_snapshot"]
            df_for_serialization.columns = [str(col_name) for col_name in df_for_serialization.columns]
            item["data_snapshot"] = df_for_serialization.to_dict(orient='records')
        serializable_new_version.append(item)
        
    updated_versions_json = [serializable_new_version] + existing_versions_json
    updated_versions_json = updated_versions_json[:MAX_DASHBOARD_VERSIONS]

    success = database.save_dashboard_to_db(username, dashboard_name, updated_versions_json, shared_with_users_list)
    # if success:
    #     st.toast(f"Dashboard '{dashboard_name}' saved!", icon="💾") # Toast can be annoying on auto-save

def load_user_dashboard(username, dashboard_name, version_index=0):
    """Loads a specific version of a named dashboard for the user from the database."""
    dashboard_data_from_db = database.load_dashboard_from_db(username, dashboard_name)

    if not dashboard_data_from_db:
        # st.warning(f"Dashboard '{dashboard_name}' not found or not accessible for user '{username}'.") # Already handled by get_dashboard_names
        return []

    versions_for_dashboard_json = dashboard_data_from_db.get("versions", [])
    if not versions_for_dashboard_json or not isinstance(versions_for_dashboard_json, list) or version_index < 0:
        return []

    if version_index >= len(versions_for_dashboard_json):
        st.warning(f"Version index {version_index} out of bounds for dashboard '{dashboard_name}'. Loading latest.")
        version_index = 0
        if not versions_for_dashboard_json: return []

    dashboard_to_load_serializable = versions_for_dashboard_json[version_index]
    
    loaded_dashboard_items = []
    if isinstance(dashboard_to_load_serializable, list):
        for item_serializable in dashboard_to_load_serializable:
            item = copy.deepcopy(item_serializable) 
            if "data_snapshot" in item and isinstance(item["data_snapshot"], list):
                try:
                    item["data_snapshot"] = pd.DataFrame.from_records(item["data_snapshot"])
                except Exception as e:
                    st.warning(f"Could not convert stored data snapshot back to DataFrame for item '{item.get('title', 'Untitled')}': {e}")
                    item["data_snapshot"] = pd.DataFrame() 
            loaded_dashboard_items.append(item)
    else:
        owner_of_dashboard = dashboard_data_from_db.get("owner_username", "Unknown owner")
        st.error(f"Dashboard version for '{dashboard_name}' (owned by {owner_of_dashboard}, version index {version_index}) is not in the expected list format. Cannot load.")
        return []
    
    return loaded_dashboard_items

def get_user_dashboard_names(username):
    """Returns a list of dashboard display names accessible to the given user (owned or shared) from DB.
       Returns a list of tuples: (raw_dashboard_name, display_name, owner_username)
    """
    return database.get_dashboard_names_for_user_from_db(username)


# --- OpenAI LLM Helper ---
def get_sql_from_openai(natural_language_query, data_schema, db_type="sqlserver", target_table=None):
    """Generates SQL query from natural language using the globally configured LLM client."""
    client = st.session_state.get("llm_client_instance")

    if not client:
        st.error("LLM client is not initialized. Please configure it in LLM Settings.")
        return None

    # Basic schema representation for the prompt
    schema_prompt_part = "Schema:\n"
    if isinstance(data_schema, dict): # For databases
        for table, columns in data_schema.items():
            schema_prompt_part += f"Table {table}:\n"
            for col_name, col_type in columns.items():
                schema_prompt_part += f"  - {col_name} ({col_type})\n"
    elif isinstance(data_schema, list): # For CSV (list of column names with types)
        schema_prompt_part += f"CSV Columns (query this as a table named 'csv_data'):\n"
        for col_info in data_schema:
            schema_prompt_part += f"  - {col_info['name']} ({col_info['type']})\n"
    else:
        schema_prompt_part = "Schema: Not available or in an unrecognized format."

    # Determine the SQL dialect hint
    sql_dialect = "SQL"
    if db_type == "sqlserver":
        sql_dialect = "Transact-SQL (T-SQL) for SQL Server"
    elif db_type == "postgresql":
        sql_dialect = "PostgreSQL SQL"
    elif db_type == "oracle":
        sql_dialect = "Oracle SQL (PL/SQL)"
    elif db_type == "csv":
        sql_dialect = "SQLite-compatible SQL (as if querying a table from a CSV)"

    focus_hint = ""
    if target_table and target_table != "All Tables / Auto-detect":
        focus_hint = f"Prioritize using the table named '{target_table}' if it is relevant to the user's question. However, you may use other tables or joins if the question clearly implies them or requires information from them. If the question is about the database schema itself (e.g., 'list all tables'), this focus hint can be ignored."

    prompt = f"""
    You are an expert AI assistant that converts natural language questions into {sql_dialect} queries.
    Given the following database schema and a user question, generate a syntactically correct {sql_dialect} query to answer the question.

    {schema_prompt_part}

    {focus_hint}

    User Question: {natural_language_query}

    Only return the SQL query, with no other explanatory text, preambles, or apologies.
    Ensure the query is directly executable.
    If the question cannot be answered with the given schema, or if it's ambiguous, try your best to formulate a query that might be relevant, or state that it's not possible within the SQL query itself as a comment (e.g. /* Cannot answer due to missing columns */ SELECT 1;).
    Do not use triple backticks in your response.
    SQL Query:
    """

    st.session_state.log_openai_prompt_str = prompt # Log the prompt

    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo", # Or "gpt-4" if you have access and prefer
            messages=[
                {"role": "system", "content": f"You are an expert AI assistant that converts natural language questions into {sql_dialect} queries. Only return the SQL query and nothing else."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2, # Lower temperature for more deterministic SQL
            max_tokens=300
        )
        sql_query = response.choices[0].message.content.strip()
        
        # Sometimes the model might still wrap the query in backticks or add "SQL Query:"
        if sql_query.lower().startswith("sql query:"):
            sql_query = sql_query[len("sql query:"):].strip()
        sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
        
        return sql_query
    except openai.APIConnectionError as e:
        st.error(f"OpenAI API Connection Error: {e}")
    except openai.RateLimitError as e:
        st.error(f"OpenAI API Rate Limit Exceeded: {e}")
    except openai.AuthenticationError as e:
        st.error(f"OpenAI API Authentication Error: Invalid API key or insufficient permissions. {e}")
    except openai.APIError as e:
        st.error(f"OpenAI API Error: {e}")
    except Exception as e:
        st.error(f"An unexpected error occurred while querying OpenAI: {e}")
    return None

# --- UI Functions ---

def show_connection_screen():
    """Displays the UI for selecting and configuring a data source."""
    st.header("Connect to Your Data Source")

    # LLM API Key Input has been MOVED to the main() function

    connection_options = ["Select Data Source", "CSV File", "PostgreSQL", "Oracle", "SQL Server"] # Added SQL Server
    selected_option = st.selectbox("Choose your data source type:", connection_options, index=0)

    if selected_option == "CSV File":
        st.session_state.connection_type = "csv"
        handle_csv_connection()
    elif selected_option == "PostgreSQL":
        st.session_state.connection_type = "postgresql"
        handle_db_connection("PostgreSQL")
    elif selected_option == "Oracle":
        st.session_state.connection_type = "oracle"
        handle_db_connection("Oracle")
    elif selected_option == "SQL Server":
        st.session_state.connection_type = "sqlserver"
        handle_db_connection("SQL Server")
    else:
        st.session_state.connection_type = None
        st.session_state.connected = False
        st.session_state.data = None


def handle_csv_connection():
    """Handles the UI and logic for connecting to a CSV file."""
    uploaded_file = st.file_uploader("Upload your CSV file", type=["csv"])
    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file)
            st.session_state.data = df
            st.session_state.connected = True
            st.success("Successfully connected to CSV and loaded data!")
            st.dataframe(df.head())

            # Infer and store schema for CSV
            csv_schema = []
            for col_name in df.columns:
                col_type = str(df[col_name].dtype)
                csv_schema.append({"name": col_name, "type": col_type})
            st.session_state.data_schema = csv_schema
            try: # Log the schema
                st.session_state.log_data_schema_str = json.dumps(st.session_state.data_schema, indent=2)
            except Exception as e:
                st.session_state.log_data_schema_str = f"Error formatting CSV schema for logs: {e}"

        except Exception as e:
            st.error(f"Error loading CSV file: {e}")
            st.session_state.connected = False
            st.session_state.data = None

def handle_db_connection(db_type):
    """Handles the UI for database connection parameters (actual connection logic to be added)."""
    st.subheader(f"Connect to {db_type}")

    if db_type == "SQL Server":
        driver_input = st.text_input(f"ODBC Driver for SQL Server", value="ODBC Driver 17 for SQL Server", key=f"{db_type.lower()}_driver", help="Ensure this ODBC driver is installed on your system and this name matches exactly.")
        host_input = st.text_input(f"{db_type} Server Name (e.g., localhost\\SQLEXPRESS or server.database.windows.net)", key=f"{db_type.lower()}_host")
        port_input = st.text_input(f"{db_type} Port (Leave blank if default or not applicable, e.g., Azure SQL)", key=f"{db_type.lower()}_port") # Port is often part of server name or handled by driver
        dbname_input = st.text_input(f"{db_type} Database Name", key=f"{db_type.lower()}_dbname")
        user_input = st.text_input(f"{db_type} User (Leave blank for Windows Authentication)", key=f"{db_type.lower()}_user")
        password_input = st.text_input(f"{db_type} Password", type="password", key=f"{db_type.lower()}_password")
        encrypt_input = st.selectbox("Encrypt Connection", options=["yes", "no", "optional"], index=0, key=f"{db_type.lower()}_encrypt", help="For Azure SQL Database, 'yes' is often required.")
        trust_cert_input = st.selectbox("Trust Server Certificate", options=["no", "yes"], index=0, key=f"{db_type.lower()}_trust_cert", help="Set to 'yes' if using a self-signed certificate or if encryption is enabled and you trust the server.")

    else:
        # Placeholder for other DB connection inputs
        host_input = st.text_input(f"{db_type} Host", key=f"{db_type.lower()}_host")
        port_input = st.text_input(f"{db_type} Port", key=f"{db_type.lower()}_port")
        dbname_input = st.text_input(f"{db_type} Database Name", key=f"{db_type.lower()}_dbname")
        user_input = st.text_input(f"{db_type} User", key=f"{db_type.lower()}_user")
        password_input = st.text_input(f"{db_type} Password", type="password", key=f"{db_type.lower()}_password")


    if st.button(f"Connect to {db_type}"):
        if db_type == "SQL Server":
            if not driver_input or not host_input or not dbname_input:
                st.error("Driver, Server Name, and Database Name are required for SQL Server connection.")
                return
            try:
                conn_str_parts = [
                    f"DRIVER={{{driver_input.strip()}}}",
                    f"SERVER={host_input.strip()}",
                    f"DATABASE={dbname_input.strip()}",
                    f"Encrypt={encrypt_input}",
                    f"TrustServerCertificate={trust_cert_input}"
                ]
                current_server_val = host_input.strip()
                if port_input.strip():
                     current_server_val = f"{host_input.strip()},{port_input.strip()}"
                conn_str_parts[1] = f"SERVER={current_server_val}"

                if user_input.strip():
                    conn_str_parts.append(f"UID={user_input.strip()}")
                    conn_str_parts.append(f"PWD={password_input}")
                else: # Windows Authentication (Trusted Connection)
                    conn_str_parts.append("Trusted_Connection=yes")

                conn_str = ";".join(conn_str_parts)
                st.info(f"Attempting to connect with: {conn_str.replace(password_input, '********') if password_input else conn_str}")

                cnxn = pyodbc.connect(conn_str, timeout=5) # Added timeout
                st.session_state.db_connection = cnxn
                st.session_state.connected = True
                st.session_state.data = None # Clear any previous CSV data

                # Fetch and store schema for SQL Server
                try:
                    cursor = cnxn.cursor()
                    db_schema = {}
                    # Get tables
                    tables_query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG = ?"
                    cursor.execute(tables_query, dbname_input.strip())
                    tables = [row[0] for row in cursor.fetchall()]
                    
                    for table_name in tables:
                        db_schema[table_name] = {}
                        # Get columns for each table
                        columns_query = f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND TABLE_CATALOG = ? ORDER BY ORDINAL_POSITION"
                        cursor.execute(columns_query, table_name, dbname_input.strip())
                        for row in cursor.fetchall():
                            db_schema[table_name][row[0]] = row[1]
                    st.session_state.data_schema = db_schema
                    try: # Log the schema
                        st.session_state.log_data_schema_str = json.dumps(st.session_state.data_schema, indent=2)
                    except Exception as e:
                        st.session_state.log_data_schema_str = f"Error formatting DB schema for logs: {e}"

                except pyodbc.Error as schema_ex:
                    st.warning(f"Could not fetch schema details: {schema_ex}")
                    st.session_state.data_schema = {"error": "Could not fetch schema"}
                    st.session_state.log_data_schema_str = f"DB Schema Fetch Error: {schema_ex}"
                except Exception as schema_e:
                    st.warning(f"An error occurred while fetching schema: {schema_e}")
                    st.session_state.data_schema = {"error": f"Could not fetch schema: {schema_e}"}
                    st.session_state.log_data_schema_str = f"DB Schema Fetch Error: {schema_e}"

                st.success(f"Successfully connected to {db_type}!")
                # We won't load data here, but we could fetch schema or table names
                # For example, fetch table names:
                # cursor = cnxn.cursor()
                # tables = [row.table_name for row in cursor.tables(tableType='TABLE')]
                # st.write("Available tables:", tables)

            except pyodbc.Error as ex:
                sqlstate = ex.args[0]
                st.error(f"Error connecting to SQL Server: {sqlstate} - {ex}")
                st.session_state.connected = False
                st.session_state.db_connection = None
            except Exception as e:
                st.error(f"An unexpected error occurred: {e}")
                st.session_state.connected = False
                st.session_state.db_connection = None

        else:
            # --- Placeholder for actual database connection logic for other DBs ---
            st.info(f"Connection logic for {db_type} is not yet implemented.")
            st.session_state.connected = False

def show_query_screen():
    """Displays the UI for asking questions and viewing results."""
    st.header("Ask Questions About Your Data")

    if st.session_state.data is not None and not st.session_state.data.empty:
        st.subheader("Data Preview (First 5 rows)")
        st.dataframe(st.session_state.data.head())
    elif st.session_state.connection_type != "csv":  # For DBs, we might not load all data initially
        st.info("Connected to database. Schema information would typically be shown here.")
        if st.session_state.data_schema and isinstance(st.session_state.data_schema, dict) and "error" not in st.session_state.data_schema:
            st.write("Available schema for LLM:")
            st.json(st.session_state.data_schema)

    # Table selection dropdown
    table_options = ["All Tables / Auto-detect"]
    if st.session_state.data_schema and isinstance(st.session_state.data_schema, dict):
        table_options.extend(list(st.session_state.data_schema.keys()))
    
    st.session_state.selected_table = st.selectbox(
        "Focus query on a specific table (optional):",
        options=table_options,
        index=table_options.index(st.session_state.selected_table) if st.session_state.selected_table in table_options else 0
    )

    natural_language_query = st.text_area("Ask your question in plain English:", height=100, key="nl_query")

    if st.button("Get Answer", disabled=(not st.session_state.llm_client_instance)):
        if not st.session_state.llm_client_instance: # Double check, though button should be disabled
            st.warning("LLM is not configured. Please visit LLM Settings.")
            return

        st.session_state.results_df = None # Clear previous results
        st.session_state.log_generated_sql_str = None
        st.session_state.log_query_execution_details_str = None

        if natural_language_query:
            with st.spinner("Generating SQL query and fetching results..."):
                # Removed direct API key check here as client instance is checked above
                
                generated_sql_query = get_sql_from_openai(
                    natural_language_query,
                    # st.session_state.llm_api_key, # REMOVED API KEY ARGUMENT
                    st.session_state.data_schema,
                    st.session_state.connection_type,
                    st.session_state.selected_table
                )

                if generated_sql_query:
                    st.subheader("Generated SQL Query by AI")
                    st.code(generated_sql_query, language="sql")
                    st.session_state.log_generated_sql_str = generated_sql_query
                else:
                    st.error("Could not generate SQL query from your question.")
                    st.session_state.log_generated_sql_str = "Error: Could not generate SQL query."
                    return

                try:
                    if st.session_state.connection_type == "csv" and st.session_state.data is not None:
                        raw_sqlite_conn = None
                        try:
                            raw_sqlite_conn = sqlite3.connect(':memory:')
                            st.session_state.data.to_sql('csv_data', raw_sqlite_conn, if_exists='replace', index=False)
                            results_df = pd.read_sql_query(generated_sql_query, raw_sqlite_conn)
                            st.session_state.log_query_execution_details_str = results_df.to_string() if not results_df.empty else "Query executed on CSV successfully, but no data was returned."
                        except sqlite3.Error as e_sqlite:
                            st.error(f"SQLite error during CSV query execution: {e_sqlite}")
                            results_df = pd.DataFrame({"error": [f"SQLite error: {e_sqlite}"]})
                            st.session_state.log_query_execution_details_str = f"SQLite Execution Error: {e_sqlite}"
                        except Exception as e_csv_sql:
                            st.error(f"Error executing SQL query on CSV: {e_csv_sql}")
                            results_df = pd.DataFrame({"error": [f"Error executing SQL on CSV: {e_csv_sql}"]})
                            st.session_state.log_query_execution_details_str = f"In-memory CSV SQL Execution Error: {e_csv_sql}"
                        finally:
                            if raw_sqlite_conn:
                                raw_sqlite_conn.close()
                    elif st.session_state.db_connection is not None and st.session_state.connection_type == 'sqlserver':
                        try:
                            cursor = st.session_state.db_connection.cursor()
                            cursor.execute(generated_sql_query)
                            rows = cursor.fetchall()
                            if rows:
                                columns = [column[0] for column in cursor.description]
                                results_df = pd.DataFrame.from_records(rows, columns=columns)
                            else:
                                results_df = pd.DataFrame({"message": ["Query executed successfully, but no data was returned."]})
                            st.session_state.log_query_execution_details_str = results_df.to_string() if not results_df.empty else "Query executed successfully, but no data was returned."
                        except pyodbc.Error as db_ex:
                            st.error(f"Database error during query execution: {db_ex}")
                            results_df = pd.DataFrame({"error": [f"Database error: {db_ex}"]})
                            st.session_state.log_query_execution_details_str = f"Database Error: {db_ex}"
                        except Exception as ex:
                            st.error(f"Error executing query on SQL Server: {ex}")
                            results_df = pd.DataFrame({"error": [f"Query execution error: {ex}"]})
                            st.session_state.log_query_execution_details_str = f"Query Execution Error: {ex}"
                    else:
                        results_df = pd.DataFrame({"error": ["Not connected to a data source or connection type not supported for querying yet."]})
                        st.session_state.log_query_execution_details_str = "Error: Not connected or connection type not supported for querying."
                    
                    st.session_state.results_df = results_df

                except Exception as e:
                    st.error(f"Error executing query or processing results: {e}")
                    st.session_state.results_df = pd.DataFrame({"error": [f"Unhandled error: {e}"]})
                    st.session_state.log_query_execution_details_str = f"Unhandled Query/Processing Error: {e}"
        else:
            st.warning("Please enter your question.")
            st.session_state.results_df = None

    if st.session_state.results_df is not None:
        results_df_to_display = st.session_state.results_df
        st.subheader("Query Results")
        st.dataframe(results_df_to_display)

        if not results_df_to_display.empty and not ("error" in results_df_to_display.columns or "message" in results_df_to_display.columns):
            st.subheader("Visualizations")
            if not results_df_to_display.empty: # Now the primary condition if results are valid
                st.markdown("#### Interactive Chart Options")
                all_columns = results_df_to_display.columns.tolist()
                numeric_columns = results_df_to_display.select_dtypes(include=['number']).columns.tolist()
                categorical_columns = results_df_to_display.select_dtypes(include=['object', 'category', 'string', 'boolean']).columns.tolist()
                geo_columns = detect_geographic_columns(results_df_to_display)
                has_geo_data = bool(geo_columns)

                chart_type_options = ["Bar Chart", "Line Chart", "Scatter Plot", "Pie Chart", "Histogram", "Table", "KPI"]
                if has_geo_data:
                    chart_type_options.append("Map")
                else: # Still allow Map option, but it will show a warning if no geo data is found
                    chart_type_options.append("Map (needs geographic data)")
                
                chart_type = st.selectbox("Select Chart Type:", chart_type_options, key="chart_type_select")
                fig = None
                kpi_config_valid = False
                kpi_params_for_dashboard = {}

                try:
                    if chart_type == "KPI":
                        st.markdown("##### KPI Configuration")
                        kpi_label = st.text_input("KPI Label (e.g., Total Revenue)", key="kpi_label_input")
                        
                        if not numeric_columns:
                            st.warning("KPI requires at least one numeric column for the value. Please adjust your query or select numeric data.")
                            kpi_value_col = None
                            kpi_delta_col = None
                        else:
                            kpi_value_col = st.selectbox("KPI Value Column (Numerical):", numeric_columns, key="kpi_value_col_select")
                            kpi_delta_col_options = [None] + numeric_columns
                            kpi_delta_col = st.selectbox("KPI Delta Column (Optional, Numerical):", kpi_delta_col_options, key="kpi_delta_col_select")

                        kpi_config_valid = bool(kpi_label and kpi_value_col)
                        if kpi_config_valid:
                             kpi_params_for_dashboard = {'label': kpi_label, 'value_col': kpi_value_col, 'delta_col': kpi_delta_col}
                        elif not kpi_label:
                            st.info("Please enter a label for the KPI.")
                        elif not kpi_value_col and numeric_columns: # numeric_columns exist but none selected (should not happen with selectbox)
                            st.info("Please select a numeric column for the KPI value.")
                        # For KPIs, actual rendering (st.metric) happens on the dashboard. No fig here.

                    elif chart_type == "Bar Chart":
                        if len(categorical_columns) > 0 and len(numeric_columns) > 0:
                            x_bar = st.selectbox("X-axis (Categorical):", categorical_columns, key="bar_x")
                            y_bar = st.selectbox("Y-axis (Numerical):", numeric_columns, key="bar_y")
                            color_bar = st.selectbox("Color (Optional, Categorical):", [None] + categorical_columns, key="bar_color")
                            if x_bar and y_bar:
                                fig = px.bar(results_df_to_display, x=x_bar, y=y_bar, color=color_bar, title=f"Bar Chart: {y_bar} by {x_bar}")
                        else:
                            st.info("Bar chart requires at least one categorical and one numerical column.")
                    
                    elif chart_type == "Line Chart":
                        if len(all_columns) >=1 and len(numeric_columns) > 0:
                            x_line = st.selectbox("X-axis:", all_columns, key="line_x")
                            y_line = st.selectbox("Y-axis (Numerical):", numeric_columns, key="line_y")
                            color_line = st.selectbox("Color (Optional, Categorical):", [None] + categorical_columns, key="line_color")
                            if x_line and y_line:
                                fig = px.line(results_df_to_display, x=x_line, y=y_line, color=color_line, title=f"Line Chart: {y_line} over {x_line}")
                        else:
                            st.info("Line chart requires at least one numerical column for Y-axis and any column for X-axis.")

                    elif chart_type == "Scatter Plot":
                        if len(numeric_columns) >= 2:
                            x_scatter = st.selectbox("X-axis (Numerical):", numeric_columns, key="scatter_x")
                            y_scatter = st.selectbox("Y-axis (Numerical):", numeric_columns, key="scatter_y")
                            color_scatter = st.selectbox("Color (Optional):", [None] + all_columns, key="scatter_color")
                            size_scatter = st.selectbox("Size (Optional, Numerical):", [None] + numeric_columns, key="scatter_size")
                            if x_scatter and y_scatter:
                                fig = px.scatter(results_df_to_display, x=x_scatter, y=y_scatter, color=color_scatter, size=size_scatter, title="Scatter Plot")
                        else:
                            st.info("Scatter plot typically requires at least two numerical columns.")

                    elif chart_type == "Pie Chart":
                        if len(categorical_columns) > 0 and len(numeric_columns) > 0:
                            names_pie = st.selectbox("Names (Categorical):", categorical_columns, key="pie_names")
                            values_pie = st.selectbox("Values (Numerical):", numeric_columns, key="pie_values")
                            if names_pie and values_pie:
                                fig = px.pie(results_df_to_display, names=names_pie, values=values_pie, title=f"Pie Chart of {values_pie} by {names_pie}")
                        else:
                            st.info("Pie chart requires one categorical column for names and one numerical column for values.")
                        
                    elif chart_type == "Histogram":
                        if len(numeric_columns) > 0:
                            hist_col = st.selectbox("Column for Histogram (Numerical):", numeric_columns, key="hist_col")
                            if hist_col:
                                fig = px.histogram(results_df_to_display, x=hist_col, title=f"Histogram of {hist_col}")
                        else:
                            st.info("Histogram requires at least one numerical column.")

                    elif chart_type == "Table":
                        selected_columns_table = st.multiselect("Select columns to include:", options=all_columns, default=all_columns[:6] if len(all_columns) > 6 else all_columns, key="table_cols_select")
                        if selected_columns_table:
                            st.write("### Table View")
                            st.dataframe(results_df_to_display[selected_columns_table], use_container_width=True)
                            # No fig for table, params stored for dashboard
                        else:
                            st.info("Please select at least one column for the table.")
                            
                    elif chart_type.startswith("Map"): # Handles "Map" and "Map (needs geographic data)"
                        st.info("Map functionality has been removed.")
                        fig = None # Ensure fig is None if Map was somehow selected (shouldn't be possible)
                                                
                    # Display Plotly chart if generated
                    if fig and chart_type != "Table" and chart_type != "KPI":
                        # Apply layout settings for better display on query screen
                        fig.update_layout(
                            template="plotly_dark", # Consistent theme
                            paper_bgcolor='rgba(0,0,0,0)',
                            plot_bgcolor='rgba(0,0,0,0)',
                            font_color='#E0E0E0',
                            title_font_color='#F1F5F9',
                            legend_font_color='#CBD5E1',
                            autosize=True, 
                            height=500  # Set a default height
                        )
                        axis_tick_label_color = '#CBD5E1'
                        axis_title_color = '#94A3B8'   
                        grid_color = '#334155'         
                        line_color = '#4A5568'         

                        fig.update_xaxes(
                            showgrid=True, gridwidth=1, gridcolor=grid_color,
                            zerolinecolor=grid_color, zerolinewidth=1,
                            linecolor=line_color, showline=True,
                            tickfont=dict(color=axis_tick_label_color),
                            title_font=dict(color=axis_title_color)
                        )
                        fig.update_yaxes(
                            showgrid=True, gridwidth=1, gridcolor=grid_color,
                            zerolinecolor=grid_color, zerolinewidth=1,
                            linecolor=line_color, showline=True,
                            tickfont=dict(color=axis_tick_label_color),
                            title_font=dict(color=axis_title_color)
                        )
                        if hasattr(fig.layout, 'legend'):
                            fig.update_layout(legend=dict(font=dict(color='#CBD5E1'), bgcolor='rgba(0,0,0,0)'))
                        
                        st.plotly_chart(fig, use_container_width=True)
                    
                    # --- "Add to My Dashboard" Button Logic ---
                    add_to_dashboard_button_shown = False
                    can_add_to_dashboard = st.session_state.current_dashboard_name is not None

                    if not can_add_to_dashboard:
                        st.warning("Please select or create a dashboard on the 'My Dashboard' page before adding items.")

                    if chart_type == "KPI":
                        if kpi_config_valid:
                            if not results_df_to_display.empty and kpi_params_for_dashboard['value_col'] in results_df_to_display.columns:
                                if st.button(f"Add KPI: {kpi_params_for_dashboard['label']} to My Dashboard", key=f"add_kpi_{kpi_params_for_dashboard['label']}", disabled=not can_add_to_dashboard):
                                    dashboard_item = {
                                        "title": kpi_params_for_dashboard['label'], "chart_type": "KPI",
                                        "params": kpi_params_for_dashboard,
                                        "data_snapshot": copy.deepcopy(results_df_to_display.head(1)), # Store first row for KPI
                                        "filter_state": {}
                                    }
                                    st.session_state.dashboard_items.append(dashboard_item)
                                    save_user_dashboard(st.session_state.logged_in_user, st.session_state.current_dashboard_name, st.session_state.dashboard_items) # Auto-save
                                    st.success(f"KPI '{kpi_params_for_dashboard['label']}' added!")
                                add_to_dashboard_button_shown = True
                            elif results_df_to_display.empty:
                                st.warning("Cannot add KPI: Query returned no data.")
                            elif kpi_params_for_dashboard['value_col'] not in results_df_to_display.columns:
                                 st.warning(f"Cannot add KPI: Value column '{kpi_params_for_dashboard['value_col']}' not found in results.")
                        # elif not kpi_params_for_dashboard.get('label'): # Handled by kpi_config_valid check
                        #     st.info("Provide a label for the KPI to enable adding to dashboard.")
                        # elif not kpi_params_for_dashboard.get('value_col') and numeric_columns:
                        #     st.info("Select a numeric value column for the KPI to enable adding to dashboard.")
                        # elif not numeric_columns: # Already warned above
                        #     pass

                    elif chart_type == "Table":
                        if selected_columns_table: # Check if columns were selected for the table
                            if st.button(f"Add Table to My Dashboard", key="add_table_to_dash", disabled=not can_add_to_dashboard):
                                dashboard_item = {
                                    "title": "Data Table", "chart_type": "Table",
                                    "params": {"columns": selected_columns_table},
                                    "data_snapshot": copy.deepcopy(results_df_to_display),
                                    "filter_state": {}
                                }
                                st.session_state.dashboard_items.append(dashboard_item)
                                save_user_dashboard(st.session_state.logged_in_user, st.session_state.current_dashboard_name, st.session_state.dashboard_items) # Auto-save
                                st.success(f"'Data Table' added to your dashboard!")
                            add_to_dashboard_button_shown = True
                        # else:
                            # st.info("Select columns for the table to enable adding to dashboard.")


                    elif fig: # For other chart types where a fig was generated
                        chart_params_for_dash = {}
                        if chart_type == "Bar Chart": chart_params_for_dash = {'x': x_bar, 'y': y_bar, 'color': color_bar}
                        elif chart_type == "Line Chart": chart_params_for_dash = {'x': x_line, 'y': y_line, 'color': color_line}
                        elif chart_type == "Scatter Plot": chart_params_for_dash = {'x': x_scatter, 'y': y_scatter, 'color': color_scatter, 'size': size_scatter}
                        elif chart_type == "Pie Chart": chart_params_for_dash = {'names': names_pie, 'values': values_pie}
                        elif chart_type == "Histogram": chart_params_for_dash = {'x': hist_col}
                        # No need for map_params_for_dashboard since map functionality is removed
                        
                        button_label_text = fig.layout.title.text if fig.layout.title and fig.layout.title.text else chart_type
                        if st.button(f"Add '{button_label_text}' to My Dashboard", key=f"add_chart_to_dash_{button_label_text.replace(' ','_')}", disabled=not can_add_to_dashboard):
                            dashboard_item = {
                                "title": button_label_text,
                                "chart_type": chart_type, # No need to normalize "Map" anymore
                                "params": chart_params_for_dash,
                                "data_snapshot": copy.deepcopy(results_df_to_display), # Will be serialized by save_user_dashboard
                                "filter_state": {}
                            }
                            st.session_state.dashboard_items.append(dashboard_item)
                            save_user_dashboard(st.session_state.logged_in_user, st.session_state.current_dashboard_name, st.session_state.dashboard_items) # Auto-save
                            st.success(f"'{button_label_text}' added to your dashboard!")
                        add_to_dashboard_button_shown = True
                    
                    # Message if chart could not be generated but type was selected
                    elif chart_type not in ["KPI", "Table"] and not fig :
                         st.warning(f"Cannot add '{chart_type}' to dashboard as it could not be generated. Please check configuration and data.")


                except Exception as e_viz:
                    st.error(f"Error during visualization setup or 'Add to Dashboard' logic: {e_viz}")

    if st.button("Disconnect and Choose Another Source"):
        # Reset relevant session state variables
        st.session_state.connected = False
        st.session_state.connection_type = None
        st.session_state.data = None
        st.session_state.db_connection = None
        st.session_state.db_engine = None
        st.session_state.data_schema = None
        st.session_state.selected_table = "All Tables / Auto-detect"
        st.session_state.log_data_schema_str = None
        st.session_state.log_openai_prompt_str = None
        st.session_state.log_generated_sql_str = None
        st.session_state.log_query_execution_details_str = None
        st.session_state.results_df = None
        st.session_state.dashboard_items = [] # Clear dashboard on disconnect
        # st.session_state.llm_api_key is intentionally preserved
        st.rerun()


# --- Main Application Logic ---
def main():
    # Removed st.title and st.markdown from here, each page function handles its own.

    # LLM API Key Input (Required for the core functionality) - Placed here to run on every script execution
    # st.sidebar.subheader("LLM Configuration") # Keep header for clarity if other general LLM info goes here
    # The direct API key input is GONE from here. It's managed by show_llm_settings_page()
    
    # Ensure LLM client is fresh if settings might have changed page
    # This call also updates the sidebar status message.
    global llm_client # Ensure we are re-assigning the global if needed
    llm_client = get_llm_client()

    # Show sidebar warning for API key only if not on login/config page and no key/client
    # This warning might need adjustment based on how llm_client status is best represented
    if st.session_state.page not in ["login", "db_config", "llm_settings"] and not llm_client:
        st.sidebar.warning("LLM is not configured. Please visit LLM Settings.")

    # --- Page Routing ---
    current_page_on_entry = st.session_state.get('page') # Capture page state at entry of this run
    logged_in = st.session_state.get('logged_in_user')

    # Section 1: Handle Database Configuration Process (may call st.rerun())
    if not st.session_state.db_configured_successfully:
        if not st.session_state.attempted_secrets_db_init:
            st.session_state.attempted_secrets_db_init = True
            conn_secrets = None 
            try:
                with st.spinner("Checking database configuration from secrets..."):
                    conn_secrets = database.get_db_connection() 
                    if conn_secrets:
                        init_secrets_success = database.init_db(conn_secrets)
                        if init_secrets_success:
                            st.session_state.db_configured_successfully = True
                            initialize_app_defaults() 
                            st.sidebar.success("DB auto-configured from secrets.")
                            # If auto-config succeeds, default to login page
                            if st.session_state.page == "db_config": # Or if it was set to db_config before
                                st.session_state.page = "login"
                            st.rerun() 
                        else:
                            st.sidebar.warning("DB connection via secrets OK, but table init failed.")
                            st.session_state.page = "db_config"
                            st.rerun()
                    else:
                        st.sidebar.info("DB secrets not found or connection failed. Please configure manually.")
                        st.session_state.page = "db_config"
                        st.rerun()
            except Exception as e_secrets_init:
                st.sidebar.error(f"Error during secrets-based DB init: {e_secrets_init}")
                st.session_state.page = "db_config"
                st.rerun()
            finally:
                if conn_secrets:
                    try: conn_secrets.close() 
                    except: pass
        else: # Secrets init already attempted and failed or was skipped.
              # Ensure we are on db_config page if db is not configured.
            if st.session_state.page != "db_config":
                st.session_state.page = "db_config"
                st.rerun()
    
    # Section 2: Page Rendering Logic
    # current_page_for_render reflects the page state for *this specific execution pass* 
    # after any potential reruns from Section 1 have completed and the script has restarted.
    current_page_for_render = st.session_state.get('page')

    if current_page_for_render == "db_config" and not st.session_state.db_configured_successfully:
        show_db_configuration_page()
    elif st.session_state.db_configured_successfully:
        # DB is configured, proceed with main app pages
        if current_page_for_render == "login":
            show_login_page()
        elif current_page_for_render == "db_config": 
            # This state means DB was configured (perhaps just now by show_db_configuration_page 
            # which then sets page to login and reruns, or secrets worked and reran).
            # If we are on "db_config" page but DB IS configured, redirect to login.
            st.session_state.page = "login"
            st.rerun()
        elif current_page_for_render == "app" and logged_in:
            if not st.session_state.connected:
                # Role-based access to connection screen
                if "admin" in st.session_state.user_roles or "superuser" in st.session_state.user_roles:
                    show_connection_screen()
                else:
                    st.warning("You do not have permission to create new data connections. Please contact an administrator.")
                    if st.button("Logout"):
                        logout()
            else:
                show_query_screen() # This function uses st.session_state.llm_api_key and llm_client
        elif current_page_for_render == "admin_panel" and logged_in and "admin" in st.session_state.user_roles:
            show_admin_panel_page()
        elif current_page_for_render == "dashboard" and logged_in:
            show_dashboard_page()
        elif current_page_for_render == "dashboard_management" and logged_in:
            show_dashboard_management_page()
        elif current_page_for_render == "llm_settings" and logged_in: # New Route
            show_llm_settings_page()
        else:
            # Default routing if no specific page matches
            if logged_in:
                st.session_state.page = "app" # Default to app (Query Data screen) if logged in
            else:
                st.session_state.page = "login" # Default to login if not logged in
            st.rerun()

    # --- Logs Expander --- (Placed in main to be available on most screens if logs exist and user is logged in)
    if st.session_state.logged_in_user and (st.session_state.log_data_schema_str or st.session_state.log_openai_prompt_str or st.session_state.log_generated_sql_str or st.session_state.log_query_execution_details_str):
        with st.sidebar.expander("View Debug Logs", expanded=False):
            if st.session_state.log_data_schema_str:
                st.subheader("Data Schema Sent to LLM")
                st.text(st.session_state.log_data_schema_str)
            if st.session_state.log_openai_prompt_str:
                st.subheader("OpenAI Prompt")
                st.text(st.session_state.log_openai_prompt_str)
            if st.session_state.log_generated_sql_str:
                st.subheader("Last Generated SQL")
                st.code(st.session_state.log_generated_sql_str, language="sql")
            if st.session_state.log_query_execution_details_str:
                st.subheader("Last Query Execution Details")
                st.text(st.session_state.log_query_execution_details_str)

# --- New Page Functions (Login, Admin Panel) ---
def show_login_page():
    # Use a container and apply custom class for centering and styling
    st.markdown("<div class='login-container'>", unsafe_allow_html=True)
    with st.container(): # This won't directly center but custom CSS will target children
        st.markdown("<div class='login-box'>", unsafe_allow_html=True)
        st.markdown("<h1>User Login</h1>", unsafe_allow_html=True)
        
        username = st.text_input("Username", key="login_username", placeholder="Enter your username")
        password = st.text_input("Password", type="password", key="login_password", placeholder="Enter your password")

        if st.button("Login", key="login_button"):
            user_data_from_db = database.get_user_by_username_from_db(username)
            
            if user_data_from_db and check_password(user_data_from_db["hashed_password"], password):
                st.session_state.logged_in_user = user_data_from_db["username"]
                st.session_state.user_roles = user_data_from_db["roles"]
                st.session_state.page = "app"
                st.session_state.connected = False
                st.session_state.connection_type = None
                st.session_state.data = None
                st.session_state.db_connection = None
                st.session_state.db_engine = None
                st.session_state.data_schema = None
                st.session_state.selected_table = "All Tables / Auto-detect"
                clear_logs()
                st.session_state.results_df = None
                
                dashboard_name_tuples = get_user_dashboard_names(username)
                if dashboard_name_tuples:
                    st.session_state.current_dashboard_name = dashboard_name_tuples[0][0]
                    st.session_state.dashboard_items = load_user_dashboard(username, st.session_state.current_dashboard_name)
                else:
                    st.session_state.current_dashboard_name = None
                    st.session_state.dashboard_items = []
                
                st.rerun()
            else:
                st.error("Invalid username or password.")
        st.markdown("</div>", unsafe_allow_html=True) # Close login-box
    st.markdown("</div>", unsafe_allow_html=True) # Close login-container

def show_admin_panel_page():
    st.header("Admin Panel - User Management")
    if "admin" not in st.session_state.user_roles:
        st.error("You do not have permission to access this page.")
        st.session_state.page = "app" 
        st.rerun()
        return

    users_from_db = database.get_all_users_from_db()

    st.subheader("Current Users")
    user_list_cols = st.columns([2, 2, 1])
    user_list_cols[0].write("**Username**")
    user_list_cols[1].write("**Roles**")
    for user_dict in users_from_db:
        user = user_dict["username"]
        roles = user_dict["roles"]
        cols = st.columns([2, 2, 1])
        cols[0].write(user)
        cols[1].write(", ".join(roles) if isinstance(roles, list) else str(roles))
        if user != st.session_state.logged_in_user: 
            if cols[2].button(f"Delete {user}", key=f"delete_{user}"):
                if database.delete_user_from_db(user):
                    st.success(f"User '{user}' deleted.")
                    database.log_app_action(st.session_state.logged_in_user, "DELETE_USER_SUCCESS", f"Admin deleted user: {user}", "SUCCESS")
                    st.rerun()
                else:
                    st.error(f"Failed to delete user '{user}'.")
                    database.log_app_action(st.session_state.logged_in_user, "DELETE_USER_FAILURE", f"Admin failed to delete user: {user}", "FAILURE")
        else:
            cols[2].write("(Current Admin)")

    st.subheader("Add/Edit User")
    with st.form("user_form"):
        edit_username = st.text_input("Username")
        edit_password = st.text_input("New Password (leave blank to keep current if editing)", type="password")
        edit_roles_str = st.text_input("Roles (comma-separated, e.g., query_user,superuser)")
        
        submitted = st.form_submit_button("Save User")
        if submitted:
            if not edit_username:
                st.error("Username cannot be empty.")
            else:
                roles_list = [r.strip() for r in edit_roles_str.split(',') if r.strip()]
                if not roles_list:
                    st.error("User must have at least one role.")
                else:
                    existing_user = database.get_user_by_username_from_db(edit_username)
                    hashed_new_password = hash_password(edit_password) if edit_password else None

                    if existing_user:
                        # Editing existing user
                        if database.update_user_in_db(edit_username, hashed_new_password, roles_list):
                            st.success(f"User '{edit_username}' updated.")
                            database.log_app_action(st.session_state.logged_in_user, "UPDATE_USER_SUCCESS", f"Admin updated user: {edit_username}", "SUCCESS")
                            st.rerun()
                        else:
                            st.error(f"Failed to update user '{edit_username}'.")
                            database.log_app_action(st.session_state.logged_in_user, "UPDATE_USER_FAILURE", f"Admin failed to update user: {edit_username}", "FAILURE")
                    else:
                        # Adding new user
                        if not hashed_new_password:
                            st.error("Password is required for new users.")
                        elif database.create_user_in_db(edit_username, hashed_new_password, roles_list):
                            st.success(f"User '{edit_username}' created.")
                            database.log_app_action(st.session_state.logged_in_user, "CREATE_USER_SUCCESS", f"Admin created new user: {edit_username}", "SUCCESS")
                            st.rerun()
                        else:
                            st.error(f"Failed to create user '{edit_username}'.")
                            database.log_app_action(st.session_state.logged_in_user, "CREATE_USER_FAILURE", f"Admin failed to create new user: {edit_username}", "FAILURE")

    if st.button("Back to App"):
        st.session_state.page = "app"
        st.rerun()


def logout():
    st.session_state.logged_in_user = None
    st.session_state.user_roles = []
    st.session_state.page = "login"
    # Clear all connection and data related session state on logout
    st.session_state.connected = False
    st.session_state.connection_type = None
    st.session_state.data = None
    st.session_state.db_connection = None
    st.session_state.db_engine = None
    st.session_state.data_schema = None
    st.session_state.selected_table = "All Tables / Auto-detect"
    clear_logs()
    st.session_state.results_df = None # Clear query results on logout
    st.session_state.dashboard_items = [] # Clear dashboard on logout
    st.session_state.current_dashboard_name = None # Clear current dashboard name
    st.rerun()

def clear_logs():
    st.session_state.log_data_schema_str = None
    st.session_state.log_openai_prompt_str = None
    st.session_state.log_generated_sql_str = None
    st.session_state.log_query_execution_details_str = None

# --- Dashboard Page ---
def show_dashboard_page():
    # global USERS_DATA # Removed, no longer needed
    st.title(f"My Dashboard: {st.session_state.current_dashboard_name if st.session_state.current_dashboard_name else 'No Dashboard Selected'}")

    if not st.session_state.logged_in_user:
        st.error("Please log in to view dashboards.")
        st.session_state.page = 'login'
        st.rerun()
        return

    st.sidebar.subheader("Manage Dashboards")
    dashboard_name_tuples = get_user_dashboard_names(st.session_state.logged_in_user)
    dashboard_display_options = [d[1] for d in dashboard_name_tuples]
    current_selected_raw_name = st.session_state.current_dashboard_name
    current_selected_display_name = current_selected_raw_name 
    current_dashboard_owner = None

    if current_selected_raw_name:
        for raw, display, owner in dashboard_name_tuples:
            if raw == current_selected_raw_name:
                current_selected_display_name = display
                current_dashboard_owner = owner
                break

    if dashboard_display_options:
        try:
            current_selection_index = dashboard_display_options.index(current_selected_display_name) if current_selected_display_name in dashboard_display_options else 0
        except ValueError:
            current_selection_index = 0 

        selected_display_name_in_sidebar = st.sidebar.selectbox(
            "Select Dashboard:", 
            options=dashboard_display_options, 
            index=current_selection_index,
            key="dashboard_selector_sidebar",
        )
        
        selected_raw_name_from_select = None
        selected_owner_from_select = None
        for raw, display, owner_in_tuple in dashboard_name_tuples:
            if display == selected_display_name_in_sidebar:
                selected_raw_name_from_select = raw
                selected_owner_from_select = owner_in_tuple
                break

        if selected_raw_name_from_select and selected_raw_name_from_select != st.session_state.current_dashboard_name:
            st.session_state.current_dashboard_name = selected_raw_name_from_select
            st.session_state.dashboard_items = load_user_dashboard(st.session_state.logged_in_user, selected_raw_name_from_select)
            current_dashboard_owner = selected_owner_from_select 
            st.rerun()
        elif not st.session_state.current_dashboard_name and selected_raw_name_from_select: 
            st.session_state.current_dashboard_name = selected_raw_name_from_select
            st.session_state.dashboard_items = load_user_dashboard(st.session_state.logged_in_user, selected_raw_name_from_select)
            current_dashboard_owner = selected_owner_from_select
            st.rerun()
    else:
        st.sidebar.info("No dashboards available (owned or shared). Create one!")
        if st.session_state.current_dashboard_name is not None: 
            st.session_state.current_dashboard_name = None
            st.session_state.dashboard_items = []
            current_dashboard_owner = None
            # st.rerun() 

    new_dash_name = st.sidebar.text_input("New Dashboard Name:", key="new_dashboard_name_input")
    if st.sidebar.button("Create Dashboard", key="create_dashboard_button"):
        if new_dash_name:
            is_owned_already = any(d[0] == new_dash_name and d[2] == st.session_state.logged_in_user for d in dashboard_name_tuples)
            if not is_owned_already:
                if database.save_dashboard_to_db(st.session_state.logged_in_user, new_dash_name, [], []):
                    st.session_state.current_dashboard_name = new_dash_name
                    st.session_state.dashboard_items = [] 
                    current_dashboard_owner = st.session_state.logged_in_user 
                    st.sidebar.success(f"Dashboard '{new_dash_name}' created!")
                    database.log_app_action(st.session_state.logged_in_user, "CREATE_DASHBOARD_SUCCESS", f"User created dashboard: {new_dash_name}", "SUCCESS")
                    st.rerun()
                else:
                    st.sidebar.error(f"Failed to create dashboard '{new_dash_name}' in database.")
                    database.log_app_action(st.session_state.logged_in_user, "CREATE_DASHBOARD_FAILURE", f"User failed to create dashboard: {new_dash_name}", "FAILURE")
            else:
                st.sidebar.error(f"You already own a dashboard named '{new_dash_name}'.")
        else:
            st.sidebar.warning("Please enter a name for the new dashboard.")
    
    st.sidebar.markdown("---GV---")

    if st.session_state.current_dashboard_name:
        if not current_dashboard_owner:
            dash_detail = database.load_dashboard_from_db(st.session_state.logged_in_user, st.session_state.current_dashboard_name)
            if dash_detail:
                current_dashboard_owner = dash_detail.get('owner_username')

        is_owner = (current_dashboard_owner == st.session_state.logged_in_user)

        with st.sidebar.expander("Dashboard Actions", expanded=False):
            if is_owner:
                st.markdown("###### Rename Current Dashboard")
                new_dashboard_rename_input = st.text_input(
                    "New name for selected dashboard:", 
                    value=st.session_state.current_dashboard_name, 
                    key="dashboard_rename_input"
                )
                if st.button("Rename Dashboard", key="rename_selected_dashboard_button"):
                    if new_dashboard_rename_input and new_dashboard_rename_input.strip():
                        old_name = st.session_state.current_dashboard_name
                        new_name = new_dashboard_rename_input.strip()
                        if old_name == new_name:
                            st.toast("New name is the same as the current name.", icon="🤷")
                        elif any(d[0] == new_name and d[2] == st.session_state.logged_in_user for d in get_user_dashboard_names(st.session_state.logged_in_user)):
                            st.error(f"You already own a dashboard named '{new_name}'. Please choose a different name.")
                        elif database.rename_dashboard_in_db(st.session_state.logged_in_user, old_name, new_name):
                            st.session_state.current_dashboard_name = new_name
                            st.success(f"Dashboard '{old_name}' renamed to '{new_name}'.")
                            database.log_app_action(st.session_state.logged_in_user, "RENAME_DASHBOARD_SUCCESS", f"Renamed '{old_name}' to '{new_name}'", "SUCCESS")
                            st.rerun()
                        else:
                            database.log_app_action(st.session_state.logged_in_user, "RENAME_DASHBOARD_FAILURE", f"Failed to rename '{old_name}' to '{new_name}'", "FAILURE")
                    else:
                        st.warning("New dashboard name cannot be empty.")

                st.markdown("---") 
                st.markdown("###### Delete Current Dashboard")
                if st.button(f"Delete Dashboard: '{st.session_state.current_dashboard_name}'", type="secondary", key="delete_selected_dashboard_button_confirm_ask"):
                    if database.delete_dashboard_from_db(st.session_state.logged_in_user, st.session_state.current_dashboard_name):
                        st.toast(f"Dashboard '{st.session_state.current_dashboard_name}' deleted.", icon="🗑️")
                        database.log_app_action(st.session_state.logged_in_user, "DELETE_DASHBOARD_SUCCESS", f"Deleted dashboard: {st.session_state.current_dashboard_name}", "SUCCESS")
                        remaining_dash_tuples = get_user_dashboard_names(st.session_state.logged_in_user)
                        if remaining_dash_tuples:
                            st.session_state.current_dashboard_name = remaining_dash_tuples[0][0] 
                            st.session_state.dashboard_items = load_user_dashboard(st.session_state.logged_in_user, st.session_state.current_dashboard_name)
                        else:
                            st.session_state.current_dashboard_name = None
                            st.session_state.dashboard_items = []
                        st.rerun()
                    else:
                        st.error(f"Failed to delete dashboard '{st.session_state.current_dashboard_name}'.")
                        database.log_app_action(st.session_state.logged_in_user, "DELETE_DASHBOARD_FAILURE", f"Failed to delete dashboard: {st.session_state.current_dashboard_name}", "FAILURE")
                    st.markdown("---") # This was likely intended to be outside the delete button's if block
                    st.markdown("###### Share Dashboard") # This was also likely intended to be outside
                    
                # Moved Share Dashboard section out of the "Delete Dashboard" button's "if" block
                    st.markdown("---")
                    st.markdown("###### Share Dashboard")
                    
                all_other_users_list = [u["username"] for u in database.get_all_users_from_db() if u["username"] != st.session_state.logged_in_user]
                
                # Fetch current sharing status for the dashboard
                current_dash_db_obj = database.load_dashboard_from_db(st.session_state.logged_in_user, st.session_state.current_dashboard_name)
                currently_shared_with_list = []
                if current_dash_db_obj and isinstance(current_dash_db_obj.get('shared_with_users'), list):
                    currently_shared_with_list = current_dash_db_obj['shared_with_users']

                users_to_share_with_selection = st.multiselect(
                        "Share with users:",
                    options=all_other_users_list,
                    default=currently_shared_with_list,
                    key=f"share_{st.session_state.current_dashboard_name}"
                )

                if st.button("Update Sharing", key=f"update_share_{st.session_state.current_dashboard_name}"):
                    if database.update_dashboard_sharing_in_db(st.session_state.logged_in_user, st.session_state.current_dashboard_name, users_to_share_with_selection):
                        st.success(f"Sharing settings for '{st.session_state.current_dashboard_name}' updated.")
                        database.log_app_action(st.session_state.logged_in_user, "UPDATE_DASHBOARD_SHARING_SUCCESS", f"Updated sharing for {st.session_state.current_dashboard_name} to {users_to_share_with_selection}", "SUCCESS")
                        st.rerun()
                    else:
                        st.error(f"Failed to update sharing for '{st.session_state.current_dashboard_name}'.")
                        database.log_app_action(st.session_state.logged_in_user, "UPDATE_DASHBOARD_SHARING_FAILURE", f"Failed to update sharing for {st.session_state.current_dashboard_name}", "FAILURE")
            elif current_dashboard_owner: # Viewing a shared dashboard, not the owner
                st.markdown(f"_This dashboard is owned by **{current_dashboard_owner}**. You are viewing it as a shared user._")
            else: # Should not happen if a dashboard is selected, implies owner could not be determined
                st.markdown("_Dashboard action unavailable. Owner could not be determined._")
    # ... (rest of dashboard display logic for KPIs and charts remains largely the same, 
    #      as it operates on st.session_state.dashboard_items which is populated by load_user_dashboard)
    # ... existing code ...

    # --- Main Dashboard Display Area ---
    if not st.session_state.current_dashboard_name:
        st.info("Please create or select a dashboard from the sidebar to view or add items.")
        return

    if not st.session_state.dashboard_items:
        st.info(f"Dashboard '{st.session_state.current_dashboard_name}' is currently empty. Go to the 'Query Data' page, generate a visualization, and click 'Add to My Dashboard'.")
        # No return here, still show download/controls if dashboard exists but is empty

    # Separate KPIs from other chart types based on st.session_state.dashboard_items
    kpi_items = [item for item in st.session_state.dashboard_items if item.get('chart_type') == "KPI"]
    other_items = [item for item in st.session_state.dashboard_items if item.get('chart_type') != "KPI"]

    # --- Download Dashboard and Send Email Buttons ---
    if st.session_state.current_dashboard_name: # Only show if a dashboard is active
        col1, col2 = st.columns(2)
        dashboard_html_content = generate_dashboard_html(st.session_state.dashboard_items)
        current_dashboard_name_for_email = st.session_state.current_dashboard_name

        with col1:
            st.markdown(get_download_link_html(dashboard_html_content, f"{current_dashboard_name_for_email.replace(' ','_')}_dashboard.html"), unsafe_allow_html=True)
        
        with col2:
            if st.button("Share Dashboard via Email", key="share_dashboard_email_button_v3"): # Added/updated key
                st.session_state.show_email_form = True # Use session state to control visibility
                st.experimental_rerun() # Rerun to ensure expander opens smoothly
        
        # --- Modal/Expander for Email Form ---
        if st.session_state.get('show_email_form', False):
            with st.expander("Send Dashboard Email Options", expanded=True):
                send_email.show_send_email_ui(
                    dashboard_html_content_for_html_email=dashboard_html_content, 
                    dashboard_name=current_dashboard_name_for_email,
                    generate_image_callback=generate_image_from_html # Pass the new image callback
                )
                if st.button("Close Email Form", key="close_email_form_button_v3"): # Updated key
                    st.session_state.show_email_form = False
                    st.experimental_rerun() # Rerun to close the expander

        st.markdown("---GV---") # Separator after buttons

    # --- Render KPIs --- 
    if kpi_items:
        st.subheader("Key Performance Indicators")
        num_kpi_cols = min(len(kpi_items), 4)
        if num_kpi_cols > 0:
            kpi_cols = st.columns(num_kpi_cols)
            for i, item in enumerate(kpi_items):
                # Get the absolute index of the item in the original dashboard_items list
                # This is crucial for callbacks to modify the correct item
                try:
                    original_kpi_item_index = next(idx for idx, dash_item in enumerate(st.session_state.dashboard_items) if dash_item == item)
                except StopIteration:
                    st.error("Error finding KPI item for editing. Please refresh.")
                    continue # Skip this item if not found

                with kpi_cols[i % num_kpi_cols]:
                    params = item['params']
                    data_snapshot = item['data_snapshot']
                    
                    current_kpi_label = params.get('label', "KPI Value")
                    if not current_kpi_label.strip(): current_kpi_label = "KPI Value"

                    # --- In-place editable KPI Label --- 
                    def handle_kpi_label_change(item_idx):
                        new_label_val = st.session_state[f"kpi_label_edit_{item_idx}"]
                        if new_label_val.strip():
                            st.session_state.dashboard_items[item_idx]['params']['label'] = new_label_val.strip()
                            save_user_dashboard(st.session_state.logged_in_user, st.session_state.current_dashboard_name, st.session_state.dashboard_items)
                            st.toast(f"KPI label updated to '{new_label_val.strip()}'", icon="📝")
                            # No st.rerun() here, on_change handles it, but be mindful of potential race conditions or need for explicit rerun if issues arise
                        else:
                            st.warning("KPI label cannot be empty.")
                            # Revert to old value if input is cleared - this needs careful state handling
                            # For now, it just warns. A full revert might need storing old value before edit.
                    
                    st.markdown(f'''
                        <div class="editable-kpi-label-input">
                        </div>
                    ''', unsafe_allow_html=True) # Placeholder for CSS to target parent if needed
                    st.text_input(
                        "KPI Label", 
                        value=current_kpi_label,
                        key=f"kpi_label_edit_{original_kpi_item_index}", 
                        on_change=handle_kpi_label_change, 
                        args=(original_kpi_item_index,),
                        label_visibility="collapsed"
                    )

                    value_col = params.get('value_col')
                    delta_col = params.get('delta_col')
                    kpi_value = None
                    kpi_delta = None

                    if not data_snapshot.empty and value_col in data_snapshot.columns:
                        try: kpi_value = pd.to_numeric(data_snapshot[value_col].iloc[0])
                        except (ValueError, TypeError): kpi_value = str(data_snapshot[value_col].iloc[0])
                        if delta_col and delta_col in data_snapshot.columns:
                            try: kpi_delta = pd.to_numeric(data_snapshot[delta_col].iloc[0])
                            except (ValueError, TypeError): kpi_delta = str(data_snapshot[delta_col].iloc[0])
                    
                    # Display the value and delta using st.metric, but without its own label
                    st.metric(label=" ", value=kpi_value if kpi_value is not None else "N/A", delta=kpi_delta if kpi_delta is not None else None, label_visibility="collapsed")

                    # --- Embed Code Expander for KPIs ---
                    with st.expander("Embed This KPI"):
                        embed_kpi_html = get_kpi_embed_html(current_kpi_label, kpi_value, kpi_delta)
                        st.code(embed_kpi_html, language="html")

        st.markdown("---")

    # --- Render Other Chart Items ---
    if not other_items:
        if not kpi_items: # If only KPIs existed and were rendered, and no other items
            pass # No message needed if only KPIs were there
        elif kpi_items and not other_items:
             st.info("No other charts or tables on the dashboard yet.")
        # If there were no items at all, the initial check at the top of the function handles it.
        return # Exit if no 'other_items' to render in columns

    st.subheader("Charts & Tables") # Optional subheader for other items
    num_items = len(other_items)
    num_cols = 2 # Number of charts per row

    # Function to handle moving items
    def move_item(current_index_in_full_list, direction):
        # Note: current_index_in_full_list is the index in st.session_state.dashboard_items
        # This function needs to operate on st.session_state.dashboard_items directly
        
        item_to_move = st.session_state.dashboard_items.pop(current_index_in_full_list)
        new_index = current_index_in_full_list + direction 
        
        # Ensure new_index is within bounds of the modified list length
        if new_index < 0:
            new_index = 0
        elif new_index > len(st.session_state.dashboard_items):
            new_index = len(st.session_state.dashboard_items)
            
        st.session_state.dashboard_items.insert(new_index, item_to_move)
        save_user_dashboard(st.session_state.logged_in_user, st.session_state.current_dashboard_name, st.session_state.dashboard_items) 
        st.rerun()

    # Iterate through a copy of other_items for display, but get original index for modifications
    temp_other_items_for_iteration = list(other_items) # Create a copy for stable iteration if items are removed
    displayed_item_count = 0
    num_display_cols = 2 # Define number of columns for display

    for i, item_in_display_list in enumerate(temp_other_items_for_iteration):
        # This outer loop now effectively goes through each item that needs to be displayed.
        # We will create columns for every `num_display_cols` items.
        
        # Determine if we are starting a new row of columns
        if displayed_item_count % num_display_cols == 0:
            # Create new columns for the current row. 
            # The number of columns created will be num_display_cols, 
            # or fewer if less than num_display_cols items remain.
            remaining_items_in_full_list = len(temp_other_items_for_iteration) - displayed_item_count
            current_row_cols_count = min(num_display_cols, remaining_items_in_full_list)
            if current_row_cols_count > 0:
                cols = st.columns(current_row_cols_count)
            else:
                break # No more items to display

        # Select the correct column for the current item
        current_col_for_item = cols[displayed_item_count % num_display_cols]

        with current_col_for_item: # Place the item content in the designated column
            try:
                # Find the true index of this item in the main st.session_state.dashboard_items list
                current_item_absolute_index = st.session_state.dashboard_items.index(item_in_display_list)
            except ValueError:
                # This can happen if the item was removed or list modified unexpectedly. Skip this item.
                st.warning("Skipping an item as it could not be found in the main dashboard list.")
                continue
            
            # --- In-place editable Chart Title --- 
            def handle_chart_title_change(item_abs_idx): # Callback needs to be defined or accessible here
                new_title_val = st.session_state[f"chart_title_edit_{item_abs_idx}"]
                if new_title_val.strip():
                    st.session_state.dashboard_items[item_abs_idx]['title'] = new_title_val.strip()
                    save_user_dashboard(st.session_state.logged_in_user, st.session_state.current_dashboard_name, st.session_state.dashboard_items)
                    st.toast(f"Chart title updated to '{new_title_val.strip()}'", icon="📝")
                else:
                    st.warning("Chart title cannot be empty.")
            
            st.markdown(f'''<div class="editable-title-input"></div>''', unsafe_allow_html=True)
            st.text_input(
                f"Chart Title for item at index {current_item_absolute_index}", 
                value=st.session_state.dashboard_items[current_item_absolute_index].get('title', st.session_state.dashboard_items[current_item_absolute_index]['chart_type']),
                key=f"chart_title_edit_{current_item_absolute_index}", 
                on_change=handle_chart_title_change,
                args=(current_item_absolute_index,),
                label_visibility="collapsed"
            )
            
            # Access data using the absolute index
            data_snapshot = st.session_state.dashboard_items[current_item_absolute_index]['data_snapshot']
            params = st.session_state.dashboard_items[current_item_absolute_index]['params']
            chart_type = st.session_state.dashboard_items[current_item_absolute_index]['chart_type']
            
            if 'filter_state' not in st.session_state.dashboard_items[current_item_absolute_index]:
                st.session_state.dashboard_items[current_item_absolute_index]['filter_state'] = {}
            filter_state = st.session_state.dashboard_items[current_item_absolute_index]['filter_state']
            
            filtered_data = data_snapshot.copy()
            
            # --- Determine data types for filtering and visualization ---
            all_columns = filtered_data.columns.tolist()
            numeric_columns = filtered_data.select_dtypes(include=['number']).columns.tolist()
            categorical_columns = filtered_data.select_dtypes(include=['object', 'category', 'string', 'boolean']).columns.tolist()
            geo_columns = detect_geographic_columns(filtered_data)
            has_geo_data = bool(geo_columns)

            # --- Add Filter Expander ---
            with st.expander("Filters"): 
                categorical_cols_for_filter = data_snapshot.select_dtypes(include=['object', 'category', 'string', 'boolean']).columns.tolist()
                if not categorical_cols_for_filter:
                    st.write("No categorical columns available for filtering.")
                else:
                    filter_applied = False # Flag to see if any filters were applied
                    for col in categorical_cols_for_filter:
                        try:
                            options = sorted(data_snapshot[col].astype(str).unique().tolist())
                            # Use current_item_absolute_index in the key for uniqueness
                            filter_key = f"filter_{current_item_absolute_index}_{col}"
                            # Get current selections from state, default to all options (no filtering)
                            current_selection = filter_state.get(col, options)
                            
                            selected_values = st.multiselect(
                                f"Filter by {col}:",
                                options=options,
                                default=current_selection,
                                key=filter_key
                            )
                            
                            # Update the filter state *directly* on the item dictionary if it changed
                            if selected_values != current_selection:
                                 filter_state[col] = selected_values
                                 # No st.rerun() needed here, widget interaction triggers it.
                                 
                            if selected_values and sorted(selected_values) != sorted(options): # Check if this filter is active
                                 filter_applied = True 
                                         
                        except Exception as e_filter_widget:
                            st.warning(f"Could not create filter for {col}: {e_filter_widget}")

            # --- Apply Filters to Data --- 
            try:
                for col, values in filter_state.items():
                    if values and sorted(values) != sorted(filtered_data[col].astype(str).unique().tolist()): # Only apply if filter has selections and not all are selected
                        if col in filtered_data.columns:
                            # Ensure comparison happens using string type for robustness
                            filtered_data = filtered_data[filtered_data[col].astype(str).isin(values)]
                        else:
                             st.warning(f"Filter column '{col}' not found in data snapshot anymore? Skipping filter.")
            except Exception as e_apply_filter:
                 st.error(f"Error applying filters: {e_apply_filter}")

            # --- Generate and Display Chart (using filtered_data) --- 
            fig = None
            try:
                # Use 'filtered_data' for chart generation
                if chart_type == "Bar Chart":
                    x_col, y_col = params.get('x'), params.get('y')
                    if x_col in filtered_data.columns and y_col in filtered_data.columns:
                        fig = px.bar(filtered_data, x=x_col, y=y_col, color=params.get('color'), title=item_in_display_list.get('title', chart_type))
                    else:
                        st.warning(f"Required columns ('{x_col}', '{y_col}') for bar chart not found in filtered data.")
                        fig = None
                
                elif chart_type == "Line Chart":
                    x_col, y_col = params.get('x'), params.get('y')
                    if x_col in filtered_data.columns and y_col in filtered_data.columns:
                        fig = px.line(filtered_data, x=x_col, y=y_col, color=params.get('color'), title=item_in_display_list.get('title', chart_type))
                    else:
                        st.warning(f"Required columns ('{x_col}', '{y_col}') for line chart not found in filtered data.")
                        fig = None
                
                elif chart_type == "Scatter Plot":
                    x_col, y_col = params.get('x'), params.get('y')
                    if x_col in filtered_data.columns and y_col in filtered_data.columns:
                        fig = px.scatter(filtered_data, x=x_col, y=y_col, color=params.get('color'), size=params.get('size'), title=item_in_display_list.get('title', chart_type))
                    else:
                        st.warning(f"Required columns ('{x_col}', '{y_col}') for scatter plot not found in filtered data.")
                        fig = None
                
                elif chart_type == "Pie Chart":
                    if params.get('names') in filtered_data.columns and params.get('values') in filtered_data.columns:
                        fig = px.pie(filtered_data, names=params.get('names'), values=params.get('values'), title=item_in_display_list.get('title', chart_type))
                    else:
                        st.warning("Required columns for pie chart not found in filtered data.")
                        fig = None
                
                elif chart_type == "Histogram":
                    if params.get('x') in filtered_data.columns:
                        fig = px.histogram(filtered_data, x=params.get('x'), title=item_in_display_list.get('title', chart_type))
                    else:
                        st.warning("Required column for histogram not found in filtered data.")
                        fig = None
                
                elif chart_type == "Table":
                    # For tables, display using st.dataframe
                    selected_columns = params.get('columns', filtered_data.columns.tolist())
                    # Ensure selected_columns_param is a list
                    if not isinstance(selected_columns, list):
                        selected_columns = filtered_data.columns.tolist()
                    
                    # Filter columns that actually exist in filtered_data
                    display_columns = [col for col in selected_columns if col in filtered_data.columns]

                    if not display_columns and selected_columns: # If intended columns are gone, show warning
                        st.warning(f"Original columns for table not found in filtered data. Showing available columns.")
                        display_columns = filtered_data.columns.tolist() # Fallback to all available
                    elif not display_columns and not selected_columns: # If no columns were ever specified and data is empty
                        display_columns = filtered_data.columns.tolist()


                    if display_columns:
                        st.dataframe(filtered_data[display_columns], use_container_width=True)
                    elif not filtered_data.empty:
                        st.dataframe(filtered_data, use_container_width=True) # Show all if specific columns failed
                    else:
                        st.info("No data to display in table after filtering.")
                    
                elif chart_type == "Map":
                    # Simplified to always be scatter_geo
                    # Map parameters from when it was added to dashboard
                    # This entire block for rendering Map on dashboard should be removed or made unreachable.
                    # For safety, we can add a message.
                    st.info(f"Map item '{item_in_display_list.get('title', 'Map')}' cannot be displayed as map functionality is removed.")
                    fig = None
                    
                if fig: # Apply dark theme if a fig object was created
                    fig.update_layout(
                        template="plotly_dark",
                        paper_bgcolor='rgba(0,0,0,0)',  # Transparent background
                        plot_bgcolor='rgba(0,0,0,0)',   # Transparent background
                        font_color='#E0E0E0', # General font color for chart
                        title_font_color='#F1F5F9', # Title font
                        legend_font_color='#CBD5E1', # Legend font
                        autosize=True, # Explicitly set autosize to help with resizing
                        height=500  # Set a default height for dashboard charts
                    )
                    # For axes, make lines and ticks white/themed
                    axis_tick_label_color = '#CBD5E1' # Color for axis tick labels (e.g., 10k, 20k on y-axis)
                    axis_title_color = '#94A3B8'    # Color for axis titles (e.g., 'Total Sales')
                    grid_color = '#334155'          # Color for grid lines
                    line_color = '#4A5568'          # Color for axis lines themselves

                    fig.update_xaxes(
                        showgrid=True, gridwidth=1, gridcolor=grid_color,
                        zerolinecolor=grid_color, zerolinewidth=1, # Ensure zeroline is also styled
                        linecolor=line_color, showline=True, # Make axis line visible
                        tickfont=dict(color=axis_tick_label_color),
                        title_font=dict(color=axis_title_color)
                    )
                    fig.update_yaxes(
                        showgrid=True, gridwidth=1, gridcolor=grid_color,
                        zerolinecolor=grid_color, zerolinewidth=1,
                        linecolor=line_color, showline=True,
                        tickfont=dict(color=axis_tick_label_color),
                        title_font=dict(color=axis_title_color)
                    )
                    # For legends, if any
                    if hasattr(fig.layout, 'legend'):
                        fig.update_layout(legend=dict(font=dict(color='#CBD5E1'), bgcolor='rgba(0,0,0,0)')) # Transparent legend background

            except Exception as e:
                st.error(f"Error generating chart: {e}")
                fig = None # Ensure fig is None if chart generation fails
                
            # Displaying the chart or info message
            if fig and chart_type != "Table" and chart_type != "KPI":  # Table and KPI are handled differently
                    st.plotly_chart(fig, use_container_width=True)
            elif not fig and chart_type !="Table" and not filtered_data.empty : # If fig is None but was expected for a non-table chart type
                    st.warning(f"Could not display chart: {item_in_display_list.get('title', item_in_display_list['chart_type'])}. Check data and filters.")
            elif filtered_data.empty and chart_type != "Table": # Data is empty after filtering for a non-table chart type
                st.info(f"No data remaining for '{item_in_display_list.get('title', item_in_display_list['chart_type'])}' after applying filters.")
            # Table display is handled within its own block above


            # --- Embed Code Expander ---
            with st.expander("Embed This Item"):
                embed_html = ""
                item_title_for_embed = item_in_display_list.get('title', item_in_display_list['chart_type'])
                try:
                    if chart_type == "Table":
                        # Use display_columns which are columns confirmed to be in filtered_data
                        # and ensure filtered_data itself is not empty before trying to access columns
                        table_df_for_embed = pd.DataFrame() # Default to empty
                        if not filtered_data.empty and display_columns and all(col in filtered_data.columns for col in display_columns):
                            table_df_for_embed = filtered_data[display_columns]
                        
                        if not table_df_for_embed.empty:
                            embed_html = get_table_html(table_df_for_embed, title=item_title_for_embed)
                        else:
                            embed_html = f"<h3>{item_title_for_embed}</h3><p>No data to embed for this table (data might be empty after filtering or required columns missing).</p>"
                    elif fig: # This is for Plotly charts that were successfully generated
                        embed_html = get_plotly_fig_html(fig, title=item_title_for_embed)
                    else: # This case handles non-Table types where fig is None (e.g., chart generation failed)
                        embed_html = f"<h3>{item_title_for_embed}</h3><p>Could not generate embed code for this item (e.g., chart failed or data empty after filtering).</p>"
                    
                    # Add Plotly JS if it's a Plotly chart and the script isn't already in embed_html
                    # get_plotly_fig_html should already include the script, this is a fallback/check.
                    embed_html_with_script = embed_html # Default to current embed_html
                    if chart_type != "Table" and fig and "<script src='https://cdn.plot.ly/plotly-latest.min.js'></script>" not in embed_html:
                        embed_html_with_script = f"<script src='https://cdn.plot.ly/plotly-latest.min.js'></script>\n{embed_html}"
                    
                    st.code(embed_html_with_script, language="html")
                except Exception as e_embed:
                    st.error(f"Could not generate embed code: {e_embed}")
                    st.code(f"<!-- Error generating embed code: {e_embed} -->", language="html")

                # --- Controls (Remove, Move Up, Move Down) --- 
                control_cols = st.columns([3, 1, 1]) 

                with control_cols[0]: 
                     if st.button(f"Remove Item", key=f"remove_dash_item_{current_item_absolute_index}"):
                        st.session_state.dashboard_items.pop(current_item_absolute_index)
                        save_user_dashboard(st.session_state.logged_in_user, st.session_state.current_dashboard_name, st.session_state.dashboard_items)
                        st.rerun()
                
                with control_cols[1]: 
                    can_move_up = current_item_absolute_index > 0
                    if st.button("↑ Up", key=f"move_up_item_{current_item_absolute_index}", disabled=not can_move_up):
                        move_item(current_item_absolute_index, -1)
                        
                with control_cols[2]: 
                    can_move_down = current_item_absolute_index < len(st.session_state.dashboard_items) - 1
                    if st.button("↓ Down", key=f"move_down_item_{current_item_absolute_index}", disabled=not can_move_down):
                        move_item(current_item_absolute_index, 1)
            
            # No st.markdown("---GV---") here as it might be inside a column.
        displayed_item_count +=1 # Increment after processing an item and placing it in a column

    # Add a separator after all items in the 'other_items' section are processed
    if displayed_item_count > 0:
        st.markdown("---GV---")
        
    if displayed_item_count == 0 and not kpi_items and st.session_state.current_dashboard_name: 
        # This case is mostly covered by the check for empty st.session_state.dashboard_items earlier
        # but this catches if only kpis existed and were filtered out, or other_items became empty for other reasons
        pass # Message already shown if dashboard_items is empty
    elif displayed_item_count == 0 and kpi_items and st.session_state.current_dashboard_name:
        st.info(f"No charts or tables in dashboard '{st.session_state.current_dashboard_name}' yet.")

# Add a logout button and admin panel link to the sidebar when logged in
if st.session_state.logged_in_user:
    st.sidebar.markdown(f"Logged in as: **{st.session_state.logged_in_user}** (`{', '.join(st.session_state.user_roles)}`)")
    
    if "admin" in st.session_state.user_roles:
        if st.sidebar.button("Admin Panel", key="admin_panel_button"):
            st.session_state.page = "admin_panel"
            st.rerun()
    if st.sidebar.button("Query Data (Connect/Ask)", key="query_data_button_sidebar"): # Renamed for clarity
        st.session_state.page = "app"
        st.rerun()
    if st.sidebar.button("My Dashboard", key="my_dashboard_button_sidebar"):
        st.session_state.page = "dashboard"
        st.rerun()
    if st.sidebar.button("Manage Sharing", key="manage_sharing_button_sidebar"):
        st.session_state.page = "dashboard_management"
        st.rerun()
    if st.sidebar.button("Logout", key="logout_button_sidebar_main"):
        logout()


# --- Helper Functions for PDF Export and Geographic Data ---

def detect_geographic_columns(df):
    """Detects potential geographic columns in the dataframe"""
    geo_columns = {'country': [], 'city': [], 'state': [], 'region': [], 'latitude': [], 'longitude': []}
    
    # Define more specific patterns for latitude and longitude
    lat_patterns = ['lat', 'latitude']
    lon_patterns = ['lon', 'lng', 'longitude']

    for col in df.columns:
        col_lower = col.lower()
        # Check for exact matches or common patterns for lat/lon first
        if any(pattern == col_lower for pattern in lat_patterns):
            geo_columns['latitude'].append(col)
            continue # Move to next column if identified as latitude
        if any(pattern == col_lower for pattern in lon_patterns):
            geo_columns['longitude'].append(col)
            continue # Move to next column if identified as longitude

        # Broader checks for other geographic types
        if 'country' in col_lower or 'nation' in col_lower:
            geo_columns['country'].append(col)
        elif 'city' in col_lower or 'town' in col_lower:
            geo_columns['city'].append(col)
        elif 'state' in col_lower or 'province' in col_lower:
            geo_columns['state'].append(col)
        elif 'region' in col_lower or 'area' in col_lower:
            geo_columns['region'].append(col)
        # Fallback for less specific lat/lon containing strings, if not already matched by exact patterns
        # This is to catch columns like 'customer_latitude' if the exact match didn't already.
        elif 'lat' in col_lower and col not in geo_columns['latitude']:
             geo_columns['latitude'].append(col)
        elif ('lon' in col_lower or 'lng' in col_lower) and col not in geo_columns['longitude']:
             geo_columns['longitude'].append(col)
    
    # Remove empty categories
    geo_columns = {k: v for k, v in geo_columns.items() if v}
    
    return geo_columns

# --- Helper Functions for Dashboard Export ---
def get_table_html(df, title="Data Table"):
    """Generates HTML for a pandas DataFrame."""
    table_html = f"<h3>{title}</h3>\\n{df.to_html(escape=False, index=False, border=0, classes=['dataframe'])}"
    return table_html

def get_plotly_fig_html(fig, title="Chart"):
    """Generates HTML for a Plotly figure, including the JS for rendering."""
    if fig:
        fig_json = fig.to_json()
        # Using a unique div ID for each chart to avoid conflicts if multiple charts are on one page
        import uuid
        chart_id = str(uuid.uuid4())
        html = f"""
        <h3>{title}</h3>
        <div id='{chart_id}' class='plotly-chart'></div>
        <script type='text/javascript'>
            var fig_data = {fig_json};
            Plotly.newPlot('{chart_id}', fig_data.data, fig_data.layout);
        </script>
        """
        return html
    return f"<h3>{title}</h3><p>Could not render chart.</p>"

def get_kpi_embed_html(label, value, delta):
    """Generates HTML for a single KPI item for embedding."""
    value_display = f"{value:,}" if isinstance(value, (int, float)) else str(value if value is not None else "N/A")
    delta_display = f"{delta:,}" if isinstance(delta, (int, float)) else str(delta if delta is not None else "")
    delta_html = f"<div class='delta'>{delta_display}</div>" if delta_display else ""

    # Using similar styles from generate_dashboard_html for consistency
    return f"""
    <div style=\
        "background-color: #1E293B; border: 1px solid #334155; border-radius: 0.5rem; padding: 1.5rem; color: #FFFFFF; min-width: 220px; text-align: left; margin-bottom: 10px; font-family: 'Segoe UI', sans-serif;"\
    >
        <div style=\"font-size: 0.9rem; color: #94A3B8; margin-bottom: 0.3rem; text-transform: uppercase; letter-spacing: 0.05em;\">{label}</div>
        <div style=\"font-size: 2.5rem; font-weight: 700; color: #F1F5F9; line-height: 1.1;\">{value_display}</div>
        {delta_html}
    </div>
    """

def generate_dashboard_html(dashboard_items):
    """Generates a single HTML string for all dashboard items."""
    html_parts = [
        """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>DBChat Dashboard Export</title> 
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                body { 
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
                    margin: 0; 
                    padding: 20px;
                    background-color: #F0F2F6; /* Light grey background */
                    color: #333333; /* Dark text for body */
                }
                h1 { 
                    text-align: center; 
                    color: #1F2937; /* Darker text for main title */
                    margin-bottom: 30px;
                    font-size: 2.5em;
                }
                .kpi-section, .charts-section { 
                    margin-bottom: 30px; 
                }
                .kpi-container { 
                    display: flex; 
                    flex-wrap: wrap; 
                    gap: 20px; 
                    justify-content: center; 
                }
                .kpi-item {
                    background-color: #FFFFFF; /* White background for KPI cards */
                    border: 1px solid #D1D5DB; /* Light grey border */
                    border-radius: 0.5rem; 
                    padding: 1.5rem;
                    text-align: left;
                    color: #1F2937; /* Dark text for KPI content */
                    min-width: 220px; 
                    flex-basis: 250px; 
                    flex-grow: 1;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.05); /* Softer shadow for light theme */
                }
                .kpi-item .label {
                    font-size: 0.9rem;
                    color: #6B7280; /* Medium grey for KPI label */
                    margin-bottom: 0.3rem;
                    text-transform: uppercase;
                    letter-spacing: 0.05em;
                }
                .kpi-item .value {
                    font-size: 2.5rem;
                    font-weight: 700;
                    color: #4A5568; /* Darker color for KPI value, was #7C3AED (purple) */
                    line-height: 1.1;
                }
                .kpi-item .delta {
                    font-size: 0.9rem;
                    color: #10B981; /* Green for positive delta (can remain) */
                }
                .dashboard-item-container { 
                    display: grid; 
                    grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); 
                    gap: 20px; 
                }
                .dashboard-item {
                    background-color: #FFFFFF; /* White background for chart/table cards */
                    border: 1px solid #D1D5DB; /* Light grey border */
                    border-radius: 0.5rem;
                    padding: 20px; 
                    box-shadow: 0 2px 4px rgba(0,0,0,0.05);
                }
                .dashboard-item h3 { 
                    margin-top: 0; 
                    border-bottom: 1px solid #E5E7EB; /* Lighter border for h3 */
                    padding-bottom: 10px; 
                    margin-bottom: 15px; 
                    color: #1F2937; /* Darker header color */
                    font-size: 1.4em;
                }
                .plotly-chart { 
                    min-height: 400px; 
                } 
                table.dataframe { 
                    border-collapse: collapse; 
                    width: 100%; 
                    color: #374151; /* Dark text for table content */
                    border-radius: 0.375rem; 
                    overflow: hidden; 
                }
                table.dataframe th, table.dataframe td { 
                    border: 1px solid #E5E7EB; /* Lighter borders for table */
                    padding: 10px; 
                    text-align: left; 
                }
                table.dataframe th { 
                    background-color: #F3F4F6; /* Very light grey for table header */
                    color: #1F2937; /* Dark text for table header */
                    font-weight: 600;
                }
                table.dataframe tr:nth-child(even) {
                    background-color: #F9FAFB; /* Slightly off-white for zebra striping */
                }
                /* Plotly chart styles will be updated in the fig.update_layout calls below */

                .dashboard-item, .kpi-item {
                    transition: transform 0.2s ease-in-out, box-shadow 0.2s ease-in-out;
                }
                .dashboard-item:hover, .kpi-item:hover {
                    transform: translateY(-5px);
                    box-shadow: 0 6px 12px rgba(0,0,0,0.1); /* Slightly more pronounced hover shadow */
                }

            </style>
        </head>
        <body>
            <h1>DBChat Dashboard</h1>
        """
    ]

    kpi_items_html = []
    other_items_html = []

    for i, item in enumerate(dashboard_items):
        item_title = item.get('title', f"Item {i+1}: {item['chart_type']}")
        # Regenerate fig or table HTML based on stored snapshot and params, respecting filters
        data_snapshot = item['data_snapshot']
        params = item['params']
        chart_type = item['chart_type']
        filter_state = item.get('filter_state', {})
        
        filtered_data = data_snapshot.copy()
        try:
            for col, values in filter_state.items():
                if values and sorted(values) != sorted(filtered_data[col].astype(str).unique().tolist()):
                    if col in filtered_data.columns:
                        filtered_data = filtered_data[filtered_data[col].astype(str).isin(values)]
        except Exception:
            filtered_data = data_snapshot.copy()
            
        item_html_content = ""
        if chart_type == "KPI":
            kpi_label_text = params.get('label', "KPI")
            value_col = params.get('value_col')
            delta_col = params.get('delta_col')
            kpi_value_text = "N/A"
            kpi_delta_text = ""

            if not filtered_data.empty and value_col in filtered_data.columns:
                try:
                    val = pd.to_numeric(filtered_data[value_col].iloc[0])
                    kpi_value_text = f"{val:,}" if isinstance(val, (int, float)) else str(val)
                except:
                    kpi_value_text = str(filtered_data[value_col].iloc[0])
                
                if delta_col and delta_col in filtered_data.columns:
                    try:
                        delta_val = pd.to_numeric(filtered_data[delta_col].iloc[0])
                        kpi_delta_text = f"{delta_val:,}" if isinstance(delta_val, (int, float)) else str(delta_val)
                    except:
                        kpi_delta_text = str(filtered_data[delta_col].iloc[0])
            
            # KPI item HTML is now styled by the global CSS block above
            item_html_content = f"""
            <div class='kpi-item'>
                <div class='label'>{kpi_label_text}</div>
                <div class='value'>{kpi_value_text}</div>
                {f"<div class='delta'>{kpi_delta_text}</div>" if kpi_delta_text else ""}
            </div>
            """
            kpi_items_html.append(item_html_content)
        else:
            # Standard chart/table items
            try:
                item_html_output = ""
                if chart_type == "Table":
                    selected_columns = params.get('columns', filtered_data.columns.tolist())
                    table_df = filtered_data[selected_columns] if all(c in filtered_data.columns for c in selected_columns) else filtered_data
                    item_html_output = get_table_html(table_df, title=item_title) 
                else: # Plotly charts
                    fig = None
                    if chart_type == "Bar Chart": fig = px.bar(filtered_data, x=params.get('x'), y=params.get('y'), color=params.get('color'), title=item_title)
                    elif chart_type == "Line Chart": fig = px.line(filtered_data, x=params.get('x'), y=params.get('y'), color=params.get('color'), title=item_title)
                    elif chart_type == "Scatter Plot": fig = px.scatter(filtered_data, x=params.get('x'), y=params.get('y'), color=params.get('color'), size=params.get('size'), title=item_title)
                    elif chart_type == "Pie Chart": fig = px.pie(filtered_data, names=params.get('names'), values=params.get('values'), title=item_title)
                    elif chart_type == "Histogram": fig = px.histogram(filtered_data, x=params.get('x'), title=item_title)
                    elif chart_type == "Map":
                        # Map rendering is removed, provide placeholder
                        item_html_output = f"<h3>{item_title}</h3><p>Map visualization is currently not supported in this export.</p>"
                        fig = None # Ensure fig is None
                    
                    if fig: # Only proceed if fig was created (i.e., not a map or failed chart)
                        # Apply light theme to Plotly charts for HTML export
                        fig.update_layout(
                            template="plotly_white", # Use a light template
                            paper_bgcolor='rgba(255,255,255,1)', # White paper
                            plot_bgcolor='rgba(255,255,255,1)',  # White plot area
                            font_color='#333333', # Dark font for chart elements
                            title_font_color='#1F2937',
                            legend_font_color='#374151'
                        )
                        fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#E5E7EB', zerolinecolor='#D1D5DB', linecolor='#D1D5DB', tickfont=dict(color='#4B5563'))
                        fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#E5E7EB', zerolinecolor='#D1D5DB', linecolor='#D1D5DB', tickfont=dict(color='#4B5563'))
                        item_html_output = get_plotly_fig_html(fig, title=item_title) 
                    elif not item_html_output: # If fig is None and not map, chart failed
                        item_html_output = f"<h3>{item_title}</h3><p>Could not reconstruct this chart for HTML export.</p>"
                
                item_html_content = f"<div class='dashboard-item'>{item_html_output}</div>"
            except Exception as e_export:
                item_html_content = f"<div class='dashboard-item'><h3>{item_title}</h3><p>Error generating HTML for this item: {e_export}</p></div>"
            other_items_html.append(item_html_content)

    # Assemble the HTML structure
    if kpi_items_html:
        html_parts.append("<div class='kpi-section'><div class='kpi-container'>")
        html_parts.extend(kpi_items_html)
        html_parts.append("</div></div>")

    if other_items_html:
        html_parts.append("<div class='charts-section'><div class='dashboard-item-container'>")
        html_parts.extend(other_items_html)
        html_parts.append("</div></div>")

    html_parts.append("</body></html>")
    return "\n".join(html_parts)


def get_download_link_html(html_content, filename="dashboard.html"):
    """Generates a download link for HTML content."""
    b64 = base64.b64encode(html_content.encode()).decode()
    # Simple button-like styling for the download link
    return f'''
    <a href="data:text/html;base64,{b64}" download="{filename}" 
       style="display: inline-block; 
              padding: 0.6em 1.2em; 
              margin: 0.5em 0;
              font-weight: 600; 
              color: #FFFFFF; 
              background-color: #8B5CF6; /* Changed to purple */
              border-radius: 0.375rem; 
              text-decoration: none;
              text-align: center;
              transition: background-color 0.3s ease;" 
       onmouseover="this.style.backgroundColor='#7C3AED'" 
       onmouseout="this.style.backgroundColor='#8B5CF6'">
       Download HTML Dashboard
    </a>
    '''

# --- Dashboard Sharing Management Page ---
def show_dashboard_management_page():
    st.title("Manage Dashboard Sharing") 

    if not st.session_state.logged_in_user:
        st.error("Please log in to manage dashboard sharing.")
        st.session_state.page = 'login'
        st.rerun()
        return

    current_user = st.session_state.logged_in_user
    st.subheader(f"Dashboards Owned by You ({current_user})") 

    owned_dashboard_tuples = [d for d in get_user_dashboard_names(current_user) if d[2] == current_user]

    if not owned_dashboard_tuples:
        st.info("You do not own any dashboards yet. Create one from the 'My Dashboard' page.")
        if st.button("Back to My Dashboard", key="back_to_dash_from_empty_manage_sharing"):
            st.session_state.page = "dashboard"
            st.rerun()
        return

    for dash_name, display_name, owner_username in sorted(owned_dashboard_tuples, key=lambda x: x[0]):
        with st.container(): 
            st.markdown(f"#### {dash_name}")
            
            dashboard_details = database.load_dashboard_from_db(owner_username, dash_name)
            shared_with_list = []
            if dashboard_details and isinstance(dashboard_details.get('shared_with_users'), list):
                shared_with_list = dashboard_details['shared_with_users']
            
            if not shared_with_list:
                st.write("_Not currently shared with any other users._")
            else:
                st.write("Currently shared with:")
                for shared_user_idx, shared_user in enumerate(shared_with_list):
                    cols = st.columns([3,1])
                    cols[0].write(f"- {shared_user}")
                    if cols[1].button(f"Revoke Access", key=f"revoke_{dash_name}_{shared_user_idx}_{shared_user}"):
                        updated_shared_list = [u for i, u in enumerate(shared_with_list) if i != shared_user_idx]
                        if database.update_dashboard_sharing_in_db(owner_username, dash_name, updated_shared_list):
                            st.success(f"Access for '{shared_user}' to dashboard '{dash_name}' has been revoked.")
                            database.log_app_action(st.session_state.logged_in_user, "REVOKE_DASHBOARD_ACCESS_SUCCESS", f"Revoked access for {shared_user} from {dash_name}", "SUCCESS")
                            st.rerun()
                        else:
                            st.error(f"Failed to revoke access for '{shared_user}'.")
                            database.log_app_action(st.session_state.logged_in_user, "REVOKE_DASHBOARD_ACCESS_FAILURE", f"Failed to revoke access for {shared_user} from {dash_name}", "FAILURE")
            
            all_other_users_list = [u["username"] for u in database.get_all_users_from_db() if u["username"] != current_user and u["username"] not in shared_with_list]
            if all_other_users_list:
                share_with_new_users = st.multiselect(
                    "Share with additional users:", 
                    options=all_other_users_list, 
                    key=f"add_share_{dash_name}"
                )
                if st.button("Add Selected Users to Sharing", key=f"confirm_add_share_{dash_name}"):
                    if share_with_new_users:
                        newly_combined_shared_list = list(set(shared_with_list + share_with_new_users))
                        if database.update_dashboard_sharing_in_db(owner_username, dash_name, newly_combined_shared_list):
                            st.success(f"Dashboard '{dash_name}' now also shared with: {', '.join(share_with_new_users)}.")
                            database.log_app_action(st.session_state.logged_in_user, "UPDATE_DASHBOARD_SHARING_SUCCESS", f"Added users {share_with_new_users} to sharing for {dash_name}", "SUCCESS")
                            st.rerun()
                        else:
                            st.error("Failed to update sharing list.")
                            database.log_app_action(st.session_state.logged_in_user, "UPDATE_DASHBOARD_SHARING_FAILURE", f"Failed to add users {share_with_new_users} to sharing for {dash_name}", "FAILURE")
            else:
                if not shared_with_list:
                    st.write("_No other users available in the system to share with._")
            st.markdown("---")

    if st.button("Back to My Dashboard", key="back_to_dash_from_manage_sharing_main"):
        st.session_state.page = "dashboard"
        st.rerun()

def show_db_configuration_page():
    st.header("Database Configuration")
    st.warning("The application requires a PostgreSQL database connection to function.")
    st.markdown("Please provide the connection details below. These settings will be used for the current session. To make them permanent, you'll need to create or update a `secrets.toml` file in the `.streamlit` directory of your application with a `[postgres]` section.")
    st.markdown("Example `secrets.toml`:")
    st.code("""
[postgres]
host = "your_db_host"
port = 5432
dbname = "your_db_name"
user = "your_db_user"
password = "your_db_password"
""", language="toml")

    st.markdown("---")
    st.subheader("Enter Connection Details")

    # Try to get existing secrets for placeholder/hint text
    existing_secrets = database.get_db_connection_params_for_display()
    if existing_secrets is None: existing_secrets = {}

    with st.form("db_config_form"): # Use a form for batch input
        db_host = st.text_input("Host", value=existing_secrets.get("host", "localhost"))
        db_port = st.number_input("Port", value=existing_secrets.get("port", 5432), min_value=1, max_value=65535)
        db_name = st.text_input("Database Name", value=existing_secrets.get("dbname", ""))
        db_user = st.text_input("User", value=existing_secrets.get("user", ""))
        db_password = st.text_input("Password", type="password", value=existing_secrets.get("password", ""))

        submitted = st.form_submit_button("Connect and Initialize Database")

        if submitted:
            if not all([db_host, db_name, db_user]): # Password can be empty for some auth methods, port has default
                st.error("Host, Database Name, and User are required.")
            else:
                provided_params = {
                    "host": db_host,
                    "port": db_port,
                    "dbname": db_name,
                    "user": db_user,
                    "password": db_password
                }
                conn = None # Initialize conn to None
                try:
                    with st.spinner("Attempting to connect to the database..."):
                        conn = database.get_db_connection(provided_params=provided_params)
                    
                    if conn:
                        st.success("Successfully connected to the database!")
                        with st.spinner("Initializing database tables (this may take a moment)..."):
                            init_success = database.init_db(conn)
                        
                        if init_success:
                            st.success("Database tables initialized successfully.")
                            st.session_state.db_configured_successfully = True
                            # current_db_params are already set in session_state by get_db_connection
                            
                            # Now initialize app defaults like admin user
                            initialize_app_defaults() 
                            
                            st.info("Configuration successful for this session. Proceeding to login.")
                            st.balloons() # A little celebration!
                            time.sleep(2) # Give user time to see messages
                            st.session_state.page = "login"
                            st.rerun()
                        else:
                            st.error("Failed to initialize database tables. Please check console logs and database permissions.")
                            # No rollback needed here, init_db handles its own rollback on DDL error
                    else:
                        st.error(f"Failed to connect to PostgreSQL with the provided details. Please verify the parameters and ensure the database server is accessible. Error: {getattr(conn, 'pgerror', 'Unknown connection error')}")
                except Exception as e_connect_init:
                    st.error(f"An unexpected error occurred during database setup: {e_connect_init}")
                finally:
                    if conn: # Close connection if it was opened by this page
                        try:
                            conn.close()
                        except Exception: # Ignore errors on close
                            pass 

# --- LLM Settings Page ---
def show_llm_settings_page():
    st.header("LLM Configuration Settings")
    st.markdown("Configure the Large Language Model provider and credentials. These settings are stored in the database and will be used globally for the application.")

    current_config = database.load_llm_config() or {}

    providers = ["OpenAI", "Local LLM (OpenAI-Compatible API)"]
    try:
        current_provider_index = providers.index(current_config.get("provider"))
    except ValueError:
        current_provider_index = 0 # Default to OpenAI if not set or invalid

    with st.form("llm_settings_form"):
        st.write("### Provider Details")
        selected_provider = st.selectbox(
            "LLM Provider", 
            providers, 
            index=current_provider_index, 
            key="llm_provider_select"
        )
        
        api_key_val = st.text_input(
            "API Key", 
            value=current_config.get("api_key", ""),
            type="password",
            key="llm_api_key_form_input",
            help="Required for OpenAI. Optional for some local LLMs."
        )

        base_url_val = "" # Initialize to ensure it's defined
        if selected_provider == "Local LLM (OpenAI-Compatible API)":
            base_url_val = st.text_input(
                "Base URL (e.g., http://localhost:1234/v1)", 
                value=current_config.get("base_url", ""),
                key="llm_base_url_form_input",
                help="Required for Local LLM (OpenAI-Compatible API)."
            )
        
        # Placeholder for custom model name - can be expanded later
        # custom_model_val = st.text_input("Custom Model Name (Optional)", value=current_config.get("custom_model_name", ""))

        submitted = st.form_submit_button("Save LLM Settings")

        if submitted:
            error_occurred = False
            if selected_provider == "OpenAI" and not api_key_val:
                st.error("API Key is required for OpenAI.")
                error_occurred = True
            if selected_provider == "Local LLM (OpenAI-Compatible API)" and not base_url_val: # base_url_val would be empty if not Local LLM, so this condition is fine
                st.error("Base URL is required for Local LLM.")
                error_occurred = True
            
            if not error_occurred:
                final_api_key = api_key_val
                final_base_url = base_url_val if selected_provider == "Local LLM (OpenAI-Compatible API)" else None
                custom_model_to_save = current_config.get("custom_model_name")

                if database.save_llm_config(selected_provider, final_api_key, final_base_url, custom_model_to_save):
                    st.success("LLM settings saved successfully to the database!")
                    # Clear cached client. The next run of get_llm_client() (after rerun) will re-evaluate.
                    st.session_state.llm_client_instance = None 
                    # Optionally, clear the message to ensure it's definitely reconstructed, 
                    # though get_llm_client() should handle this.
                    if 'sidebar_llm_status_message' in st.session_state:
                        del st.session_state.sidebar_llm_status_message 
                    st.rerun() # Rerun to reflect status changes in sidebar and re-init LLM client
                else:
                    st.error("Failed to save LLM settings to the database. Check server logs.")
            
    st.markdown("---")
    if st.button("Back to App", key="llm_settings_back_button"):
        st.session_state.page = "app" # Or previous page if tracked
        st.rerun()

# --- PDF Generation Callback ---


if __name__ == "__main__":
    load_custom_css() # Load custom CSS globally
    # Initialize session state 'page' if not set, before main()
    # db_configured_successfully and attempted_secrets_db_init are now initialized with other session states
    if 'page' not in st.session_state:
        st.session_state.page = "login" # Default to login
    if 'logged_in_user' not in st.session_state: 
        st.session_state.logged_in_user = None
    main() 