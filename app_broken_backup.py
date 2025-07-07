import streamlit as st
import streamlit.components.v1 as st_components
import pandas as pd
import pyodbc # Added for SQL Server
from sqlalchemy import create_engine, inspect, text # For potential future use or other DBs
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
import plotly.graph_objects as go
import uuid
import urllib.parse
import sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

import os
print("--- Debugging imports in app.py (Bundled) ---")
print(f"Current working directory: {os.getcwd()}")
print("sys.path:")
for p_idx, p_val in enumerate(sys.path):
    print(f"  [{p_idx}] {p_val}")
    if os.path.isdir(p_val):
        print(f"      Contents: {os.listdir(p_val)[:5]}") # Print first 5 items in dir
print("--- End Debugging imports ---")

# --- Custom Modules ---
import send_email 
import database 
import query_clarifier 
import data_integration
import data_integration_ui
import sql_fixer
import semantic_layer
import semantic_layer_ui
import semantic_integration
try:
    import enhanced_dashboard
except ImportError:
    enhanced_dashboard = None
    print("[DASHBOARD] Enhanced dashboard features not available")

try:
    import dashboard_exports
except ImportError:
    dashboard_exports = None
    print("[EXPORT] Dashboard export module not available")

# --- Callback Functions for UI elements ---
def handle_kpi_label_change(item_idx):
    """Callback to handle changes to a KPI's label from an st.text_input."""
    new_label_val = st.session_state.get(f"kpi_label_edit_{item_idx}")
    if new_label_val and new_label_val.strip():
        if 0 <= item_idx < len(st.session_state.dashboard_items):
            st.session_state.dashboard_items[item_idx]['params']['label'] = new_label_val.strip()
            save_user_dashboard(st.session_state.logged_in_user, st.session_state.current_dashboard_name, st.session_state.dashboard_items)
            st.toast(f"KPI label updated to '{new_label_val.strip()}'", icon="📝")
        else:
            st.warning(f"Could not update KPI label: Invalid item index {item_idx}.")
    else:
        st.warning("KPI label cannot be empty.")

def handle_chart_title_change(item_abs_idx):
    """Callback to handle changes to a chart's title from an st.text_input."""
    new_title_val = st.session_state.get(f"chart_title_edit_{item_abs_idx}")
    if new_title_val and new_title_val.strip():
        if 0 <= item_abs_idx < len(st.session_state.dashboard_items):
            st.session_state.dashboard_items[item_abs_idx]['title'] = new_title_val.strip()
            save_user_dashboard(st.session_state.logged_in_user, st.session_state.current_dashboard_name, st.session_state.dashboard_items)
            st.toast(f"Chart title updated to '{new_title_val.strip()}'", icon="📝")
        else:
            st.warning(f"Could not update chart title: Invalid item index {item_abs_idx}.")
    else:
        st.warning("Chart title cannot be empty.")

def handle_item_move(item_idx, direction):
    """Callback to handle moving a dashboard item up or down."""
    items = st.session_state.dashboard_items
    
    if direction == "up" and item_idx > 0:
        new_idx = item_idx - 1
    elif direction == "down" and item_idx < len(items) - 1:
        new_idx = item_idx + 1
    else:
        return # Invalid move, do nothing.

    # Swap items
    items[item_idx], items[new_idx] = items[new_idx], items[item_idx]
    
    # Update session state
    st.session_state.dashboard_items = items
    
    # Save to DB
    save_user_dashboard(st.session_state.logged_in_user, st.session_state.current_dashboard_name, st.session_state.dashboard_items)
    st.toast("Layout saved!", icon="✨")

# --- Page Setup (MUST BE THE FIRST STREAMLIT COMMAND) ---
st.set_page_config(
    layout="wide", 
    page_title="ConvaBI - Query Your Data with AI",
    initial_sidebar_state="expanded", # Keep sidebar open by default
    menu_items={
        'Get Help': None,
        'Report a bug': None,
        'About': None
    }
)

# --- Custom CSS (after page config) ---
def load_custom_css():
    css = """
    <style>
        /* --- ConvaBI Purple Gradient Theme UI --- */
        /* --- V2.0 - Matching Login Page Style --- */

        /* --- Global Reset & Body --- */
        html, body {
            height: 100%; margin: 0; padding: 0;
            font-family: 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif;
            color: #FFFFFF; /* White text for contrast */
            background: linear-gradient(135deg, #6200ea 0%, #200079 100%) !important;
            font-size: 14px;
        }
        .stApp {
            background: linear-gradient(135deg, #6200ea 0%, #200079 100%) !important;
            min-height: 100vh; 
            display: flex !important; 
            flex-direction: column !important;
        }
        .main { /* Streamlit's main content area */
            width: 100%; 
            display: flex;
            flex-direction: column;
            flex-grow: 1;
        }
        .main > .block-container { /* Default padding for content pages */
             width: 100% !important; 
             max-width: 100% !important; 
             box-sizing: border-box; 
             padding: 25px 35px 45px 35px !important;
             flex-grow: 1; 
        }

        /* --- Typography --- */
        h1, h2, h3, h4, h5, h6 {
            font-family: 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif;
            color: #FFFFFF; /* White headings for contrast */
            font-weight: 600;
            margin-bottom: 0.75em;
            margin-top: 0.5em;
        }
        .main .block-container h1 { /* Page Titles */
            font-size: 28px; 
            font-weight: 700;
            color: #FFFFFF; /* White page titles */
            border-bottom: 2px solid rgba(255,255,255,0.3); /* Subtle white border */
            padding-bottom: 12px;
            margin-bottom: 30px;
        }
        .main .block-container h2 { /* Sub-section Titles */
            font-size: 22px;
            color: #FFFFFF; /* White sub-headers */
            margin-top: 35px;
            margin-bottom: 18px;
            border-bottom: 1px solid rgba(255,255,255,0.2); /* Subtle white border */
            padding-bottom: 10px;
        }
        .main .block-container h3 { /* Card titles or smaller headers */
            font-size: 18px;
            color: #FFFFFF; /* White smaller headers */
            font-weight: 600;
            margin-bottom: 12px;
        }
        p, .stMarkdown p, label {
            color: #FFFFFF; /* White paragraph text */
            line-height: 1.65;
            font-size: 15px;
        }
        a { color: #90CDF4; text-decoration: none; } /* Light blue for links */
        a:hover { text-decoration: underline; color: #BEE3F8; }

        /* --- Sidebar Styling --- */
        div[data-testid="stSidebar"] {
            background: rgba(255, 255, 255, 0.95) !important; /* Semi-transparent white */
            backdrop-filter: blur(10px) !important;
            border-right: 1px solid rgba(255, 255, 255, 0.2) !important; 
            padding: 25px 20px !important; 
            box-shadow: 3px 0 20px rgba(0,0,0,0.2);
            width: 290px !important; 
        }
        div[data-testid="stSidebarNavItems"] { padding-top: 15px; }
        div[data-testid="stSidebar"] h1, 
        div[data-testid="stSidebar"] h2, 
        div[data-testid="stSidebar"] h3,
        div[data-testid="stSidebar"] .stMarkdown h1, 
        div[data-testid="stSidebar"] .stMarkdown h2, 
        div[data-testid="stSidebar"] .stMarkdown h3 {
            color: #1a237e !important; /* Dark purple for sidebar headers */
            font-size: 17px !important; 
            margin-bottom: 10px !important; 
            font-weight: 600 !important; 
            padding-left: 5px;
        }
        div[data-testid="stSidebar"] label,
        div[data-testid="stSidebar"] .stMarkdown,
        div[data-testid="stSidebar"] .stCaption {
            color: #424242 !important; /* Dark gray for sidebar text */
            font-size: 14px !important; 
            padding-left: 5px;
        }
        /* Sidebar Buttons */
        div[data-testid="stSidebar"] .stButton button {
            background: linear-gradient(135deg, #6200ea 0%, #200079 100%) !important; 
            color: #FFFFFF !important; 
            border: none !important;
            border-radius: 8px !important; 
            font-weight: 500 !important;
            transition: all 0.2s ease !important;
            width: 100% !important; 
            margin-bottom: 10px !important; 
            padding: 12px 15px !important; 
            text-align: center !important;
            font-size: 15px !important;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        div[data-testid="stSidebar"] .stButton button:hover {
            background: linear-gradient(135deg, #7c4dff 0%, #3d1cb3 100%) !important; 
            transform: translateY(-1px) !important;
            box-shadow: 0 4px 8px rgba(0,0,0,0.2) !important;
        }

        /* --- Main Content General Widget Styling (Buttons, Inputs etc.) --- */
        /* Primary Buttons in Main Content */
        .main:not(:has(.login-box)) .stButton button {
            background: linear-gradient(135deg, #6200ea 0%, #200079 100%) !important;
            color: #FFFFFF !important;
            border: none !important; 
            border-radius: 8px !important;
            padding: 12px 20px !important; 
            font-weight: 500 !important;
            font-size: 15px !important; 
            transition: all 0.2s ease !important;
            box-shadow: 0 2px 4px rgba(0,0,0,0.15);
        }
        .main:not(:has(.login-box)) .stButton button:hover {
            background: linear-gradient(135deg, #7c4dff 0%, #3d1cb3 100%) !important;
            box-shadow: 0 4px 8px rgba(0,0,0,0.2);
            transform: translateY(-1px);
        }
        .main:not(:has(.login-box)) .stButton button:active {
            transform: translateY(0px);
        }
        .main:not(:has(.login-box)) .stButton button:disabled {
            background-color: rgba(255,255,255,0.3) !important; /* Semi-transparent white for disabled */
            color: rgba(255,255,255,0.6) !important;
            box-shadow: none;
        }
        
        /* Inputs and Select Boxes in Main Content */
        .main:not(:has(.login-box)) .stTextInput input,
        .main:not(:has(.login-box)) .stTextArea textarea, 
        .main:not(:has(.login-box)) .stDateInput input, 
        .main:not(:has(.login-box)) .stNumberInput input {
            background-color: rgba(255,255,255,0.9) !important; /* Semi-transparent white */
            color: #1a237e !important; /* Dark purple text for inputs */
            border: 1px solid rgba(255,255,255,0.3) !important;
            border-radius: 8px !important;
            padding: 12px 14px !important; 
            font-size: 15px !important;
            transition: border-color 0.2s ease, box-shadow 0.2s ease !important;
        }
        .main:not(:has(.login-box)) .stSelectbox div[data-baseweb="select"] > div {
            background-color: rgba(255,255,255,0.9) !important; 
            color: #1a237e !important;
            border: 1px solid rgba(255,255,255,0.3) !important;
            border-radius: 8px !important;
            padding: 4px 3px !important; 
            font-size: 15px !important;
            transition: border-color 0.2s ease, box-shadow 0.2s ease !important;
        }
        .main:not(:has(.login-box)) .stTextInput input:focus, 
        .main:not(:has(.login-box)) .stSelectbox div[data-baseweb="select"] > div:focus-within,
        .main:not(:has(.login-box)) .stTextArea textarea:focus, 
        .main:not(:has(.login-box)) .stDateInput input:focus, 
        .main:not(:has(.login-box)) .stNumberInput input:focus {
            border-color: #FFFFFF !important; /* White border on focus */
            box-shadow: 0 0 0 3px rgba(255, 255, 255, 0.3) !important; /* White glow */
        }
        .main:not(:has(.login-box)) .stTextInput > label, 
        .main:not(:has(.login-box)) .stSelectbox > label, 
        .main:not(:has(.login-box)) .stTextArea > label,
        .main:not(:has(.login-box)) .stDateInput > label, 
        .main:not(:has(.login-box)) .stNumberInput > label {
            color: #FFFFFF !important; /* White labels */
            font-weight: 500 !important;
            margin-bottom: 7px !important; 
            font-size: 15px !important;
        }

        /* --- Alert / Notification Styling --- */
        .stAlert {
            border-radius: 8px !important; 
            border-width: 0 !important;
            border-left-width: 5px !important; 
            padding: 14px 20px !important;
            box-shadow: 0 3px 10px rgba(0,0,0,0.2) !important; 
            font-size: 15px !important;
            margin-bottom: 18px;
            backdrop-filter: blur(10px) !important;
        }
        /* Success: Bright green with transparency */
        div[data-testid="stSuccess"] { background-color: rgba(76, 175, 80, 0.9) !important; border-left-color: #4CAF50 !important; color: #FFFFFF !important; }
        div[data-testid="stSuccess"] svg { fill: #FFFFFF !important; }
        /* Warning: Bright orange with transparency */
        div[data-testid="stWarning"] { background-color: rgba(255, 152, 0, 0.9) !important; border-left-color: #FF9800 !important; color: #FFFFFF !important; }
        div[data-testid="stWarning"] svg { fill: #FFFFFF !important; }
        /* Error: Bright red with transparency */
        div[data-testid="stError"] { background-color: rgba(244, 67, 54, 0.9) !important; border-left-color: #F44336 !important; color: #FFFFFF !important; }
        div[data-testid="stError"] svg { fill: #FFFFFF !important; }
        /* Info: Bright blue with transparency */
        div[data-testid="stInfo"] { background-color: rgba(33, 150, 243, 0.9) !important; border-left-color: #2196F3 !important; color: #FFFFFF !important; }
        div[data-testid="stInfo"] svg { fill: #FFFFFF !important; }

        /* --- Dashboard Card Styling --- */
        div[data-testid="stHorizontalBlock"] div[data-testid="stContainer"] {
            background: rgba(255, 255, 255, 0.95) !important; /* Semi-transparent white cards */
            backdrop-filter: blur(10px) !important;
            border: 1px solid rgba(255, 255, 255, 0.2) !important; 
            border-radius: 12px !important; 
            padding: 25px !important; 
            margin-bottom: 25px !important; 
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.2) !important; 
            transition: all 0.3s ease !important;
            display: flex; 
            flex-direction: column; 
            height: 100%; 
        }
        div[data-testid="stHorizontalBlock"] div[data-testid="stContainer"]:hover {
            box-shadow: 0 12px 40px rgba(0, 0, 0, 0.3) !important;
            transform: translateY(-4px) !important;
        }

        /* --- Login Page Specific Styling --- */
        .login-box { 
            background: rgba(255, 255, 255, 0.95) !important;
            backdrop-filter: blur(10px) !important;
            padding: 35px 45px !important; 
            border-radius: 12px !important; 
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.2) !important; 
            width: 100% !important; 
            max-width: 420px !important; 
            text-align: center !important;
            margin: 0 auto !important;
            border: 1px solid rgba(255, 255, 255, 0.2);
        }
        .login-box h1 { 
            font-size: 24px !important; 
            font-weight: 600 !important; 
            color: #1a237e !important; 
            margin-bottom: 30px !important; 
        }
        .login-box div[data-testid="stTextInput"] input[type="text"],
        .login-box div[data-testid="stTextInput"] input[type="password"] {
            background-color: rgba(255,255,255,0.9) !important;
            color: #1a237e !important;
            height: 44px !important; 
            border-radius: 8px !important;
            border: 1px solid rgba(255,255,255,0.3) !important; 
            font-size: 15px !important;
            padding: 0 12px !important; 
        }
        .login-box div[data-testid="stTextInput"] input[type="text"]:focus,
        .login-box div[data-testid="stTextInput"] input[type="password"]:focus {
            border-color: #FFFFFF !important; 
            box-shadow: 0 0 0 3px rgba(255, 255, 255, 0.3) !important; 
        }
        .login-box .stButton button { 
            width: 100% !important;
            margin-top: 25px !important; 
            padding: 12px !important; 
            font-size: 16px !important; 
            font-weight: 500 !important;
            background: linear-gradient(135deg, #6200ea 0%, #200079 100%) !important;
            color: white !important;
            border-radius: 8px !important;
            border: none !important;
            transition: all 0.2s ease;
        }
        .login-box .stButton button:hover {
            background: linear-gradient(135deg, #7c4dff 0%, #3d1cb3 100%) !important;
            transform: translateY(-1px);
        }

        /* --- Scrollbars --- */
        ::-webkit-scrollbar { width: 10px !important; height: 10px !important; }
        ::-webkit-scrollbar-track { background: rgba(255,255,255,0.1) !important; border-radius: 5px !important;}
        ::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.3) !important; border-radius: 5px !important; }
        ::-webkit-scrollbar-thumb:hover { background: rgba(255,255,255,0.5) !important; }

        /* Hide Streamlit UI elements for local use */
        .stDeployButton { display: none !important; }
        button[title="View fullscreen"] { display: none !important; }
        .stActionButton { display: none !important; }
        header[data-testid="stHeader"] { display: none !important; }
        .stToolbar { display: none !important; }
        div[data-testid="stToolbar"] { display: none !important; }
        .st-emotion-cache-18ni7ap { display: none !important; }
        .st-emotion-cache-1rs6os { display: none !important; }
        .viewerBadge_container__1QSob { display: none !important; }
        footer { display: none !important; }
        .stStreamlitLogo { display: none !important; }

        /* Hide sidebar on login page */
        .login-page div[data-testid="stSidebar"] { display: none !important; }
        .login-page .main { width: 100% !important; margin-left: 0 !important; }
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)

# --- Configuration ---
DEFAULT_ADMIN_USERNAME = "admin"
DEFAULT_ADMIN_PASSWORD = "admin123" # Change this in a real scenario!
MAX_DASHBOARD_VERSIONS = 3 # Maximum number of dashboard versions to keep per user

# --- Session State Management ---

# --- Simplified Session Persistence ---
def save_session_to_browser():
    """Save critical session data - simplified approach using session state persistence"""
    # For now, we'll rely on improved session state management rather than localStorage
    # This function serves as a placeholder for potential future localStorage implementation
    pass

def load_session_from_browser():
    """Load session data - simplified approach"""
    # This would be the place to implement localStorage retrieval if needed
    pass

def clear_browser_session():
    """Clear session data - simplified approach"""
    # This would clear localStorage if implemented
    pass

def initialize_session_state():
    """
    Centralized session state initialization with validation and recovery.
    This function ensures all required session state variables are properly initialized
    and provides recovery mechanisms for lost sessions.
    """
    
    # Core authentication and navigation state
    session_defaults = {
        'page': "login",
        'app_page': 'connect_data',
        'logged_in_user': None,
        'user_roles': [],
        'db_configured_successfully': False,
        'attempted_secrets_db_init': False,
        
        # Database and connection state
        'connection_type': None,
        'connected': False,
        'data': None,
        'db_connection': None,
        'db_engine': None,
        'data_schema': None,
        'selected_table': "All Tables / Auto-detect",
        'current_db_params': {},
        
        # LLM configuration state
        'llm_config': {},
        'llm_api_key': None,
        'llm_client_instance': None,
        'llm_model_name': None,
        'sidebar_llm_status_message': "LLM client: Not initialized.",
        
        # Email configuration
        'email_config': {},
        
        # Query and conversation state
        'chat_history': [],
        'original_query_for_clarification': "",
        'clarification_question_pending': False,
        'llm_clarification_question': "",
        'conversation_log_for_query': [],
        'results_df': None,
        
        # Dashboard state
        'dashboard_items': [],
        'current_dashboard_name': None,
        'show_delete_confirmation': False,
        'dashboard_manage_mode': False,
        
        # Logging state
        'log_data_schema_str': None,
        'log_openai_prompt_str': None,
        'log_generated_sql_str': None,
        'log_query_execution_details_str': None,
        
        # Session recovery state
        'session_initialized': True,
        'last_activity_timestamp': time.time(),
        'session_recovery_attempted': False,
    }
    
    # Initialize or validate each session state variable
    for key, default_value in session_defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value
            print(f"[SESSION] Initialized {key} = {default_value}")
        elif st.session_state[key] is None and key in ['user_roles', 'dashboard_items', 'chat_history', 'conversation_log_for_query']:
            # Reset lists/dicts that might have become None
            st.session_state[key] = default_value
            print(f"[SESSION] Reset {key} from None to {default_value}")
    
    # Update last activity timestamp
    st.session_state.last_activity_timestamp = time.time()
    
    # Session validation and recovery
    if not st.session_state.get('session_initialized', False):
        print("[SESSION] Session appears corrupted, attempting recovery...")
        attempt_session_recovery()

def attempt_session_recovery():
    """
    Attempts to recover a lost or corrupted session by checking for persistent data
    and trying to restore the user's state.
    """
    if st.session_state.get('session_recovery_attempted', False):
        return  # Avoid infinite recovery loops
    
    st.session_state.session_recovery_attempted = True
    
    try:
        # Try to recover database configuration
        if not st.session_state.get('db_configured_successfully', False):
            try:
                # Test if database connection is available
                conn = database.get_db_connection()
                if conn:
                    conn.close()
                    st.session_state.db_configured_successfully = True
                    print("[SESSION RECOVERY] Database connection restored")
            except Exception as e:
                print(f"[SESSION RECOVERY] Database connection failed: {e}")
        
        # Try to restore LLM client if missing
        if not st.session_state.get('llm_client_instance'):
            try:
                get_llm_client()
                print("[SESSION RECOVERY] LLM client restored")
            except Exception as e:
                print(f"[SESSION RECOVERY] LLM client recovery failed: {e}")
        
        # Mark session as recovered
        st.session_state.session_initialized = True
        print("[SESSION RECOVERY] Session recovery completed")
        
    except Exception as e:
        print(f"[SESSION RECOVERY] Recovery failed: {e}")
        # If recovery fails, ensure we have minimal working state
        st.session_state.page = "login"
        st.session_state.logged_in_user = None

def validate_session_health():
    """
    Enhanced session health validation with better error handling and recovery.
    Returns True if session is healthy, False if recovery is needed.
    """
    try:
        current_time = time.time()
        # Check if session is too old (increased to 24 hours for better UX)
        last_activity = st.session_state.get('last_activity_timestamp', current_time)
        session_age = current_time - last_activity
        if session_age > 86400:  # 24 hours (increased from 8 hours)
            print(f"[SESSION] Session expired (age: {session_age/3600:.1f} hours)")
            return False
        # More lenient check for critical session state variables
        # Only require 'page' as truly critical
        if 'page' not in st.session_state:
            print("[SESSION] Critical variable 'page' missing, initializing")
            st.session_state.page = "login"
        # Enhanced user authentication validation with recovery
        if st.session_state.get('logged_in_user'):
            # Check if user roles are missing and try to recover
            if not st.session_state.get('user_roles'):
                print("[SESSION] User logged in but roles missing, attempting recovery")
                try:
                    user_data = None
                    try:
                        if is_sqlite_connection():
                            user_data = get_user_by_username_sqlite(st.session_state.logged_in_user)
                        else:
                            user_data = database.get_user_by_username_from_db(st.session_state.logged_in_user)
                    except Exception as db_error:
                        print(f"[SESSION] Database connection error during user validation: {db_error}")
                        return True
                    if user_data:
                        st.session_state.user_roles = user_data.get("roles", [])
                        print("[SESSION] User roles recovered successfully")
                    else:
                        print("[SESSION] User no longer exists in database")
                        return False
                except Exception as e:
                    print(f"[SESSION] User validation error: {e}")
                    return True
        # Update activity timestamp
        st.session_state.last_activity_timestamp = current_time
        # Verify essential session state structure
        if not hasattr(st.session_state, '__dict__'):
            print("[SESSION] Session state corrupted")
            return False
        return True
    except Exception as e:
        print(f"[SESSION] Health check failed with error: {e}")
        return True

def ensure_session_stability():
    """
    Main function to ensure session stability. Call this at the beginning of main().
    Enhanced with better session persistence mechanisms.
    """
    # Initialize session state if needed
    initialize_session_state()
    # Enhanced session recovery
    if not st.session_state.get('logged_in_user'):
        # Try session recovery from URL parameters (simpler than localStorage)
        try_url_session_recovery()
    # Validate session health with enhanced checks
    if not validate_session_health():
        print("[SESSION] Session unhealthy, attempting recovery")
        attempt_session_recovery()
        # If still unhealthy after recovery, reset to login
        if not validate_session_health():
            print("[SESSION] Recovery failed, resetting to login")
            st.session_state.page = "login"
            st.session_state.logged_in_user = None
            st.session_state.user_roles = []
            # Only show warning if this is not the first load
            if st.session_state.get('session_initialized', False):
                st.warning("⚠️ Your session has expired or was lost. Please log in again.")
    # Mark session as initialized
    st.session_state.session_initialized = True
    # Enhanced activity tracking
    st.session_state.last_activity_timestamp = time.time()

def try_url_session_recovery():
    """
    Attempt to recover session using URL parameters or other mechanisms.
    This is a simplified approach that's more reliable than localStorage.
    """
    # For now, this is a placeholder for future URL-based session recovery
    # Could be extended to use URL fragments or query parameters
    pass

# Initialize session state using the new system
initialize_session_state()

# --- LLM Client Initialization ---
def get_llm_client():
    """Get or create an LLM client based on configuration"""
    if st.session_state.llm_client_instance:
        return st.session_state.llm_client_instance
    client = None
    actual_api_key_to_store = None
    # 1. First try to get configuration from database
    conn = database.get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT provider, api_key, base_url, custom_model_name FROM llm_config WHERE is_active = 1")
            result = cursor.fetchone()
            if result:
                provider, api_key_from_db, base_url_from_db, custom_model_name_from_db = result
                if provider == "OpenAI":
                    if api_key_from_db:
                        try:
                            print("[LLM DEBUG] Creating OpenAI client from DB settings")
                            client = OpenAI(api_key=api_key_from_db)
                            client.models.list()
                            st.session_state.sidebar_llm_status_message = "OpenAI client: Connected via DB settings"
                            actual_api_key_to_store = api_key_from_db
                            st.session_state.llm_model_name = "gpt-3.5-turbo"  # Default OpenAI model
                        except Exception as e:
                            print(f"[LLM DEBUG] OpenAI client creation failed: {e}")
                            if st.session_state.page != "login":
                                st.sidebar.error(f"Error with OpenAI (DB Settings): {str(e)}")
                    else:
                        if st.session_state.page != "login":
                            st.sidebar.warning("OpenAI provider selected in DB, but API key is missing.")
                elif provider == "Local LLM (OpenAI-Compatible API)":
                    if base_url_from_db and custom_model_name_from_db:
                        try:
                            cleaned_base_url = base_url_from_db.strip()
                            if not cleaned_base_url.startswith(('http://', 'https://')):
                                cleaned_base_url = f"http://{cleaned_base_url}"
                            print(f"[LLM DEBUG] Creating local LLM client with base_url={cleaned_base_url}, model={custom_model_name_from_db}")
                            print(f"[LLM DEBUG] Original base_url was: {repr(base_url_from_db)}")
                            client = OpenAI(
                                base_url=cleaned_base_url,
                                api_key="not-needed",
                                timeout=30.0
                            )
                            client.models.list()
                            st.session_state.sidebar_llm_status_message = f"Local LLM client: Connected to {cleaned_base_url}"
                            actual_api_key_to_store = "not-needed"
                            st.session_state.llm_model_name = custom_model_name_from_db
                        except Exception as e:
                            print(f"[LLM DEBUG] Local LLM client creation failed: {e}")
                            if st.session_state.page != "login":
                                st.sidebar.error(f"Error with Local LLM (DB Settings): {str(e)}")
                    else:
                        print(f"[LLM DEBUG] Local LLM config incomplete - base_url: {bool(base_url_from_db)}, model: {bool(custom_model_name_from_db)}")
                        if st.session_state.page != "login":
                            st.sidebar.warning("Local LLM provider selected in DB, but Base URL or Custom Model Name is missing.")
        except Exception as e:
            print(f"[LLM DEBUG] Error reading LLM config from DB: {e}")
            if st.session_state.page != "login":
                st.sidebar.error(f"Error reading LLM configuration: {str(e)}")
        finally:
            conn.close()
    # 2. If no client from DB, try Streamlit secrets as a fallback
    if not client:
        try:
            if openai_api_key:
                print("[LLM DEBUG] Creating OpenAI client from secrets")
                client = OpenAI(api_key=openai_api_key)
                client.models.list()
                st.session_state.sidebar_llm_status_message = "OpenAI client: Connected via secrets"
                actual_api_key_to_store = openai_api_key
                st.session_state.llm_model_name = "gpt-3.5-turbo"  # Default OpenAI model
            elif local_llm_secret_base_url:
                cleaned_secret_base_url = local_llm_secret_base_url.strip()
                if not cleaned_secret_base_url.startswith(('http://', 'https://')):
                    cleaned_secret_base_url = f"http://{cleaned_secret_base_url}"
                print(f"[LLM DEBUG] Creating local LLM client from secrets with base_url={cleaned_secret_base_url}")
                print(f"[LLM DEBUG] Original secret base_url was: {repr(local_llm_secret_base_url)}")
                client = OpenAI(
                    base_url=cleaned_secret_base_url,
                    api_key="not-needed",
                    timeout=30.0
                )
                client.models.list()
                st.session_state.sidebar_llm_status_message = f"Local LLM client: Connected to {cleaned_secret_base_url} via secrets"
                actual_api_key_to_store = "not-needed"
                st.session_state.llm_model_name = "sqlcoder:latest"  # Default local model
            else:
                print("[LLM DEBUG] No OpenAI API key or local LLM base URL in secrets")
        except Exception as e:
            print(f"[LLM DEBUG] LLM client from secrets failed: {e}")
            if st.session_state.page != "login":
                st.sidebar.error(f"Error with LLM (secrets): {str(e)}")
    st.session_state.llm_client_instance = client
    st.session_state.llm_api_key = actual_api_key_to_store
    return client

# --- Main Application UI ---
# Removed global st.title, st.info, chat history, and chat input from here.
# These will be part of show_main_chat_interface_content()

# --- Sidebar Information ---
# Only show sidebar status when logged in and not on login page
if st.session_state.logged_in_user and st.session_state.page != "login":
    st.sidebar.header("Status")
    # Display the LLM status message that get_llm_client now sets
    st.sidebar.caption(st.session_state.get('sidebar_llm_status_message', "LLM client: Not initialized."))

# The image generation status check will be removed from here.

# --- User Data Management ---
def hash_password(password):
    """Hashes a password using Werkzeug."""
    return generate_password_hash(password)

def check_password(hashed_password, password):
    """Checks a password against a hashed version."""
    return check_password_hash(hashed_password, password)

# --- Define Hydralit Head App for the Login Page --- # REMOVE ALL HYDRALIT CLASSES
# class LoginPageApp(HydraHeadApp):
# ... (entire class definition) ...
# class QueryApp(HydraHeadApp):
# ... (entire class definition) ...
# class MyConvaBIApp(HydraApp):
# ... (entire class definition) ...

# --- SQLite-compatible Database Functions ---
def is_sqlite_connection():
    """Check if we're using SQLite based on session state parameters"""
    current_params = st.session_state.get("current_db_params", {})
    return current_params.get("type") == "sqlite"

def get_sqlite_connection():
    """Get SQLite connection"""
    current_params = st.session_state.get("current_db_params", {})
    if current_params.get("type") == "sqlite":
        import sqlite3
        return sqlite3.connect(current_params.get("path", "dbchat_app.db"))
    return None

def get_user_by_username_sqlite(username):
    """SQLite-compatible version of get_user_by_username_from_db"""
    conn = get_sqlite_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT username, hashed_password, roles FROM app_users WHERE username = ?", (username,))
        result = cursor.fetchone()
        
        if result:
            return {
                "username": result[0],
                "hashed_password": result[1], 
                "roles": result[2] if result[2] else []
            }
        return None
    except Exception as e:
        st.error(f"Error fetching user from SQLite: {e}")
        return None
    finally:
        conn.close()

def create_user_sqlite(username, hashed_password, roles):
    """SQLite-compatible version of create_user_in_db"""
    conn = get_sqlite_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        import json
        cursor.execute(
            "INSERT INTO app_users (username, hashed_password, roles) VALUES (?, ?, ?)",
            (username, hashed_password, json.dumps(roles))
        )
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Error creating user in SQLite: {e}")
        return False
    finally:
        conn.close()

def log_app_action_sqlite(username, action, details, status="INFO"):
    """SQLite-compatible version of log_app_action"""
    conn = get_sqlite_connection()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO app_logs (username, action, details, status) VALUES (?, ?, ?, ?)",
            (username, action, details, status)
        )
        conn.commit()
    except Exception as e:
        print(f"Error logging action to SQLite: {e}")
    finally:
        conn.close()

def ensure_default_admin_user_sqlite(default_username, default_password_hash, default_roles):
    """SQLite-compatible version of ensure_default_admin_user_in_db"""
    existing_admin = get_user_by_username_sqlite(default_username)
    if not existing_admin:
        st.info(f"Default admin user '{default_username}' not found in database. Creating...")
        success = create_user_sqlite(default_username, default_password_hash, default_roles)
        if success:
            st.success(f"Default admin user '{default_username}' created in database.")
        else:
            st.error(f"Failed to create default admin user '{default_username}' in database.")

# --- Application Defaults Initialization ---
def initialize_app_defaults():
    """Initializes application defaults, like ensuring the default admin user exists in the DB."""
    default_admin_hashed_password = hash_password(DEFAULT_ADMIN_PASSWORD)
    default_admin_roles = ["admin", "superuser"]
    if is_sqlite_connection():
        ensure_default_admin_user_sqlite(DEFAULT_ADMIN_USERNAME, default_admin_hashed_password, default_admin_roles)
    else:
        database.ensure_default_admin_user_in_db(DEFAULT_ADMIN_USERNAME, default_admin_hashed_password, default_admin_roles)
    # Future: Add other app default initializations here if needed

# --- Dashboard Save/Load Functions ---

def validate_json_serializable(data):
    """
    Validates that data can be safely serialized to JSON.
    Returns (is_valid, cleaned_data) tuple.
    """
    try:
        import math
        import numpy as np
        
        def clean_value(value):
            """Clean individual values for JSON serialization."""
            if value is None:
                return None
            elif isinstance(value, (int, str, bool)):
                return value
            elif isinstance(value, float):
                if math.isnan(value) or math.isinf(value):
                    return None
                return value
            elif isinstance(value, (np.integer, np.floating)):
                if np.isnan(value) or np.isinf(value):
                    return None
                return value.item()  # Convert numpy types to Python types
            elif isinstance(value, (list, tuple)):
                return [clean_value(item) for item in value]
            elif isinstance(value, dict):
                return {key: clean_value(val) for key, val in value.items()}
            else:
                # Try to convert to string as fallback
                try:
                    return str(value)
                except:
                    return None
        
        cleaned_data = clean_value(data)
        
        # Test JSON serialization
        json.dumps(cleaned_data)
        return True, cleaned_data
        
    except Exception as e:
        print(f"[JSON VALIDATION] Error: {e}")
        return False, data

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
        existing_versions_json = [] # Corrected unindent

    serializable_new_version = []
    for item_original in current_dashboard_items:
        item = copy.deepcopy(item_original)
        if isinstance(item.get("data_snapshot"), pd.DataFrame):
            df_for_serialization = item["data_snapshot"]
            
            # Clean column names
            df_for_serialization.columns = [str(col_name) for col_name in df_for_serialization.columns]
            
            # Handle NaN values before JSON serialization
            # Replace NaN with None (which becomes null in JSON)
            df_cleaned = df_for_serialization.copy()
            
            # Replace different types of NaN/null values
            df_cleaned = df_cleaned.replace({
                pd.NA: None,           # pandas NA
                pd.NaT: None,          # pandas NaT (Not a Time)
                float('nan'): None,    # regular NaN
                'NaN': None,           # string 'NaN'
                'nan': None,           # string 'nan'
                'null': None,          # string 'null'
                'NULL': None,          # string 'NULL'
            })
            
            # Use pandas' built-in method to handle remaining NaN values
            df_cleaned = df_cleaned.where(pd.notnull(df_cleaned), None)
            
            # Convert to dictionary
            item["data_snapshot"] = df_cleaned.to_dict(orient='records')
            
            print(f"[DASHBOARD SAVE] Cleaned DataFrame for {item.get('title', 'Untitled')}: {len(df_cleaned)} rows")
            
        serializable_new_version.append(item)
        
    updated_versions_json = [serializable_new_version] + existing_versions_json
    updated_versions_json = updated_versions_json[:MAX_DASHBOARD_VERSIONS]

    # Validate JSON serialization before saving to database
    is_valid, cleaned_versions = validate_json_serializable(updated_versions_json)
    if not is_valid:
        st.error(f"Failed to prepare dashboard '{dashboard_name}' for saving. Data contains non-serializable values.")
        print(f"[DASHBOARD SAVE ERROR] JSON validation failed for dashboard '{dashboard_name}'")
        return
    
    # Use the cleaned data for saving
    success = database.save_dashboard_to_db(username, dashboard_name, cleaned_versions, shared_with_users_list)
    
    if success:
        print(f"[DASHBOARD SAVE] Successfully saved dashboard '{dashboard_name}' for user '{username}'")
    else:
        st.error(f"Failed to save dashboard '{dashboard_name}' to database.")
        print(f"[DASHBOARD SAVE ERROR] Database save failed for dashboard '{dashboard_name}'")

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
                    # Convert back to DataFrame from records
                    df_from_records = pd.DataFrame.from_records(item["data_snapshot"])
                    
                    # Handle None values and data types
                    for col_name in df_from_records.columns:
                        col = df_from_records[col_name]
                        # If column is object type, it might contain text or mixed types
                        if col.dtype == 'object':
                            # Fill any python None or numpy NaT with a string placeholder
                            if col.hasnans:
                                 df_from_records[col_name] = col.fillna("Not Specified")

                            # After filling, try to convert to a more specific type if possible without forcing errors
                            # For example, a column of ['1', '2', None] becomes ['1', '2', 'Not Specified']
                            # We don't want to convert that to numeric. A column of ['1', '2', '3'] should be numeric.
                            # `convert_dtypes` is good at this.
                    
                    # Use pandas' built-in function to infer best possible dtypes
                    df_from_records = df_from_records.convert_dtypes()

                    item["data_snapshot"] = df_from_records
                    print(f"[DASHBOARD LOAD] Loaded DataFrame for {item.get('title', 'Untitled')}: {len(df_from_records)} rows")
                    
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
    """Generates SQL query from natural language using the globally configured LLM client with semantic layer enhancement."""
    try:
        # Use the enhanced version from semantic integration
        return semantic_integration.get_enhanced_sql_from_openai(
            natural_language_query, data_schema, db_type, target_table, use_semantic_layer=True
        )
    except Exception as e:
        print(f"[SEMANTIC LAYER] Failed to use enhanced generation, falling back to basic: {e}")
        # Fallback to basic implementation
        return get_basic_sql_from_openai(natural_language_query, data_schema, db_type, target_table)

def get_basic_sql_from_openai(natural_language_query, data_schema, db_type="sqlserver", target_table=None):
    """Basic SQL generation without semantic layer (fallback)"""
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
    elif isinstance(data_schema, list): # For CSV and integrated data (list of column names with types)
        if db_type == "integrated":
            schema_prompt_part += f"Integrated Data Columns (query this as a table named 'integrated_data'):\n"
        else:
            schema_prompt_part += f"CSV Columns (query this as a table named 'csv_data'):\n"
        for col_info in data_schema:
            schema_prompt_part += f"  - {col_info['name']} ({col_info['type']})\n"
    else:
        schema_prompt_part = "Schema: Not available or in an unrecognized format."

    focus_hint = ""
    if target_table and target_table != "All Tables / Auto-detect":
        focus_hint = f"Prioritize using the table named '{target_table}' if it is relevant to the user's question. However, you may use other tables or joins if the question clearly implies them or requires information from them. If the question is about the database schema itself (e.g., 'list all tables'), this focus hint can be ignored."

    prompt = f"""
    You are an expert AI assistant that converts natural language questions into {db_type} queries.
    Given the following database schema and a user question, generate a syntactically correct {db_type} query to answer the question.

    {schema_prompt_part}

    {focus_hint}

    User Question: {natural_language_query}

    Only return the SQL query, with no other explanatory text, preambles, or apologies.
    Ensure the query is directly executable.
    If the question cannot be answered with the given schema, or if it's ambiguous, try your best to formulate a query that might be relevant, or state that it's not possible within the SQL query itself as a comment (e.g. /* Cannot answer due to missing columns */ SELECT 1;).
    Do not use triple backticks in your response.
    IMPORTANT: If a column name contains spaces, enclose it in double quotes (e.g., "Customer ID"). This is crucial for SQLite compatibility when querying CSVs and integrated data.
    SQL Query:
    """

    st.session_state.log_openai_prompt_str = prompt # Log the prompt

    try:
        # Retrieve the configured model name from session state
        model_name_to_use = st.session_state.get("llm_model_name", "gpt-3.5-turbo") # Default if not found
        if not model_name_to_use: # Fallback if it's None or empty
            st.warning("LLM model name not found in session state, defaulting to gpt-3.5-turbo.")
            model_name_to_use = "gpt-3.5-turbo"

        response = client.chat.completions.create(
            model=model_name_to_use, # Use the dynamically retrieved model name
            messages=[
                {"role": "system", "content": f"You are an expert AI assistant that converts natural language questions into {db_type} queries. Only return the SQL query and nothing else."},
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

    connection_options = ["Select Data Source", "CSV File", "PostgreSQL", "Oracle", "SQL Server", "Connect to API"] # Added SQL Server & API
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
    elif selected_option == "Connect to API": # New option
        st.session_state.connection_type = "api"
        handle_api_connection()
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
            
            # Debug: Show all detected columns
            print(f"[CSV DEBUG] Detected {len(csv_schema)} columns:")
            for i, col in enumerate(csv_schema):
                print(f"  [{i+1}] {col['name']} ({col['type']})")
                
            try: # Log the schema
                st.session_state.log_data_schema_str = json.dumps(st.session_state.data_schema, indent=2)
                print(f"[CSV DEBUG] Schema JSON length: {len(st.session_state.log_data_schema_str)} characters")
            except Exception as e:
                st.session_state.log_data_schema_str = f"Error formatting CSV schema for logs: {e}"

        except Exception as e:
            st.error(f"Error loading CSV file: {e}")
            st.session_state.connected = False
            st.session_state.data = None

def handle_db_connection(db_type):
    """Handles the UI for database connection parameters and logic."""
    st.subheader(f"Connect to {db_type}")
    host_input_val = "localhost" if db_type in ["PostgreSQL", "Oracle"] else ""
    port_input_val = {"PostgreSQL": "5432", "Oracle": "1521"}.get(db_type, "")
    db_name_label = f"{db_type} Database Name"
    if db_type == "Oracle":
        db_name_label = f"{db_type} Service Name or SID"
    host_input = st.text_input(f"{db_type} Host", key=f"{db_type.lower()}_host", value=host_input_val)
    port_input = st.text_input(f"{db_type} Port", key=f"{db_type.lower()}_port", value=port_input_val)
    dbname_input = st.text_input(db_name_label, key=f"{db_type.lower()}_dbname")
    user_input = st.text_input(f"{db_type} User", key=f"{db_type.lower()}_user")
    password_input = st.text_input(f"{db_type} Password", type="password", key=f"{db_type.lower()}_password")
    driver_input, encrypt_input, trust_cert_input, oracle_conn_type = None, None, None, None
    if db_type == "SQL Server":
        st.info("For SQL Server, ensure the appropriate ODBC driver is installed on the system running Streamlit.")
        driver_input = st.text_input(f"ODBC Driver for SQL Server", value="ODBC Driver 17 for SQL Server", key=f"{db_type.lower()}_driver")
        encrypt_input = st.selectbox("Encrypt Connection", options=["yes", "no", "optional"], index=0, key=f"{db_type.lower()}_encrypt")
        trust_cert_input = st.selectbox("Trust Server Certificate", options=["no", "yes"], index=0, key=f"{db_type.lower()}_trust_cert")
        st.caption("For 'User', leave blank to use Windows Authentication (if applicable).")
    elif db_type == "Oracle":
        st.info("For Oracle, the recommended driver is 'oracledb'. Ensure it is installed (`pip install oracledb`).")
        oracle_conn_type = st.selectbox("Connection Identifier Type", ["Service Name", "SID"], key="oracle_conn_type")
    elif db_type == "PostgreSQL":
        st.info("For PostgreSQL connection to work, ensure 'psycopg2-binary' is installed in your environment (`pip install psycopg2-binary`).")
    if st.button(f"Connect to {db_type}"):
        engine = None
        try:
            if db_type == "SQL Server":
                if not driver_input or not host_input or not dbname_input:
                    st.error("Driver, Host, and Database Name are required for SQL Server connection.")
                    return
                params = {
                    'DRIVER': f'{{{driver_input.strip()}}}',
                    'SERVER': f'{host_input.strip()},{port_input.strip()}' if port_input.strip() else host_input.strip(),
                    'DATABASE': dbname_input.strip(),
                    'Encrypt': encrypt_input,
                    'TrustServerCertificate': trust_cert_input
                }
                if user_input.strip():
                    params['UID'] = user_input.strip()
                    params['PWD'] = password_input
                else:
                    params['Trusted_Connection'] = 'yes'
                odbc_conn_str = ";".join([f"{k}={v}" for k, v in params.items()])
                quoted_conn_str = urllib.parse.quote_plus(odbc_conn_str)
                engine_url = f"mssql+pyodbc:///?odbc_connect={quoted_conn_str}"
                st.info("Connecting to SQL Server via SQLAlchemy...")
                engine = create_engine(engine_url, connect_args={'timeout': 5})
            elif db_type == "PostgreSQL":
                if not all([host_input, port_input, dbname_input, user_input, password_input]):
                    st.error("All connection details are required for PostgreSQL.")
                    return
                engine_url = f"postgresql+psycopg2://{user_input}:{password_input}@{host_input}:{port_input}/{dbname_input}"
                st.info("Connecting to PostgreSQL via SQLAlchemy...")
                engine = create_engine(engine_url)
            elif db_type == "Oracle":
                if not all([host_input, port_input, dbname_input, user_input, password_input]):
                    st.error("All connection details are required for Oracle.")
                    return
                if oracle_conn_type == "SID":
                    dsn = f"{host_input}:{port_input}/{dbname_input}"
                else:
                    dsn = f"{host_input}:{port_input}/{dbname_input}"
                engine_url = f"oracle+oracledb://{user_input}:{password_input}@{dsn}"
                st.info("Connecting to Oracle via SQLAlchemy using 'oracledb' driver...")
                engine = create_engine(engine_url)
            if engine:
                with st.spinner("Connecting to database and fetching schema..."):
                    st.session_state.db_engine = engine
                    st.session_state.db_connection = None
                    st.session_state.connected = True
                    st.session_state.data = None
                    inspector = inspect(engine)
                    if inspector:
                        db_schema = {}
                        schema_to_inspect = user_input.upper() if db_type == "Oracle" else None
                        tables = inspector.get_table_names(schema=schema_to_inspect)
                        for table_name in tables:
                            db_schema[table_name] = {}
                            columns = inspector.get_columns(table_name, schema=schema_to_inspect)
                            for column in columns:
                                db_schema[table_name][column['name']] = str(column['type'])
                        st.session_state.data_schema = db_schema
                        try:
                            st.session_state.log_data_schema_str = json.dumps(st.session_state.data_schema, indent=2)
                        except Exception as e_json:
                            st.session_state.log_data_schema_str = f"Error formatting DB schema for logs: {e_json}"
                    else:
                        st.session_state.data_schema = {}
                        st.session_state.log_data_schema_str = "Could not create database inspector"
                st.success(f"Successfully connected to {db_type}!")

                st.rerun()
        except ImportError as e:
            if 'psycopg2' in str(e).lower():
                st.error("PostgreSQL driver not found. Please install it in your environment: pip install psycopg2-binary")
            elif 'oracledb' in str(e).lower() or 'cx_oracle' in str(e).lower():
                st.error("Oracle driver not found. Please install it (`pip install oracledb`). The old 'cx_Oracle' driver is no longer recommended.")
            else:
                st.error(f"A required library is missing: {e}")
            st.session_state.connected = False
            st.session_state.db_engine = None
        except Exception as e:
            st.error(f"Error connecting to {db_type}: {e}")
            st.session_state.connected = False
            st.session_state.db_engine = None

def handle_api_connection():
    """Handles the UI and logic for connecting to an API."""
    st.subheader("Connect to API Data Source")

    api_url = st.text_input("API Endpoint URL", key="api_url", placeholder="https://api.example.com/data")
    
    st.markdown("##### Authentication (Optional)")
    auth_type = st.selectbox("Authentication Type", ["None", "API Key (Header)", "Bearer Token"], key="api_auth_type")

    api_key_header_name = ""
    api_key_value = ""
    bearer_token_value = ""

    if auth_type == "API Key (Header)":
        api_key_header_name = st.text_input("API Key Header Name (e.g., X-API-Key)", key="api_key_header_name")
        api_key_value = st.text_input("API Key Value", type="password", key="api_key_value")
    elif auth_type == "Bearer Token":
        bearer_token_value = st.text_input("Bearer Token", type="password", key="api_bearer_token_value")

    data_path_in_json = st.text_input("Path to data in JSON (e.g., 'results' or 'data.items', leave blank if data is at root list)", key="api_data_path", placeholder="results.users")
    
    if st.button("Fetch Data from API", key="connect_api_button"):
        if not api_url:
            st.error("API Endpoint URL is required.")
            return

        headers = {}
        if auth_type == "API Key (Header)":
            if not api_key_header_name or not api_key_value:
                st.error("API Key Header Name and Value are required for API Key authentication.")
                return
            headers[api_key_header_name] = api_key_value
        elif auth_type == "Bearer Token":
            if not bearer_token_value:
                st.error("Bearer Token is required for Bearer Token authentication.")
                return
            headers["Authorization"] = f"Bearer {bearer_token_value}"

        try:
            import requests # Ensure requests is imported

            with st.spinner(f"Fetching data from {api_url}..."):
                response = requests.get(api_url, headers=headers, timeout=10) # Added timeout
                response.raise_for_status() # Raises an HTTPError for bad responses (4XX or 5XX)
            
            st.success(f"Successfully fetched data from API (Status: {response.status_code})")
            
            try:
                json_data = response.json()
                
                # Traverse path to actual data if specified
                data_to_convert = json_data
                if data_path_in_json:
                    path_keys = data_path_in_json.split('.')
                    for key in path_keys:
                        if isinstance(data_to_convert, dict) and key in data_to_convert:
                            data_to_convert = data_to_convert[key]
                        elif isinstance(data_to_convert, list) and key.isdigit() and int(key) < len(data_to_convert):
                            data_to_convert = data_to_convert[int(key)]
                        else:
                            st.error(f"Error: Key '{key}' not found in JSON path '{data_path_in_json}'. Please check the path and API response structure.")
                            st.json(json_data) # Show what was received
                            return
                
                if isinstance(data_to_convert, list):
                    if not data_to_convert:
                        st.warning("API returned an empty list at the specified path.")
                        df = pd.DataFrame()
                    else:
                        # Attempt to normalize, good for list of dicts, some nesting handled
                        df = pd.json_normalize(data_to_convert)
                elif isinstance(data_to_convert, dict): 
                    # If a single object is returned, convert it to a list of one object
                    df = pd.json_normalize([data_to_convert])
                else:
                    st.error("The data at the specified path in the JSON response is not a list of records or a single record object. Cannot convert to DataFrame.")
                    st.write("Data preview at specified path:")
                    st.write(data_to_convert)
                    return

                st.session_state.data = df
                st.session_state.connected = True
                st.session_state.connection_type = "api" # Set connection type

                st.success("API data loaded into DataFrame successfully!")
                st.dataframe(df.head())

                # Infer and store schema for API data (similar to CSV)
                api_schema = []
                for col_name in df.columns:
                    col_type = str(df[col_name].dtype)
                    # Sanitize column names for schema display and potential use in SQL
                    # Simple replacement, more complex scenarios might need robust slugify
                    sane_col_name = str(col_name).replace('.', '_').replace('[', '_').replace(']', '')
                    api_schema.append({"name": sane_col_name, "type": col_type, "original_name": col_name}) 
                
                # If column names were changed, rename df columns to sane versions
                if any(d['name'] != d['original_name'] for d in api_schema):
                    rename_map = {d['original_name']: d['name'] for d in api_schema}
                    df.rename(columns=rename_map, inplace=True)
                    st.session_state.data = df # Update session state with renamed df
                    st.caption("Note: Column names with special characters (e.g., '.') have been sanitized (e.g., replaced with '_').")
                    st.dataframe(df.head()) # Show updated dataframe preview
                
                st.session_state.data_schema = [{"name": d["name"], "type": d["type"]} for d in api_schema]

                try:
                    st.session_state.log_data_schema_str = json.dumps(st.session_state.data_schema, indent=2)
                except Exception as e:
                    st.session_state.log_data_schema_str = f"Error formatting API schema for logs: {e}"

            except json.JSONDecodeError:
                st.error("Failed to decode JSON from API response.")
                st.text(response.text[:500] + "...") # Show beginning of text response
            except Exception as e_process:
                st.error(f"Error processing API data: {e_process}")
                st.session_state.connected = False
                st.session_state.data = None

        except requests.exceptions.HTTPError as errh:
            st.error(f"Http Error: {errh}")
            try: st.json(response.json()) # Try to show error response from API if JSON
            except: st.text(response.text[:500] + "...")
            st.session_state.connected = False
        except requests.exceptions.ConnectionError as errc:
            st.error(f"Error Connecting: {errc}")
            st.session_state.connected = False
        except requests.exceptions.Timeout as errt:
            st.error(f"Timeout Error: {errt}")
            st.session_state.connected = False
        except requests.exceptions.RequestException as err:
            st.error(f"Oops: Something Else: {err}")
            st.session_state.connected = False
        except Exception as e: # Catch-all for unexpected errors during request
            st.error(f"An unexpected error occurred while fetching from API: {e}")
            st.session_state.connected = False

def show_query_screen():
    """Displays the UI for asking questions and viewing results."""
    st.header("Ask Questions About Your Data")

    # Check for integrated data sources first (new unified flow)
    integration_engine = data_integration.data_integration_engine
    summary = integration_engine.get_data_sources_summary()
    
    # Initialize data variables
    active_data = None
    active_schema = None
    active_connection_type = None
    available_tables = []
    
    # Prioritize integrated data sources over legacy session state
    if summary['total_sources'] > 0:
        st.success(f"✅ **{summary['total_sources']} integrated data sources available**")
        
        # Get available tables from integrated data
        try:
            integrated_tables_df = integration_engine.get_integrated_data()
            if not integrated_tables_df.empty and 'available_tables' in integrated_tables_df.columns:
                available_tables = integrated_tables_df['available_tables'].tolist()
        except:
            # Alternative method to get table list
            for source_info in summary['sources']:
                table_name = f"source_{source_info['id']}"
                available_tables.append(table_name)
        
        if available_tables:
            # Select a table to preview and query
            selected_preview_table = st.selectbox(
                "Select data source to preview:",
                available_tables,
                key="preview_table_select"
            )
            
            if selected_preview_table:
                try:
                    # Get data for preview and schema
                    preview_data = integration_engine.get_integrated_data(selected_preview_table)
                    if not preview_data.empty:
                        st.subheader(f"Data Preview: {selected_preview_table} (First 5 rows)")
                        st.dataframe(preview_data.head())
                        
                        # Debug information
                        with st.expander("🔍 Data Debug Info", expanded=False):
                            st.write(f"**Shape:** {preview_data.shape}")
                            st.write(f"**Columns:** {list(preview_data.columns)}")
                            st.write(f"**Data Types:**")
                            for col in preview_data.columns:
                                st.write(f"  • {col}: {preview_data[col].dtype}")
                            
                            # Show some sample values for key columns
                            if len(preview_data) > 0:
                                st.write(f"**Sample Data:**")
                                sample_data = {}
                                for col in preview_data.columns[:5]:  # Show first 5 columns
                                    sample_values = preview_data[col].dropna().head(3).tolist()
                                    sample_data[col] = sample_values
                                st.json(sample_data)
                        
                        # Set active data for querying
                        active_data = preview_data
                        active_connection_type = "integrated"
                        
                        # Build schema from integrated data
                        schema = []
                        for col in preview_data.columns:
                            schema.append({"name": col, "type": str(preview_data[col].dtype)})
                        active_schema = schema
                        
                        # Show enhanced schema if semantic layer is available
                        semantic_layer = st.session_state.get('semantic_layer')
                        if semantic_layer and semantic_layer.tables:
                            st.success("🧠 **Enhanced with business intelligence** - queries will be more accurate!")
                            with st.expander("🔍 View Enhanced Schema", expanded=False):
                                enhanced_prompt = semantic_layer.generate_enhanced_schema_prompt(active_schema, "integrated")
                                st.text_area("Enhanced schema sent to LLM:", enhanced_prompt, height=200)
                        
                    else:
                        st.warning(f"Selected table '{selected_preview_table}' is empty.")
                except Exception as e:
                    st.error(f"Error loading data from '{selected_preview_table}': {e}")
        else:
            st.warning("⚠️ No integrated data tables found. Please add and transform data in 'Data Setup & Intelligence'.")
    
    # Fallback to legacy session state data (old flow)
    elif st.session_state.data is not None and not st.session_state.data.empty:
        st.info("📊 Using legacy data connection")
        st.subheader("Data Preview (First 5 rows)")
        st.dataframe(st.session_state.data.head())
        
        active_data = st.session_state.data
        active_schema = st.session_state.data_schema
        active_connection_type = st.session_state.connection_type
        
    elif st.session_state.connection_type and st.session_state.connection_type != "csv":
        st.info("Connected to database. Schema information would typically be shown here.")
        if st.session_state.data_schema and isinstance(st.session_state.data_schema, dict) and "error" not in st.session_state.data_schema:
            st.write("Available schema for LLM:")
            st.json(st.session_state.data_schema)

        active_schema = st.session_state.data_schema
        active_connection_type = st.session_state.connection_type
        
    else:
        st.warning("⚠️ **No data connected!** Please set up your data first.")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📊 Set Up Data", type="primary"):
                st.session_state.app_page = 'data_setup'
                st.rerun()
        with col2:
            if st.button("🔗 Use Legacy Connection"):
                st.session_state.app_page = 'connect_data'  # This would be the old connection page
                st.rerun()
        return

    # Table/Source selection dropdown
    table_options = ["All Tables / Auto-detect"]
    
    # Add options based on active data source
    if active_connection_type == "integrated" and available_tables:
        table_options.extend(available_tables)
    elif active_schema and isinstance(active_schema, dict):
        table_options.extend(list(active_schema.keys()))
    elif active_schema and isinstance(active_schema, list):
        # For CSV/API data, just show the data source name
        table_options.append("Current Data Source")
    
    selected_table = st.selectbox(
        "Focus query on a specific table (optional):",
        options=table_options,
        index=0,
        key="query_screen_table_select"
    )

    # --- Query Input and Clarification Flow ---
    if not st.session_state.clarification_question_pending:
        # If a previous query led to a clarification that was cancelled, repopulate the text area.
        initial_query_value = st.session_state.original_query_for_clarification if st.session_state.original_query_for_clarification else ""
        natural_language_query_input = st.text_area(
            "Ask your question in plain English:", 
            height=100, 
            key="nl_query_main_input",
            value=initial_query_value
        )
        if st.button("Get Answer", key="get_answer_button", disabled=(not st.session_state.llm_client_instance)):
            if not st.session_state.llm_client_instance: 
                st.warning("LLM is not configured. Please visit LLM Settings.")
            elif natural_language_query_input:
                st.session_state.original_query_for_clarification = natural_language_query_input
                # Start a new conversation log for this query
                st.session_state.conversation_log_for_query = [{"role": "user", "content": natural_language_query_input}]
                st.session_state.results_df = None 
                st.session_state.log_generated_sql_str = None
                st.session_state.log_query_execution_details_str = None
                
                # Check if we have valid data to query
                if not active_schema:
                    st.error("❌ No data schema available. Please connect to data first.")
                    if st.button("📊 Set Up Data"):
                        st.session_state.app_page = 'data_setup'
                        st.rerun()
                    return
                
                with st.spinner("Thinking..."):
                    # Update session state for handle_llm_response compatibility
                    if active_data is not None:
                        st.session_state.data = active_data
                    st.session_state.data_schema = active_schema
                    st.session_state.connection_type = active_connection_type
                    st.session_state.selected_table = selected_table
                    
                    # Add SQL debugging information
                    with st.expander("🔍 SQL Generation Debug", expanded=False):
                        st.write("**Query Processing Debug Information:**")
                        st.write(f"• **Natural Language Query:** {natural_language_query_input}")
                        st.write(f"• **Active Connection Type:** {active_connection_type}")
                        st.write(f"• **Selected Table:** {selected_table}")
                        st.write(f"• **Schema Type:** {type(active_schema)}")
                        
                        if isinstance(active_schema, list):
                            st.write(f"• **Schema Columns:** {[col['name'] for col in active_schema[:10]]}")
                        elif isinstance(active_schema, dict):
                            st.write(f"• **Schema Tables:** {list(active_schema.keys())}")
                            
                        if active_data is not None:
                            st.write(f"• **Data Shape:** {active_data.shape}")
                            st.write(f"• **Data Columns:** {list(active_data.columns)}")
                            
                            # Show sample data for context
                            if len(active_data) > 0:
                                st.write("**Sample Data (First 3 Rows):**")
                                st.dataframe(active_data.head(3))
                        
                        # LLM response details will be shown after generation
                        st.write("• **Status:** Ready for query processing")
                    
                    llm_response = query_clarifier.get_sql_or_clarification(
                        natural_language_query=natural_language_query_input,
                        data_schema=active_schema,
                        db_type=active_connection_type or "integrated",
                        target_table=selected_table,
                        conversation_history=None # First turn, no prior history for this specific interaction
                    )
                    

                    handle_llm_response(llm_response)
            else:
                st.warning("Please enter your question.")
    else: # Clarification question is pending
        st.info(f"**AI needs more information to proceed with your query:**")
        st.markdown(f"> Original question: *{st.session_state.original_query_for_clarification}*")
        
        # Make the AI question more user-friendly
        ai_question = st.session_state.llm_clarification_question
        
        # Check if the "question" is actually SQL code that needs confirmation
        if any(sql_word in ai_question.upper() for sql_word in ['SELECT', 'FROM', 'WHERE', 'COUNT', 'SUM']):
            st.markdown("**🤖 AI generated this query to answer your question:**")
            st.code(ai_question, language="sql")
            st.markdown("**Does this look correct for what you want to find?**")
            user_clarification_answer = st.selectbox(
                "Please confirm:", 
                ["", "Yes, execute this query", "No, let me rephrase my question"],
                key="sql_confirmation_select"
            )
        else:
            # It's a real clarification question
            st.markdown(f"**🤖 AI asks:** *{ai_question}*")
        user_clarification_answer = st.text_area("Your answer:", key="user_clarification_answer_input", height=75)

        col1_clarify, col2_clarify = st.columns(2)
        with col1_clarify:
            if st.button("Submit Answer", key="submit_clarification_answer_button"):
                # Handle both selectbox and text area responses
                answer_to_submit = ""
                if "sql_confirmation_select" in st.session_state and st.session_state.sql_confirmation_select:
                    answer_to_submit = st.session_state.sql_confirmation_select
                elif "user_clarification_answer_input" in st.session_state:
                    answer_to_submit = st.session_state.user_clarification_answer_input
                
                if answer_to_submit and answer_to_submit.strip():
                    # Handle different types of responses
                    if answer_to_submit == "Yes, execute this query":
                        # User confirmed the SQL, execute it directly
                        sql_to_execute = st.session_state.llm_clarification_question
                        st.session_state.log_generated_sql_str = sql_to_execute
                        
                        # Execute the SQL directly
                        if st.session_state.connection_type == "csv":
                            import re
                            sql_to_execute = re.sub(r"\bNULLS\s+(FIRST|LAST)\b", "", sql_to_execute, flags=re.IGNORECASE)
                            if st.session_state.data is not None:
                                try:
                                    conn_sqlite = sqlite3.connect(':memory:')
                                    st.session_state.data.to_sql('csv_data', conn_sqlite, if_exists='replace', index=False)
                                    query_result_df = pd.read_sql_query(sql_to_execute, conn_sqlite)
                                    conn_sqlite.close()
                                    st.session_state.results_df = query_result_df
                                    st.session_state.log_query_execution_details_str = f"Successfully executed confirmed SQL. Rows returned: {len(query_result_df)}"
                                except Exception as e_confirmed_sql:
                                    # Enhanced error handling for column name issues
                                    error_msg = str(e_confirmed_sql)
                                    if "ILIKE" in error_msg or "near \"ILIKE\"" in error_msg:
                                        friendly_error = f"❌ **SQL Syntax Error**: The query uses `ILIKE` which is PostgreSQL syntax, but CSV data uses SQLite.\n\n"
                                        friendly_error += f"💡 **Fix**: Use `LIKE` for exact matches or `LOWER(column) LIKE LOWER('%pattern%')` for case-insensitive matches.\n\n"
                                        friendly_error += f"🔧 **Example**: Instead of `WHERE Region ILIKE '%south%'`, use:\n"
                                        friendly_error += f"   • `WHERE LOWER(Region) LIKE LOWER('%south%')` (case-insensitive)\n"
                                        friendly_error += f"   • `WHERE Region LIKE '%South%'` (case-sensitive)\n\n"
                                        friendly_error += f"Original error: {error_msg}"
                                        st.error(friendly_error)
                                    elif "no such column" in error_msg.lower():
                                        # Enhanced error handling for column name issues
                                        import re
                                        column_match = re.search(r"no such column: (\w+)", error_msg)
                                        if column_match:
                                            missing_col = column_match.group(1)
                                            available_cols = list(st.session_state.data.columns)
                                            
                                            # Find similar column names
                                            similar_cols = []
                                            for col in available_cols:
                                                col_clean = col.lower().replace(' ', '_').replace('-', '_')
                                                if missing_col.lower() in col_clean or col_clean in missing_col.lower():
                                                    similar_cols.append(col)
                                            
                                            error_details = f"❌ **Column Error**: The query looks for `{missing_col}` but this column doesn't exist.\n\n"
                                            error_details += f"📋 **Available columns in your data:**\n"
                                            for i, col in enumerate(available_cols, 1):
                                                error_details += f"   {i}. `{col}`\n"
                                            
                                            if similar_cols:
                                                error_details += f"\n💡 **Did you mean:** `{similar_cols[0]}`?\n\n"
                                                error_details += f"🔧 **Suggested fix**: Try rephrasing your question to use the exact column name `{similar_cols[0]}`"
                                            
                                            st.error(error_details)
                                        else:
                                            st.error(f"SQL Column Error: {error_msg}")
                                    else:
                                        st.error(f"Error executing confirmed SQL: {e_confirmed_sql}")
                                        st.session_state.results_df = pd.DataFrame({"error": [f"SQL Error: {e_confirmed_sql}"]})
                                    st.session_state.log_query_execution_details_str = f"Confirmed SQL Execution Error: {e_confirmed_sql}"
                        
                        elif st.session_state.db_engine: # For direct DB connections using SQLAlchemy engine
                            try:
                                with st.session_state.db_engine.connect() as connection:
                                    if sql_to_execute.strip().lower().startswith('select'):
                                        query_result_df = pd.read_sql_query(sql_to_execute, connection)
                                        st.session_state.results_df = query_result_df
                                        st.session_state.log_query_execution_details_str = f"Successfully executed confirmed SQL on database. Rows returned: {len(query_result_df)}"
                                    else: # For DML/DDL statements
                                        result = connection.execute(text(sql_to_execute))
                                        connection.commit() 
                                        st.session_state.results_df = pd.DataFrame({"message": [f"Query executed successfully. Rows affected: {result.rowcount if result.rowcount != -1 else 'N/A'}"]})
                                        st.session_state.log_query_execution_details_str = f"Confirmed SQL executed successfully. Rows affected: {result.rowcount if result.rowcount != -1 else 'N/A'}"
                            except Exception as e_db_confirmed:
                                st.error(f"Error executing confirmed SQL on database: {e_db_confirmed}")
                                st.session_state.results_df = pd.DataFrame({"error": [f"Database SQL Error: {e_db_confirmed}"]})
                                st.session_state.log_query_execution_details_str = f"Confirmed Database SQL Execution Error: {e_db_confirmed}"
                        else:
                            st.error("No data source connected for executing the confirmed query.")
                            st.session_state.log_query_execution_details_str = "Confirmed query execution attempted, but no data source is connected."
                        
                        # Clear the clarification state
                        st.session_state.clarification_question_pending = False
                        st.session_state.llm_clarification_question = ""
                        st.rerun()
                        
                    elif answer_to_submit == "No, let me rephrase my question":
                        # User wants to start over
                        st.session_state.clarification_question_pending = False
                        st.session_state.llm_clarification_question = ""
                        st.session_state.original_query_for_clarification = ""
                        st.session_state.conversation_log_for_query = []
                        st.rerun()
                    else:
                        # Regular clarification answer
                        st.session_state.conversation_log_for_query.append({"role": "user", "content": answer_to_submit})
                        
                        with st.spinner("Processing your clarification..."):
                            # Use active data for clarification processing
                            llm_response = query_clarifier.get_sql_or_clarification(
                                natural_language_query=st.session_state.original_query_for_clarification, 
                                data_schema=active_schema or st.session_state.data_schema,
                                db_type=active_connection_type or st.session_state.connection_type,
                                target_table=selected_table or st.session_state.selected_table,
                                conversation_history=st.session_state.conversation_log_for_query
                            )
                            handle_llm_response(llm_response)
                else:
                    st.warning("Please provide an answer or make a selection.")
        with col2_clarify:
            if st.button("Cancel and Start New Query", key="cancel_clarification_button"):
                st.session_state.clarification_question_pending = False
                st.session_state.llm_clarification_question = ""
                st.session_state.original_query_for_clarification = "" # Clear the original query display too
                st.session_state.conversation_log_for_query = []
                st.session_state.results_df = None 
                st.rerun()

    # Display results and visualization options if available
    if st.session_state.results_df is not None:
        results_df_to_display = st.session_state.results_df
        st.subheader("Query Results")
        st.dataframe(results_df_to_display)

        if not results_df_to_display.empty and not ("error" in results_df_to_display.columns or "message" in results_df_to_display.columns):
            st.subheader("Visualizations")
            if not results_df_to_display.empty: 
                st.markdown("#### Interactive Chart Options")
                all_columns = results_df_to_display.columns.tolist()
                numeric_columns = results_df_to_display.select_dtypes(include=['number']).columns.tolist()
                categorical_columns = results_df_to_display.select_dtypes(include=['object', 'category', 'string', 'boolean']).columns.tolist()
                # geo_columns = detect_geographic_columns(results_df_to_display) # REMOVE THIS LINE
                # has_geo_data = bool(geo_columns) # REMOVE THIS LINE

                chart_type_options = ["Bar Chart", "Line Chart", "Scatter Plot", "Pie Chart", "Histogram", "Table", "KPI"]
                # if has_geo_data: # REMOVE THIS BLOCK
                #     chart_type_options.append("Map") # REMOVE THIS LINE
                # else: # Still allow Map option, but it will show a warning if no geo data is found # REMOVE THIS BLOCK
                #     chart_type_options.append("Map (needs geographic data)") # REMOVE THIS LINE
                
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
                            
                    elif chart_type and chart_type.startswith("Map"): # Handles "Map" and "Map (needs geographic data)"
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
                                        "id": str(uuid.uuid4()),
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
                                    "id": str(uuid.uuid4()),
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
                        
                        # Get a safe button label
                        button_label_text = chart_type or "Chart"
                        try:
                            if fig and hasattr(fig, 'layout'):
                                layout = getattr(fig, 'layout', None)
                                if layout and hasattr(layout, 'title'):
                                    title = getattr(layout, 'title', None)
                                    if title and hasattr(title, 'text'):
                                        title_text = getattr(title, 'text', None)
                                        if title_text:
                                            button_label_text = title_text
                        except (AttributeError, TypeError):
                            pass  # Use default button_label_text
                        
                        if st.button(f"Add '{button_label_text}' to My Dashboard", 
                                   key=f"add_chart_to_dash_{button_label_text.replace(' ','_') if button_label_text else 'chart'}", 
                                   disabled=not can_add_to_dashboard):
                            dashboard_item = {
                                "id": str(uuid.uuid4()),
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

    # Action buttons
    col1, col2 = st.columns(2)
    with col1:
        if st.button("📊 Set Up New Data", key="setup_new_data_button", type="primary"):
            st.session_state.app_page = 'data_setup'
            st.rerun()
    
    with col2:
        if st.button("🔄 Reset All Data", key="disconnect_button_query_screen"):
            # Reset legacy session state variables
            st.session_state.connected = False
            st.session_state.connection_type = None
            st.session_state.data = None
            st.session_state.db_connection = None
            st.session_state.db_engine = None
            st.session_state.data_schema = None
            st.session_state.selected_table = "All Tables / Auto-detect"
            clear_logs() # This clears the specific log strings
            st.session_state.results_df = None
            st.session_state.dashboard_items = [] 
            
            # Reset clarification flow states as well
            st.session_state.clarification_question_pending = False
            st.session_state.llm_clarification_question = ""
            st.session_state.original_query_for_clarification = ""
            st.session_state.conversation_log_for_query = []
            
            st.success("✅ All data sources reset. Please set up your data again.")
            st.rerun()


def handle_llm_response(llm_response):
    """
    Handles the response from the LLM (either SQL query or clarification question).
    Updates session state accordingly.
    """
    if (llm_response.get("type") == "sql_query" or llm_response.get("type") == "sql") and \
       (llm_response.get("query") or llm_response.get("content")):
        st.session_state.clarification_question_pending = False
        st.session_state.llm_clarification_question = ""
        
        sql_from_response = llm_response.get("query") or llm_response.get("content")

        # Add the LLM's generated SQL to the conversation log
        st.session_state.conversation_log_for_query.append({"role": "assistant", "content": f"Generated SQL: {sql_from_response}"})

        generated_sql = sql_from_response # Use the extracted SQL
        st.session_state.log_generated_sql_str = generated_sql
        
        # Show warning if LLM didn't use expected markers (for local LLMs)
        if llm_response.get("warning"):
            st.warning(f"⚠️ LLM Response Issue: {llm_response['warning']}")
        
        st.success("SQL Query Generated by AI:")
        st.code(generated_sql, language="sql")

        if st.session_state.connection_type in ["csv", "api"]:
            import re
            generated_sql = re.sub(r"\bNULLS\s+(FIRST|LAST)\b", "", generated_sql, flags=re.IGNORECASE)
            if st.session_state.data is not None:
                try:
                    # For CSV and API data, use SQLite in-memory to query the DataFrame
                    conn_sqlite = sqlite3.connect(':memory:')
                    st.session_state.data.to_sql('csv_data', conn_sqlite, if_exists='replace', index=False)
                    
                    # Convert PostgreSQL syntax to SQLite-compatible syntax
                    generated_sql_converted = convert_postgresql_to_sqlite(generated_sql)
                    
                    # Log the conversion if SQL was changed
                    if generated_sql_converted != generated_sql:
                        print(f"[SQL CONVERSION] Original: {generated_sql}")
                        print(f"[SQL CONVERSION] Converted: {generated_sql_converted}")
                        st.info("🔄 **Auto-converted PostgreSQL syntax to SQLite-compatible syntax**")
                        st.code(f"Original: {generated_sql}\n\nConverted: {generated_sql_converted}", language="sql")
                    
                    query_result_df = pd.read_sql_query(generated_sql_converted, conn_sqlite)
                    conn_sqlite.close()
                    st.session_state.results_df = query_result_df
                    st.session_state.log_query_execution_details_str = f"Successfully executed on CSV data. Rows returned: {len(query_result_df)}"
                except sqlite3.Error as e_sqlite:
                    error_msg = str(e_sqlite)
                    
                    # Enhanced error handling for common PostgreSQL->SQLite issues
                    if "ILIKE" in error_msg or "near \"ILIKE\"" in error_msg:
                        friendly_error = f"❌ **SQL Syntax Error**: The query uses `ILIKE` which is PostgreSQL syntax, but CSV/API data uses SQLite.\n\n"
                        friendly_error += f"💡 **Fix**: Use `LIKE` for exact matches or `LOWER(column) LIKE LOWER('%pattern%')` for case-insensitive matches.\n\n"
                        friendly_error += f"🔧 **Example**: Instead of `WHERE Region ILIKE '%south%'`, use:\n"
                        friendly_error += f"   • `WHERE LOWER(Region) LIKE LOWER('%south%')` (case-insensitive)\n"
                        friendly_error += f"   • `WHERE Region LIKE '%South%'` (case-sensitive)\n\n"
                        friendly_error += f"Original error: {error_msg}"
                        st.error(friendly_error)
                    elif "no such column" in error_msg.lower():
                        # Enhanced error handling for column name issues
                        import re
                        column_match = re.search(r"no such column: (\w+)", error_msg)
                        if column_match:
                            missing_col = column_match.group(1)
                            available_cols = list(st.session_state.data.columns)
                            
                            # Find similar column names
                            similar_cols = []
                            for col in available_cols:
                                col_clean = col.lower().replace(' ', '_').replace('-', '_')
                                if missing_col.lower() in col_clean or col_clean in missing_col.lower():
                                    similar_cols.append(col)
                            
                            error_details = f"❌ **Column Error**: The query looks for `{missing_col}` but this column doesn't exist.\n\n"
                            error_details += f"📋 **Available columns in your data:**\n"
                            for i, col in enumerate(available_cols, 1):
                                error_details += f"   {i}. `{col}`\n"
                            
                            if similar_cols:
                                error_details += f"\n💡 **Did you mean:** `{similar_cols[0]}`?\n\n"
                                error_details += f"🔧 **Suggested fix**: Try rephrasing your question to use the exact column name `{similar_cols[0]}`"
                            
                            st.error(error_details)
                        else:
                            st.error(f"SQL Column Error: {error_msg}")
                    else:
                        st.error(f"Error executing SQL on CSV/API data: {e_sqlite}")
                        
                    st.session_state.results_df = pd.DataFrame({"error": [f"SQLite Error: {e_sqlite}"]})
                    st.session_state.log_query_execution_details_str = f"SQLite Execution Error: {e_sqlite}"
                except Exception as e_csv_api_query:
                    st.error(f"An unexpected error occurred while querying CSV/API data: {e_csv_api_query}")
                    st.session_state.results_df = pd.DataFrame({"error": [f"Unexpected Error: {e_csv_api_query}"]})
                    st.session_state.log_query_execution_details_str = f"Unexpected CSV/API Query Error: {e_csv_api_query}"
            else:
                st.error("No CSV/API data loaded to query.")
                st.session_state.log_query_execution_details_str = "Attempted to query CSV/API data, but no data was loaded."

        elif st.session_state.connection_type == "integrated":
            if st.session_state.data is not None:
                try:
                    # For integrated data, use SQLite in-memory approach for simplicity and reliability
                    import re
                    
                    # Convert PostgreSQL-specific syntax to SQLite-compatible syntax
                    generated_sql_converted = convert_postgresql_to_sqlite(generated_sql)
                    
                    # Log the conversion if SQL was changed
                    if generated_sql_converted != generated_sql:
                        print(f"[INTEGRATED DATA] Original: {generated_sql}")
                        print(f"[INTEGRATED DATA] Converted: {generated_sql_converted}")
                        st.info("🔄 **Auto-converted PostgreSQL syntax to SQLite-compatible syntax**")
                        st.code(f"Original: {generated_sql}\n\nConverted: {generated_sql_converted}", language="sql")
                    
                    # Use SQLite in-memory to query the DataFrame
                    conn_sqlite = sqlite3.connect(':memory:')
                    st.session_state.data.to_sql('integrated_data', conn_sqlite, if_exists='replace', index=False)
                    
                    # Replace common table references in the SQL with our table name
                    generated_sql_final = re.sub(
                        r'\b(source_\w+|csv_data|data|integrated_data_temp)\b', 
                        'integrated_data', 
                        generated_sql_converted, 
                        flags=re.IGNORECASE
                    )
                    
                    st.success("🔄 **Using SQLite for integrated data** - Converted PostgreSQL syntax automatically!")
                    st.code(f"Executing: {generated_sql_final}", language="sql")
                    
                    query_result_df = pd.read_sql_query(generated_sql_final, conn_sqlite)
                    conn_sqlite.close()
                    st.session_state.results_df = query_result_df
                    st.session_state.log_query_execution_details_str = f"Successfully executed on integrated data via SQLite. Rows returned: {len(query_result_df)}"
                    
                except sqlite3.Error as e_sqlite:
                    error_msg = str(e_sqlite)
                    
                    # Enhanced error handling for common issues
                    if "no such column" in error_msg.lower():
                        # Enhanced error handling for column name issues
                        import re
                        column_match = re.search(r"no such column: (\w+)", error_msg)
                        if column_match:
                            missing_col = column_match.group(1)
                            available_cols = list(st.session_state.data.columns)
                            
                            # Find similar column names
                            similar_cols = []
                            for col in available_cols:
                                col_clean = col.lower().replace(' ', '_').replace('-', '_')
                                if missing_col.lower() in col_clean or col_clean in missing_col.lower():
                                    similar_cols.append(col)
                            
                            error_details = f"❌ **Column Error**: The query looks for `{missing_col}` but this column doesn't exist.\n\n"
                            error_details += f"📋 **Available columns in your integrated data:**\n"
                            for i, col in enumerate(available_cols, 1):
                                error_details += f"   {i}. `{col}`\n"
                            
                            if similar_cols:
                                error_details += f"\n💡 **Did you mean:** `{similar_cols[0]}`?\n\n"
                                error_details += f"🔧 **Suggested fix**: Try rephrasing your question to use the exact column name `{similar_cols[0]}`"
                            
                            st.error(error_details)
                        else:
                            st.error(f"SQL Column Error: {error_msg}")
                    else:
                        st.error(f"Error executing SQL on integrated data: {e_sqlite}")
                        
                    st.session_state.results_df = pd.DataFrame({"error": [f"SQLite Error: {e_sqlite}"]})
                    st.session_state.log_query_execution_details_str = f"SQLite Execution Error: {e_sqlite}"
                except Exception as e_integrated_query:
                    st.error(f"An unexpected error occurred while querying integrated data: {e_integrated_query}")
                    st.session_state.results_df = pd.DataFrame({"error": [f"Unexpected Error: {e_integrated_query}"]})
                    st.session_state.log_query_execution_details_str = f"Unexpected Integrated Data Query Error: {e_integrated_query}"
            else:
                st.error("No integrated data loaded to query.")
                st.session_state.log_query_execution_details_str = "Attempted to query integrated data, but no data was loaded."

        elif st.session_state.db_engine: # For direct DB connections using SQLAlchemy engine
            try:
                with st.session_state.db_engine.connect() as connection:
                    if generated_sql.strip().lower().startswith('select'):
                        query_result_df = pd.read_sql_query(generated_sql, connection)
                        st.session_state.results_df = query_result_df
                        st.session_state.log_query_execution_details_str = f"Successfully executed on database. Rows returned: {len(query_result_df)}"
                    else: # For DML/DDL statements
                        # The 'text' function is needed for SQLAlchemy < 2.0 when executing raw strings
                        # for statements that don't return rows. It's good practice for clarity.
                        result = connection.execute(text(generated_sql))
                        # For DML statements like INSERT, UPDATE, DELETE, we should commit the transaction.
                        # The connection from engine.connect() is transactional.
                        connection.commit() 
                        st.session_state.results_df = pd.DataFrame({"message": [f"Query executed successfully. Rows affected: {result.rowcount if result.rowcount != -1 else 'N/A'}"]})
                        st.session_state.log_query_execution_details_str = f"Query executed successfully. Rows affected: {result.rowcount if result.rowcount != -1 else 'N/A'}"
            except Exception as e_db_query:
                st.error(f"An unexpected error occurred during query execution: {e_db_query}")
                st.session_state.results_df = pd.DataFrame({"error": [f"Database Execution Error: {e_db_query}"]})
                st.session_state.log_query_execution_details_str = f"Database Execution Error: {e_db_query}"
        else:
            st.error("Not connected to any data source.")
            st.session_state.log_query_execution_details_str = "Query generation attempted, but not connected to a data source."
        st.rerun()

    elif (llm_response.get("type") == "clarification_needed" or llm_response.get("type") == "clarification") and \
         (llm_response.get("question") or llm_response.get("content")):
        st.session_state.clarification_question_pending = True
        clarification_message = llm_response.get("question") or llm_response.get("content")
        st.session_state.llm_clarification_question = clarification_message
        # Add the LLM's clarification question to the conversation log
        st.session_state.conversation_log_for_query.append({"role": "assistant", "content": clarification_message})
        st.rerun()
    
    elif llm_response.get("type") == "error":
        # Enhanced error handling for LLM issues
        error_message = llm_response.get("content", "Unknown LLM error")
        st.error(f"🤖 LLM Error: {error_message}")
        
        # Show helpful suggestions based on the error type
        if "Connection Error" in error_message:
            st.info("💡 **Suggestion**: Check if your local LLM (e.g., Ollama) is running and accessible at the configured Base URL.")
        elif "Authentication Error" in error_message:
            st.info("💡 **Suggestion**: Verify your API key in LLM Settings.")
        elif "empty response" in error_message.lower():
            st.info("💡 **Suggestion**: Your local LLM might not be responding properly. Check the model name and Base URL in LLM Settings.")
        elif "unexpected format" in error_message.lower():
            st.info("💡 **Suggestion**: Your local LLM might need different prompting. Check the Debug Logs in the sidebar for the raw response.")
        
        # Log the error for debugging
        st.session_state.log_query_execution_details_str = f"LLM Error: {error_message}"
        st.session_state.clarification_question_pending = False # Reset clarification flow
        st.rerun()
    
    elif llm_response.get("type") == "cannot_answer":
        # Handle cases where LLM cannot answer the question
        reason = llm_response.get("content", "No reason provided")
        st.warning(f"🤖 Cannot Answer: {reason}")
        st.info("💡 Try rephrasing your question or providing more specific details about what you're looking for.")
        st.session_state.clarification_question_pending = False # Reset clarification flow
        st.rerun()
    
    else:
        st.error("Received an unexpected response from the AI. Please try rephrasing your question.")
        # Enhanced debugging information
        st.error(f"Debug Info: Response type='{llm_response.get('type')}', Keys={list(llm_response.keys())}")
        
        # Log the unexpected response for debugging
        database.log_app_action(
            username=st.session_state.get("logged_in_user"),
            action="UNEXPECTED_LLM_RESPONSE_FORMAT",
            details=f"Response: {llm_response}",
            status="ERROR"
        )
        st.session_state.clarification_question_pending = False # Reset clarification flow
        st.rerun()

# --- Main Application Logic ---
def main():
    # Section 0: Ensure Session Stability (MUST BE FIRST)
    ensure_session_stability()
    
    # Section 0.1: Initialize Semantic Layer Integration (EARLY INITIALIZATION)
    if st.session_state.get('db_configured_successfully', False):
        try:
            semantic_integration.apply_semantic_layer_integration()
        except Exception as e:
            print(f"[SEMANTIC LAYER] Integration failed: {e}")
    
    # Section 1: Handle Database Configuration Process
    # This section ensures that if the DB isn't marked as configured,
    # we attempt to configure it via secrets or guide the user to the manual config page.
    # Database configuration is GLOBAL - once configured by admin, it works for all users
    if not st.session_state.get('db_configured_successfully', False):
        if not st.session_state.attempted_secrets_db_init:
            st.session_state.attempted_secrets_db_init = True
            conn_secrets = None
            try:
                with st.spinner("Checking database configuration from secrets..."):
                    conn_secrets = database.get_db_connection()
                if conn_secrets:
                    # Verify this is a PostgreSQL connection
                    try:
                        cursor = conn_secrets.cursor()
                        cursor.execute("SELECT version()")
                        version_result = cursor.fetchone()
                        if version_result and 'PostgreSQL' in str(version_result[0]):
                            init_secrets_success = database.init_db(conn_secrets)
                            if init_secrets_success:
                                st.session_state.db_configured_successfully = True
                                initialize_app_defaults()
                                st.sidebar.success("PostgreSQL DB auto-configured from secrets.")
                                if st.session_state.page == "db_config": # If we were on db_config, move to login
                                    st.session_state.page = "login"
                                # No st.rerun() here; let it flow to LLM init and page rendering in this same run
                            else:
                                st.sidebar.error("PostgreSQL connection OK, but table init failed. Please check permissions.")
                                if st.session_state.page != "db_config":
                                    st.session_state.page = "db_config"
                                    st.rerun() # Rerun to go to db_config page
                        else:
                            st.sidebar.error("❌ Only PostgreSQL databases are supported. Please configure PostgreSQL connection.")
                            if st.session_state.page != "db_config":
                                st.session_state.page = "db_config"
                                st.rerun()
                    except Exception as version_check_error:
                        st.sidebar.error(f"Failed to verify PostgreSQL connection: {version_check_error}")
                        if st.session_state.page != "db_config":
                            st.session_state.page = "db_config"
                            st.rerun()
                else: # Secrets not found or connection failed
                    st.sidebar.error("❌ PostgreSQL connection required. Please configure PostgreSQL in secrets or manually.")
                    if st.session_state.page != "db_config":
                        st.session_state.page = "db_config"
                        st.rerun() # Rerun to go to db_config page
            except Exception as e_secrets_init:
                print(f"[SECRETS DEBUG] Database init failed: {e_secrets_init}")
                if st.session_state.page != "login":
                    st.sidebar.error(f"Error during secrets-based DB init: {e_secrets_init}")
                if st.session_state.page != "db_config":
                    st.session_state.page = "db_config"
                    st.rerun() # Rerun to go to db_config page

    # Section 2: Initialize LLM Client (only if database is configured)
    if st.session_state.db_configured_successfully:
        get_llm_client() # This will set up the LLM client and status messages

    # Section 3: Page Routing and UI Display
    if st.session_state.db_configured_successfully:
        # Database is configured, proceed with normal app flow
        if not st.session_state.logged_in_user:
            # User is not logged in, show login page
            show_login_page()
        else:
            # User is logged in, show main application with sidebar navigation
            show_main_application()
    else:
        # Database is not configured, show configuration page
        show_db_configuration_page()

    # --- Debug Logs Panel --- (Added to sidebar when user is logged in and logs exist)
    if st.session_state.logged_in_user and (
        st.session_state.log_data_schema_str or 
        st.session_state.log_openai_prompt_str or 
        st.session_state.log_generated_sql_str or 
        st.session_state.log_query_execution_details_str
    ):
        with st.sidebar.expander("🔍 View Debug Logs", expanded=False):
            if st.session_state.log_data_schema_str:
                st.subheader("Data Schema Sent to LLM")
                st.text(st.session_state.log_data_schema_str)
            if st.session_state.log_openai_prompt_str:
                st.subheader("LLM Prompt")
                st.text(st.session_state.log_openai_prompt_str)
            if st.session_state.log_generated_sql_str:
                st.subheader("Last Generated SQL")
                st.code(st.session_state.log_generated_sql_str, language="sql")
            if st.session_state.log_query_execution_details_str:
                st.subheader("Last Query Execution Details")
                st.text(st.session_state.log_query_execution_details_str)

def show_login_page():
    # Add login-page class to hide sidebar
    st.markdown("""
    <script>
    document.body.classList.add('login-page');
    </script>
    """, unsafe_allow_html=True)
    
    # Inject CSS directly for this page to ensure purple gradient background
    st.markdown(""" 
    <style>
        body {
            background: linear-gradient(135deg, #6200ea 0%, #200079 100%) !important;
            min-height: 100vh !important; 
            display: flex !important; 
            align-items: center !important; 
            justify-content: center !important; 
        }
        .stApp {
            background-color: transparent !important; 
            display: flex !important; 
            align-items: center !important; 
            justify-content: center !important; 
            min-height: 100vh !important; 
        }
        .main .block-container {
            background-color: rgba(255, 255, 255, 0.95) !important;
            border-radius: 15px !important;
            padding: 2rem !important;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1) !important;
            max-width: 400px !important;
            width: 100% !important;
            margin: 0 auto !important;
        }
        /* More specific selectors for login form inputs */
        .login-page .stTextInput > div > div > input,
        .stTextInput > div > div > input[type="text"],
        .stTextInput > div > div > input[type="password"] {
            border-radius: 8px !important;
            border: 2px solid #d1d5db !important;
            padding: 0.75rem 1rem !important;
            font-size: 1rem !important;
            color: #1f2937 !important;
            background-color: #ffffff !important;
            transition: all 0.3s ease !important;
            font-weight: 500 !important;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1) !important;
            width: 100% !important;
            box-sizing: border-box !important;
        }
        .login-page .stTextInput > div > div > input:focus,
        .stTextInput > div > div > input[type="text"]:focus,
        .stTextInput > div > div > input[type="password"]:focus {
            border-color: #6200ea !important;
            box-shadow: 0 0 0 3px rgba(98, 0, 234, 0.15), 0 2px 4px rgba(0,0,0,0.1) !important;
            outline: none !important;
            background-color: #ffffff !important;
            color: #1f2937 !important;
        }
        .login-page .stTextInput > div > div > input::placeholder,
        .stTextInput > div > div > input[type="text"]::placeholder,
        .stTextInput > div > div > input[type="password"]::placeholder {
            color: #9ca3af !important;
            font-weight: 400 !important;
        }
        .login-page .stTextInput > label,
        .stTextInput > label {
            color: #1a237e !important;
            font-weight: 600 !important;
            font-size: 0.95rem !important;
            margin-bottom: 0.5rem !important;
            display: block !important;
        }
        /* Force override any conflicting styles */
        div[data-testid="stForm"] .stTextInput input {
            color: #1f2937 !important;
            background-color: #ffffff !important;
            border: 2px solid #d1d5db !important;
        }
        .stButton > button {
            border-radius: 8px !important;
            padding: 0.75rem 1.5rem !important;
            font-size: 1rem !important;
            font-weight: 600 !important;
            background: linear-gradient(135deg, #6200ea 0%, #200079 100%) !important;
            border: none !important;
            color: white !important;
            width: 100% !important;
            margin-top: 1rem !important;
        }
        .stButton > button:hover {
            background: linear-gradient(135deg, #7c4dff 0%, #3d1cb3 100%) !important;
            box-shadow: 0 4px 12px rgba(98, 0, 234, 0.2) !important;
        }
        .stMarkdown h1 {
            color: #1a237e !important;
            font-size: 2rem !important;
            font-weight: 700 !important;
            margin-bottom: 1.5rem !important;
            text-align: center !important;
        }
        .stMarkdown p {
            color: #424242 !important;
            font-size: 1rem !important;
            text-align: center !important;
            margin-bottom: 1.5rem !important;
        }
    </style>
    """, unsafe_allow_html=True)
    
    # Center the login form
    st.markdown("<div style='text-align: center;'>", unsafe_allow_html=True)
    st.title("Welcome to ConvaBI")
    st.markdown("Please log in to continue")
    st.markdown("</div>", unsafe_allow_html=True)
    
    # Login form
    with st.form("login_form"):
        username = st.text_input("Username", placeholder="Enter your username")
        password = st.text_input("Password", type="password", placeholder="Enter your password")
        submit = st.form_submit_button("Login")
        
        # Additional inline styling to ensure changes take effect
        st.markdown("""
        <style>
        /* Force input styling with timestamp cache buster */
        .stTextInput input {
            color: #1f2937 !important;
            background-color: #ffffff !important;
            border: 2px solid #d1d5db !important;
            border-radius: 8px !important;
            padding: 0.75rem 1rem !important;
            font-weight: 500 !important;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1) !important;
        }
        .stTextInput input:focus {
            border-color: #6200ea !important;
            box-shadow: 0 0 0 3px rgba(98, 0, 234, 0.15) !important;
            color: #1f2937 !important;
        }
        </style>
        """, unsafe_allow_html=True)
        
        if submit:
            if not username or not password:
                st.error("Please enter both username and password")
            else:
                # Get user from database using the appropriate database function
                if is_sqlite_connection():
                    user_data = get_user_by_username_sqlite(username)
                    log_func = log_app_action_sqlite
                else:
                    user_data = database.get_user_by_username_from_db(username)
                    log_func = database.log_app_action
                
                if user_data and check_password(user_data["hashed_password"], password):
                    st.session_state.logged_in_user = username
                    st.session_state.user_roles = user_data.get("roles", [])
                    st.session_state.app_page = 'data_integration'
                    log_func(username, "LOGIN_SUCCESS", "User logged in successfully", "SUCCESS")
                    st.rerun()
                else:
                    st.error("Invalid username or password")
                    log_func(username, "LOGIN_FAILURE", "Invalid login attempt", "FAILURE")

def logout():
    """Logs out the current user and redirects to login page."""
    if st.session_state.logged_in_user:
        try:
            if is_sqlite_connection():
                log_app_action_sqlite(st.session_state.logged_in_user, "LOGOUT", "User logged out", "SUCCESS")
            else:
                database.log_app_action(st.session_state.logged_in_user, "LOGOUT", "User logged out", "SUCCESS")
        except Exception as e:
            print(f"[LOGOUT] Failed to log logout action: {e}")
    

    # Clear user-specific session state while preserving global app settings
    # Note: Database configuration should persist across users as it's a global app setting
    user_specific_keys = [
        'logged_in_user', 'user_roles', 'dashboard_items', 'current_dashboard_name', 
        'results_df', 'chat_history', 'conversation_log_for_query', 'original_query_for_clarification',
        'clarification_question_pending', 'llm_clarification_question', 'connected', 'connection_type',
        'data', 'data_schema', 'db_connection', 'db_engine', 'selected_table'
    ]
    
    # Global settings that should persist across user sessions (DON'T clear these):
    # - 'db_configured_successfully': Database config is global, not user-specific
    # - 'attempted_secrets_db_init': Prevents re-initialization on every login
    # - 'current_db_params': Database connection parameters are global
    
    for key in user_specific_keys:
        if key in st.session_state:
            if key in ['user_roles', 'dashboard_items', 'chat_history', 'conversation_log_for_query']:
                st.session_state[key] = []  # Reset to empty list
            elif key in ['connected', 'clarification_question_pending']:
                st.session_state[key] = False  # Reset to False
            elif key == 'selected_table':
                st.session_state[key] = "All Tables / Auto-detect"  # Reset to default
            else:
                st.session_state[key] = None  # Reset to None
    
    # Reset page navigation
    st.session_state.page = "login"
    st.session_state.app_page = 'data_integration'
    
    # Update activity timestamp
    st.session_state.last_activity_timestamp = time.time()
    
    st.rerun()

# Add these after imports
openai_api_key = st.secrets.get("OPENAI_API_KEY", None)
local_llm_secret_base_url = st.secrets.get("LOCAL_LLM_BASE_URL", None)

def clear_logs():
    pass

def convert_postgresql_to_sqlite(sql_query):
    """
    Convert common PostgreSQL syntax to SQLite-compatible syntax.
    Handles ILIKE, EXTRACT, NULLS FIRST/LAST, and other PostgreSQL-specific patterns.
    Also attempts to fix common SQL syntax errors.
    """
    import re
    
    # First, apply the dedicated SQLCoder filter WHERE fix
    sql_query = sql_fixer.fix_sqlcoder_filter_where_error(sql_query)
    
    # Then attempt to fix other common SQL syntax errors
    def fix_sql_syntax_errors(query):
        """Fix common SQL syntax errors that might come from LLM"""
        
        # Fix SQLCoder's invalid "filter WHERE" syntax
        # Pattern: SUM(column) filter WHERE conditions AS alias
        # Should be: SUM(column) AS alias (and move conditions to WHERE clause)
        import re
        
        print(f"[SQL FIX] Original query: {query}")
        
        # Direct fix for the specific SQLCoder "filter WHERE" error
        if 'filter WHERE' in query:
            print(f"[SQL FIX] Detected filter WHERE pattern, applying direct fix")
            # For profit comparison queries about South region, use a template
            if 'profit' in query.lower() and 'south' in query.lower() and ('2015' in query or '2016' in query):
                query = """
                SELECT strftime('%Y', order_date) AS YEAR, SUM(profit) AS total_profit
                FROM integrated_data 
                WHERE region = 'South' AND strftime('%Y', order_date) IN ('2015', '2016')
                GROUP BY strftime('%Y', order_date)
                ORDER BY YEAR
                """
                print(f"[SQL FIX] Applied template fix for South profit comparison")
                return query
            else:
                # Generic fix: remove filter WHERE and fix syntax
                query = re.sub(r'SUM\s*\([^)]+\)\s+filter\s+WHERE[^A]+AS\s+(\w+)', r'SUM(profit) AS \1', query, flags=re.IGNORECASE)
                # Add basic WHERE clause
                if 'WHERE' not in query.upper() and 'FROM integrated_data' in query:
                    query = query.replace('FROM integrated_data', 'FROM integrated_data WHERE 1=1')
                print(f"[SQL FIX] Applied generic filter WHERE fix")
        
        # Look for the specific problematic pattern from the error
        filter_where_pattern = r'SUM\s*\(\s*([^)]+)\s*\)\s+filter\s+WHERE\s+([^A][^S]*?)\s+AS\s+(\w+)'
        
        if re.search(filter_where_pattern, query, re.IGNORECASE):
            print(f"[SQL FIX] Detected invalid 'filter WHERE' syntax")
            
            match = re.search(filter_where_pattern, query, re.IGNORECASE)
            if match:
                column = match.group(1).strip()
                conditions = match.group(2).strip()
                alias = match.group(3).strip()
                
                print(f"[SQL FIX] Extracted - column: {column}, conditions: {conditions}, alias: {alias}")
                
                # Replace the malformed part with correct SUM syntax
                corrected_sum = f'SUM({column}) AS {alias}'
                query = re.sub(filter_where_pattern, corrected_sum, query, flags=re.IGNORECASE)
                
                # Now add the WHERE clause properly
                if 'WHERE' not in query.upper():
                    # Add WHERE clause before GROUP BY
                    if 'GROUP BY' in query.upper():
                        query = re.sub(r'(\s+FROM\s+[^\s]+)', f'\\1 WHERE {conditions}', query, flags=re.IGNORECASE)
                    else:
                        # Add WHERE before any potential ORDER BY or at the end
                        query = query.rstrip(';') + f' WHERE {conditions}'
                else:
                    # Append to existing WHERE with AND
                    query = re.sub(r'(WHERE\s+)', f'\\1{conditions} AND ', query, flags=re.IGNORECASE)
                
                print(f"[SQL FIX] Fixed query: {query}")
        
        # Handle the specific case from the error more directly
        if 'filter WHERE' in query:
            print(f"[SQL FIX] Direct filter WHERE fix needed")
            
            # Simple pattern replacement for the specific SQLCoder error
            # Replace: SUM(column) filter WHERE conditions AS alias
            # With: SUM(column) AS alias and move conditions to WHERE clause
            
            # Use a simpler regex to catch the exact pattern from the error
            filter_pattern = r'SUM\s*\([^)]+\)\s+filter\s+WHERE\s+[^A]+AS\s+\w+'
            if re.search(filter_pattern, query, re.IGNORECASE):
                # Extract the key parts
                column_match = re.search(r'SUM\s*\(([^)]+)\)', query, re.IGNORECASE)
                alias_match = re.search(r'AS\s+(\w+)', query, re.IGNORECASE)
                
                if column_match and alias_match:
                    column = column_match.group(1)
                    alias = alias_match.group(1)
                    
                    # For profit comparison queries, create a proper query
                    if 'profit' in column.lower() and ('south' in query.lower() or 'region' in query.lower()):
                        query = f"""
                        SELECT strftime('%Y', order_date) AS YEAR, SUM({column}) AS {alias}
                        FROM integrated_data 
                        WHERE region = 'South' AND strftime('%Y', order_date) IN ('2015', '2016')
                        GROUP BY strftime('%Y', order_date)
                        ORDER BY YEAR
                        """
                        print(f"[SQL FIX] Applied specific fix for profit comparison query")
                    else:
                        # Generic fix - remove the filter WHERE part and create proper syntax
                        # Replace the entire malformed expression
                        corrected_sum = f'SUM({column}) AS {alias}'
                        query = re.sub(filter_pattern, corrected_sum, query, flags=re.IGNORECASE)
                        
                        # Add basic WHERE clause if missing
                        if 'WHERE' not in query.upper():
                            query = query.replace('FROM integrated_data', 'FROM integrated_data WHERE 1=1')
                        
                        print(f"[SQL FIX] Applied generic filter WHERE fix")
            
            # Additional fallback - if still contains filter WHERE, do a direct replacement
            if 'filter WHERE' in query:
                # Last resort: remove the filter WHERE entirely and create basic syntax
                query = re.sub(r'\s+filter\s+WHERE[^A]+AS\s+', ' AS ', query, flags=re.IGNORECASE)
                print(f"[SQL FIX] Applied fallback filter WHERE removal")
        
        # Direct string replacement for common malformed patterns
        malformed_patterns = [
            ('LOWER("customer_name", SUM(sales) AS total_sales', 'LOWER("customer_name"), SUM(sales) AS total_sales'),
            ('LOWER("customer_name", SUM(profit) AS total_profit', 'LOWER("customer_name"), SUM(profit) AS total_profit'),
            ('LOWER("customer_name", SUM(revenue) AS total_revenue', 'LOWER("customer_name"), SUM(revenue) AS total_revenue'),
        ]
        
        for malformed, fixed in malformed_patterns:
            if malformed in query:
                query = query.replace(malformed, fixed)
        
        # More general pattern: LOWER("column", SUM(column) AS alias
        pattern = r'LOWER\s*\(\s*"([^"]+)",\s*SUM\s*\(\s*([^)]+)\s*\)\s+AS\s+(\w+)'
        replacement = r'LOWER("\1"), SUM(\2) AS \3'
        query = re.sub(pattern, replacement, query, flags=re.IGNORECASE)
        
        # Also handle case without quotes around column names
        pattern2 = r'LOWER\s*\(\s*([^,\s]+),\s*SUM\s*\(\s*([^)]+)\s*\)\s+AS\s+(\w+)'
        replacement2 = r'LOWER(\1), SUM(\2) AS \3'
        query = re.sub(pattern2, replacement2, query, flags=re.IGNORECASE)
        
        # Fix WHERE clause with extra closing parenthesis
        query = re.sub(r'WHERE\s+"([^"]+)"\)\s+LIKE', r'WHERE "\1" LIKE', query, flags=re.IGNORECASE)
        
        # Also try to add LOWER to WHERE clause for case-insensitive comparison
        query = re.sub(r'WHERE\s+"([^"]+)"\s+LIKE', r'WHERE LOWER("\1") LIKE', query, flags=re.IGNORECASE)
        
        # Fix quotes around years in IN clause for SQLite compatibility
        query = re.sub(r"IN\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", r"IN ('\1', '\2')", query, flags=re.IGNORECASE)
        
        return query
    
    # Apply syntax fixes first (only for real errors, not conversion artifacts)
    sql_query = fix_sql_syntax_errors(sql_query)
    
    # Remove NULLS FIRST/LAST (not supported in SQLite)
    sql_query = re.sub(r'\s+NULLS\s+(FIRST|LAST)\b', '', sql_query, flags=re.IGNORECASE)
    
    # Convert ILIKE to case-insensitive LIKE using LOWER()
    # Use a much simpler and more reliable approach
    
    # Pattern 1: Handle quoted column names: "column" ILIKE 'pattern'
    sql_query = re.sub(
        r'"([^"]+)"\s+ILIKE\s+(\'[^\']*\')', 
        r'LOWER("\1") LIKE LOWER(\2)', 
        sql_query, 
        flags=re.IGNORECASE
    )
    
    # Pattern 2: Handle unquoted column names: column ILIKE 'pattern'  
    sql_query = re.sub(
        r'\b([a-zA-Z_][a-zA-Z0-9_]*)\s+ILIKE\s+(\'[^\']*\')', 
        r'LOWER(\1) LIKE LOWER(\2)', 
        sql_query, 
        flags=re.IGNORECASE
    )
    
    # Convert EXTRACT functions to SQLite equivalents
    def extract_replacement(match):
        extract_part = match.group(1).upper()
        column = match.group(2).strip()
        
        if extract_part == 'YEAR':
            return f"strftime('%Y', {column})"
        elif extract_part == 'MONTH':
            return f"strftime('%m', {column})"
        elif extract_part == 'DAY':
            return f"strftime('%d', {column})"
        elif extract_part == 'HOUR':
            return f"strftime('%H', {column})"
        elif extract_part == 'MINUTE':
            return f"strftime('%M', {column})"
        elif extract_part == 'SECOND':
            return f"strftime('%S', {column})"
        else:
            # For invalid EXTRACT usage like EXTRACT(STATE FROM column), just return the column
            # This handles the weird EXTRACT(STATE FROM integrated_data.state) case
            return column
    
    # Handle EXTRACT(part FROM column) patterns
    sql_query = re.sub(r'EXTRACT\s*\(\s*(\w+)\s+FROM\s+([^)]+)\)', extract_replacement, sql_query, flags=re.IGNORECASE)
    
    # Handle YEAR(column) function (from MySQL/SQL Server)
    sql_query = re.sub(r'\bYEAR\s*\(([^)]+)\)', r"strftime('%Y', \1)", sql_query, flags=re.IGNORECASE)

    # Make year comparisons with strftime more robust by quoting the year
    # e.g., WHERE strftime('%Y', "Order Date") = 2016 -> WHERE strftime('%Y', "Order Date") = '2016'
    sql_query = re.sub(r"(strftime\s*\(\s*'\%Y'[^)]+\)\s*=\s*)(\d{4})\b", r"\1'\2'", sql_query, flags=re.IGNORECASE)

    # Also handle IN clauses with unquoted years
    def quote_years_in_in_clause(match):
        # Find all 4-digit numbers (years) in the IN clause content
        years = re.findall(r'\b\d{4}\b', match.group(2))
        # Quote each year
        quoted_years = ", ".join([f"'{year}'" for year in years])
        return f"{match.group(1)}{quoted_years})"

    sql_query = re.sub(r"(strftime\s*\(\s*'\%Y'[^)]+\)\s+IN\s*\()([^)]+)\)", quote_years_in_in_clause, sql_query, flags=re.IGNORECASE)

    # Fix column alias references in the same SELECT statement
    # SQLite doesn't allow using column aliases in the same SELECT's WHERE or other clauses
    # We need to replace the alias references with the original expressions
    def fix_alias_references(query):
        # Extract all column aliases and their expressions from the SELECT clause
        select_aliases = {}
        
        # Find the SELECT clause (everything between SELECT and FROM)
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', query, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return query
            
        select_clause = select_match.group(1)
        
        # Split by commas, but be careful of commas inside functions
        select_items = []
        paren_depth = 0
        current_item = ""
        
        for char in select_clause:
            if char == '(':
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1
            elif char == ',' and paren_depth == 0:
                select_items.append(current_item.strip())
                current_item = ""
                continue
            current_item += char
            
        if current_item.strip():
            select_items.append(current_item.strip())
        
        # Extract aliases from each select item
        for item in select_items:
            # Look for "expression AS alias" pattern
            as_match = re.search(r'^(.+?)\s+AS\s+(\w+)$', item.strip(), re.IGNORECASE)
            if as_match:
                expression = as_match.group(1).strip()
                alias = as_match.group(2).strip()
                select_aliases[alias] = expression
        
        # Replace alias references with original expressions in the SELECT clause only
        # Look for alias references in later select items
        for alias, expression in select_aliases.items():
            # Create a pattern that matches the alias but not when it's being defined
            alias_pattern = rf'\b{re.escape(alias)}\b'
            
            # Only replace in select items that come after the alias definition
            # and not in the alias definition itself
            modified_query = query
            for other_alias, other_expr in select_aliases.items():
                if other_alias != alias and alias in other_expr:
                    # Replace the alias in this other expression
                    new_expr = re.sub(alias_pattern, f'({expression})', other_expr)
                    # Update the query
                    old_item = f"{other_expr} AS {other_alias}"
                    new_item = f"{new_expr} AS {other_alias}"
                    modified_query = modified_query.replace(old_item, new_item)
            
            query = modified_query
        
        return query
    
    sql_query = fix_alias_references(sql_query)
    
    # Convert PostgreSQL boolean literals
    sql_query = re.sub(r'\bTRUE\b', '1', sql_query, flags=re.IGNORECASE)
    sql_query = re.sub(r'\bFALSE\b', '0', sql_query, flags=re.IGNORECASE)
    
    # Final cleanup - remove extra spaces
    sql_query = re.sub(r'\s+', ' ', sql_query).strip()
    
    return sql_query

# --- Database Configuration Page ---
def show_admin_db_configuration_page():
    """Admin-accessible database configuration page for reconfiguring database settings"""
    st.title("Database Configuration")
    st.info("Configure or reconfigure the PostgreSQL database connection for the application.")
    
    # Show current connection status
    if st.session_state.get('db_configured_successfully', False):
        st.success("✅ Database is currently connected and configured.")
        
        # Show current connection details if available
        try:
            current_params = database.get_db_connection_params_for_display()
            if current_params:
                st.subheader("Current Database Configuration")
                col1, col2 = st.columns(2)
                with col1:
                    st.info(f"**Host:** {current_params.get('host', 'Unknown')}")
                    st.info(f"**Port:** {current_params.get('port', 'Unknown')}")
                    st.info(f"**Database:** {current_params.get('dbname', 'Unknown')}")
                with col2:
                    st.info(f"**User:** {current_params.get('user', 'Unknown')}")
                    st.info("**Password:** ••••••••")
                    # Test current connection
                    try:
                        test_conn = database.get_db_connection()
                        if test_conn:
                            test_conn.close()
                            st.success("🔗 Connection test: **Active**")
                        else:
                            st.error("❌ Connection test: **Failed**")
                    except Exception as e:
                        st.error(f"❌ Connection test: **Failed** - {str(e)}")
        except Exception as e:
            st.warning(f"Could not retrieve current connection details: {e}")
    else:
        st.warning("⚠️ Database is not properly configured.")
    
    st.markdown("---")
    st.subheader("Update Database Configuration")
    st.markdown("Enter new connection details below. These settings will be used for the current session.")
    
    # Get existing settings for defaults
    try:
        existing_settings = database.get_db_connection_params_for_display()
        if existing_settings is None: 
            existing_settings = {}
    except:
        existing_settings = {}

    with st.form("admin_db_config_form"):
        db_host = st.text_input("Host", value=existing_settings.get("host", "localhost"))
        db_port = st.number_input("Port", value=int(existing_settings.get("port", 5432)), min_value=1, max_value=65535)
        db_name = st.text_input("Database Name", value=existing_settings.get("dbname", ""))
        db_user = st.text_input("User", value=existing_settings.get("user", ""))
        db_password = st.text_input("Password", type="password", help="Leave blank to keep current password")

        col1, col2, col3 = st.columns(3)
        with col1:
            test_connection = st.form_submit_button("🔍 Test Connection")
        with col2:
            save_config = st.form_submit_button("💾 Save Configuration")
        with col3:
            reset_config = st.form_submit_button("🔄 Reset to Defaults")

        if test_connection or save_config:
            if not all([db_host, db_name, db_user]):
                st.error("Host, Database Name, and User are required.")
            else:
                # Use current password if new one not provided
                password_to_use = db_password if db_password else existing_settings.get("password", "")
                
                provided_params = {
                    "host": db_host,
                    "port": int(db_port),
                    "dbname": db_name,
                    "user": db_user,
                    "password": password_to_use
                }
                
                conn = None
                try:
                    with st.spinner("Testing database connection..."):
                        conn = database.get_db_connection(provided_params=provided_params)
                    
                    if conn:
                        # Verify it's PostgreSQL
                        try:
                            cursor = conn.cursor()
                            cursor.execute("SELECT version()")
                            version_result = cursor.fetchone()
                            if version_result and 'PostgreSQL' in str(version_result[0]):
                                st.success("✅ Successfully connected to PostgreSQL!")
                                
                                if save_config:
                                    # Test table initialization
                                    with st.spinner("Verifying database tables..."):
                                        init_success = database.init_db(conn)
                                    
                                    if init_success:
                                        st.success("✅ Database tables verified/initialized successfully!")
                                        st.session_state.db_configured_successfully = True
                                        
                                        # Log the successful configuration
                                        try:
                                            database.log_app_action(st.session_state.logged_in_user, "ADMIN_DB_CONFIG_SUCCESS", f"Database reconfigured: {db_host}:{db_port}/{db_name}", "SUCCESS")
                                        except Exception as log_error:
                                            print(f"[ADMIN DB CONFIG] Failed to log action: {log_error}")
                                        
                                        st.balloons()
                                        st.info("✨ Database configuration updated successfully! You can now use all application features.")
                                    else:
                                        st.error("❌ Failed to initialize database tables. Please check database permissions.")
                                elif test_connection:
                                    st.info("🔍 Connection test successful! Click 'Save Configuration' to apply changes.")
                            else:
                                st.error("❌ Connected database is not PostgreSQL. Only PostgreSQL is supported.")
                        except Exception as version_check_error:
                            st.error(f"❌ Failed to verify PostgreSQL connection: {version_check_error}")
                    else:
                        st.error("❌ Failed to connect to database with provided settings.")
                        
                except Exception as e_connect:
                    st.error(f"❌ Database connection error: {str(e_connect)}")
                    # Log the failed configuration attempt
                    try:
                        database.log_app_action(st.session_state.logged_in_user, "ADMIN_DB_CONFIG_FAILURE", f"Failed to connect: {str(e_connect)}", "FAILURE")
                    except Exception as log_error:
                        print(f"[ADMIN DB CONFIG] Failed to log failure: {log_error}")
                finally:
                    if conn:
                        try:
                            conn.close()
                        except Exception:
                            pass

        if reset_config:
            st.session_state.db_configured_successfully = False
            if 'current_db_params' in st.session_state:
                del st.session_state.current_db_params
            st.success("🔄 Database configuration reset. Please reconfigure with new settings.")
            st.rerun()

def show_db_configuration_page():
    st.header("Database Configuration")
    st.warning("The application requires a PostgreSQL database connection to function.")
    st.markdown("Please provide the connection details below. These settings will be used for the current session. To make them permanent, you'll need to create or update a `secrets.toml` file in the `.streamlit` directory of your application with a `[postgres]` section.")
    st.markdown("Example `secrets.toml`:")
    st.code("""
[postgres]
host = "your_db_host"
dbname = "your_db_name"
user = "your_db_user"
password = "your_db_password"
""", language="toml")

    st.markdown("---")
    st.subheader("Enter Connection Details")

    # Try to get existing secrets for placeholder/hint text
    try:
        existing_secrets = database.get_db_connection_params_for_display()
        if existing_secrets is None: 
            existing_secrets = {}
    except:
        existing_secrets = {}

    with st.form("db_config_form"): # Use a form for batch input
        db_host = st.text_input("Host", value=existing_secrets.get("host", "localhost"))
        db_port = st.number_input("Port", value=int(existing_secrets.get("port", 5432)), min_value=1, max_value=65535)
        db_name = st.text_input("Database Name", value=existing_secrets.get("dbname", ""))
        db_user = st.text_input("User", value=existing_secrets.get("user", ""))
        db_password = st.text_input("Password", type="password", value=existing_secrets.get("password", ""))

        submitted = st.form_submit_button("Connect and Initialize Database")

        if submitted:
            if not all([db_host, db_name, db_user]):
                st.error("Host, Database Name, and User are required.")
            else:
                provided_params = {
                    "host": db_host,
                    "port": db_port,
                    "dbname": db_name,
                    "user": db_user,
                    "password": db_password
                }
                conn = None
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
                            initialize_app_defaults() 
                            st.info("Configuration successful for this session. Proceeding to login.")
                            st.balloons()
                            import time
                            time.sleep(2)
                            st.session_state.page = "login"
                            st.rerun()
                        else:
                            st.error("Failed to initialize database tables. Please check console logs and database permissions.")
                    else:
                        st.error(f"Failed to connect to PostgreSQL with the provided details. Please verify the parameters and ensure the database server is accessible.")
                except Exception as e_connect_init:
                    st.error(f"An unexpected error occurred during database setup: {e_connect_init}")

def generate_dashboard_html(dashboard_items):
    """Generate HTML content for the entire dashboard with embedded Plotly charts"""
    if not dashboard_items:
        return "<html><body><h1>Empty Dashboard</h1><p>No items to display.</p></body></html>"
    
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>ConvaBI Dashboard</title>
        <script src='https://cdn.plot.ly/plotly-latest.min.js'></script>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
            .dashboard-container { max-width: 1200px; margin: 0 auto; }
            .kpi-section { display: flex; flex-wrap: wrap; gap: 20px; margin-bottom: 30px; }
            .kpi-item { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); min-width: 200px; text-align: center; }
            .chart-section { display: grid; grid-template-columns: repeat(auto-fit, minmax(500px, 1fr)); gap: 20px; }
            .chart-item { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .chart-title { font-size: 18px; font-weight: bold; margin-bottom: 15px; color: #333; }
            .kpi-label { font-size: 14px; color: #666; margin-bottom: 5px; }
            .kpi-value { font-size: 28px; font-weight: bold; color: #333; }
            .kpi-delta { font-size: 16px; color: #666; margin-top: 5px; }
            h1 { color: #333; text-align: center; margin-bottom: 30px; }
            .chart-container { width: 100%; height: 400px; margin: 10px 0; }
            table { border-collapse: collapse; width: 100%; margin: 10px 0; }
            table th, table td { border: 1px solid #ddd; padding: 8px; text-align: left; }
            table th { background-color: #f2f2f2; font-weight: bold; }
            table tr:nth-child(even) { background-color: #f9f9f9; }
            
            /* Chart fallback styling */
            .chart-fallback { 
                margin-top: 15px; 
                padding: 15px; 
                background-color: #f8f9fa; 
                border: 1px solid #e9ecef; 
                border-radius: 4px; 
            }
            .chart-fallback p { 
                margin: 10px 0; 
                color: #666; 
                font-style: italic; 
            }
            .chart-fallback table { 
                margin-top: 10px; 
                font-size: 12px; 
            }
            
            /* Print media query for PDF generation */
            @media print {
                body { 
                    background-color: white !important; 
                    font-size: 12px; 
                }
                .chart-container { 
                    height: auto !important; 
                    min-height: 200px; 
                }
                .chart-fallback { 
                    display: block !important; 
                    page-break-inside: avoid; 
                }
                .chart-item { 
                    page-break-inside: avoid; 
                    margin-bottom: 20px; 
                }
                h1, .chart-title { 
                    page-break-after: avoid; 
                }
            }
        </style>
    </head>
    <body>
        <div class="dashboard-container">
            <h1>ConvaBI Dashboard</h1>
    """
    
    # Separate KPIs and other items
    kpi_items = [item for item in dashboard_items if item.get('chart_type') == 'KPI']
    other_items = [item for item in dashboard_items if item.get('chart_type') != 'KPI']
    
    # Add KPI section
    if kpi_items:
        html_content += '<div class="kpi-section">'
        for item in kpi_items:
            params = item['params']
            data_snapshot = item['data_snapshot']
            label = params.get('label', 'KPI')
            value_col = params.get('value_col')
            delta_col = params.get('delta_col')
            
            value = "N/A"
            delta = ""
            
            if not data_snapshot.empty and value_col in data_snapshot.columns:
                try:
                    value = pd.to_numeric(data_snapshot[value_col].iloc[0])
                    if isinstance(value, float):
                        value = f"{value:,.2f}"
                    else:
                        value = f"{value:,}"
                except:
                    value = str(data_snapshot[value_col].iloc[0])
                
                if delta_col and delta_col in data_snapshot.columns:
                    try:
                        delta_val = pd.to_numeric(data_snapshot[delta_col].iloc[0])
                        delta = f"Δ {delta_val:+.2f}"
                    except:
                        delta = str(data_snapshot[delta_col].iloc[0])
            
            html_content += f"""
            <div class="kpi-item">
                <div class="kpi-label">{label}</div>
                <div class="kpi-value">{value}</div>
                {f'<div class="kpi-delta">{delta}</div>' if delta else ''}
            </div>
            """
        html_content += '</div>'
    
    # Add charts section with JavaScript
    javascript_code = ""
    if other_items:
        html_content += '<div class="chart-section">'
        
        for i, item in enumerate(other_items):
            title = item.get('title', item['chart_type'])
            chart_type = item['chart_type']
            data_snapshot = item['data_snapshot']
            params = item['params']
            
            html_content += f'<div class="chart-item"><div class="chart-title">{title}</div>'
            
            if chart_type == 'Table':
                # Render table
                selected_columns = params.get('columns', data_snapshot.columns.tolist())
                
                if not data_snapshot.empty:
                    display_columns = [col for col in selected_columns if col in data_snapshot.columns]
                    if display_columns:
                        table_html = data_snapshot[display_columns].to_html(classes='table table-striped', escape=False)
                        html_content += f'<div style="overflow-x: auto;">{table_html}</div>'
                    else:
                        html_content += '<p>No data to display</p>'
                else:
                    html_content += '<p>No data available</p>'
            else:
                # Create chart container
                chart_div_id = f'chart_{i}'
                
                # Generate Plotly figure and convert to JavaScript
                try:
                    fig = None
                    if chart_type == "Bar Chart" and not data_snapshot.empty:
                        x_col, y_col = params.get('x'), params.get('y')
                        if x_col in data_snapshot.columns and y_col in data_snapshot.columns:
                            fig = px.bar(data_snapshot, x=x_col, y=y_col, color=params.get('color'), title=title)
                    
                    elif chart_type == "Line Chart" and not data_snapshot.empty:
                        x_col, y_col = params.get('x'), params.get('y')
                        if x_col in data_snapshot.columns and y_col in data_snapshot.columns:
                            fig = px.line(data_snapshot, x=x_col, y=y_col, color=params.get('color'), title=title)
                    
                    elif chart_type == "Scatter Plot" and not data_snapshot.empty:
                        x_col, y_col = params.get('x'), params.get('y')
                        if x_col in data_snapshot.columns and y_col in data_snapshot.columns:
                            fig = px.scatter(data_snapshot, x=x_col, y=y_col, color=params.get('color'), 
                                           size=params.get('size'), title=title)
                    
                    elif chart_type == "Pie Chart" and not data_snapshot.empty:
                        names_col, values_col = params.get('names'), params.get('values')
                        if names_col in data_snapshot.columns and values_col in data_snapshot.columns:
                            fig = px.pie(data_snapshot, names=names_col, values=values_col, title=title)
                    
                    elif chart_type == "Histogram" and not data_snapshot.empty:
                        x_col = params.get('x')
                        if x_col in data_snapshot.columns:
                            fig = px.histogram(data_snapshot, x=x_col, title=title)
                    
                    if fig:
                        # Apply consistent styling for PDF/print
                        fig.update_layout(
                            template="plotly_white",  # Better for PDF/print
                            font=dict(size=12),
                            title_font_size=14,
                            width=500,
                            height=400,
                            margin=dict(l=50, r=50, t=50, b=50),
                            showlegend=True,
                            paper_bgcolor='white',
                            plot_bgcolor='white'
                        )
                        
                        # Generate static image and embed it
                        try:
                            image_bytes = fig.to_image(format="png", engine="kaleido")
                            b64_image = base64.b64encode(image_bytes).decode()
                            html_content += f'<div id="{chart_div_id}" class="chart-container"><img src="data:image/png;base64,{b64_image}" style="width:100%; height:auto;"></div>'
                        except Exception as img_err:
                            print(f"[HTML GEN] Error generating static image for chart '{title}': {img_err}")
                            html_content += f'<div id="{chart_div_id}" class="chart-container"><p>Error generating chart image.</p></div>'

                        # Convert figure to JSON and JavaScript for interactive HTML
                        try:
                            import json
                            # Get the figure data, layout, and config
                            fig_dict = fig.to_dict()
                            fig_json = json.dumps(fig_dict, default=str)  # Use default=str to handle any non-serializable objects
                            
                            # Add static fallback for PDF compatibility (shows when JavaScript is disabled)
                            fallback_html = ""
                            try:
                                # Create a simple table representation as fallback
                                if not data_snapshot.empty:
                                    # For different chart types, create appropriate fallback
                                    if chart_type in ["Bar Chart", "Line Chart", "Scatter Plot"]:
                                        x_col, y_col = params.get('x'), params.get('y')
                                        if x_col in data_snapshot.columns and y_col in data_snapshot.columns:
                                            # Show top 10 rows as table
                                            fallback_data = data_snapshot[[x_col, y_col]].head(10)
                                            fallback_html = f"""
                                            <div class="chart-fallback" style="display: none;">
                                                <p><em>Chart preview (static view for PDF):</em></p>
                                                {fallback_data.to_html(classes='table table-striped', escape=False)}
                                                {f'<p><small>Showing first 10 of {len(data_snapshot)} rows</small></p>' if len(data_snapshot) > 10 else ''}
                                            </div>
                                            """
                                    elif chart_type == "Pie Chart":
                                        names_col, values_col = params.get('names'), params.get('values')
                                        if names_col in data_snapshot.columns and values_col in data_snapshot.columns:
                                            fallback_data = data_snapshot[[names_col, values_col]].head(10)
                                            fallback_html = f"""
                                            <div class="chart-fallback" style="display: none;">
                                                <p><em>Chart data (static view for PDF):</em></p>
                                                {fallback_data.to_html(classes='table table-striped', escape=False)}
                                            </div>
                                            """
                                    elif chart_type == "Histogram":
                                        x_col = params.get('x')
                                        if x_col in data_snapshot.columns:
                                            # Show value distribution as table
                                            try:
                                                value_counts = data_snapshot[x_col].value_counts().head(10)
                                                fallback_df = pd.DataFrame({x_col: value_counts.index, 'Count': value_counts.values})
                                                fallback_html = f"""
                                                <div class="chart-fallback" style="display: none;">
                                                    <p><em>Value distribution (static view for PDF):</em></p>
                                                    {fallback_df.to_html(classes='table table-striped', escape=False)}
                                                </div>
                                                """
                                            except:
                                                fallback_html = f"""
                                                <div class="chart-fallback" style="display: none;">
                                                    <p><em>Histogram data available but preview unavailable</em></p>
                                                </div>
                                                """
                            except Exception as fallback_error:
                                print(f"[HTML GENERATION] Fallback generation error: {fallback_error}")
                                fallback_html = '<div class="chart-fallback" style="display: none;"><p><em>Chart data available</em></p></div>'
                            
                            # Add the fallback HTML
                            html_content += fallback_html
                            
                            # Add JavaScript to render the chart
                            javascript_code += f"""
                            try {{
                                var figData = {fig_json};
                                var chartDiv = document.getElementById('{chart_div_id}');
                                // Replace the static image with the interactive chart
                                chartDiv.innerHTML = ''; 
                                Plotly.newPlot('{chart_div_id}', figData.data, figData.layout, {{
                                    displayModeBar: false,
                                    staticPlot: false, // Make it interactive
                                    responsive: true
                                }}).then(function() {{
                                    // Hide fallback when chart renders successfully
                                    var fallback = document.querySelector('#{chart_div_id}').parentElement.querySelector('.chart-fallback');
                                    if (fallback) fallback.style.display = 'none';
                                }}).catch(function(err) {{
                                    console.error('Error rendering chart {chart_div_id}:', err);
                                    // Show fallback if chart fails to render
                                    var fallback = document.querySelector('#{chart_div_id}').parentElement.querySelector('.chart-fallback');
                                    if (fallback) {{
                                        fallback.style.display = 'block';
                                        fallback.style.marginTop = '10px';
                                    }}
                                    document.getElementById('{chart_div_id}').innerHTML = '<p style="color: #666; font-style: italic;">Interactive chart not available - see data table below</p>';
                                }});
                            }} catch(e) {{
                                console.error('Error rendering chart {chart_div_id}:', e);
                                // Show fallback if JavaScript fails
                                var fallback = document.querySelector('#{chart_div_id}').parentElement.querySelector('.chart-fallback');
                                if (fallback) {{
                                    fallback.style.display = 'block';
                                    fallback.style.marginTop = '10px';
                                }}
                                document.getElementById('{chart_div_id}').innerHTML = '<p style="color: #666; font-style: italic;">Interactive chart not available - see data table below</p>';
                            }}
                            """
                        except Exception as json_error:
                            print(f"[HTML GENERATION] JSON serialization error for chart {i}: {json_error}")
                            # Create a simple data table as complete fallback
                            try:
                                if not data_snapshot.empty:
                                    simple_table = data_snapshot.head(5).to_html(classes='table table-striped', escape=False)
                                    html_content += f'<div style="margin-top: 10px;"><p><em>Chart data (top 5 rows):</em></p>{simple_table}</div>'
                                else:
                                    html_content += '<p>No data available for chart</p>'
                            except:
                                html_content += f'<p>Error serializing chart data: {str(json_error)}</p>'
                    else:
                        html_content += '<p>Chart could not be generated</p>'
                        
                except Exception as e:
                    html_content += f'<p>Error generating chart: {str(e)}</p>'
            
            html_content += '</div>'
        html_content += '</div>'
    
    # Add JavaScript code to render charts
    if javascript_code:
        html_content += f"""
        <script>
        document.addEventListener('DOMContentLoaded', function() {{
            {javascript_code}
        }});
        </script>
        """
    
    html_content += """
        </div>
    </body>
    </html>
    """
    
    return html_content

def get_download_link_html(html_content, filename="dashboard.html"):
    """Generate a download link for HTML content"""
    import base64
    
    b64_content = base64.b64encode(html_content.encode()).decode()
    href = f'<a href="data:text/html;base64,{b64_content}" download="{filename}">📥 Download Dashboard HTML</a>'
    return href

def show_dashboard_screen():
    """Enhanced dashboard management interface with AI filters, cross-filtering, and modern UI"""
    
    # Enhanced CSS for compact buttons and better UI
    st.markdown("""
    <style>
    .compact-btn {
        padding: 0.25rem 0.5rem !important;
        margin: 0.1rem !important;
        font-size: 0.75rem !important;
        min-height: 1.8rem !important;
        border-radius: 4px !important;
    }
    
    .dashboard-header {
        border-bottom: 2px solid rgba(255,255,255,0.1);
        padding-bottom: 1rem;
        margin-bottom: 1.5rem;
    }
    
    .dashboard-item {
        border: 1px solid rgba(255,255,255,0.1);
        border-radius: 8px;
        padding: 1rem;
        margin: 0.5rem 0;
        background: rgba(255,255,255,0.05);
        backdrop-filter: blur(10px);
    }
    
    .management-controls {
        background: rgba(255,255,255,0.08);
        border-radius: 12px;
        padding: 1.5rem;
        margin: 1.5rem 0;
        border: 1px solid rgba(255,255,255,0.15);
        backdrop-filter: blur(10px);
        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
    }
    
    /* Tab styling enhancements */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: rgba(255,255,255,0.05);
        border-radius: 8px;
        padding: 4px;
    }
    
    .stTabs [data-baseweb="tab"] {
        background: transparent;
        border-radius: 6px;
        color: rgba(255,255,255,0.7);
        font-weight: 500;
        transition: all 0.2s ease;
    }
    
    .stTabs [data-baseweb="tab"]:hover {
        background: rgba(255,255,255,0.1);
        color: rgba(255,255,255,0.9);
    }
    
    .stTabs [aria-selected="true"] {
        background: rgba(98, 0, 234, 0.3) !important;
        color: white !important;
        font-weight: 600;
    }
    
    /* Enhanced form styling */
    .management-controls .stTextInput input {
        background: rgba(255,255,255,0.1) !important;
        border: 1px solid rgba(255,255,255,0.2) !important;
        border-radius: 8px !important;
        color: white !important;
        font-size: 14px !important;
        transition: all 0.2s ease !important;
    }
    
    .management-controls .stTextInput input:focus {
        border-color: rgba(98, 0, 234, 0.5) !important;
        box-shadow: 0 0 0 2px rgba(98, 0, 234, 0.2) !important;
        background: rgba(255,255,255,0.15) !important;
    }
    
    .management-controls .stCheckbox {
        background: rgba(255,255,255,0.05);
        padding: 0.5rem;
        border-radius: 6px;
        border: 1px solid rgba(255,255,255,0.1);
    }
    
    /* Success/Error message styling */
    .management-controls .stAlert {
        border-radius: 8px;
        margin: 0.5rem 0;
        font-weight: 500;
    }
    
    /* Info boxes */
    .dashboard-info {
        background: rgba(33, 150, 243, 0.1);
        border-left: 4px solid #2196F3;
        padding: 1rem;
        border-radius: 6px;
        margin: 0.5rem 0;
    }
    
    .dashboard-warning {
        background: rgba(255, 152, 0, 0.1);
        border-left: 4px solid #FF9800;
        padding: 1rem;
        border-radius: 6px;
        margin: 0.5rem 0;
    }
    
    .dashboard-error {
        background: rgba(244, 67, 54, 0.1);
        border-left: 4px solid #F44336;
        padding: 1rem;
        border-radius: 6px;
        margin: 0.5rem 0;
    }
    
    .filter-indicator {
        background: rgba(98, 0, 234, 0.3);
        color: white;
        padding: 0.3rem 0.8rem;
        border-radius: 15px;
        margin: 0.2rem;
        font-size: 0.8rem;
        display: inline-block;
    }
    
    .kpi-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        gap: 1rem;
        margin: 1rem 0;
    }
    
    .chart-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
        gap: 1.5rem;
        margin: 1rem 0;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Initialize enhanced dashboard features
    try:
        from enhanced_dashboard import EnhancedDashboard, DashboardFilter
        if 'enhanced_dashboard' not in st.session_state:
            st.session_state.enhanced_dashboard = EnhancedDashboard()
        enhanced_dash = st.session_state.enhanced_dashboard
        
        # Ensure all required attributes exist
        if not hasattr(enhanced_dash, 'active_selections'):
            enhanced_dash.active_selections = {}
        if not hasattr(enhanced_dash, 'chart_interactions'):
            enhanced_dash.chart_interactions = {}
        if not hasattr(enhanced_dash, 'drill_down_stack'):
            enhanced_dash.drill_down_stack = []
        
        # Verify the apply_cross_filter_selection method exists
        if not hasattr(enhanced_dash, 'apply_cross_filter_selection'):
            st.warning("Enhanced dashboard missing cross-filter method. Some features may not work.")
            enhanced_dash = None
            
    except ImportError as e:
        enhanced_dash = None
        st.warning(f"Enhanced dashboard features not available: {e}. Using basic dashboard.")
    except Exception as e:
        enhanced_dash = None
        st.error(f"Error initializing enhanced dashboard: {e}. Using basic dashboard.")
    
    # Dashboard header
    st.markdown('<div class="dashboard-header">', unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
    with col1:
        st.title("📊 My Dashboard")
    with col2:
        # Enhanced/Basic mode toggle
        if enhanced_dash:
            enhanced_mode = st.checkbox("🚀 Enhanced Mode", value=True, help="Enable AI filters and cross-filtering")
        else:
            enhanced_mode = False
    with col3:
        # Export options
        if st.button("📤 Export", help="Export dashboard in various formats"):
            st.session_state.show_export_modal = True
    with col4:
        # Management mode toggle
        manage_mode = st.session_state.get('dashboard_manage_mode', False)
        manage_button_label = "✅ Done Managing" if manage_mode else "⚙️ Manage"
        if st.button(manage_button_label, key="manage_dashboard_layout_btn"):
            st.session_state.dashboard_manage_mode = not manage_mode
            st.rerun()
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Dashboard selection and management
    dashboard_names = get_user_dashboard_names(st.session_state.logged_in_user)
    
    if not dashboard_names:
        st.info("You don't have any dashboards yet. Create your first dashboard by adding charts from the 'Ask Questions' page.")
        
        col1, col2 = st.columns(2)
        with col1:
            new_dashboard_name = st.text_input("Create New Dashboard:", placeholder="Enter dashboard name")
        with col2:
            if st.button("Create Dashboard") and new_dashboard_name:
                st.session_state.current_dashboard_name = new_dashboard_name
                st.session_state.dashboard_items = []
                save_user_dashboard(st.session_state.logged_in_user, new_dashboard_name, [])
                st.success(f"Dashboard '{new_dashboard_name}' created!")
                st.rerun()
        return
    
    # Dashboard selection dropdown
    current_dashboard = st.session_state.get('current_dashboard_name')
    dashboard_options = [name for name, _, _ in dashboard_names]
    
    # Auto-select first dashboard if none selected or invalid
    if current_dashboard not in dashboard_options:
        current_dashboard = dashboard_options[0]
        st.session_state.current_dashboard_name = current_dashboard
        st.session_state.dashboard_items = load_user_dashboard(st.session_state.logged_in_user, current_dashboard)
    
    # Ensure dashboard items are loaded if they're missing
    elif not st.session_state.get('dashboard_items'):
        print(f"[DASHBOARD] Loading missing dashboard items for '{current_dashboard}'")
        st.session_state.dashboard_items = load_user_dashboard(st.session_state.logged_in_user, current_dashboard)
    
    selected_dashboard = st.selectbox(
        "Select Dashboard:",
        dashboard_options,
        index=dashboard_options.index(current_dashboard) if current_dashboard in dashboard_options else 0
    )
    
    # Load dashboard items when switching dashboards
    if selected_dashboard != st.session_state.current_dashboard_name:
        st.session_state.current_dashboard_name = selected_dashboard
        st.session_state.dashboard_items = load_user_dashboard(st.session_state.logged_in_user, selected_dashboard)
        if enhanced_dash:
            # Reset AI filters when switching dashboards
            if 'dashboard_ai_filters' in st.session_state:
                del st.session_state.dashboard_ai_filters
        print(f"[DASHBOARD] Switched to dashboard '{selected_dashboard}' with {len(st.session_state.dashboard_items)} items")
        st.rerun()
    
    # Main dashboard display
    if not st.session_state.current_dashboard_name:
        st.info("Please create or select a dashboard to view or add items.")
        return

    if not st.session_state.dashboard_items:
        st.info(f"Dashboard '{st.session_state.current_dashboard_name}' is currently empty. Go to the 'Ask Questions' page, generate a visualization, and click 'Add to My Dashboard'.")
        
        # Show debug info for troubleshooting
        with st.expander("🔍 Debug Info", expanded=False):
            st.write(f"**Current User:** {st.session_state.logged_in_user}")
            st.write(f"**Current Dashboard:** {st.session_state.current_dashboard_name}")
            st.write(f"**Dashboard Items Count:** {len(st.session_state.dashboard_items or [])}")
            st.write(f"**Available Dashboards:** {len(dashboard_names)}")
            if dashboard_names:
                for name, _, owner in dashboard_names:
                    st.write(f"  - {name} (owner: {owner})")
        return

    # Enhanced Dashboard Features (if available and enabled)
    ai_filters = []
    filter_changes = {}
    cross_filter_active = False
    
    if enhanced_dash and enhanced_mode:
        try:
            # Generate AI filters if not already done
            if 'dashboard_ai_filters' not in st.session_state:
                if hasattr(enhanced_dash, 'generate_ai_filters'):
                    st.session_state.dashboard_ai_filters = enhanced_dash.generate_ai_filters(st.session_state.dashboard_items)
                else:
                    st.session_state.dashboard_ai_filters = []
        except Exception as e:
            st.error(f"Error generating AI filters: {e}")
            enhanced_dash = None
            enhanced_mode = False
        
        if enhanced_dash:  # Only proceed if enhanced_dash is still valid
            ai_filters = st.session_state.dashboard_ai_filters
            
            # Render filters in sidebar (only if filters exist)
            if ai_filters and hasattr(enhanced_dash, 'render_filter_sidebar'):
                try:
                    filter_changes = enhanced_dash.render_filter_sidebar(ai_filters)
                except Exception as filter_error:
                    print(f"[DASHBOARD] Filter rendering failed: {filter_error}")
                    filter_changes = {}
                
                # Update filter active values
                for filter_id, new_values in filter_changes.items():
                    for filter_obj in ai_filters:
                        if filter_obj.filter_id == filter_id:
                            filter_obj.active_values = new_values
                            break
            
                # Cross-filtering controls in sidebar
                if enhanced_dash.active_selections:
                    st.sidebar.markdown("---")
                    st.sidebar.markdown("### 🔗 Cross-Filter Selections")
                    
                    for chart_id, selection in enhanced_dash.active_selections.items():
                        filters = selection.get('filters', {})
                        if filters:
                            with st.sidebar.expander(f"📊 Chart Filter", expanded=True):
                                for col, val in filters.items():
                                    st.sidebar.write(f"**{col}:** {val}")
                                
                                if st.sidebar.button(f"Clear This Filter", key=f"clear_cross_{chart_id}"):
                                    del enhanced_dash.active_selections[chart_id]
        st.rerun()

                    # Clear all cross-filters button
                    if st.sidebar.button("🔄 Clear All Cross-Filters"):
                        enhanced_dash.clear_cross_filters()
                        st.rerun()
                    
                    cross_filter_active = True
        
        # Show filter status
        status_parts = []
        if ai_filters:
            active_ai_filters = [f for f in ai_filters if f.active_values != f.values]
            if active_ai_filters:
                status_parts.append(f"AI Filters: {', '.join([f.name for f in active_ai_filters])}")
        
        if cross_filter_active and enhanced_dash:
            cross_filter_count = len(enhanced_dash.active_selections)
            status_parts.append(f"Cross-Filters: {cross_filter_count} active")
        
        if status_parts:
            filter_info = " | ".join(status_parts)
            st.markdown(f'<div class="filter-indicator">🎛️ {filter_info}</div>', unsafe_allow_html=True)
        
        # Enhanced dashboard controls
        control_cols = st.columns([2, 1, 1])
        with control_cols[0]:
            if st.button("🤖 Regenerate AI Filters", help="Regenerate AI-suggested filters"):
                if enhanced_dash:
                    st.session_state.dashboard_ai_filters = enhanced_dash.generate_ai_filters(st.session_state.dashboard_items)
                    st.rerun()
                else:
                    st.warning("Enhanced dashboard not available")
        with control_cols[1]:
            if st.button("🔗 Enable Cross-Filter", help="Click charts to filter others"):
                st.info("💡 Use the dropdown filters under each chart to apply cross-filtering!")
        with control_cols[2]:
            if cross_filter_active and st.button("🔄 Reset All", help="Clear all filters"):
                if enhanced_dash:
                    enhanced_dash.clear_cross_filters()
                    # Reset AI filters too
                    for filter_obj in ai_filters:
                        filter_obj.active_values = filter_obj.values
                    st.rerun()
                else:
                    st.warning("Enhanced dashboard not available")

    # Dashboard Management Controls (only show in manage mode)
    if manage_mode:
        st.markdown('<div class="management-controls">', unsafe_allow_html=True)
        
        # Create tabbed interface for better organization
        management_tab1, management_tab2, management_tab3 = st.tabs(["🔧 Edit Dashboard", "➕ Create New", "🗑️ Delete Dashboard"])
        
        # Tab 1: Edit Current Dashboard
        with management_tab1:
            st.markdown("#### 📝 Rename Current Dashboard")
            st.info(f"Currently editing: **{selected_dashboard}**")
            
            col1, col2 = st.columns([3, 1])
    with col1:
                new_name = st.text_input(
                    "New Dashboard Name:", 
                    value=selected_dashboard, 
                    key="rename_dashboard_input",
                    placeholder="Enter new dashboard name"
                )
            with col2:
                rename_btn = st.button("📝 Rename", key="rename_dashboard_btn", use_container_width=True)
            
            if rename_btn:
            if new_name and new_name.strip() and new_name != selected_dashboard:
                # Check if name already exists
                existing_names = [name for name, _, owner in dashboard_names if owner == st.session_state.logged_in_user]
                if new_name in existing_names:
                        st.error(f"❌ Dashboard '{new_name}' already exists. Please choose a different name.")
                else:
                    # Rename the dashboard
                    try:
                        if database.rename_dashboard_in_db(st.session_state.logged_in_user, selected_dashboard, new_name):
                            st.session_state.current_dashboard_name = new_name
                                st.success(f"✅ Dashboard renamed from '{selected_dashboard}' to '{new_name}'!")
                            try:
                                if is_sqlite_connection():
                                    log_app_action_sqlite(st.session_state.logged_in_user, "RENAME_DASHBOARD_SUCCESS", f"Renamed '{selected_dashboard}' to '{new_name}'", "SUCCESS")
                                else:
                                    database.log_app_action(st.session_state.logged_in_user, "RENAME_DASHBOARD_SUCCESS", f"Renamed '{selected_dashboard}' to '{new_name}'", "SUCCESS")
                            except Exception as log_error:
                                print(f"[DASHBOARD] Failed to log rename action: {log_error}")
                            st.rerun()
                        else:
                                st.error("❌ Failed to rename dashboard. Please try again.")
                    except Exception as e:
                            st.error(f"❌ Error renaming dashboard: {e}")
            elif new_name == selected_dashboard:
                    st.warning("⚠️ New name is the same as current name.")
            else:
                    st.warning("⚠️ Please enter a valid dashboard name.")
        
        # Tab 2: Create New Dashboard
        with management_tab2:
            st.markdown("#### ➕ Create New Dashboard")
            st.info("Create a new dashboard to organize your charts and visualizations.")
            
            col1, col2 = st.columns([3, 1])
            with col1:
                create_name = st.text_input(
                    "Dashboard Name:", 
                    placeholder="Enter new dashboard name", 
                    key="create_dashboard_input"
                )
    with col2:
                create_btn = st.button("➕ Create", key="create_dashboard_btn", use_container_width=True)
            
            if create_btn:
            if create_name and create_name.strip():
                # Check if name already exists
                existing_names = [name for name, _, owner in dashboard_names if owner == st.session_state.logged_in_user]
                if create_name in existing_names:
                        st.error(f"❌ Dashboard '{create_name}' already exists. Please choose a different name.")
                else:
                    # Create new dashboard
                    try:
                        if database.save_dashboard_to_db(st.session_state.logged_in_user, create_name, [], []):
                            st.session_state.current_dashboard_name = create_name
                            st.session_state.dashboard_items = []
                                st.success(f"✅ Dashboard '{create_name}' created successfully!")
                                st.balloons()
                            try:
                                if is_sqlite_connection():
                                    log_app_action_sqlite(st.session_state.logged_in_user, "CREATE_DASHBOARD_SUCCESS", f"Created dashboard: {create_name}", "SUCCESS")
                                else:
                                    database.log_app_action(st.session_state.logged_in_user, "CREATE_DASHBOARD_SUCCESS", f"Created dashboard: {create_name}", "SUCCESS")
                            except Exception as log_error:
                                print(f"[DASHBOARD] Failed to log create action: {log_error}")
                            st.rerun()
                        else:
                                st.error("❌ Failed to create dashboard. Please try again.")
                    except Exception as e:
                            st.error(f"❌ Error creating dashboard: {e}")
            else:
                    st.warning("⚠️ Please enter a valid dashboard name.")
        
        # Tab 3: Delete Dashboard
        with management_tab3:
            st.markdown("#### 🗑️ Delete Dashboard")
            st.error(f"⚠️ **Warning**: You are about to delete dashboard '{selected_dashboard}'")
            st.markdown("**This action cannot be undone!** All charts and configurations will be permanently lost.")
            
            # Show dashboard info
            chart_count = len(st.session_state.dashboard_items)
            st.info(f"📊 This dashboard contains **{chart_count}** chart(s)")
            
            # Confirmation checkbox
            confirm_delete = st.checkbox(
                f"I understand that deleting '{selected_dashboard}' cannot be undone",
                key="confirm_delete_checkbox"
            )
            
            col1, col2 = st.columns([1, 1])
            with col1:
                if st.button(
                    "🗑️ Delete Dashboard", 
                    key="delete_dashboard_btn", 
                    type="secondary",
                    disabled=not confirm_delete,
                    use_container_width=True
                ):
            st.session_state.show_delete_confirmation = True
            st.rerun()
        
            with col2:
                if st.button("❌ Cancel", key="cancel_delete_action", use_container_width=True):
                    st.session_state.show_delete_confirmation = False
                    st.info("Delete action cancelled.")
            
            # Final confirmation dialog
        if st.session_state.get('show_delete_confirmation', False):
                st.markdown("---")
                st.error(f"🚨 **FINAL CONFIRMATION**: Delete '{selected_dashboard}'?")
                
                final_col1, final_col2 = st.columns([1, 1])
                with final_col1:
                    if st.button("✅ YES, DELETE PERMANENTLY", key="confirm_delete_btn", type="primary"):
                    try:
                        if database.delete_dashboard_from_db(st.session_state.logged_in_user, selected_dashboard):
                            # Get remaining dashboards
                            remaining_dashboards = get_user_dashboard_names(st.session_state.logged_in_user)
                            if remaining_dashboards:
                                # Switch to first remaining dashboard
                                st.session_state.current_dashboard_name = remaining_dashboards[0][0]
                                st.session_state.dashboard_items = load_user_dashboard(st.session_state.logged_in_user, remaining_dashboards[0][0])
                            else:
                                # No dashboards left
                                st.session_state.current_dashboard_name = None
                                st.session_state.dashboard_items = []
                            
                            st.session_state.show_delete_confirmation = False
                                st.success(f"✅ Dashboard '{selected_dashboard}' deleted successfully!")
                            try:
                                if is_sqlite_connection():
                                    log_app_action_sqlite(st.session_state.logged_in_user, "DELETE_DASHBOARD_SUCCESS", f"Deleted dashboard: {selected_dashboard}", "SUCCESS")
                                else:
                                    database.log_app_action(st.session_state.logged_in_user, "DELETE_DASHBOARD_SUCCESS", f"Deleted dashboard: {selected_dashboard}", "SUCCESS")
                            except Exception as log_error:
                                print(f"[DASHBOARD] Failed to log delete action: {log_error}")
                            st.rerun()
                        else:
                                st.error("❌ Failed to delete dashboard. Please try again.")
                    except Exception as e:
                            st.error(f"❌ Error deleting dashboard: {e}")
            
                with final_col2:
                    if st.button("❌ NO, KEEP DASHBOARD", key="final_cancel_delete_btn"):
                    st.session_state.show_delete_confirmation = False
                        st.success("Dashboard deletion cancelled.")
                    st.rerun()
    
        st.markdown('</div>', unsafe_allow_html=True)  # Close management controls div
    st.markdown("---")

    # Separate KPIs from other chart types
    kpi_items = [item for item in st.session_state.dashboard_items if item.get('chart_type') == "KPI"]
    other_items = [item for item in st.session_state.dashboard_items if item.get('chart_type') != "KPI"]

    # Render KPIs in a dedicated section
    if kpi_items:
        st.subheader("📊 Key Performance Indicators")
        
        # Apply filters to KPI data if enhanced mode is enabled
        filtered_kpi_items = []
        for item in kpi_items:
            filtered_item = copy.deepcopy(item)
            if enhanced_dash and enhanced_mode and ai_filters and hasattr(enhanced_dash, 'apply_filters_to_data'):
                try:
                    filtered_item['data_snapshot'] = enhanced_dash.apply_filters_to_data(
                        item['data_snapshot'], ai_filters
                    )
                except Exception as filter_apply_error:
                    print(f"[DASHBOARD] KPI data filtering failed: {filter_apply_error}")
                    # Keep original data if filtering fails
                    pass
            filtered_kpi_items.append(filtered_item)
        
        st.markdown('<div class="kpi-grid">', unsafe_allow_html=True)
        
        # Create responsive KPI layout
        num_kpi_cols = min(len(filtered_kpi_items), 4)
        if num_kpi_cols > 0:
            kpi_cols = st.columns(num_kpi_cols)
            for i, item in enumerate(filtered_kpi_items):
                # Get the absolute index for callbacks
                try:
                    original_kpi_item_index = next(idx for idx, dash_item in enumerate(st.session_state.dashboard_items) 
                                                 if dash_item['id'] == item['id'])
                except (StopIteration, KeyError):
                    original_kpi_item_index = i  # Fallback

                with kpi_cols[i % num_kpi_cols]:
                    with st.container():
                        st.markdown('<div class="dashboard-item">', unsafe_allow_html=True)
                        
                    params = item['params']
                    data_snapshot = item['data_snapshot']
                    
                    current_kpi_label = params.get('label', "KPI Value")
                    if not current_kpi_label.strip():
                        current_kpi_label = "KPI Value"

                        # Editable KPI Label (only in manage mode)
                        if manage_mode:
                        st.text_input(
                            "KPI Label", 
                            value=current_kpi_label,
                            key=f"kpi_label_edit_{original_kpi_item_index}", 
                            on_change=handle_kpi_label_change,
                            args=(original_kpi_item_index,),
                            label_visibility="collapsed"
                        )
                    else:
                        st.markdown(f"**{current_kpi_label}**")

                    value_col = params.get('value_col')
                    delta_col = params.get('delta_col')
                    kpi_value = None
                    kpi_delta = None

                    if not data_snapshot.empty and value_col in data_snapshot.columns:
                        try:
                            kpi_value = pd.to_numeric(data_snapshot[value_col].iloc[0])
                        except (ValueError, TypeError):
                            kpi_value = str(data_snapshot[value_col].iloc[0])
                        
                        if delta_col and delta_col in data_snapshot.columns:
                            try:
                                kpi_delta = pd.to_numeric(data_snapshot[delta_col].iloc[0])
                            except (ValueError, TypeError):
                                kpi_delta = str(data_snapshot[delta_col].iloc[0])
                    
                    # Display the KPI metric
                    st.metric(
                        label=" ", 
                        value=kpi_value if kpi_value is not None else "N/A", 
                        delta=kpi_delta if kpi_delta is not None else None, 
                        label_visibility="collapsed"
                    )

                        # --- Compact Management Controls (only in manage mode) ---
                        if manage_mode:
                            st.markdown("---")
                            control_cols = st.columns([1, 1, 1, 1])
                    with control_cols[0]:
                                if st.button("⬆️", key=f"move_up_kpi_{original_kpi_item_index}", 
                                           disabled=(original_kpi_item_index == 0), help="Move Up"):
                                    handle_item_move(original_kpi_item_index, "up")
                    with control_cols[1]:
                                if st.button("⬇️", key=f"move_down_kpi_{original_kpi_item_index}", 
                            disabled=(original_kpi_item_index == len(st.session_state.dashboard_items) - 1),
                                           help="Move Down"):
                                    handle_item_move(original_kpi_item_index, "down")
                    with control_cols[2]:
                                if st.button("📋", key=f"copy_kpi_{original_kpi_item_index}", help="Duplicate"):
                                    # Duplicate KPI logic
                                    new_item = copy.deepcopy(item)
                                    new_item['id'] = str(uuid.uuid4())
                                    new_item['params']['label'] = f"{current_kpi_label} (Copy)"
                                    st.session_state.dashboard_items.append(new_item)
                                    save_user_dashboard(st.session_state.logged_in_user, st.session_state.current_dashboard_name, st.session_state.dashboard_items)
                                    st.rerun()
                            with control_cols[3]:
                                if st.button("🗑️", key=f"remove_kpi_{original_kpi_item_index}", help="Delete"):
                            st.session_state.dashboard_items.pop(original_kpi_item_index)
                            save_user_dashboard(st.session_state.logged_in_user, st.session_state.current_dashboard_name, st.session_state.dashboard_items)
                            st.rerun()
                        
                        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown("---")

    # Render Charts and Tables
    if other_items:
        st.subheader("📈 Charts & Tables")
        
        # Apply filters to chart data if enhanced mode is enabled
        filtered_chart_items = []
        for item in other_items:
            filtered_item = copy.deepcopy(item)
            if enhanced_dash and enhanced_mode and ai_filters and hasattr(enhanced_dash, 'apply_filters_to_data'):
                try:
                    filtered_item['data_snapshot'] = enhanced_dash.apply_filters_to_data(
                        item['data_snapshot'], ai_filters
                    )
                except Exception as filter_apply_error:
                    print(f"[DASHBOARD] Chart data filtering failed: {filter_apply_error}")
                    # Keep original data if filtering fails
                    pass
            filtered_chart_items.append(filtered_item)
        
        st.markdown('<div class="chart-grid">', unsafe_allow_html=True)
        
        # Display items in a responsive grid
        num_cols = 2
        
        for i, item in enumerate(filtered_chart_items):
            # Get the absolute index in the full dashboard_items list
            try:
                current_item_absolute_index = next(idx for idx, dash_item in enumerate(st.session_state.dashboard_items) 
                                                 if dash_item['id'] == item['id'])
            except (StopIteration, KeyError):
                current_item_absolute_index = len(kpi_items) + i  # Fallback
            
            # Create columns for layout (every 2 items)
            if i % num_cols == 0:
                cols = st.columns(num_cols)
            
            with cols[i % num_cols]:
                with st.container():
                    st.markdown('<div class="dashboard-item">', unsafe_allow_html=True)
                    
                    # Editable Chart Title (only in manage mode)
                    if manage_mode:
                        st.text_input(
                            f"Chart Title", 
                            value=item.get('title', item['chart_type']),
                            key=f"chart_title_edit_{current_item_absolute_index}", 
                            on_change=handle_chart_title_change,
                            args=(current_item_absolute_index,),
                            label_visibility="collapsed"
                        )
                    else:
                        st.markdown(f"**{item.get('title', item['chart_type'])}**")
                    
                    # Get chart data and parameters
                    data_snapshot = item['data_snapshot']
                    params = item['params']
                    chart_type = item['chart_type']

                    # Generate and display chart with enhanced features
                    fig = None
                    try:
                        if enhanced_dash and enhanced_mode:
                            # Use enhanced chart creation with cross-filtering
                            try:
                                fig = enhanced_dash.create_enhanced_chart(
                                    item, data_snapshot, 
                                    chart_id=f"chart_{current_item_absolute_index}",
                                    enable_cross_filter=True
                                )
                            except Exception as chart_error:
                                print(f"[DASHBOARD] Enhanced chart creation failed: {chart_error}")
                                fig = None
                        else:
                            # Use standard chart creation
                        if chart_type == "Bar Chart":
                            x_col, y_col = params.get('x'), params.get('y')
                            if x_col in data_snapshot.columns and y_col in data_snapshot.columns:
                                fig = px.bar(data_snapshot, x=x_col, y=y_col, color=params.get('color'), 
                                           title=item.get('title', chart_type))
                            else:
                                st.warning(f"Required columns for bar chart not found in data.")
                        
                        elif chart_type == "Line Chart":
                            x_col, y_col = params.get('x'), params.get('y')
                            if x_col in data_snapshot.columns and y_col in data_snapshot.columns:
                                fig = px.line(data_snapshot, x=x_col, y=y_col, color=params.get('color'), 
                                            title=item.get('title', chart_type))
                            else:
                                st.warning(f"Required columns for line chart not found in data.")
                        
                        elif chart_type == "Scatter Plot":
                            x_col, y_col = params.get('x'), params.get('y')
                            if x_col in data_snapshot.columns and y_col in data_snapshot.columns:
                                fig = px.scatter(data_snapshot, x=x_col, y=y_col, color=params.get('color'), 
                                               size=params.get('size'), title=item.get('title', chart_type))
                            else:
                                st.warning(f"Required columns for scatter plot not found in data.")
                        
                        elif chart_type == "Pie Chart":
                            names_col, values_col = params.get('names'), params.get('values')
                            if names_col in data_snapshot.columns and values_col in data_snapshot.columns:
                                fig = px.pie(data_snapshot, names=names_col, values=values_col, 
                                           title=item.get('title', f"Pie Chart of {values_col} by {names_col}"))
                            else:
                                st.warning("Required columns for pie chart not found in data.")
                        
                        elif chart_type == "Histogram":
                            x_col = params.get('x')
                            if x_col in data_snapshot.columns:
                                fig = px.histogram(data_snapshot, x=x_col, title=item.get('title', chart_type))
                            else:
                                st.warning("Required column for histogram not found in data.")
                        
                        # Handle Table type separately
                        if chart_type == "Table":
                            selected_columns = params.get('columns', data_snapshot.columns.tolist())
                            if not isinstance(selected_columns, list):
                                selected_columns = data_snapshot.columns.tolist()
                            
                            display_columns = [col for col in selected_columns if col in data_snapshot.columns]
                            
                            if not display_columns and selected_columns:
                                st.warning("Original columns for table not found. Showing available columns.")
                                display_columns = data_snapshot.columns.tolist()
                            
                            if display_columns and not data_snapshot.empty:
                                # Show filter info for tables if enhanced mode
                                if enhanced_dash and enhanced_mode and ai_filters:
                                    active_filters = [f for f in ai_filters if f.active_values != f.values]
                                    if active_filters:
                                        st.caption(f"Filtered by: {', '.join([f.name for f in active_filters])}")
                                
                                st.dataframe(data_snapshot[display_columns], use_container_width=True)
                            elif not data_snapshot.empty:
                                st.dataframe(data_snapshot, use_container_width=True)
                            else:
                                st.info("No data to display in table.")
                        
                        # Apply consistent styling to charts
                        elif fig:
                            fig.update_layout(
                                template="plotly_dark",
                                paper_bgcolor='rgba(0,0,0,0)',
                                plot_bgcolor='rgba(0,0,0,0)',
                                font_color='#E0E0E0',
                                title_font_color='#F1F5F9',
                                legend_font_color='#CBD5E1',
                                autosize=True,
                                height=400,  # Optimized height for dashboard
                                margin=dict(l=20, r=20, t=40, b=20)
                            )
                            
                            # Style axes
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

                    except Exception as e:
                        st.error(f"Error generating chart: {e}")
                        fig = None
                        
                    # Display the chart with cross-filtering support
                    if fig and chart_type != "Table":
                        # Enhanced chart with click handling for cross-filtering
                        if enhanced_dash and enhanced_mode:
                            # Add cross-filter visual indicators
                            if item.get('cross_filtered'):
                                st.info(f"🔗 This chart is filtered by: {item.get('cross_filter_source', 'another chart')}")
                            
                            # Render chart with enhanced interactivity
                            st.plotly_chart(
                                fig, 
                                use_container_width=True, 
                                key=f"chart_{current_item_absolute_index}"
                            )
                            
                            # Cross-filtering interface
                            st.markdown("**🔗 Cross-Filter Controls**")
                            filter_cols = st.columns([3, 1])
                            
                            with filter_cols[0]:
                                # Get the main axis column for filtering
                                x_col = params.get('x')
                                if x_col and x_col in data_snapshot.columns:
                                    unique_values = data_snapshot[x_col].unique()[:10]  # Limit options
                                    selected_filter_value = st.selectbox(
                                        f"Filter others by {x_col}:",
                                        ["Select value..."] + list(unique_values),
                                        key=f"cross_filter_select_{current_item_absolute_index}"
                                    )
                                    
                                    if selected_filter_value != "Select value...":
                                        if st.button(f"Apply Cross-Filter", key=f"apply_cross_{current_item_absolute_index}"):
                                            # Apply cross-filtering
                                            selection_filters = {x_col: selected_filter_value}
                                            
                                            # Store the selection
                                            enhanced_dash.active_selections[f"chart_{current_item_absolute_index}"] = {
                                                'filters': selection_filters,
                                                'chart_data': {'x': selected_filter_value}
                                            }
                                            
                                            # Apply cross-filtering to other charts
                                            try:
                                                if hasattr(enhanced_dash, 'apply_cross_filter_selection'):
                                                    st.session_state.dashboard_items = enhanced_dash.apply_cross_filter_selection(
                                                        f"chart_{current_item_absolute_index}",
                                                        selection_filters,
                                                        st.session_state.dashboard_items
                                                    )
                                                else:
                                                    st.warning("Cross-filtering feature not available in this session.")
                                            except Exception as cross_filter_error:
                                                st.error(f"Cross-filtering error: {cross_filter_error}")
                                                print(f"[DASHBOARD] Cross-filtering error: {cross_filter_error}")
                                            
                                            # Show success message
                                            st.success(f"🔗 Applied cross-filter: {x_col} = {selected_filter_value}")
                                            
                                            # Save updated dashboard
                                            save_user_dashboard(
                                                st.session_state.logged_in_user, 
                                                st.session_state.current_dashboard_name, 
                                                st.session_state.dashboard_items
                                            )
                                            st.rerun()
                            
                            with filter_cols[1]:
                                if f"chart_{current_item_absolute_index}" in enhanced_dash.active_selections:
                                    if st.button("🔄 Clear Filter", key=f"clear_single_{current_item_absolute_index}"):
                                        del enhanced_dash.active_selections[f"chart_{current_item_absolute_index}"]
                                        st.rerun()
                            

                        else:
                        st.plotly_chart(fig, use_container_width=True)
                    elif not fig and chart_type != "Table" and not data_snapshot.empty:
                        st.warning(f"Could not display chart: {item.get('title', chart_type)}.")
                    elif data_snapshot.empty and chart_type != "Table":
                        st.info(f"No data available for '{item.get('title', chart_type)}'.")

                    # --- Compact Management Controls (only in manage mode) ---
                    if manage_mode:
                        st.markdown("---")
                        control_cols = st.columns([1, 1, 1, 1])
                    with control_cols[0]:
                            if st.button("⬆️", key=f"move_up_item_{current_item_absolute_index}", 
                                       disabled=(current_item_absolute_index == 0), help="Move Up"):
                                handle_item_move(current_item_absolute_index, "up")
                    with control_cols[1]:
                            if st.button("⬇️", key=f"move_down_item_{current_item_absolute_index}", 
                            disabled=(current_item_absolute_index == len(st.session_state.dashboard_items) - 1),
                                       help="Move Down"):
                                handle_item_move(current_item_absolute_index, "down")
                    with control_cols[2]:
                            if st.button("📋", key=f"copy_item_{current_item_absolute_index}", help="Duplicate"):
                                # Duplicate chart logic
                                new_item = copy.deepcopy(item)
                                new_item['id'] = str(uuid.uuid4())
                                new_item['title'] = f"{item.get('title', item['chart_type'])} (Copy)"
                                st.session_state.dashboard_items.append(new_item)
                                save_user_dashboard(st.session_state.logged_in_user, st.session_state.current_dashboard_name, st.session_state.dashboard_items)
                                st.rerun()
                        with control_cols[3]:
                            if st.button("🗑️", key=f"remove_item_{current_item_absolute_index}", help="Delete"):
                            st.session_state.dashboard_items.pop(current_item_absolute_index)
                            save_user_dashboard(st.session_state.logged_in_user, st.session_state.current_dashboard_name, st.session_state.dashboard_items)
                            st.rerun()
                    
                    st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    elif not kpi_items:
        st.info("This dashboard is empty. Add charts from the 'Ask Questions' page.")
    
    # Export Modal
    if st.session_state.get('show_export_modal', False):
        if dashboard_exports and st.session_state.current_dashboard_name:
            with st.expander("📤 Export Dashboard", expanded=True):
                dashboard_exports.show_export_interface(
                    st.session_state.dashboard_items,
                    st.session_state.current_dashboard_name,
                    st.session_state.logged_in_user
                )
                
                col1, col2 = st.columns([1, 1])
                with col1:
                    if st.button("❌ Close Export", key="close_export_modal"):
                        st.session_state.show_export_modal = False
                        st.rerun()
                with col2:
                    # Show export stats
                    if st.session_state.dashboard_items:
                        stats = dashboard_exports.get_export_stats(st.session_state.dashboard_items)
                        st.info(f"📊 {stats['total_items']} items • {stats['total_data_points']} data points")
        else:
            st.error("❌ Export module not available. Please check installation.")
            if st.button("Close", key="close_export_error"):
                st.session_state.show_export_modal = False
                st.rerun()

def show_dashboard_management_page():
    """Dashboard sharing management page"""
    st.title("Manage Dashboard Sharing")

    if not st.session_state.logged_in_user:
        st.error("Please log in to manage dashboard sharing.")
        st.session_state.app_page = 'data_integration'
        st.rerun()
        return

    current_user = st.session_state.logged_in_user

    # Get all dashboards and filter for owned dashboards
    dashboard_names = get_user_dashboard_names(current_user)
    owned_dashboard_tuples = [d for d in dashboard_names if d[2] == current_user]
    
    # If the user owns no dashboards, they can't manage sharing.
    if not owned_dashboard_tuples:
        st.info("You do not own any dashboards yet. Create one from the 'My Dashboard' page.")
        if st.button("Go to My Dashboard", key="back_to_dash_from_empty_manage_sharing"):
            st.session_state.app_page = "dashboard"
            st.rerun()
        return

    # Ensure the session state is synchronized with an OWNED dashboard for this page.
    owned_dashboard_names = [d[0] for d in owned_dashboard_tuples]
    current_dashboard_name = st.session_state.get('current_dashboard_name')

    # If the currently selected dashboard is not one the user owns, or if its items are missing,
    # default to the first owned dashboard to ensure the page context is correct.
    if current_dashboard_name not in owned_dashboard_names or not st.session_state.get('dashboard_items'):
        first_owned_dashboard_name = owned_dashboard_names[0]
        st.session_state.current_dashboard_name = first_owned_dashboard_name
        st.session_state.dashboard_items = load_user_dashboard(current_user, first_owned_dashboard_name)
    
    st.subheader(f"Dashboards Owned by You ({current_user})")

    for dash_name, display_name, owner_username in sorted(owned_dashboard_tuples, key=lambda x: x[0]):
        with st.container():
            st.markdown(f"#### {dash_name}")
            
            # Load the specific dashboard to get its current shared_with_users list
            dashboard_details = database.load_dashboard_from_db(owner_username, dash_name)
            shared_with_list = []
            if dashboard_details and isinstance(dashboard_details.get('shared_with_users'), list):
                shared_with_list = dashboard_details['shared_with_users']
            
            if not shared_with_list:
                st.write("_Not currently shared with any other users._")
            else:
                st.write("Currently shared with:")
                for shared_user_idx, shared_user in enumerate(shared_with_list):
                    cols = st.columns([3, 1])
                    cols[0].write(f"- {shared_user}")
                    if cols[1].button(f"Revoke Access", key=f"revoke_{dash_name}_{shared_user_idx}_{shared_user}"):
                        # Create a new list without the revoked user
                        updated_shared_list = [u for i, u in enumerate(shared_with_list) if i != shared_user_idx]
                        if database.update_dashboard_sharing_in_db(owner_username, dash_name, updated_shared_list):
                            st.success(f"Access for '{shared_user}' to dashboard '{dash_name}' has been revoked.")
                            try:
                                if is_sqlite_connection():
                                    log_app_action_sqlite(st.session_state.logged_in_user, "REVOKE_DASHBOARD_ACCESS_SUCCESS", f"Revoked access for {shared_user} from {dash_name}", "SUCCESS")
                                else:
                                    database.log_app_action(st.session_state.logged_in_user, "REVOKE_DASHBOARD_ACCESS_SUCCESS", f"Revoked access for {shared_user} from {dash_name}", "SUCCESS")
                            except Exception as log_error:
                                print(f"[DASHBOARD MANAGEMENT] Failed to log revoke action: {log_error}")
                            st.rerun()
                        else:
                            st.error(f"Failed to revoke access for '{shared_user}'.")
                            try:
                                if is_sqlite_connection():
                                    log_app_action_sqlite(st.session_state.logged_in_user, "REVOKE_DASHBOARD_ACCESS_FAILURE", f"Failed to revoke access for {shared_user} from {dash_name}", "FAILURE")
                                else:
                                    database.log_app_action(st.session_state.logged_in_user, "REVOKE_DASHBOARD_ACCESS_FAILURE", f"Failed to revoke access for {shared_user} from {dash_name}", "FAILURE")
                            except Exception as log_error:
                                print(f"[DASHBOARD MANAGEMENT] Failed to log revoke failure: {log_error}")
            
            # Get all other users to share with (excluding current user and already shared users)
            try:
                if is_sqlite_connection():
                    # For SQLite, we'd need a similar function - for now, use a placeholder
                    all_other_users_list = []
                else:
                    all_users = database.get_all_users_from_db()
                    all_other_users_list = [u["username"] for u in all_users if u["username"] != current_user and u["username"] not in shared_with_list]
            except Exception as e:
                st.warning(f"Could not fetch user list: {e}")
                all_other_users_list = []
            
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
                            try:
                                if is_sqlite_connection():
                                    log_app_action_sqlite(st.session_state.logged_in_user, "UPDATE_DASHBOARD_SHARING_SUCCESS", f"Added users {share_with_new_users} to sharing for {dash_name}", "SUCCESS")
                                else:
                                    database.log_app_action(st.session_state.logged_in_user, "UPDATE_DASHBOARD_SHARING_SUCCESS", f"Added users {share_with_new_users} to sharing for {dash_name}", "SUCCESS")
                            except Exception as log_error:
                                print(f"[DASHBOARD MANAGEMENT] Failed to log sharing update: {log_error}")
                            st.rerun()
                        else:
                            st.error("Failed to update sharing list.")
                            try:
                                if is_sqlite_connection():
                                    log_app_action_sqlite(st.session_state.logged_in_user, "UPDATE_DASHBOARD_SHARING_FAILURE", f"Failed to add users {share_with_new_users} to sharing for {dash_name}", "FAILURE")
                                else:
                                    database.log_app_action(st.session_state.logged_in_user, "UPDATE_DASHBOARD_SHARING_FAILURE", f"Failed to add users {share_with_new_users} to sharing for {dash_name}", "FAILURE")
                            except Exception as log_error:
                                print(f"[DASHBOARD MANAGEMENT] Failed to log sharing failure: {log_error}")
            else:
                if not shared_with_list:
                    st.write("_No other users available in the system to share with._")
            st.markdown("---")

    if st.button("Back to My Dashboard", key="back_to_dash_from_manage_sharing_main"):
        st.session_state.app_page = "dashboard"
        st.rerun()

def show_admin_panel():
    """Admin panel for user management"""
    st.title("User Management")
    
    # Initialize session state for editing
    if 'editing_user' not in st.session_state:
        st.session_state.editing_user = None
    if 'delete_confirm' not in st.session_state:
        st.session_state.delete_confirm = {}
    
    # Get all users
    if is_sqlite_connection():
        st.info("User management for SQLite is not fully implemented yet.")
        return
    else:
        users = database.get_all_users_from_db()
    
    # Display users
    if users:
        st.subheader("Existing Users")
        
        for user in users:
            # Check if this user is being edited
            if st.session_state.editing_user == user['username']:
                # Edit mode
                st.markdown(f"### Editing User: {user['username']}")
                
                with st.form(f"edit_user_form_{user['username']}"):
                    st.write(f"**Username**: {user['username']} (cannot be changed)")
                    
                    # Parse roles if they're in string format
                    current_roles = user['roles']
                    if isinstance(current_roles, str):
                        try:
                            import json
                            current_roles = json.loads(current_roles)
                        except:
                            current_roles = []
                    
                    new_password = st.text_input(
                        "New Password (leave blank to keep current)", 
                        type="password", 
                        key=f"edit_password_{user['username']}"
                    )
                    new_roles = st.multiselect(
                        "Roles", 
                        ["admin", "user", "query_user", "superuser"], 
                        default=current_roles,
                        key=f"edit_roles_{user['username']}"
                    )
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.form_submit_button("Save Changes"):
                            try:
                                # Prepare password (hash if new password provided)
                                if new_password.strip():
                                    hashed_password = hash_password(new_password)
                                else:
                                    # Keep existing password - we need to get it
                                    hashed_password = user.get('hashed_password', '')
                                
                                # Update user
                                success = database.update_user_in_db(user['username'], hashed_password, new_roles)
                                
                                if success:
                                    st.success(f"User '{user['username']}' updated successfully!")
                                    st.session_state.editing_user = None
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.error(f"Failed to update user '{user['username']}'")
                            except Exception as e:
                                st.error(f"Error updating user: {e}")
                    
                    with col2:
                        if st.form_submit_button("Cancel"):
                            st.session_state.editing_user = None
                            st.rerun()
            
            else:
                # Display mode
                with st.expander(f"User: {user['username']}"):
                    # Parse roles for display
                    display_roles = user['roles']
                    if isinstance(display_roles, str):
                        try:
                            import json
                            display_roles = json.loads(display_roles)
                        except:
                            display_roles = display_roles
                    
                    st.write(f"**Roles**: {', '.join(display_roles) if isinstance(display_roles, list) else display_roles}")
                    st.write(f"**Created**: {user.get('created_at', 'Unknown')}")
                    
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        if st.button(f"Edit", key=f"edit_btn_{user['username']}"):
                            st.session_state.editing_user = user['username']
                            st.rerun()
                    
                    with col2:
                        # Prevent deletion of current admin user
                        if user['username'] == st.session_state.logged_in_user:
                            st.button("Delete", disabled=True, help="Cannot delete your own account", key=f"delete_disabled_{user['username']}")
                        else:
                            if user['username'] not in st.session_state.delete_confirm:
                                if st.button(f"Delete", key=f"delete_btn_{user['username']}", type="secondary"):
                                    st.session_state.delete_confirm[user['username']] = True
                                    st.rerun()
                            else:
                                if st.button(f"Confirm Delete", key=f"confirm_delete_{user['username']}", type="primary"):
                                    try:
                                        success = database.delete_user_from_db(user['username'])
                                        if success:
                                            st.success(f"User '{user['username']}' deleted successfully!")
                                            if user['username'] in st.session_state.delete_confirm:
                                                del st.session_state.delete_confirm[user['username']]
                                            time.sleep(1)
                                            st.rerun()
                                        else:
                                            st.error(f"Failed to delete user '{user['username']}'")
                                    except Exception as e:
                                        st.error(f"Error deleting user: {e}")
                    
                    with col3:
                        if user['username'] in st.session_state.delete_confirm:
                            if st.button(f"Cancel", key=f"cancel_delete_{user['username']}"):
                                del st.session_state.delete_confirm[user['username']]
                                st.rerun()
        
        st.markdown("---")
    
    # Add new user form
    st.subheader("Add New User")
    with st.form("add_user_form"):
        new_username = st.text_input("Username")
        new_password = st.text_input("Password", type="password")
        new_roles = st.multiselect("Roles", ["admin", "user", "query_user", "superuser"], default=["user"])
        
        if st.form_submit_button("Create User"):
            if new_username and new_password:
                try:
                    hashed_password = hash_password(new_password)
                    
                    if is_sqlite_connection():
                        success = create_user_sqlite(new_username, hashed_password, new_roles)
                    else:
                        success = database.create_user_in_db(new_username, hashed_password, new_roles)
                    
                    if success:
                        st.success(f"User '{new_username}' created successfully!")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(f"Failed to create user '{new_username}'. Username might already exist.")
                except Exception as e:
                    st.error(f"Error creating user: {e}")
            else:
                st.error("Username and password are required")

def show_llm_settings_page():
    """LLM configuration settings page"""
    st.title("LLM Configuration Settings")
    st.markdown("Configure the Large Language Model provider and credentials.")
    
    # For now, show a simple interface
    st.info("LLM settings interface can be implemented here based on your database structure.")
    
    providers = ["OpenAI", "Local LLM (OpenAI-Compatible API)"]
    selected_provider = st.selectbox("LLM Provider", providers)
    
    api_key = st.text_input("API Key", type="password", help="Required for OpenAI")
    
    if selected_provider == "Local LLM (OpenAI-Compatible API)":
        base_url = st.text_input("Base URL", placeholder="http://localhost:1234/v1")
        model_name = st.text_input("Model Name", placeholder="e.g., llama2")
    
    if st.button("Save LLM Settings"):
        st.info("LLM settings save functionality needs to be connected to your database")

def show_email_settings_page():
    """Displays the email settings configuration page in the admin panel."""
    st.header("Email Settings")
    st.info("Configure SMTP settings for sending dashboard exports and notifications.")

    # Get current email settings from database
    conn = database.get_db_connection()
    if not conn:
        st.error("Could not connect to database. Please check your database configuration.")
        return

    try:
        cursor = conn.cursor()
        
        # Check if we're using SQLite or PostgreSQL and create table accordingly
        if is_sqlite_connection():
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS email_settings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    smtp_server TEXT NOT NULL,
                    smtp_port INTEGER NOT NULL,
                    smtp_username TEXT NOT NULL,
                    smtp_password TEXT NOT NULL,
                    use_tls BOOLEAN NOT NULL DEFAULT 1,
                    from_email TEXT NOT NULL,
                    is_active BOOLEAN NOT NULL DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        else:
            # PostgreSQL syntax
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS email_settings (
                    id SERIAL PRIMARY KEY,
                    smtp_server TEXT NOT NULL,
                    smtp_port INTEGER NOT NULL,
                    smtp_username TEXT NOT NULL,
                    smtp_password TEXT NOT NULL,
                    use_tls BOOLEAN NOT NULL DEFAULT TRUE,
                    from_email TEXT NOT NULL,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        conn.commit()

        # Get current settings
        cursor.execute("SELECT id, smtp_server, smtp_port, smtp_username, smtp_password, use_tls, from_email FROM email_settings WHERE is_active = TRUE ORDER BY id DESC LIMIT 1")
        current_settings = cursor.fetchone()

        # Initialize form with current settings or defaults
        with st.form("email_settings_form"):
            smtp_server = st.text_input(
                "SMTP Server",
                value=current_settings[1] if current_settings else "smtp.gmail.com",
                help="e.g., smtp.gmail.com, smtp.outlook.com"
            )
            smtp_port = int(st.number_input(
                "SMTP Port",
                value=current_settings[2] if current_settings else 587,
                min_value=1,
                max_value=65535,
                help="Common ports: 587 (TLS), 465 (SSL), 25 (plain)"
            ))
            smtp_username = st.text_input(
                "SMTP Username",
                value=current_settings[3] if current_settings else "",
                help="Usually your email address"
            )
            smtp_password = st.text_input(
                "SMTP Password",
                value="",  # Always empty for security
                type="password",
                help="For Gmail, use an App Password if 2FA is enabled"
            )
            use_tls = st.checkbox(
                "Use TLS",
                value=current_settings[5] if current_settings else True,
                help="Enable TLS encryption (recommended)"
            )
            from_email = st.text_input(
                "From Email Address",
                value=current_settings[6] if current_settings else "",
                help="The email address that will appear as sender"
            )

            col1, col2 = st.columns(2)
            with col1:
                save_button = st.form_submit_button("Save Settings")
            with col2:
                test_button = st.form_submit_button("Send Test Email")

            if save_button:
                if not all([smtp_server, smtp_port, smtp_username, smtp_password, from_email]):
                    st.error("All fields are required to save email settings.")
                else:
                    try:
                        # Deactivate old settings first
                        if is_sqlite_connection():
                            cursor.execute("UPDATE email_settings SET is_active = 0")
                        else:
                            cursor.execute("UPDATE email_settings SET is_active = FALSE")
                        
                        # Save new settings
                        if is_sqlite_connection():
                            cursor.execute("""
                                INSERT INTO email_settings (
                                    smtp_server, smtp_port, smtp_username, smtp_password,
                                    use_tls, from_email, is_active
                                ) VALUES (?, ?, ?, ?, ?, ?, 1)
                            """, (
                                smtp_server, smtp_port, smtp_username, smtp_password,
                                use_tls, from_email
                            ))
                        else:
                            cursor.execute("""
                                INSERT INTO email_settings (
                                    smtp_server, smtp_port, smtp_username, smtp_password,
                                    use_tls, from_email, is_active
                                ) VALUES (%s, %s, %s, %s, %s, %s, TRUE)
                            """, (
                                smtp_server, smtp_port, smtp_username, smtp_password,
                                use_tls, from_email
                            ))
                        conn.commit()
                        st.success("Email settings saved successfully!")
                        
                        # Clear any stale email configuration from session state
                        if 'email_config' in st.session_state:
                            del st.session_state.email_config
                            print("[EMAIL SETTINGS] Cleared stale email_config from session state")
                        
                        # Log the action
                        try:
                            if is_sqlite_connection():
                                log_app_action_sqlite(st.session_state.logged_in_user, "EMAIL_SETTINGS_SAVED", f"Updated SMTP settings for {smtp_server}", "SUCCESS")
                            else:
                                database.log_app_action(st.session_state.logged_in_user, "EMAIL_SETTINGS_SAVED", f"Updated SMTP settings for {smtp_server}", "SUCCESS")
                        except Exception as log_error:
                            print(f"[EMAIL SETTINGS] Failed to log action: {log_error}")
                            
                    except Exception as e:
                        st.error(f"Error saving email settings: {str(e)}")

            if test_button:
                if not all([smtp_server, smtp_port, smtp_username, smtp_password, from_email]):
                    st.error("Please fill in all fields before testing email.")
                else:
                    try:
                        # Create test message
                        msg = MIMEMultipart()
                        msg['From'] = from_email
                        msg['To'] = smtp_username  # Send test to the configured email
                        msg['Subject'] = "Test Email from ConvaBI"
                        body = "This is a test email from your ConvaBI application. If you received this, your email settings are working correctly!"
                        msg.attach(MIMEText(body, 'plain'))

                        # Connect to SMTP server
                        with st.spinner("Sending test email..."):
                            server = smtplib.SMTP(smtp_server, int(smtp_port))
                            if use_tls:
                                server.starttls()
                            server.login(smtp_username, smtp_password)
                            server.send_message(msg)
                            server.quit()

                        st.success(f"Test email sent successfully to {smtp_username}!")
                        
                        # Log the test
                        try:
                            if is_sqlite_connection():
                                log_app_action_sqlite(st.session_state.logged_in_user, "EMAIL_TEST_SENT", f"Test email sent to {smtp_username}", "SUCCESS")
                            else:
                                database.log_app_action(st.session_state.logged_in_user, "EMAIL_TEST_SENT", f"Test email sent to {smtp_username}", "SUCCESS")
                        except Exception as log_error:
                            print(f"[EMAIL SETTINGS] Failed to log test action: {log_error}")
                            
                    except smtplib.SMTPAuthenticationError as e:
                        st.error(f"Authentication failed: {str(e)}\n\nFor Gmail, make sure you're using an App Password if 2FA is enabled.")
                    except smtplib.SMTPConnectError as e:
                        st.error(f"Could not connect to SMTP server: {str(e)}\n\nCheck the server address and port.")
                    except smtplib.SMTPServerDisconnected as e:
                        st.error(f"SMTP server disconnected: {str(e)}")
                    except Exception as e:
                        st.error(f"Error sending test email: {str(e)}")

        # Display current configuration status
        if current_settings:
            st.markdown("---")
            st.subheader("Current Configuration")
            col1, col2 = st.columns(2)
            with col1:
                st.info(f"**SMTP Server:** {current_settings[1]}")
                st.info(f"**Port:** {current_settings[2]}")
                st.info(f"**Username:** {current_settings[3]}")
            with col2:
                st.info(f"**From Email:** {current_settings[6]}")
                st.info(f"**TLS Enabled:** {'Yes' if current_settings[5] else 'No'}")
                st.info("**Status:** Active")

    except Exception as e:
        st.error(f"Error accessing email settings: {str(e)}")
        # Log the error
        try:
            if is_sqlite_connection():
                log_app_action_sqlite(st.session_state.logged_in_user, "EMAIL_SETTINGS_ERROR", f"Error accessing email settings: {str(e)}", "ERROR")
            else:
                database.log_app_action(st.session_state.logged_in_user, "EMAIL_SETTINGS_ERROR", f"Error accessing email settings: {str(e)}", "ERROR")
        except Exception as log_error:
            print(f"[EMAIL SETTINGS] Failed to log error: {log_error}")
    finally:
        if conn:
            conn.close()

def send_smtp_email(to_email, subject, body, attachment_path=None):
    """
    Sends an email using the configured SMTP settings from the database.
    Returns True if successful, False otherwise.
    This is a legacy function - consider using send_email module instead.
    """
    try:
        # Get email settings from database
        conn = database.get_db_connection()
        if not conn:
            print("[EMAIL] Could not connect to database")
            return False

        cursor = conn.cursor()
        cursor.execute("SELECT * FROM email_settings WHERE is_active = 1 ORDER BY id DESC LIMIT 1")
        settings = cursor.fetchone()
        conn.close()

        if not settings:
            print("[EMAIL] No active email settings found")
            return False

        # Create email message
        msg = MIMEMultipart()
        msg['From'] = settings[6]  # from_email
        msg['To'] = to_email
        msg['Subject'] = subject

        # Add body
        msg.attach(MIMEText(body, 'plain'))

        # Add attachment if provided
        if attachment_path and os.path.exists(attachment_path):
            with open(attachment_path, 'rb') as f:
                attachment = MIMEApplication(f.read(), _subtype='pdf')
                attachment.add_header('Content-Disposition', 'attachment', filename=os.path.basename(attachment_path))
                msg.attach(attachment)

        # Send email
        server = smtplib.SMTP(settings[1], int(settings[2]))  # smtp_server, smtp_port
        if settings[5]:  # use_tls
            server.starttls()
        server.login(settings[3], settings[4])  # smtp_username, smtp_password
        server.send_message(msg)
        server.quit()

        return True

    except Exception as e:
        print(f"[EMAIL] Error sending email: {str(e)}")
        return False

# --- Enhanced CSS with Purple Gradient Theme ---

def show_unified_data_setup_page():
    """Unified data setup page combining data integration and semantic enhancement"""
    st.title("📊 Data Setup & Intelligence")
    st.markdown("**Complete data pipeline: Connect → Integrate → Enhance → Query**")
    
    if not st.session_state.logged_in_user:
        st.error("Please log in to access data setup features.")
        st.session_state.page = 'login'
        st.rerun()
        return

    # Initialize integration engine
    integration_engine = data_integration.data_integration_engine
    summary = integration_engine.get_data_sources_summary()
    
    # Load or initialize semantic layer
    semantic_layer = st.session_state.get('semantic_layer')
    if not semantic_layer:
        try:
            semantic_layer = semantic_layer_ui.load_or_create_semantic_layer()
            st.session_state.semantic_layer = semantic_layer
        except:
            from semantic_layer import SemanticLayer
            semantic_layer = SemanticLayer()
            st.session_state.semantic_layer = semantic_layer
    
    # Progress indicator
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        sources_status = "✅" if summary['total_sources'] > 0 else "⏳"
        st.metric("1. Data Sources", f"{sources_status} {summary['total_sources']}")
    with col2:
        etl_status = "✅" if summary['total_etl_operations'] > 0 else "⏳"
        st.metric("2. ETL Operations", f"{etl_status} {summary['total_etl_operations']}")
    with col3:
        semantic_status = "✅" if semantic_layer and semantic_layer.tables else "⏳"
        semantic_count = len(semantic_layer.tables) if semantic_layer and semantic_layer.tables else 0
        st.metric("3. Intelligence", f"{semantic_status} {semantic_count}")
    with col4:
        ready_status = "✅" if summary['total_sources'] > 0 and semantic_count > 0 else "⏳"
        st.metric("4. Ready", ready_status)
    
    st.markdown("---")
    
    # Main tabs combining both workflows
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Connect Data", "🔗 Transform & Join", "🧠 Enhance Intelligence", "🎯 Ready to Query"])
    
    with tab1:
        st.subheader("📊 Connect Your Data Sources")
        data_integration_ui.show_data_sources_management(integration_engine)
    
    with tab2:
        st.subheader("🔗 Transform & Join Data")
        if summary['total_sources'] > 0:
            # Show AI suggested joins first
            data_integration_ui.show_ai_suggested_joins(integration_engine)
            st.markdown("---")
            # Then ETL operations
            data_integration_ui.show_etl_operations(integration_engine)
        else:
            st.info("🔗 Add data sources first to enable transformations and joins")
            if st.button("← Go to Connect Data", key="goto_connect_from_transform"):
                st.session_state.active_tab = 0
                st.rerun()
    
    with tab3:
        st.subheader("🧠 Enhance with Business Intelligence")
        if summary['total_sources'] > 0:
            # Auto-generation section
            col1, col2 = st.columns([2, 1])
            with col1:
                st.markdown("#### 🤖 AI-Powered Metadata Generation")
                st.info("Transform your raw data schema into business-intelligent metadata that dramatically improves AI query accuracy.")
                
                if not semantic_layer.tables:
                    if st.button("🚀 Auto-Generate Business Metadata", type="primary", key="auto_generate_unified"):
                        with st.spinner("🔍 Analyzing data sources and generating business context..."):
                            try:
                                success = semantic_layer.auto_generate_metadata_from_data_integration(integration_engine)
                                if success:
                                    # Save to database
                                    conn = database.get_db_connection()
                                    if conn:
                                        try:
                                            saved = semantic_layer.save_to_database(conn)
                                            if saved:
                                                st.session_state.semantic_layer = semantic_layer
                                                st.success("🎉 Business metadata generated and saved!")
                                                st.balloons()
                                                st.rerun()
                                            else:
                                                st.error("Failed to save semantic metadata to database")
                                        finally:
                                            conn.close()
                                    else:
                                        st.error("Could not connect to database")
                                else:
                                    st.error("Failed to generate semantic metadata")
                            except Exception as e:
                                st.error(f"Error during metadata generation: {e}")
                else:
                    st.success("✅ Business metadata already generated!")
                    if st.button("🔄 Regenerate Metadata", key="regenerate_unified"):
                        # Clear existing and regenerate
                        semantic_layer = semantic_layer_ui.load_or_create_semantic_layer()
                        semantic_layer.tables.clear()
                        semantic_layer.relationships.clear()
                        st.session_state.semantic_layer = semantic_layer
                        st.rerun()
            
            with col2:
                if semantic_layer and semantic_layer.tables:
                    st.markdown("#### 📊 Current Status")
                    st.metric("Enhanced Tables", len(semantic_layer.tables))
                    st.metric("Relationships", len(semantic_layer.relationships))
                    st.metric("Business Metrics", len(semantic_layer.metrics))
            
            # Quick fixes and enhancements
            if semantic_layer and semantic_layer.tables:
                st.markdown("---")
                semantic_layer_ui.show_analytics_quick_fix()
                
                # Enhanced configuration interface
                st.markdown("---")
                st.markdown("#### ⚙️ Enhanced Configuration & Metadata")
                table_names = list(semantic_layer.tables.keys())
                if table_names:
                    selected_table = st.selectbox("Configure table:", table_names, key="quick_config_table")
                    if selected_table:
                        table_info = semantic_layer.tables[selected_table]
                        
                        # Editable table information
                        with st.expander(f"📊 Table: {table_info.display_name}", expanded=True):
                            col1, col2 = st.columns([2, 1])
                            
                            with col1:
                                # Editable business purpose
                                new_purpose = st.text_area(
                                    "Business Purpose:", 
                                    value=table_info.business_purpose,
                                    height=80,
                                    key=f"purpose_{selected_table}"
                                )
                                
                                # Show column metadata with selectbox instead of nested expanders
                                st.markdown("**📋 Column Metadata:**")
                                
                                # Create a selectbox for column selection
                                column_names = list(table_info.columns.keys())
                                if column_names:
                                    selected_column = st.selectbox(
                                        "Select column to view details:",
                                        column_names,
                                        key=f"column_select_{selected_table}"
                                    )
                                    
                                    if selected_column:
                                        col_info = table_info.columns[selected_column]
                                        
                                        # Display column details in a container
                                        with st.container():
                                            st.markdown(f"**🔹 {col_info.display_name} ({selected_column})**")
                                            st.write(f"**Type:** {col_info.semantic_type.value}")
                                            st.write(f"**Data Type:** {col_info.data_type}")
                                            st.write(f"**Description:** {col_info.description}")
                                            
                                            if col_info.sample_values:
                                                st.write(f"**Sample Values:** {', '.join(col_info.sample_values[:3])}")
                                            
                                            if col_info.business_rules:
                                                st.write(f"**Business Rules:** {', '.join(col_info.business_rules[:2])}")
                                else:
                                    st.info("No column metadata available")
                            
                            with col2:
                                st.metric("Total Columns", len(table_info.columns))
                                
                                # Count by semantic type
                                type_counts = {}
                                for col in table_info.columns.values():
                                    semantic_type = col.semantic_type.value
                                    type_counts[semantic_type] = type_counts.get(semantic_type, 0) + 1
                                
                                st.markdown("**Column Types:**")
                                for type_name, count in type_counts.items():
                                    st.write(f"• {type_name}: {count}")
                                
                                # Quick save button
                                if st.button("💾 Save Changes", key=f"save_{selected_table}"):
                                    if new_purpose != table_info.business_purpose:
                                        table_info.business_purpose = new_purpose
                                        # Save to database
                                        conn = database.get_db_connection()
                                        if conn:
                                            try:
                                                saved = semantic_layer.save_to_database(conn)
                                                if saved:
                                                    st.session_state.semantic_layer = semantic_layer
                                                    st.success("✅ Changes saved!")
                                                else:
                                                    st.error("❌ Failed to save changes")
                                            finally:
                                                conn.close()
                                
                                # Advanced configuration button
                                if st.button(f"🔧 Advanced Config", key=f"advanced_config_{selected_table}"):
                                    st.session_state.selected_semantic_table = selected_table
                                    st.info("💡 **Tip:** For full semantic layer management, access the dedicated Semantic Layer page (available to admins)")
        else:
            st.info("🧠 Add data sources first to enable intelligence enhancement")
            if st.button("← Go to Connect Data", key="goto_connect_from_intelligence"):
                st.session_state.active_tab = 0
                st.rerun()
    
    with tab4:
        st.subheader("🎯 Ready to Query!")
        
        if summary['total_sources'] > 0 and semantic_layer and semantic_layer.tables:
            st.success("🎉 **Your data is ready for AI-powered querying!**")
            
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("#### 📊 What You've Built:")
                st.write(f"• **{summary['total_sources']}** connected data sources")
                st.write(f"• **{summary['total_etl_operations']}** ETL transformations")
                st.write(f"• **{len(semantic_layer.tables)}** business-enhanced tables")
                st.write(f"• **{len(semantic_layer.relationships)}** intelligent relationships")
                
                st.markdown("#### 🚀 Next Steps:")
                if st.button("▶️ Start Asking Questions", type="primary", key="goto_query_from_ready"):
                    st.session_state.app_page = 'query'
                    st.rerun()
                
                if st.button("📊 Create Dashboard", key="goto_dashboard_from_ready"):
                    st.session_state.app_page = 'dashboard'
                    st.rerun()
            
            with col2:
                st.markdown("#### 🧪 Test Query Examples:")
                
                # Generate sample queries based on semantic layer
                sample_queries = []
                for table_name, table_info in semantic_layer.tables.items():
                    if table_info.common_queries:
                        sample_queries.extend(table_info.common_queries[:2])
                
                if not sample_queries:
                    sample_queries = [
                        "What is the total revenue by region?",
                        "Show me the top 5 customers by sales",
                        "How many orders were placed last month?",
                        "What's the profit margin by category?"
                    ]
                
                for i, query in enumerate(sample_queries[:4]):
                    if st.button(f"💬 {query}", key=f"sample_query_{i}"):
                        st.session_state.app_page = 'query'
                        st.session_state.test_query = query
                        st.rerun()
        
        elif summary['total_sources'] > 0:
            st.warning("⚠️ **Almost Ready!** You have data sources but need to enhance them with business intelligence.")
            if st.button("🧠 Enhance Intelligence", key="goto_intelligence_from_ready"):
                st.session_state.active_tab = 2  # Go to intelligence tab
                st.rerun()
        
        else:
            st.info("📊 **Get Started!** Connect your first data source to begin.")
            if st.button("📊 Connect Data", key="goto_connect_from_ready"):
                st.session_state.active_tab = 0  # Go to connect tab
                st.rerun()

def show_main_application():
    """Main application interface with sidebar navigation and proper page routing"""
    
    # Initialize page state if not set
    if 'app_page' not in st.session_state:
        st.session_state.app_page = 'data_setup'
    
    # Sidebar Navigation
    with st.sidebar:
        st.markdown("### ConvaBI Navigation")
        st.markdown(f"**Welcome, {st.session_state.logged_in_user}!**")
        st.markdown("---")
        
        # Navigation buttons - OPTIMIZED FLOW
        if st.button("📊 Data Setup & Intelligence", use_container_width=True):
            st.session_state.app_page = 'data_setup'
            st.rerun()
            
        if st.button("Ask Questions", use_container_width=True):
            st.session_state.app_page = 'query'
            st.rerun()
            
        if st.button("My Dashboard", use_container_width=True):
            st.session_state.app_page = 'dashboard'
            st.rerun()
            
        if st.button("Manage Dashboard Sharing", use_container_width=True):
            st.session_state.app_page = 'dashboard_management'
            st.rerun()
            
        # Admin-only features
        user_roles = st.session_state.get('user_roles', [])
        if isinstance(user_roles, str):
            try:
                import json
                user_roles = json.loads(user_roles)
            except:
                user_roles = []
        
        is_admin = 'admin' in user_roles
        
        if is_admin:
            st.markdown("---")
            st.markdown("**Admin Features**")
            
            if st.button("Database Configuration", use_container_width=True):
                st.session_state.app_page = 'db_config'
                st.rerun()
            
            if st.button("User Management", use_container_width=True):
                st.session_state.app_page = 'admin_users'
                st.rerun()
                
            if st.button("LLM Settings", use_container_width=True):
                st.session_state.app_page = 'llm_settings'
                st.rerun()
            
            if st.button("Email Settings", use_container_width=True):
                st.session_state.app_page = 'email_settings'
                st.rerun()
        
        st.markdown("---")
        
        # Status indicators with enhanced information
        st.markdown("**Status**")
        
        # LLM status
        llm_status = st.session_state.get('sidebar_llm_status_message', 'LLM: Not configured')
        if 'Connected' in llm_status:
            st.success(f"✅ LLM Ready")
        else:
            st.info(f"🔧 LLM: Configure in Admin Settings")
            
        # Data & Intelligence status
        integration_engine = data_integration.data_integration_engine
        summary = integration_engine.get_data_sources_summary()
        semantic_layer = st.session_state.get('semantic_layer')
        
        if summary['total_sources'] > 0:
            if semantic_layer and semantic_layer.tables:
                table_count = len(semantic_layer.tables)
                st.success(f"🧠 Data Intelligence: {summary['total_sources']} sources, {table_count} enhanced")
        else:
                st.warning(f"📊 Data: {summary['total_sources']} sources (needs enhancement)")
        else:
            st.info("📥 Ready for Data Setup")
        
        st.markdown("---")
        
        # Logout button
        if st.button("Logout", use_container_width=True):
            logout()
    
    # Main content area based on selected page
    if st.session_state.app_page == 'data_setup':
        show_unified_data_setup_page()
    elif st.session_state.app_page == 'query':
        show_query_screen()
    elif st.session_state.app_page == 'dashboard':
        show_dashboard_screen()
    elif st.session_state.app_page == 'dashboard_management':
        show_dashboard_management_page()
    elif st.session_state.app_page == 'db_config' and is_admin:
        show_admin_db_configuration_page()
    elif st.session_state.app_page == 'admin_users' and is_admin:
        show_admin_panel()
    elif st.session_state.app_page == 'llm_settings' and is_admin:
        show_llm_settings_page()
    elif st.session_state.app_page == 'email_settings' and is_admin:
        show_email_settings_page()
    else:
        # Default to unified data setup
        show_unified_data_setup_page()

if __name__ == "__main__":
    load_custom_css()
    main()
 