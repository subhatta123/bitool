import streamlit as st
import psycopg2
import json # For serializing/deserializing JSONB data

# --- Database Connection ---

def _get_params_from_session_or_secrets():
    """Internal: Tries session_state (current_db_params) first, then secrets.
       Returns None if not found or error during secrets access.
    """
    if "current_db_params" in st.session_state and st.session_state.current_db_params:
        # Ensure it's a dictionary with necessary keys, simple check
        if isinstance(st.session_state.current_db_params, dict) and 'host' in st.session_state.current_db_params:
            return st.session_state.current_db_params
    try:
        return st.secrets["postgres"]
    except KeyError:  # No secrets["postgres"]
        return None
    except AttributeError: # st.secrets might not be fully initialized in some contexts if not set up
        return None
    except Exception:  # Other error with secrets
        return None  # Treat as missing for this purpose

def get_db_connection_params_for_display():
    """Retrieves PostgreSQL connection parameters from Streamlit secrets for display purposes only.
       Returns None if not found, does not call st.error()."""
    try:
        return st.secrets["postgres"]
    except KeyError:
        return None
    except AttributeError: # st.secrets might not be fully initialized
        return None
    except Exception:
        return None

def get_db_connection(provided_params=None):
    """
    Establishes a connection to PostgreSQL.
    Uses `provided_params` if given. Otherwise, tries to get params from
    session state (populated by successful user input or secrets check) or Streamlit secrets.
    Stores successfully used parameters in st.session_state.current_db_params.
    """
    params_to_use = None
    source_of_params = "unknown"

    if provided_params:
        params_to_use = provided_params
        source_of_params = "provided"
    else:
        params_to_use = _get_params_from_session_or_secrets()
        if params_to_use == st.session_state.get("current_db_params"):
            source_of_params = "session_state"
        elif params_to_use: # Implies it came from st.secrets
            source_of_params = "streamlit_secrets"

    if not params_to_use:
        if source_of_params == "provided": # Should not happen if form validates
             st.error("DB connection attempted with no parameters provided.")
        # If not provided, and _get_params_from_session_or_secrets returned None,
        # it means neither session nor secrets had valid params.
        # The calling context (e.g., initial app setup) should handle this gracefully.
        # Avoid st.error here to prevent premature app halts before config page.
        return None

    try:
        conn = psycopg2.connect(**params_to_use)
        # If connection is successful, store/update these params in session_state
        st.session_state.current_db_params = params_to_use
        return conn
    except psycopg2.Error as e:
        # If params were explicitly provided (e.g., from user form), let the form handler show the error.
        # If params came from session/secrets, then it's an unexpected runtime error.
        if source_of_params != "provided":
            st.error(f"Error connecting to PostgreSQL ({source_of_params}): {e}")
            safe_params = {k: v for k, v in params_to_use.items() if k != 'password'}
            st.error(f"Connection parameters used (password hidden): {safe_params}")
        return None # Let caller handle specific error messaging

# --- Table Definitions and Initialization ---

TABLE_DEFINITIONS = {
    "app_users": """
        CREATE TABLE IF NOT EXISTS app_users (
            id SERIAL PRIMARY KEY,
            username VARCHAR(255) UNIQUE NOT NULL,
            hashed_password VARCHAR(255) NOT NULL,
            roles JSONB,  -- Example: '["admin", "user"]'
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    """,
    "app_dashboards": """
        CREATE TABLE IF NOT EXISTS app_dashboards (
            id SERIAL PRIMARY KEY,
            dashboard_name VARCHAR(255) NOT NULL,
            owner_username VARCHAR(255) NOT NULL,
            versions JSONB, -- Stores list of dashboard versions (complex JSON)
            shared_with_users JSONB, -- Stores list of usernames dashboard is shared with
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT fk_owner_username
                FOREIGN KEY(owner_username) 
                REFERENCES app_users(username)
                ON DELETE CASCADE,
            UNIQUE (owner_username, dashboard_name) 
        );
    """,
    "app_logs": """
        CREATE TABLE IF NOT EXISTS app_logs (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            username VARCHAR(255), -- Can be NULL if action is not user-specific
            action VARCHAR(255) NOT NULL,
            details TEXT,
            status VARCHAR(50) -- e.g., SUCCESS, FAILURE, INFO
        );
    """,
    "app_llm_config": """
        CREATE TABLE IF NOT EXISTS app_llm_config (
            id INTEGER PRIMARY KEY DEFAULT 1, -- Assuming a single global config row
            provider VARCHAR(100), -- e.g., "OpenAI", "Local LLM (OpenAI-Compatible API)"
            api_key TEXT, -- Store securely if possible (e.g., encrypted)
            base_url TEXT, -- For local LLMs
            custom_model_name TEXT, -- Optional: for specific model selection if provider supports it
            last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT llm_config_singleton CHECK (id = 1) -- Enforce single row
        );
    """,
    "app_email_config": """
        CREATE TABLE IF NOT EXISTS app_email_config (
            id INTEGER PRIMARY KEY DEFAULT 1, -- Assuming a single global config row
            smtp_host VARCHAR(255),
            smtp_port INTEGER,
            smtp_user VARCHAR(255),
            smtp_password TEXT, -- Store securely if possible (e.g., encrypted)
            sender_email VARCHAR(255),
            use_tls BOOLEAN DEFAULT TRUE,
            use_ssl BOOLEAN DEFAULT FALSE,
            last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT email_config_singleton CHECK (id = 1) -- Enforce single row
        );
    """
    # Add more table definitions here for schedules, etc. as needed
}

def init_db(conn):
    """Initializes the database by creating tables if they don't exist, using the provided connection.
    Returns True on success, False on failure."""
    if not conn:
        # This function now expects a valid connection.
        # The caller is responsible for ensuring conn is not None.
        # st.error("init_db called with no database connection.") # Avoid direct st.error if possible, let caller handle
        return False

    try:
        with conn.cursor() as cur:
            for table_name, ddl_statement in TABLE_DEFINITIONS.items():
                cur.execute(ddl_statement)
        conn.commit()
        return True # Success
    except psycopg2.Error as e:
        # Log or handle the error appropriately.
        # st.error(f"Error initializing database tables: {e}") # Avoid direct st.error if possible
        try:
            conn.rollback()  # Attempt to rollback
        except psycopg2.Error as rb_e:
            # st.error(f"Error during rollback: {rb_e}") # Log this too
            pass # Suppress rollback error reporting here, primary error is more important
        return False # Failure
    # The connection 'conn' is managed by the caller of init_db. It's not closed here.

# --- User Management Functions --- (For login, default admin, and admin panel)

def create_user_in_db(username, hashed_password, roles):
    conn = get_db_connection()
    if not conn: return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO app_users (username, hashed_password, roles) VALUES (%s, %s, %s)",
                (username, hashed_password, json.dumps(roles))
            )
        conn.commit()
        return True
    except psycopg2.Error as e:
        st.warning(f"Error creating user {username} in DB: {e}")
        conn.rollback()
        return False
    finally:
        if conn: conn.close()

def get_user_by_username_from_db(username):
    conn = get_db_connection()
    if not conn: return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT username, hashed_password, roles FROM app_users WHERE username = %s", (username,))
            user_data = cur.fetchone()
            if user_data:
                return {
                    "username": user_data[0],
                    "hashed_password": user_data[1],
                    "roles": user_data[2]
                }
            return None
    except psycopg2.Error as e:
        st.warning(f"Error fetching user {username} from DB: {e}")
        return None
    finally:
        if conn: conn.close()

def ensure_default_admin_user_in_db(default_username, default_password_hash, default_roles):
    existing_admin = get_user_by_username_from_db(default_username)
    if not existing_admin:
        st.info(f"Default admin user '{default_username}' not found in database. Creating...")
        success = create_user_in_db(default_username, default_password_hash, default_roles)
        if success:
            st.success(f"Default admin user '{default_username}' created in database.")
        else:
            st.error(f"Failed to create default admin user '{default_username}' in database.")

def get_all_users_from_db():
    """Fetches all users from the app_users table for admin panel."""
    conn = get_db_connection()
    if not conn: return []
    users_list = []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT username, roles FROM app_users ORDER BY username")
            for row in cur.fetchall():
                users_list.append({"username": row[0], "roles": row[1]})
        return users_list
    except psycopg2.Error as e:
        st.error(f"Error fetching all users from DB: {e}")
        return []
    finally:
        if conn: conn.close()

def update_user_in_db(username, new_hashed_password, new_roles):
    """Updates a user's password and/or roles for admin panel."""
    conn = get_db_connection()
    if not conn: return False
    try:
        with conn.cursor() as cur:
            if new_hashed_password:
                cur.execute("UPDATE app_users SET hashed_password = %s, roles = %s WHERE username = %s", 
                            (new_hashed_password, json.dumps(new_roles), username))
            else: # Password not changed
                cur.execute("UPDATE app_users SET roles = %s WHERE username = %s", 
                            (json.dumps(new_roles), username))
        conn.commit()
        return cur.rowcount > 0
    except psycopg2.Error as e:
        st.error(f"Error updating user {username} in DB: {e}")
        conn.rollback()
        return False
    finally:
        if conn: conn.close()

def delete_user_from_db(username):
    """Deletes a user from the app_users table for admin panel."""
    conn = get_db_connection()
    if not conn: return False
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM app_users WHERE username = %s", (username,))
        conn.commit()
        return cur.rowcount > 0
    except psycopg2.Error as e:
        st.error(f"Error deleting user {username} from DB: {e}")
        conn.rollback()
        return False
    finally:
        if conn: conn.close()

# --- LLM Configuration Functions ---

def save_llm_config(provider, api_key, base_url, custom_model_name=None):
    """Saves or updates the global LLM configuration."""
    print(f"[DB DEBUG] Attempting to save LLM config: Provider={provider}, APIKey Present={bool(api_key)}, BaseURL Present={bool(base_url)}, CustomModel={custom_model_name}")
    conn = get_db_connection()
    if not conn:
        print("[DB DEBUG] save_llm_config: Failed to get DB connection.")
        return False
    try:
        with conn.cursor() as cur:
            sql_statement = """INSERT INTO app_llm_config (id, provider, api_key, base_url, custom_model_name, last_updated)
                   VALUES (1, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                   ON CONFLICT (id) 
                   DO UPDATE SET provider = EXCLUDED.provider, 
                                 api_key = EXCLUDED.api_key, 
                                 base_url = EXCLUDED.base_url,
                                 custom_model_name = EXCLUDED.custom_model_name,
                                 last_updated = CURRENT_TIMESTAMP;"""
            params_tuple = (provider, api_key, base_url, custom_model_name)
            print(f"[DB DEBUG] save_llm_config: Executing SQL with params: {params_tuple}")
            cur.execute(sql_statement, params_tuple)
            print("[DB DEBUG] save_llm_config: SQL executed.")
            conn.commit()
            print("[DB DEBUG] save_llm_config: conn.commit() called.")

            # TEMP DEBUG: Immediately try to read back the data
            print("[DB DEBUG] save_llm_config: Attempting to read back data immediately...")
            cur.execute("SELECT provider, api_key, base_url, custom_model_name, last_updated FROM app_llm_config WHERE id = 1")
            row = cur.fetchone()
            if row:
                print(f"[DB DEBUG] save_llm_config: Read back: Provider={row[0]}, APIKey Present={bool(row[1])}, BaseURL={row[2]}, CustomModel={row[3]}, Updated={row[4]}")
            else:
                print("[DB DEBUG] save_llm_config: Read back failed, no row found with id=1.")
            
        log_app_action(None, "SAVE_LLM_CONFIG_DB_SUCCESS", f"Provider: {provider}", "SUCCESS")
        print("[DB DEBUG] save_llm_config: Save successful, returning True.")
        return True
    except psycopg2.Error as e:
        print(f"[DB DEBUG] save_llm_config: psycopg2.Error: {e}")
        st.error(f"Error saving LLM config to DB: {e}")
        log_app_action(None, "SAVE_LLM_CONFIG_DB_FAILURE", str(e), "FAILURE")
        if conn: # Ensure conn is not None before trying to rollback
            try:
                conn.rollback()
                print("[DB DEBUG] save_llm_config: conn.rollback() called due to error.")
            except Exception as rb_e:
                print(f"[DB DEBUG] save_llm_config: Error during rollback: {rb_e}")
        return False
    except Exception as ex:
        print(f"[DB DEBUG] save_llm_config: Unexpected Exception: {ex}")
        st.error(f"Unexpected error saving LLM config: {ex}")
        if conn: # Ensure conn is not None before trying to rollback
            try:
                conn.rollback()
                print("[DB DEBUG] save_llm_config: conn.rollback() called due to unexpected error.")
            except Exception as rb_ex:
                print(f"[DB DEBUG] save_llm_config: Error during rollback (unexpected ex): {rb_ex}")
        return False
    finally:
        if conn:
            conn.close()
            print("[DB DEBUG] save_llm_config: DB connection closed.")

def load_llm_config():
    """Loads the global LLM configuration."""
    conn = get_db_connection()
    if not conn: return None
    config = None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT provider, api_key, base_url, custom_model_name FROM app_llm_config WHERE id = 1")
            row = cur.fetchone()
            if row:
                config = {
                    "provider": row[0],
                    "api_key": row[1],
                    "base_url": row[2],
                    "custom_model_name": row[3]
                }
    except psycopg2.Error as e:
        st.error(f"Error loading LLM config from DB: {e}")
        # Do not log this as a failure in app_logs from here, as it's a read operation
        # and might be called frequently. The st.error is user feedback.
    finally:
        if conn: conn.close()
    return config

# --- Email Configuration Functions ---

def save_email_config(smtp_host, smtp_port, smtp_user, smtp_password, sender_email, use_tls, use_ssl):
    """Saves or updates the global Email configuration."""
    conn = get_db_connection()
    if not conn: return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO app_email_config (id, smtp_host, smtp_port, smtp_user, smtp_password, sender_email, use_tls, use_ssl, last_updated)
                   VALUES (1, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                   ON CONFLICT (id) 
                   DO UPDATE SET smtp_host = EXCLUDED.smtp_host,
                                 smtp_port = EXCLUDED.smtp_port,
                                 smtp_user = EXCLUDED.smtp_user,
                                 smtp_password = EXCLUDED.smtp_password,
                                 sender_email = EXCLUDED.sender_email,
                                 use_tls = EXCLUDED.use_tls,
                                 use_ssl = EXCLUDED.use_ssl,
                                 last_updated = CURRENT_TIMESTAMP;""",
                (smtp_host, smtp_port, smtp_user, smtp_password, sender_email, use_tls, use_ssl)
            )
        conn.commit()
        log_app_action(None, "SAVE_EMAIL_CONFIG_DB_SUCCESS", f"Host: {smtp_host}, Sender: {sender_email}", "SUCCESS")
        return True
    except psycopg2.Error as e:
        st.error(f"Error saving Email config to DB: {e}")
        log_app_action(None, "SAVE_EMAIL_CONFIG_DB_FAILURE", str(e), "FAILURE")
        conn.rollback()
        return False
    finally:
        if conn: conn.close()

def load_email_config():
    """Loads the global Email configuration."""
    conn = get_db_connection()
    if not conn: return None
    config = None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT smtp_host, smtp_port, smtp_user, smtp_password, sender_email, use_tls, use_ssl FROM app_email_config WHERE id = 1")
            row = cur.fetchone()
            if row:
                config = {
                    "smtp_host": row[0],
                    "smtp_port": row[1],
                    "smtp_user": row[2],
                    "smtp_password": row[3],
                    "sender_email": row[4],
                    "use_tls": row[5],
                    "use_ssl": row[6]
                }
    except psycopg2.Error as e:
        st.error(f"Error loading Email config from DB: {e}")
    finally:
        if conn: conn.close()
    return config

# --- Dashboard Management Functions ---

def save_dashboard_to_db(owner_username, dashboard_name, versions_list, shared_with_list):
    conn = get_db_connection()
    if not conn: return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO app_dashboards (owner_username, dashboard_name, versions, shared_with_users)
                   VALUES (%s, %s, %s, %s)
                   ON CONFLICT (owner_username, dashboard_name) 
                   DO UPDATE SET versions = EXCLUDED.versions, shared_with_users = EXCLUDED.shared_with_users;""",
                (owner_username, dashboard_name, json.dumps(versions_list), json.dumps(shared_with_list))
            )
        conn.commit()
        return True
    except psycopg2.Error as e:
        st.error(f"Error saving dashboard '{dashboard_name}' for user '{owner_username}' to DB: {e}")
        conn.rollback()
        return False
    finally:
        if conn: conn.close()

def load_dashboard_from_db(owner_username_or_shared_user, dashboard_name):
    conn = get_db_connection()
    if not conn: return None
    dashboard_data = None
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT owner_username, dashboard_name, versions, shared_with_users FROM app_dashboards WHERE owner_username = %s AND dashboard_name = %s",
                (owner_username_or_shared_user, dashboard_name)
            )
            data = cur.fetchone()
            if data:
                dashboard_data = {
                    "owner_username": data[0],
                    "dashboard_name": data[1],
                    "versions": data[2],
                    "shared_with_users": data[3]
                }
            else:
                cur.execute(
                    """SELECT owner_username, dashboard_name, versions, shared_with_users 
                       FROM app_dashboards 
                       WHERE dashboard_name = %s AND shared_with_users::jsonb @> %s::jsonb""",
                    (dashboard_name, json.dumps([owner_username_or_shared_user]))
                )
                data = cur.fetchone()
                if data:
                    dashboard_data = {
                        "owner_username": data[0],
                        "dashboard_name": data[1],
                        "versions": data[2],
                        "shared_with_users": data[3]
                    }
        return dashboard_data
    except psycopg2.Error as e:
        st.error(f"Error loading dashboard '{dashboard_name}' for user '{owner_username_or_shared_user}' from DB: {e}")
        return None
    finally:
        if conn: conn.close()

def get_dashboard_names_for_user_from_db(username):
    conn = get_db_connection()
    if not conn: return []
    accessible_dashboards = []
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT dashboard_name, owner_username FROM app_dashboards WHERE owner_username = %s ORDER BY dashboard_name", 
                (username,)
            )
            for row in cur.fetchall():
                accessible_dashboards.append((row[0], row[0], row[1]))
            cur.execute(
                """SELECT dashboard_name, owner_username FROM app_dashboards 
                   WHERE shared_with_users::jsonb @> %s::jsonb ORDER BY dashboard_name""",
                (json.dumps([username]),)
            )
            for row in cur.fetchall():
                if not any(d[0] == row[0] and d[2] == username for d in accessible_dashboards): 
                    display_name = f"{row[0]} (shared by {row[1]})"
                    accessible_dashboards.append((row[0], display_name, row[1]))
            final_list = []
            seen_dashboards = set()
            for raw_name, disp_name, owner in accessible_dashboards:
                if (raw_name, owner) not in seen_dashboards:
                    final_list.append((raw_name, disp_name, owner))
                    seen_dashboards.add((raw_name, owner))
            return sorted(final_list, key=lambda x: x[1])
    except psycopg2.Error as e:
        st.error(f"Error fetching dashboard names for user '{username}' from DB: {e}")
        return []
    finally:
        if conn: conn.close()

def update_dashboard_sharing_in_db(owner_username, dashboard_name, shared_with_list):
    conn = get_db_connection()
    if not conn: return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE app_dashboards SET shared_with_users = %s WHERE owner_username = %s AND dashboard_name = %s",
                (json.dumps(shared_with_list), owner_username, dashboard_name)
            )
        conn.commit()
        return cur.rowcount > 0
    except psycopg2.Error as e:
        st.error(f"Error updating sharing for dashboard '{dashboard_name}': {e}")
        conn.rollback()
        return False
    finally:
        if conn: conn.close()

def rename_dashboard_in_db(owner_username, old_dashboard_name, new_dashboard_name):
    conn = get_db_connection()
    if not conn: return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE app_dashboards SET dashboard_name = %s WHERE owner_username = %s AND dashboard_name = %s",
                (new_dashboard_name, owner_username, old_dashboard_name)
            )
        conn.commit()
        return cur.rowcount > 0
    except psycopg2.IntegrityError as e:
        st.error(f"Error renaming dashboard: A dashboard named '{new_dashboard_name}' may already exist for this user. {e}")
        conn.rollback()
        return False
    except psycopg2.Error as e:
        st.error(f"Error renaming dashboard '{old_dashboard_name}': {e}")
        conn.rollback()
        return False
    finally:
        if conn: conn.close()

def delete_dashboard_from_db(owner_username, dashboard_name):
    conn = get_db_connection()
    if not conn: return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM app_dashboards WHERE owner_username = %s AND dashboard_name = %s",
                (owner_username, dashboard_name)
            )
        conn.commit()
        return cur.rowcount > 0
    except psycopg2.Error as e:
        st.error(f"Error deleting dashboard '{dashboard_name}': {e}")
        conn.rollback()
        return False
    finally:
        if conn: conn.close()

# --- Logging Functions ---
def log_app_action(username, action, details, status="INFO"):
    conn = get_db_connection()
    if not conn: return
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO app_logs (username, action, details, status) VALUES (%s, %s, %s, %s)",
                (username, action, details, status)
            )
        conn.commit()
    except psycopg2.Error as e:
        st.warning(f"DB Logging Error for action '{action}': {e}") 
        conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == '__main__':
    pass 