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

# --- Configuration ---
USERS_FILE = "users.json"
DEFAULT_ADMIN_USERNAME = "admin"
DEFAULT_ADMIN_PASSWORD = "admin123" # Change this in a real scenario!

st.set_page_config(layout="wide", page_title="DBChat - Query Your Data with AI")

# --- Session State Initialization ---
if 'page' not in st.session_state:
    st.session_state.page = "login" # Default page is login
if 'logged_in_user' not in st.session_state:
    st.session_state.logged_in_user = None
if 'user_roles' not in st.session_state:
    st.session_state.user_roles = []

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

# --- User Data Management ---
def load_users():
    """Loads users from the JSON file."""
    if not os.path.exists(USERS_FILE):
        return {}
    try:
        with open(USERS_FILE, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_users(users_data):
    """Saves users to the JSON file."""
    with open(USERS_FILE, 'w') as f:
        json.dump(users_data, f, indent=4)

def hash_password(password):
    """Hashes a password using Werkzeug."""
    return generate_password_hash(password)

def check_password(hashed_password, password):
    """Checks a password against a hashed version."""
    return check_password_hash(hashed_password, password)

def initialize_users_file():
    """Initializes the users file with a default admin if it doesn't exist or is empty."""
    users = load_users()
    if not users:
        users[DEFAULT_ADMIN_USERNAME] = {
            "hashed_password": hash_password(DEFAULT_ADMIN_PASSWORD),
            "roles": ["admin", "superuser"]
        }
        save_users(users)
        st.info(f"Initialized users file. Default admin: '{DEFAULT_ADMIN_USERNAME}', password: '{DEFAULT_ADMIN_PASSWORD}'. Please change this password.")
    return users

# Initialize users data on first load
USERS_DATA = initialize_users_file()


# --- OpenAI LLM Helper ---
def get_sql_from_openai(natural_language_query, api_key, data_schema, db_type="sqlserver", target_table=None):
    """Generates SQL query from natural language using OpenAI LLM."""
    if not api_key:
        st.error("OpenAI API Key is not set. Please enter it in the sidebar.")
        return None

    client = openai.OpenAI(api_key=api_key)

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
    elif st.session_state.connection_type != "csv": # For DBs, we might not load all data initially
        st.info("Connected to database. Schema information would typically be shown here.")
        if st.session_state.data_schema and isinstance(st.session_state.data_schema, dict) and "error" not in st.session_state.data_schema:
            st.write("Available schema for LLM:")
            st.json(st.session_state.data_schema)

    # Table selection dropdown
    table_options = ["All Tables / Auto-detect"]
    if st.session_state.data_schema and isinstance(st.session_state.data_schema, dict):
        # Assuming data_schema is a dict with table names as keys for DBs
        table_options.extend(list(st.session_state.data_schema.keys()))
    elif st.session_state.data_schema and isinstance(st.session_state.data_schema, list) and st.session_state.connection_type == "csv":
        # For CSV, if we want to treat the CSV as a single named table for focus purposes
        # We might need a conventional name or let the user define one if they load multiple CSVs (future feature)
        # For now, if it's a CSV, the concept of 'focusing' on a table within it isn't as relevant as for a DB.
        # However, if the LLM treats the CSV as a table named 'csv_data' for example:
        # table_options.append("csv_data") # Example
        pass # For CSVs, focusing on a specific 'table' is less direct with current schema setup

    st.session_state.selected_table = st.selectbox(
        "Focus query on a specific table (optional):",
        options=table_options,
        index=table_options.index(st.session_state.selected_table) if st.session_state.selected_table in table_options else 0
    )

    natural_language_query = st.text_area("Ask your question in plain English:", height=100, key="nl_query")

    if st.button("Get Answer", disabled=(not st.session_state.llm_api_key)):
        if not st.session_state.llm_api_key:
            st.warning("Please enter your LLM API Key in the sidebar to enable queries.")
            return

        st.session_state.results_df = None # Clear previous results before new query
        # Clear previous logs related to query execution for this new attempt
        st.session_state.log_generated_sql_str = None
        st.session_state.log_query_execution_details_str = None

        if natural_language_query:
            with st.spinner("Generating SQL query and fetching results..."):
                # --- 1. LLM Integration ---
                if not st.session_state.llm_api_key:
                    st.error("OpenAI API Key is missing. Cannot generate SQL query.")
                    return
                if not st.session_state.data_schema:
                    st.warning("Data schema is not available. LLM may produce inaccurate queries.")
                    # return # Optionally, prevent query generation if schema is crucial
                
                generated_sql_query = get_sql_from_openai(
                    natural_language_query,
                    st.session_state.llm_api_key,
                    st.session_state.data_schema,
                    st.session_state.connection_type,
                    st.session_state.selected_table # Pass the selected table
                )

                if generated_sql_query:
                    st.subheader("Generated SQL Query by AI")
                    st.code(generated_sql_query, language="sql")
                    st.session_state.log_generated_sql_str = generated_sql_query # Log generated SQL
                else:
                    st.error("Could not generate SQL query from your question.")
                    st.session_state.log_generated_sql_str = "Error: Could not generate SQL query." # Log error
                    return # Stop if no SQL query was generated

                # --- 2. Execute Query ---
                try:
                    if st.session_state.connection_type == "csv" and st.session_state.data is not None:
                        st.info(f"Preparing in-memory SQL table 'csv_data' for CSV query: {generated_sql_query}")
                        raw_sqlite_conn = None # Initialize raw_sqlite_conn
                        try:
                            # Create an in-memory SQLite database using sqlite3 directly
                            raw_sqlite_conn = sqlite3.connect(':memory:')
                            
                            # Load the DataFrame into the in-memory SQLite database
                            # The table name here MUST match what the LLM is instructed to use (i.e., 'csv_data')
                            st.session_state.data.to_sql('csv_data', raw_sqlite_conn, if_exists='replace', index=False)
                            
                            # Execute the query using pandas read_sql_query with the raw sqlite3 connection
                            results_df = pd.read_sql_query(generated_sql_query, raw_sqlite_conn)
                            
                            st.session_state.log_query_execution_details_str = results_df.to_string() if not results_df.empty else "Query executed on CSV successfully, but no data was returned."
                        except sqlite3.Error as e_sqlite:
                            st.error(f"SQLite error during CSV query execution: {e_sqlite}")
                            results_df = pd.DataFrame({"error": [f"SQLite error: {e_sqlite}"]})
                            st.session_state.log_query_execution_details_str = f"SQLite Execution Error: {e_sqlite}"
                        except Exception as e_csv_sql: # Catch other potential errors
                            st.error(f"Error executing SQL query on CSV: {e_csv_sql}")
                            results_df = pd.DataFrame({"error": [f"Error executing SQL on CSV: {e_csv_sql}"]})
                            st.session_state.log_query_execution_details_str = f"In-memory CSV SQL Execution Error: {e_csv_sql}"
                        finally:
                            if raw_sqlite_conn:
                                raw_sqlite_conn.close()

                    elif st.session_state.db_connection is not None and st.session_state.connection_type == 'sqlserver': # For actual DBs
                        try:
                            # For pyodbc, execute directly and fetch
                            cursor = st.session_state.db_connection.cursor()
                            cursor.execute(generated_sql_query)
                            rows = cursor.fetchall()
                            # Convert to DataFrame
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

                    elif st.session_state.db_connection is not None: # For other DBs (not yet implemented)
                        results_df = pd.DataFrame({"message": [f"Actual query execution for {st.session_state.connection_type} not implemented yet."]})
                        st.warning(f"Actual query execution for {st.session_state.connection_type} not implemented yet.")
                        st.session_state.log_query_execution_details_str = f"Query execution for {st.session_state.connection_type} not implemented."
                    else:
                        results_df = pd.DataFrame({"error": ["Not connected to a data source or connection type not supported for querying yet."]})
                        st.session_state.log_query_execution_details_str = "Error: Not connected or connection type not supported for querying."

                    st.session_state.results_df = results_df # Store results in session state

                except Exception as e:
                    st.error(f"Error executing query or processing results: {e}")
                    st.session_state.results_df = pd.DataFrame({"error": [f"Unhandled error: {e}"]})
                    st.session_state.log_query_execution_details_str = f"Unhandled Query/Processing Error: {e}"
        else:
            st.warning("Please enter your question.")
            st.session_state.results_df = None # Clear results if no question was asked

    # Display Query Results and Visualizations if results_df exists in session state
    if st.session_state.results_df is not None:
        results_df_to_display = st.session_state.results_df
        st.subheader("Query Results")
        st.dataframe(results_df_to_display)

        # --- 3. Visualization (Plotly Express) ---
        if not results_df_to_display.empty and not ("error" in results_df_to_display.columns or "message" in results_df_to_display.columns):
            st.subheader("Visualizations")

            # Handle single value result with st.metric
            if results_df_to_display.shape == (1, 1):
                metric_label = results_df_to_display.columns[0]
                metric_value = results_df_to_display.iloc[0, 0]
                st.metric(label=str(metric_label), value=str(metric_value))
            
            # Plotly Express Charting for multi-column data
            elif not results_df_to_display.empty: # Check again, though outer check should cover
                st.markdown("#### Interactive Chart Options")
                all_columns = results_df_to_display.columns.tolist()
                numeric_columns = results_df_to_display.select_dtypes(include=['number']).columns.tolist()
                categorical_columns = results_df_to_display.select_dtypes(include=['object', 'category', 'string', 'boolean']).columns.tolist()

                chart_type = st.selectbox("Select Chart Type:", 
                                         ["Bar Chart", "Line Chart", "Scatter Plot", "Pie Chart", "Histogram"], 
                                         key="chart_type_select")
                fig = None

                try:
                    if chart_type == "Bar Chart":
                        if len(categorical_columns) > 0 and len(numeric_columns) > 0:
                            x_bar = st.selectbox("X-axis (Categorical):", categorical_columns, key="bar_x")
                            y_bar = st.selectbox("Y-axis (Numerical):", numeric_columns, key="bar_y")
                            color_bar = st.selectbox("Color (Optional, Categorical):", [None] + categorical_columns, key="bar_color")
                            if x_bar and y_bar:
                                fig = px.bar(results_df_to_display, x=x_bar, y=y_bar, color=color_bar, title=f"{chart_type}: {y_bar} by {x_bar}")
                        else:
                            st.info("Bar chart requires at least one categorical and one numerical column.")

                    elif chart_type == "Line Chart":
                        if len(all_columns) >=1 and len(numeric_columns) > 0: # X can be numeric or categorical (like time)
                            x_line = st.selectbox("X-axis:", all_columns, key="line_x")
                            y_line = st.selectbox("Y-axis (Numerical):", numeric_columns, key="line_y")
                            color_line = st.selectbox("Color (Optional, Categorical):", [None] + categorical_columns, key="line_color")
                            if x_line and y_line:
                                fig = px.line(results_df_to_display, x=x_line, y=y_line, color=color_line, title=f"{chart_type}: {y_line} over {x_line}")
                        else:
                            st.info("Line chart requires at least one numerical column for Y-axis and any column for X-axis.")

                    elif chart_type == "Scatter Plot":
                        if len(numeric_columns) >= 2:
                            x_scatter = st.selectbox("X-axis (Numerical):", numeric_columns, key="scatter_x")
                            y_scatter = st.selectbox("Y-axis (Numerical):", numeric_columns, key="scatter_y")
                            color_scatter = st.selectbox("Color (Optional):", [None] + all_columns, key="scatter_color")
                            size_scatter = st.selectbox("Size (Optional, Numerical):", [None] + numeric_columns, key="scatter_size")
                            if x_scatter and y_scatter:
                                fig = px.scatter(results_df_to_display, x=x_scatter, y=y_scatter, color=color_scatter, size=size_scatter, title=f"{chart_type}")
                        else:
                            st.info("Scatter plot typically requires at least two numerical columns.")

                    elif chart_type == "Pie Chart":
                        if len(categorical_columns) > 0 and len(numeric_columns) > 0:
                            names_pie = st.selectbox("Names (Categorical):", categorical_columns, key="pie_names")
                            values_pie = st.selectbox("Values (Numerical):", numeric_columns, key="pie_values")
                            if names_pie and values_pie:
                                fig = px.pie(results_df_to_display, names=names_pie, values=values_pie, title=f"{chart_type} of {values_pie} by {names_pie}")
                        else:
                            st.info("Pie chart requires one categorical column for names and one numerical column for values.")
                        
                    elif chart_type == "Histogram":
                        if len(numeric_columns) > 0:
                            hist_col = st.selectbox("Column for Histogram (Numerical):", numeric_columns, key="hist_col")
                            if hist_col:
                                fig = px.histogram(results_df_to_display, x=hist_col, title=f"Histogram of {hist_col}")
                        else:
                            st.info("Histogram requires at least one numerical column.")

                    if fig:
                        st.plotly_chart(fig, use_container_width=True)
                        # Add to Dashboard Button
                        # Store necessary info to recreate the chart
                        chart_params = {}
                        if chart_type == "Bar Chart": chart_params = {'x': x_bar, 'y': y_bar, 'color': color_bar}
                        elif chart_type == "Line Chart": chart_params = {'x': x_line, 'y': y_line, 'color': color_line}
                        elif chart_type == "Scatter Plot": chart_params = {'x': x_scatter, 'y': y_scatter, 'color': color_scatter, 'size': size_scatter}
                        elif chart_type == "Pie Chart": chart_params = {'names': names_pie, 'values': values_pie}
                        elif chart_type == "Histogram": chart_params = {'x': hist_col}
                        
                        # Use a unique key for the button based on chart type and params to avoid Streamlit key collision if options change slightly but chart is similar
                        # A simpler key might be just a counter or hash of params if needed for more robustness
                        button_key_suffix = f"{chart_type}_{'_'.join(map(str, chart_params.values()))}".replace(" ", "_")

                        if st.button(f"Add {chart_type} to My Dashboard", key=f"add_to_dash_{button_key_suffix}"):
                            dashboard_item = {
                                "title": fig.layout.title.text if fig.layout.title else chart_type,
                                "chart_type": chart_type,
                                "params": chart_params,
                                "data_snapshot": copy.deepcopy(results_df_to_display) # Important: store a copy of the data
                            }
                            st.session_state.dashboard_items.append(dashboard_item)
                            st.success(f"'{dashboard_item['title']}' added to your dashboard!")

                    elif chart_type != "Select Chart Type": 
                        pass 

                except Exception as e_plotly:
                    st.error(f"Error generating Plotly chart: {e_plotly}")
            
            # Fallback or message if no suitable chart could be made by Plotly due to column types/counts
            # The specific info messages within each chart type block already cover this.
            # else:
                # st.info("Data is not suitable for the selected Plotly chart type or general plotting.")
                
        elif "error" in results_df_to_display.columns or "message" in results_df_to_display.columns:
            pass # Don't try to visualize error messages
        else:
            st.info("No data returned to visualize.")

    if st.button("Disconnect and Choose Another Source"):
        st.session_state.connected = False
        st.session_state.connection_type = None
        st.session_state.data = None
        st.session_state.db_connection = None
        st.session_state.db_engine = None # Clear engine as well
        st.session_state.data_schema = None # Clear schema
        st.session_state.selected_table = "All Tables / Auto-detect" # Reset selected table
        # Clear logs
        st.session_state.log_data_schema_str = None
        st.session_state.log_openai_prompt_str = None
        st.session_state.log_generated_sql_str = None
        st.session_state.log_query_execution_details_str = None
        st.session_state.results_df = None # Clear query results on disconnect
        # Do not clear llm_api_key here, user might want to reuse it
        st.session_state.dashboard_items = [] # Clear dashboard on disconnect
        st.rerun()


# --- Main Application Logic ---
def main():
    st.title("DBChat: Ask Questions, Get Answers, Visualize Insights")
    st.markdown("Connect to your data source, ask questions in plain English, and let AI assist you in fetching and visualizing the answers.")

    # LLM API Key Input (Required for the core functionality) - Placed here to run on every script execution
    st.sidebar.subheader("LLM Configuration")
    # Use a different key for the text_input to avoid conflicts if it was used elsewhere with the same key
    # And ensure a default value for st.session_state.llm_api_key if it's None from initialization
    current_api_key = st.sidebar.text_input(
        "Enter your LLM API Key", 
        type="password", 
        key="llm_api_key_input", # Unique key for this input widget
        value=st.session_state.get("llm_api_key", "") # Use existing session state value or empty string
    )
    if current_api_key != st.session_state.llm_api_key: # Update session state only if input changes
        st.session_state.llm_api_key = current_api_key
        st.rerun() # Rerun to reflect the new API key state immediately for button disable logic

    # Show sidebar warning for API key only if not on login page and no key
    if st.session_state.page != "login" and not st.session_state.llm_api_key:
        st.sidebar.warning("LLM API Key is required to use the query functionality.")

    # --- Page Routing ---
    if st.session_state.page == "login":
        show_login_page()
    elif st.session_state.page == "app" and st.session_state.logged_in_user:
        if not st.session_state.connected:
            # Role-based access to connection screen
            if "admin" in st.session_state.user_roles or "superuser" in st.session_state.user_roles:
                show_connection_screen()
            else:
                st.warning("You do not have permission to create new data connections. Please contact an administrator.")
                if st.button("Logout"):
                    logout()
        else:
            show_query_screen()
    elif st.session_state.page == "admin_panel" and "admin" in st.session_state.user_roles:
        show_admin_panel_page()
    elif st.session_state.page == "dashboard" and st.session_state.logged_in_user:
        show_dashboard_page()
    else:
        # If somehow in a weird state, redirect to login
        st.session_state.page = "login"
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
    st.header("Login")
    username = st.text_input("Username", key="login_username")
    password = st.text_input("Password", type="password", key="login_password")

    if st.button("Login", key="login_button"):
        users = load_users() # Load fresh user data
        if username in users and check_password(users[username]["hashed_password"], password):
            st.session_state.logged_in_user = username
            st.session_state.user_roles = users[username]["roles"]
            st.session_state.page = "app"
            # Clear connection details from any previous session if a new user logs in
            st.session_state.connected = False
            st.session_state.connection_type = None
            st.session_state.data = None
            st.session_state.db_connection = None
            st.session_state.db_engine = None
            st.session_state.data_schema = None
            st.session_state.selected_table = "All Tables / Auto-detect"
            clear_logs() # Clear logs from previous session
            st.session_state.results_df = None # Clear query results on logout
            st.session_state.dashboard_items = [] # Clear dashboard on logout
            st.rerun()
        else:
            st.error("Invalid username or password.")

def show_admin_panel_page():
    st.header("Admin Panel - User Management")
    if "admin" not in st.session_state.user_roles:
        st.error("You do not have permission to access this page.")
        st.session_state.page = "app" # Redirect to app
        st.rerun()
        return

    users = load_users()

    st.subheader("Current Users")
    user_list_cols = st.columns([2, 2, 1])
    user_list_cols[0].write("**Username**")
    user_list_cols[1].write("**Roles**")
    for user, data in users.items():
        cols = st.columns([2, 2, 1])
        cols[0].write(user)
        cols[1].write(", ".join(data["roles"]))
        if user != st.session_state.logged_in_user: # Admin cannot delete themselves
            if cols[2].button(f"Delete {user}", key=f"delete_{user}"):
                del users[user]
                save_users(users)
                st.success(f"User '{user}' deleted.")
                st.rerun()
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
                    users = load_users() # Load fresh before saving
                    if edit_username in users and not edit_password: # Editing existing user, password not changed
                        users[edit_username]["roles"] = roles_list
                    else: # New user or password change
                        if not edit_password and edit_username not in users: # New user must have password
                             st.error("Password is required for new users.")
                             return # Must return here to avoid saving without password for new user
                        users[edit_username] = {
                            "hashed_password": hash_password(edit_password) if edit_password else users[edit_username]["hashed_password"],
                            "roles": roles_list
                        }
                    save_users(users)
                    st.success(f"User '{edit_username}' saved.")
                    st.rerun()

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
    st.rerun()

def clear_logs():
    st.session_state.log_data_schema_str = None
    st.session_state.log_openai_prompt_str = None
    st.session_state.log_generated_sql_str = None
    st.session_state.log_query_execution_details_str = None

# --- Dashboard Page ---
def show_dashboard_page():
    st.header("My Dashboard")

    if not st.session_state.dashboard_items:
        st.info("Your dashboard is currently empty. Go to the 'Query Data' page, generate a visualization, and click 'Add to My Dashboard'.")
        return

    for i, item in enumerate(st.session_state.dashboard_items):
        st.markdown(f"### {i+1}. {item.get('title', item['chart_type'])}")
        fig = None
        data = item['data_snapshot'] # Use the stored data snapshot
        params = item['params']
        chart_type = item['chart_type']

        try:
            if chart_type == "Bar Chart":
                fig = px.bar(data, x=params.get('x'), y=params.get('y'), color=params.get('color'), title=item.get('title', chart_type))
            elif chart_type == "Line Chart":
                fig = px.line(data, x=params.get('x'), y=params.get('y'), color=params.get('color'), title=item.get('title', chart_type))
            elif chart_type == "Scatter Plot":
                fig = px.scatter(data, x=params.get('x'), y=params.get('y'), color=params.get('color'), size=params.get('size'), title=item.get('title', chart_type))
            elif chart_type == "Pie Chart":
                fig = px.pie(data, names=params.get('names'), values=params.get('values'), title=item.get('title', chart_type))
            elif chart_type == "Histogram":
                fig = px.histogram(data, x=params.get('x'), title=item.get('title', chart_type))
            
            if fig:
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning(f"Could not reconstruct chart: {item.get('title', chart_type)}")

            if st.button(f"Remove from Dashboard", key=f"remove_dash_{i}"):
                st.session_state.dashboard_items.pop(i)
                st.rerun()
        except Exception as e_dash_chart:
            st.error(f"Error displaying dashboard chart '{item.get('title', chart_type)}': {e_dash_chart}")
        st.markdown("---") # Separator

# Add a logout button and admin panel link to the sidebar when logged in
if st.session_state.logged_in_user:
    st.sidebar.markdown(f"Logged in as: **{st.session_state.logged_in_user}** (`{', '.join(st.session_state.user_roles)}`)")
    if "admin" in st.session_state.user_roles:
        if st.sidebar.button("Admin Panel", key="admin_panel_button"):
            st.session_state.page = "admin_panel"
            st.rerun()
    if st.sidebar.button("Logout", key="logout_button_sidebar"):
        logout()
    if st.sidebar.button("Query Data", key="query_data_button_sidebar"):
        st.session_state.page = "app"
        st.rerun()
    if st.sidebar.button("My Dashboard", key="my_dashboard_button_sidebar"):
        st.session_state.page = "dashboard"
        st.rerun()


if __name__ == "__main__":
    main() 