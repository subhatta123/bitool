"""
Data Integration UI Components
User interface functions for multi-source data integration and ETL operations
"""

import streamlit as st
import pandas as pd
import data_integration
from sqlalchemy import create_engine, inspect, text
import urllib.parse
import json

def show_data_integration_page():
    """Main page for data integration and ETL operations"""
    st.title("🔄 Data Integration & ETL")
    st.markdown("**Combine multiple data sources with AI-powered joins and transformations**")
    
    if not st.session_state.logged_in_user:
        st.error("Please log in to access data integration features.")
        st.session_state.page = 'login'
        st.rerun()
        return

    # Initialize integration engine reference
    integration_engine = data_integration.data_integration_engine
    
    # Create tabs for different sections
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Data Sources", "🔗 AI Suggested Joins", "⚙️ ETL Operations", "📈 Integrated Data"])
    
    with tab1:
        show_data_sources_management(integration_engine)
    
    with tab2:
        show_ai_suggested_joins(integration_engine)
    
    with tab3:
        show_etl_operations(integration_engine)
    
    with tab4:
        show_integrated_data_viewer(integration_engine)

def show_data_sources_management(integration_engine):
    """UI for managing data sources"""
    st.subheader("📊 Data Source Management")
    
    # Summary of current data sources
    summary = integration_engine.get_data_sources_summary()
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Sources", summary['total_sources'])
    with col2:
        st.metric("Relationships Found", summary['total_relationships'])
    with col3:
        st.metric("ETL Operations", summary['total_etl_operations'])
    with col4:
        csv_count = summary['sources_by_type'].get('csv', 0)
        db_count = summary['sources_by_type'].get('database', 0)
        api_count = summary['sources_by_type'].get('api', 0)
        st.metric("Source Types", f"CSV:{csv_count} DB:{db_count} API:{api_count}")
    
    st.markdown("---")
    
    # Add new data source section
    st.subheader("➕ Add New Data Source")
    
    source_type = st.selectbox(
        "Data Source Type:",
        ["CSV File", "Database Connection", "API Data"],
        key="integration_source_type"
    )
    
    source_name = st.text_input("Data Source Name:", key="integration_source_name")
    
    if source_type == "CSV File":
        uploaded_file = st.file_uploader("Upload CSV File", type=["csv"], key="integration_csv_upload")
        
        if uploaded_file and source_name:
            if st.button("Add CSV Data Source", key="add_csv_integration"):
                try:
                    df = pd.read_csv(uploaded_file)
                    connection_info = {"filename": uploaded_file.name, "type": "csv"}
                    
                    source_id = integration_engine.add_data_source(
                        name=source_name,
                        source_type="csv",
                        connection_info=connection_info,
                        data=df
                    )
                    
                    st.success(f"✅ Added CSV data source '{source_name}' with {len(df)} rows and {len(df.columns)} columns")
                    st.info(f"Source ID: {source_id}")
                    st.dataframe(df.head())
                    
                    # Debug info
                    current_summary = integration_engine.get_data_sources_summary()
                    st.write(f"Total sources now: {current_summary['total_sources']}")
                    
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"❌ Failed to add CSV data source: {e}")
    
    elif source_type == "Database Connection":
        # Option 1: Use existing connection
        if st.session_state.get('connected') and (st.session_state.get('db_connection') or st.session_state.get('db_engine')):
            st.success("✅ Existing database connection detected from 'Query Data' page.")
            
            if st.button("Import Current Database Connection", key="import_db_connection"):
                try:
                    # Get schema from existing connection
                    schema = st.session_state.get('data_schema', {})
                    connection_info = {
                        "type": st.session_state.get('connection_type', 'database'),
                        "schema": schema
                    }
                    
                    # For database connections, we need to handle multiple tables
                    if isinstance(schema, dict) and 'tables' not in schema:
                        # This is the multi-table schema format
                        for table_name, table_columns in schema.items():
                            table_source_name = f"{source_name}_{table_name}" if source_name else table_name
                            
                            # Get data for this table
                            try:
                                if st.session_state.get('db_engine'):
                                    # Use SQLAlchemy engine
                                    table_df = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 1000", st.session_state.db_engine)
                                else:
                                    # Use direct connection (legacy)
                                    table_df = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 1000", st.session_state.db_connection)
                                
                                source_id = integration_engine.add_data_source(
                                    name=table_source_name,
                                    source_type="database",
                                    connection_info={**connection_info, "table_name": table_name},
                                    data=table_df
                                )
                                
                                st.success(f"✅ Added database table '{table_name}' with {len(table_df)} rows")
                                st.write(f"Source ID: {source_id}")
                                
                            except Exception as e:
                                st.warning(f"⚠️ Could not import table '{table_name}': {e}")
                    
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"❌ Failed to import database connection: {e}")
        
        # Option 2: Configure new database connection
        st.markdown("---")
        st.markdown("##### 🔗 Or Configure New Database Connection")
        
        db_type = st.selectbox(
            "Database Type:",
            ["PostgreSQL", "Oracle", "SQL Server"],
            key="integration_db_type"
        )
        
        # Generic inputs that are common to most databases
        host_input_val = "localhost" if db_type in ["PostgreSQL", "Oracle"] else ""
        port_input_val = {"PostgreSQL": "5432", "Oracle": "1521"}.get(db_type if db_type else "", "")
        db_name_label = f"{db_type} Database Name"
        if db_type == "Oracle":
            db_name_label = f"{db_type} Service Name or SID"

        col1, col2 = st.columns(2)
        with col1:
            host_input = st.text_input(f"{db_type} Host", key=f"integration_{db_type.lower() if db_type else 'none'}_host", value=host_input_val)
            port_input = st.text_input(f"{db_type} Port", key=f"integration_{db_type.lower() if db_type else 'none'}_port", value=port_input_val)
            dbname_input = st.text_input(db_name_label, key=f"integration_{db_type.lower() if db_type else 'none'}_dbname")
        
        with col2:
            user_input = st.text_input(f"{db_type} User", key=f"integration_{db_type.lower() if db_type else 'none'}_user")
            password_input = st.text_input(f"{db_type} Password", type="password", key=f"integration_{db_type.lower() if db_type else 'none'}_password")

        # DB-specific UI elements
        driver_input, encrypt_input, trust_cert_input, oracle_conn_type = None, None, None, None

        if db_type == "SQL Server":
            st.info("For SQL Server, ensure the appropriate ODBC driver is installed.")
            driver_input = st.text_input(f"ODBC Driver for SQL Server", value="ODBC Driver 17 for SQL Server", key=f"integration_{db_type.lower() if db_type else 'none'}_driver")
            col3, col4 = st.columns(2)
            with col3:
                encrypt_input = st.selectbox("Encrypt Connection", options=["yes", "no", "optional"], index=0, key=f"integration_{db_type.lower() if db_type else 'none'}_encrypt")
            with col4:
                trust_cert_input = st.selectbox("Trust Server Certificate", options=["no", "yes"], index=0, key=f"integration_{db_type.lower() if db_type else 'none'}_trust_cert")
            st.caption("For 'User', leave blank to use Windows Authentication (if applicable).")

        elif db_type == "Oracle":
            st.info("For Oracle, ensure you have 'oracledb' installed (`pip install oracledb`).")
            oracle_conn_type = st.selectbox("Connection Identifier Type", ["Service Name", "SID"], key="integration_oracle_conn_type")

        elif db_type == "PostgreSQL":
            st.info("For PostgreSQL, ensure 'psycopg2-binary' is installed (`pip install psycopg2-binary`).")

        if st.button(f"Connect to {db_type} & Import Tables", key=f"connect_integration_{db_type.lower() if db_type else 'none'}"):
            if not source_name:
                st.error("Please provide a Data Source Name.")
                return
                
            engine = None
            try:
                if db_type == "SQL Server":
                    if not driver_input or not host_input or not dbname_input:
                        st.error("Driver, Host, and Database Name are required for SQL Server connection.")
                        return

                    params = {
                        'DRIVER': f'{{{driver_input.strip() if driver_input else ""}}}',
                        'SERVER': f'{host_input.strip() if host_input else ""},{port_input.strip() if port_input else ""}' if port_input else (host_input.strip() if host_input else ""),
                        'DATABASE': dbname_input.strip() if dbname_input else "",
                        'Encrypt': encrypt_input,
                        'TrustServerCertificate': trust_cert_input
                    }
                    if user_input and user_input.strip():
                        params['UID'] = user_input.strip()
                        params['PWD'] = password_input
                    else:
                        params['Trusted_Connection'] = 'yes'

                    odbc_conn_str = ";".join([f"{k}={v}" for k, v in params.items()])
                    quoted_conn_str = urllib.parse.quote_plus(odbc_conn_str)
                    engine_url = f"mssql+pyodbc:///?odbc_connect={quoted_conn_str}"
                    engine = create_engine(engine_url, connect_args={'timeout': 5})

                elif db_type == "PostgreSQL":
                    if not all([host_input, port_input, dbname_input, user_input, password_input]):
                         st.error("All connection details are required for PostgreSQL.")
                         return
                    engine_url = f"postgresql+psycopg2://{user_input}:{password_input}@{host_input}:{port_input}/{dbname_input}"
                    engine = create_engine(engine_url)

                elif db_type == "Oracle":
                    if not all([host_input, port_input, dbname_input, user_input, password_input]):
                        st.error("All connection details are required for Oracle.")
                        return

                    # Construct the DSN for oracledb
                    if oracle_conn_type == "SID":
                        dsn = f"{host_input}:{port_input}/{dbname_input}"
                    else: # Service Name is default
                        dsn = f"{host_input}:{port_input}/{dbname_input}"
                    
                    engine_url = f"oracle+oracledb://{user_input}:{password_input}@{dsn}"
                    engine = create_engine(engine_url)

                if engine:
                    with st.spinner(f"Connecting to {db_type} and importing tables..."):
                        # Test connection
                        connection_test = engine.connect()
                        try:
                            pass  # Connection test successful
                        finally:
                            try:
                                connection_test.close()
                            except (AttributeError, Exception):
                                pass

                        # Fetch schema using SQLAlchemy Inspector
                        inspector = inspect(engine)
                        if inspector:
                            db_schema = {}
                            # For Oracle, you may need to specify the schema (which is often the username, in uppercase)
                            schema_to_inspect = user_input.upper() if db_type == "Oracle" else None
                            tables = inspector.get_table_names(schema=schema_to_inspect)

                            tables_imported = 0
                            for table_name in tables:
                                try:
                                    # Import each table as a separate data source
                                    table_df = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 1000", engine)
                                    
                                    if not table_df.empty:
                                        table_source_name = f"{source_name}_{table_name}"
                                        connection_info = {
                                            "type": "database",
                                            "db_type": db_type.lower() if db_type else 'unknown',
                                            "table_name": table_name,
                                            "host": host_input,
                                            "database": dbname_input
                                        }
                                        
                                        source_id = integration_engine.add_data_source(
                                            name=table_source_name,
                                            source_type="database",
                                            connection_info=connection_info,
                                            data=table_df
                                        )
                                        
                                        tables_imported += 1
                                        st.success(f"✅ Imported table '{table_name}' with {len(table_df)} rows")
                                    
                                except Exception as e:
                                    st.warning(f"⚠️ Could not import table '{table_name}': {e}")

                            if tables_imported > 0:
                                st.success(f"🎉 Successfully imported {tables_imported} tables from {db_type}!")
                                # Optionally store the engine for future use
                                st.session_state.integration_db_engine = engine
                                st.rerun()
                            else:
                                st.warning("No tables were imported. Please check your database and permissions.")
                        else:
                            st.error("Could not create database inspector")

            except ImportError as e:
                if 'psycopg2' in str(e).lower():
                    st.error("PostgreSQL driver not found. Please install it: `pip install psycopg2-binary`")
                elif 'oracledb' in str(e).lower():
                    st.error("Oracle driver not found. Please install it: `pip install oracledb`")
                else:
                    st.error(f"A required library is missing: {e}")
            except Exception as e:
                st.error(f"❌ Failed to connect to {db_type}: {e}")
                
        if not st.session_state.get('connected') and not st.session_state.get('integration_db_engine'):
            st.info("💡 **Tip:** You can also connect to a database on the 'Query Data' page first, then import it here.")
    
    elif source_type == "API Data":
        st.info("🔄 Use the existing API connection from the 'Query Data' page.")
        
        if st.session_state.get('connected') and st.session_state.get('connection_type') == 'api':
            if st.button("Import Current API Data", key="import_api_data"):
                try:
                    data = st.session_state.get('data')
                    if data is not None:
                        connection_info = {"type": "api", "imported_from": "query_page"}
                        
                        source_id = integration_engine.add_data_source(
                            name=source_name or "API_Data",
                            source_type="api",
                            connection_info=connection_info,
                            data=data
                        )
                        
                        st.success(f"✅ Added API data source with {len(data)} rows")
                        st.rerun()
                        
                except Exception as e:
                    st.error(f"❌ Failed to import API data: {e}")
        else:
            st.warning("⚠️ No active API connection. Please connect to an API in the 'Query Data' page first.")
    
    st.markdown("---")
    
    # Display existing data sources
    if summary['total_sources'] > 0:
        st.subheader("📋 Current Data Sources")
        
        for source_info in summary['sources']:
            with st.expander(f"📊 {source_info['name']} ({source_info['type'].upper()})", expanded=False):
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    st.write(f"**ID:** `{source_info['id']}`")
                    st.write(f"**Type:** {source_info['type']}")
                    st.write(f"**Status:** {source_info['status']}")
                    st.write(f"**Tables:** {source_info['table_count']}")
                    created_at = source_info.get('created_at', '')
                    st.write(f"**Created:** {created_at[:19] if created_at else 'Unknown'}")
                
                with col2:
                    if st.button(f"Preview Data", key=f"preview_{source_info['id']}"):
                        try:
                            table_name = f"source_{source_info['id']}"
                            preview_data = integration_engine.get_integrated_data(table_name)
                            st.dataframe(preview_data.head(10))
                        except Exception as e:
                            st.error(f"Could not preview data: {e}")
                    
                    if st.button(f"Remove Source", key=f"remove_{source_info['id']}", type="secondary"):
                        try:
                            success = integration_engine.remove_data_source(source_info['id'])
                            if success:
                                st.success(f"✅ Removed data source '{source_info['name']}'")
                                st.rerun()
                            else:
                                st.error(f"❌ Failed to remove data source '{source_info['name']}'")
                        except Exception as e:
                            st.error(f"❌ Error removing data source: {e}")

def show_ai_suggested_joins(integration_engine):
    """UI for AI-suggested joins between data sources"""
    st.subheader("🔗 AI-Suggested Data Relationships")
    
    suggested_joins = integration_engine.get_suggested_joins()
    
    if not suggested_joins:
        st.info("🤖 No data relationships detected yet. Add multiple data sources to see AI suggestions.")
        return
    
    st.markdown(f"**Found {len(suggested_joins)} potential relationships:**")
    
    for i, suggestion in enumerate(suggested_joins):
        relationship = suggestion['relationship']
        
        with st.expander(
            f"🔗 {suggestion['source1_name']} ↔ {suggestion['source2_name']} "
            f"(Confidence: {suggestion['confidence']:.1%})", 
            expanded=i < 3  # Expand first 3 by default
        ):
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.markdown(f"**Relationship:** {suggestion['suggestion_text']}")
                st.markdown(f"**Join Type:** {suggestion['join_type']} JOIN")
                st.markdown(f"**Relationship Type:** {relationship.relationship_type}")
                st.markdown(f"**Confidence Score:** {suggestion['confidence']:.1%}")
                
                # Show column details
                st.markdown("**Column Details:**")
                st.write(f"• **{suggestion['source1_name']}**.{relationship.source1_column}")
                st.write(f"• **{suggestion['source2_name']}**.{relationship.source2_column}")
            
            with col2:
                if st.button(f"Create ETL Join", key=f"create_join_{i}"):
                    # Auto-create JOIN ETL operation
                    try:
                        left_table = f"source_{relationship.source1_id}"
                        right_table = f"source_{relationship.source2_id}"
                        
                        etl_name = f"Join_{suggestion['source1_name']}_{suggestion['source2_name']}"
                        
                        parameters = {
                            'join_type': suggestion['join_type'],
                            'left_column': relationship.source1_column,
                            'right_column': relationship.source2_column
                        }
                        
                        operation_id = integration_engine.create_etl_operation(
                            name=etl_name,
                            operation_type='join',
                            source_tables=[left_table, right_table],
                            parameters=parameters
                        )
                        
                        st.success(f"✅ Created JOIN operation: {etl_name}")
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"❌ Failed to create JOIN: {e}")

def show_etl_operations(integration_engine):
    """UI for creating and managing ETL operations"""
    st.subheader("⚙️ ETL Operations")
    
    # ETL Operation Builder
    st.markdown("### ➕ Create New ETL Operation")
    
    operation_type = st.selectbox(
        "Operation Type:",
        ["Join", "Union", "Aggregate", "Filter"],
        key="etl_operation_type"
    )
    
    operation_name = st.text_input("Operation Name:", key="etl_operation_name")
    
    # Get available tables
    summary = integration_engine.get_data_sources_summary()
    available_tables = [f"source_{source['id']}" for source in summary['sources']]
    table_names = [source['name'] for source in summary['sources']]
    
    if not available_tables:
        st.warning("⚠️ No data sources available. Please add data sources first.")
        return
    
    if operation_type == "Join":
        st.markdown("#### Join Configuration")
        
        col1, col2 = st.columns(2)
        with col1:
            left_table_idx = st.selectbox("Left Table:", range(len(table_names)), 
                                        format_func=lambda x: table_names[x], key="join_left_table")
            if left_table_idx is not None:
                left_table = available_tables[left_table_idx]
                
                # Get columns for left table
                try:
                    left_data = integration_engine.get_integrated_data(left_table)
                    left_columns = left_data.columns.tolist()
                    left_column = st.selectbox("Left Column:", left_columns, key="join_left_column")
                except:
                    left_columns = []
                    left_column = None
            else:
                left_table = None
                left_column = None
        
        with col2:
            right_table_idx = st.selectbox("Right Table:", range(len(table_names)), 
                                         format_func=lambda x: table_names[x], key="join_right_table")
            if right_table_idx is not None:
                right_table = available_tables[right_table_idx]
                
                # Get columns for right table
                try:
                    right_data = integration_engine.get_integrated_data(right_table)
                    right_columns = right_data.columns.tolist()
                    right_column = st.selectbox("Right Column:", right_columns, key="join_right_column")
                except:
                    right_columns = []
                    right_column = None
            else:
                right_table = None
                right_column = None
        
        join_type = st.selectbox("Join Type:", ["INNER", "LEFT", "RIGHT", "FULL"], key="join_type_select")
        
        if st.button("Create JOIN Operation", key="create_join_operation"):
            if operation_name and left_column and right_column:
                try:
                    parameters = {
                        'join_type': join_type,
                        'left_column': left_column,
                        'right_column': right_column
                    }
                    
                    operation_id = integration_engine.create_etl_operation(
                        name=operation_name,
                        operation_type='join',
                        source_tables=[left_table, right_table],
                        parameters=parameters
                    )
                    
                    st.success(f"✅ Created JOIN operation: {operation_name}")
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"❌ Failed to create JOIN operation: {e}")
            else:
                st.warning("⚠️ Please fill in all required fields")
    
    elif operation_type == "Union":
        st.markdown("#### Union Configuration")
        
        selected_tables_idx = st.multiselect(
            "Select Tables to Union:", 
            range(len(table_names)), 
            format_func=lambda x: table_names[x],
            key="union_tables"
        )
        
        union_type = st.selectbox("Union Type:", ["UNION", "UNION ALL"], key="union_type_select")
        
        if st.button("Create UNION Operation", key="create_union_operation"):
            if operation_name and len(selected_tables_idx) >= 2:
                try:
                    selected_tables = [available_tables[i] for i in selected_tables_idx]
                    
                    parameters = {'union_type': union_type}
                    
                    operation_id = integration_engine.create_etl_operation(
                        name=operation_name,
                        operation_type='union',
                        source_tables=selected_tables,
                        parameters=parameters
                    )
                    
                    st.success(f"✅ Created UNION operation: {operation_name}")
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"❌ Failed to create UNION operation: {e}")
            else:
                st.warning("⚠️ Please select at least 2 tables and provide a name")
    
    elif operation_type == "Aggregate":
        st.markdown("#### Aggregation Configuration")
        
        source_table_idx = st.selectbox("Source Table:", range(len(table_names)), 
                                      format_func=lambda x: table_names[x], key="agg_source_table")
        
        if source_table_idx is not None:
            source_table = available_tables[source_table_idx]
            
            # Get columns for aggregation
            try:
                source_data = integration_engine.get_integrated_data(source_table)
                all_columns = source_data.columns.tolist()
                numeric_columns = source_data.select_dtypes(include=['number']).columns.tolist()
                
                group_by_columns = st.multiselect("Group By Columns:", all_columns, key="agg_group_by")
                
                st.markdown("**Aggregations:**")
                aggregations = {}
                for i, col in enumerate(numeric_columns[:5]):  # Limit to 5 for UI space
                    agg_func = st.selectbox(f"Function for {col}:", 
                                          ["None", "SUM", "AVG", "COUNT", "MIN", "MAX"], 
                                          key=f"agg_func_{i}")
                    if agg_func != "None":
                        aggregations[col] = agg_func
                
                if st.button("Create AGGREGATE Operation", key="create_agg_operation"):
                    if operation_name and aggregations:
                        try:
                            parameters = {
                                'group_by': group_by_columns,
                                'aggregations': aggregations
                            }
                            
                            operation_id = integration_engine.create_etl_operation(
                                name=operation_name,
                                operation_type='aggregate',
                                source_tables=[source_table],
                                parameters=parameters
                            )
                            
                            st.success(f"✅ Created AGGREGATE operation: {operation_name}")
                            st.rerun()
                            
                        except Exception as e:
                            st.error(f"❌ Failed to create AGGREGATE operation: {e}")
                    else:
                        st.warning("⚠️ Please provide a name and select at least one aggregation")
                        
            except Exception as e:
                st.error(f"❌ Could not load table data: {e}")
    
    # Display existing ETL operations
    st.markdown("---")
    st.markdown("### 📋 Existing ETL Operations")
    
    if integration_engine.etl_operations:
        for i, operation in enumerate(integration_engine.etl_operations):
            with st.expander(f"⚙️ {operation.name} ({operation.operation_type.upper()})", expanded=False):
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    st.write(f"**Operation ID:** `{operation.id}`")
                    st.write(f"**Type:** {operation.operation_type}")
                    st.write(f"**Source Tables:** {', '.join(operation.source_tables)}")
                    st.write(f"**Output Table:** `{operation.output_table_name}`")
                    operation_created_at = getattr(operation, 'created_at', '') or ''
                    st.write(f"**Created:** {operation_created_at[:19] if operation_created_at else 'Unknown'}")
                    
                    # Replace nested expander with a button to show/hide SQL
                    show_sql_key = f"show_sql_{i}"
                    if st.button("View SQL Query", key=f"sql_btn_{i}"):
                        if show_sql_key not in st.session_state:
                            st.session_state[show_sql_key] = True
                        else:
                            st.session_state[show_sql_key] = not st.session_state[show_sql_key]
                    
                    # Show SQL if button was clicked
                    if st.session_state.get(show_sql_key, False):
                        st.markdown("**SQL Query:**")
                        st.code(operation.sql_query, language="sql")
                
                with col2:
                    if st.button(f"View Results", key=f"view_etl_{i}"):
                        try:
                            result_data = integration_engine.get_integrated_data(operation.output_table_name)
                            st.dataframe(result_data)
                        except Exception as e:
                            st.error(f"Could not load results: {e}")
    else:
        st.info("📭 No ETL operations created yet.")

def show_integrated_data_viewer(integration_engine):
    """UI for viewing integrated data and creating queries"""
    st.subheader("📈 Integrated Data Viewer")
    
    # Get available tables
    try:
        available_tables_df = integration_engine.get_integrated_data()
        if not available_tables_df.empty:
            available_tables = available_tables_df['available_tables'].tolist()
        else:
            available_tables = []
    except:
        available_tables = []
    
    if not available_tables:
        st.info("📭 No integrated data available yet. Add data sources and create ETL operations.")
        return
    
    st.markdown(f"**Available Tables:** {len(available_tables)}")
    
    # Table selector
    selected_table = st.selectbox("Select Table to View:", available_tables, key="integrated_data_table_select")
    
    if selected_table:
        try:
            # Load and display data
            table_data = integration_engine.get_integrated_data(selected_table)
            
            if not table_data.empty:
                # Data summary
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Rows", len(table_data))
                with col2:
                    st.metric("Columns", len(table_data.columns))
                with col3:
                    numeric_cols = len(table_data.select_dtypes(include=['number']).columns)
                    st.metric("Numeric Columns", numeric_cols)
                with col4:
                    null_count = table_data.isnull().sum().sum()
                    st.metric("Total Nulls", null_count)
                
                # Data preview
                st.markdown("### 📊 Data Preview")
                st.dataframe(table_data.head(20), use_container_width=True)
                
                # --- New Data Type Customization Section ---
                with st.expander("✏️ Customize Data Types"):
                    if 'temp_types' not in st.session_state:
                        st.session_state.temp_types = {col: str(table_data[col].dtype) for col in table_data.columns}

                    edited_df = table_data.copy()
                    
                    # Create a dictionary to hold the new type selections
                    new_types = {}

                    st.markdown("Review and change the data types for each column below:")
                    
                    cols_per_row = 3
                    column_list = table_data.columns.tolist()
                    
                    for i in range(0, len(column_list), cols_per_row):
                        row_cols = st.columns(cols_per_row)
                        for j in range(cols_per_row):
                            if i + j < len(column_list):
                                col_name = column_list[i+j]
                                with row_cols[j]:
                                    st.markdown(f"**{col_name}**")
                                    current_type = st.session_state.temp_types.get(col_name, str(table_data[col_name].dtype))
                                    
                                    type_options = ['object', 'int64', 'float64', 'datetime64[ns]', 'bool', 'category']
                                    
                                    # Ensure current_type is a valid option
                                    if current_type not in type_options:
                                        # Heuristic to map dtypes to simple categories
                                        if 'int' in current_type: current_type = 'int64'
                                        elif 'float' in current_type: current_type = 'float64'
                                        elif 'date' in current_type: current_type = 'datetime64[ns]'
                                        elif 'bool' in current_type: current_type = 'bool'
                                        else: current_type = 'object'
                                        
                                    try:
                                        current_type_index = type_options.index(current_type)
                                    except ValueError:
                                        current_type_index = 0 # Default to object/text

                                    new_type = st.selectbox(
                                        f"Type for {col_name}",
                                        options=type_options,
                                        index=current_type_index,
                                        key=f"dtype_{selected_table}_{col_name}"
                                    )
                                    new_types[col_name] = new_type

                    if st.button("Apply Data Type Changes", key=f"apply_types_{selected_table}"):
                        with st.spinner("Applying data type changes..."):
                            try:
                                # Create a copy to avoid modifying the original dataframe during iteration
                                temp_df = edited_df.copy()
                                conversion_errors = []
                                
                                for col, new_dtype in new_types.items():
                                    current_dtype = str(temp_df[col].dtype)
                                    if new_dtype != current_dtype:
                                        try:
                                            if new_dtype == 'datetime64[ns]':
                                                temp_df[col] = pd.to_datetime(temp_df[col], errors='coerce')
                                            elif new_dtype == 'int64':
                                                # Coerce errors to NaN, then fill with a placeholder (like 0) if needed
                                                temp_df[col] = pd.to_numeric(temp_df[col], errors='coerce').fillna(0).astype(int)
                                            elif new_dtype == 'float64':
                                                temp_df[col] = pd.to_numeric(temp_df[col], errors='coerce').astype(float)
                                            else:
                                                temp_df[col] = temp_df[col].astype(new_dtype)
                                        except Exception as e:
                                            conversion_errors.append(f"Could not convert **{col}** to **{new_dtype}**: {e}")
                                            # Revert change for this column on error
                                            temp_df[col] = edited_df[col]
                                
                                # Update the main data in the integration engine
                                integration_engine.update_data_for_table(selected_table, temp_df)
                                st.session_state.temp_types = {col: str(temp_df[col].dtype) for col in temp_df.columns}

                                if conversion_errors:
                                    for error in conversion_errors:
                                        st.warning(f"⚠️ {error}")
                                else:
                                    st.success("✅ Data types updated successfully!")
                                
                                # Rerun to reflect changes in the data preview
                                st.rerun()

                            except Exception as e:
                                st.error(f"❌ An unexpected error occurred: {e}")

                # Quick stats
                if len(table_data.select_dtypes(include=['number']).columns) > 0:
                    with st.expander("📈 Quick Statistics"):
                        st.dataframe(table_data.describe())
                
                # Query this data
                st.markdown("### 🔍 Query This Data")
                
                if st.button(f"Use '{selected_table}' for AI Queries", key=f"use_table_{selected_table}"):
                    # Set this as the active data for querying
                    st.session_state.data = table_data
                    st.session_state.connected = True
                    st.session_state.connection_type = "integrated"
                    
                    # Create schema for this table
                    schema = []
                    for col in table_data.columns:
                        schema.append({"name": col, "type": str(table_data[col].dtype)})
                    st.session_state.data_schema = schema
                    
                    st.success(f"✅ Set '{selected_table}' as active data source for AI queries!")
                    st.info("🔄 Go to 'Query Data' page to ask questions about this integrated dataset.")
            else:
                st.warning("⚠️ Selected table is empty.")
                
        except Exception as e:
            st.error(f"❌ Could not load table data: {e}") 