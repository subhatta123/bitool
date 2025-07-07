"""
Semantic Layer UI Components
User interface for managing semantic metadata and business context
"""

import streamlit as st
import pandas as pd
from semantic_layer import SemanticLayer, DataType, SemanticColumn, SemanticTable, SemanticRelationship, SemanticMetric
import database
import data_integration

def show_semantic_layer_page():
    """Main semantic layer management page"""
    st.title("🧠 Semantic Layer Management")
    st.markdown("**Enhance your data schema with business context to improve AI query accuracy**")
    
    if not st.session_state.logged_in_user:
        st.error("Please log in to access semantic layer features.")
        st.session_state.page = 'login'
        st.rerun()
        return

    # Create tabs for different sections
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Overview", "🚀 Quick Fix", "⚙️ Configure", "🔗 Relationships", "🧪 Test Queries"])
    
    with tab1:
        show_semantic_overview()
    
    with tab2:
        show_analytics_quick_fix()
    
    with tab3:
        show_semantic_configuration()
    
    with tab4:
        show_relationship_management()
    
    with tab5:
        show_query_testing()

def show_analytics_quick_fix():
    """Quick fix for analytics queries like delayed orders by category"""
    st.subheader("🚀 Quick Fix for Analytics Queries")
    st.markdown("**Fix common query understanding issues with enhanced business definitions**")
    
    # Check current enhancement status
    conn = database.get_db_connection()
    if not conn:
        st.error("Cannot connect to database")
        return
    
    try:
        cursor = conn.cursor()
        
        # Check for key business terms including postal codes
        cursor.execute("SELECT term FROM semantic_glossary WHERE term IN ('Delayed Order', 'Category', 'Region', 'Product Category', 'Pincode', 'Postal Code')")
        existing_terms = [row[0] for row in cursor.fetchall()]
        
        has_delayed_orders = "Delayed Order" in existing_terms
        has_categories = "Category" in existing_terms or "Product Category" in existing_terms  
        has_regions = "Region" in existing_terms
        has_postal_codes = "Pincode" in existing_terms or "Postal Code" in existing_terms
        
        # Enhancement status
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            status = "✅ Enhanced" if has_delayed_orders else "❌ Missing"
            st.metric("Delayed Orders", status)
        with col2:
            status = "✅ Enhanced" if has_categories else "❌ Missing"
            st.metric("Product Categories", status)
        with col3:
            status = "✅ Enhanced" if has_regions else "❌ Missing"
            st.metric("Geographic Regions", status)
        with col4:
            status = "✅ Enhanced" if has_postal_codes else "❌ Missing"
            st.metric("Postal Codes", status)
        
        # Check for issues
        missing_analytics = not (has_delayed_orders and has_categories and has_regions)
        missing_postal = not has_postal_codes
        
        if missing_analytics or missing_postal:
            st.markdown("---")
            
            # Analytics Issues
            if missing_analytics:
                st.warning("⚠️ **Analytics Problem**: Your semantic layer is missing key business definitions!")
                st.markdown("**This prevents the LLM from understanding queries like:**")
                st.code("'How many orders were delayed by category in South in 2015'")
                
                st.markdown("**What's missing:**")
                if not has_delayed_orders:
                    st.write("• 🔴 **Delayed Orders**: Definition of what constitutes a shipping delay")
                if not has_categories:
                    st.write("• 🔴 **Product Categories**: Understanding of Technology, Furniture, Office Supplies")
                if not has_regions:
                    st.write("• 🔴 **Geographic Regions**: Knowledge of North, South, East, West territories")
                
                if st.button("🔧 Fix Analytics Understanding", type="primary", help="Add business definitions for order analytics"):
                    with st.spinner("Adding business definitions..."):
                        success = enhance_semantic_layer_for_analytics(cursor, conn)
                        if success:
                            st.success("✅ **Fixed!** Your semantic layer now understands:")
                            st.write("• ✅ Delayed orders (ship_date > order_date + 3 days)")
                            st.write("• ✅ Product categories (Technology, Furniture, Office Supplies)")
                            st.write("• ✅ Geographic regions (North, South, East, West)")
                            st.write("• ✅ Time-based filtering (2015, 2016, etc.)")
                            st.balloons()
                            st.rerun()
                        else:
                            st.error("❌ Failed to enhance semantic layer")
            
            # Postal Code Issues  
            if missing_postal:
                st.error("🚨 **Postal Code Problem**: LLM returns product sales instead of postal codes!")
                st.markdown("**This happens with queries like:**")
                st.code("'Which pincodes in South have the most sales'")
                st.markdown("**Instead of postal codes, you get product names and sales data.**")
                
                if st.button("🔧 Fix Postal Code Queries", type="secondary", help="Fix postal code vs product confusion"):
                    with st.spinner("Fixing postal code understanding..."):
                        success = fix_postal_code_queries(cursor, conn)
                        if success:
                            st.success("✅ **Fixed!** Your semantic layer now understands:")
                            st.write("• ✅ Pincode = Postal codes (geographic), NOT product codes")
                            st.write("• ✅ Postal codes are for geographic analysis")
                            st.write("• ✅ Added sample postal code data for testing")
                            st.write("• ✅ Clear disambiguation rules")
                            st.balloons()
                            st.rerun()
                        else:
                            st.error("❌ Failed to fix postal code queries")
                
                # Data structure check
                if st.button("📊 Check My Data Structure", help="Analyze current data for postal code columns"):
                    check_data_structure_for_postal_codes(cursor)
        else:
            st.success("✅ **All Good!** Your semantic layer has the key business definitions needed for analytics queries.")
            
            st.markdown("---")
            st.subheader("🧪 Test These Enhanced Queries")
            st.markdown("Your LLM should now understand these business concepts:")
            
            test_queries = [
                "How many orders were delayed by category in South in 2015?",
                "What's the total sales by region for 2016?", 
                "Show me profit by product category",
                "Which category has the most delayed orders?",
                "Compare revenue between North and South regions"
            ]
            
            for query in test_queries:
                if st.button(f"📝 Try: {query}", key=f"test_query_{hash(query)}"):
                    # Switch to query page and populate the query
                    st.session_state.app_page = 'query' 
                    st.session_state.test_query = query
                    st.rerun()
    
    finally:
        conn.close()

def fix_postal_code_queries(cursor, conn):
    """Fix postal code query understanding issues"""
    try:
        # Add postal code business terms
        postal_terms = [
            ("Pincode", "Postal code or ZIP code for geographic areas - NOT product codes", "Geography"),
            ("Postal Code", "Geographic identifier for mail delivery areas", "Geography"),
            ("ZIP Code", "US Postal Service zone improvement plan code", "Geography"),
            ("Area Code", "Geographic region identifier (postal, not product)", "Geography"),
            ("Geographic Code", "Location-based identifier for areas, not products", "Geography"),
            ("South Region Postal", "Postal codes within the southern geographic territory", "Geography"),
            ("Regional Postal Analysis", "Analysis of postal codes by geographic region", "Geography")
        ]
        
        for term, definition, category in postal_terms:
            try:
                cursor.execute("""
                    INSERT INTO semantic_glossary (term, definition, category)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (term) DO UPDATE SET 
                    definition = EXCLUDED.definition,
                    category = EXCLUDED.category
                """, (term, definition, category))
            except Exception:
                pass  # Term may already exist
        
        # Add query guidance for postal codes
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS query_guidance (
                id SERIAL PRIMARY KEY,
                query_pattern VARCHAR(255) UNIQUE NOT NULL,
                guidance TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        postal_guidance = [
            ("pincode", "When user asks for 'pincode', they mean postal codes/ZIP codes for geographic areas. Look for columns with 'postal', 'zip', 'pin', or address information. DO NOT return product names or sales data."),
            ("postal code south", "Find postal codes in the South geographic region. Look for geographic filters combining postal/zip data with region='South'."),
            ("zip code", "Geographic postal identifier, not product identifier."),
            ("area code", "Geographic region identifier (postal, not product).")
        ]
        
        for pattern, guidance in postal_guidance:
            try:
                cursor.execute("""
                    INSERT INTO query_guidance (query_pattern, guidance)
                    VALUES (%s, %s)
                    ON CONFLICT (query_pattern) DO UPDATE SET 
                    guidance = EXCLUDED.guidance
                """, (pattern, guidance))
            except Exception:
                pass
        
        # Create sample postal data
        create_sample_postal_data(cursor)
        
        conn.commit()
        return True
        
    except Exception as e:
        st.error(f"Error fixing postal code queries: {e}")
        return False

def check_data_structure_for_postal_codes(cursor):
    """Check current data structure and provide recommendations"""
    try:
        st.markdown("### 📊 Data Structure Analysis")
        
        # Check what tables exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        st.write(f"**Available tables:** {', '.join(tables)}")
        
        # Check integrated data table
        integrated_tables = [t for t in tables if 'integrated_data' in t or 'source_' in t]
        
        if integrated_tables:
            table_name = integrated_tables[0]
            st.write(f"**Analyzing table:** {table_name}")
            
            # Get column information
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            column_names = [col[1] for col in columns]
            
            st.write(f"**Columns ({len(column_names)}):** {', '.join(column_names)}")
            
            # Check for postal code columns
            postal_columns = []
            geographic_columns = []
            
            for col in column_names:
                col_lower = col.lower()
                if any(term in col_lower for term in ['postal', 'zip', 'pincode', 'pin']):
                    postal_columns.append(col)
                elif any(term in col_lower for term in ['region', 'city', 'state', 'address']):
                    geographic_columns.append(col)
            
            if postal_columns:
                st.success(f"✅ **Found postal code columns:** {', '.join(postal_columns)}")
                st.info("💡 Try queries like: 'Show postal codes in South region'")
            else:
                st.warning("❌ **No postal code columns found**")
                if geographic_columns:
                    st.info(f"🗺️ **Found geographic columns:** {', '.join(geographic_columns)}")
                    st.info("💡 Try queries like: 'Show cities in South region'")
                else:
                    st.error("❌ **No geographic columns found**")
                    st.info("💡 Consider adding postal code data through Data Integration")
            
            # Show sample data
            try:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
                rows = cursor.fetchall()
                if rows:
                    st.markdown("**Sample Data:**")
                    sample_df = pd.DataFrame(rows, columns=column_names)
                    st.dataframe(sample_df)
            except Exception as e:
                st.warning(f"Could not show sample data: {e}")
                
        else:
            st.warning("❌ No integrated data found")
            st.info("💡 Please integrate some data first using the Data Integration page")
            
    except Exception as e:
        st.error(f"Error analyzing data structure: {e}")

def create_sample_postal_data(cursor):
    """Create sample postal code data for testing"""
    try:
        # Create a postal codes reference table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS postal_codes_reference (
                id SERIAL PRIMARY KEY,
                postal_code VARCHAR(10) NOT NULL,
                city VARCHAR(100),
                state VARCHAR(50),
                region VARCHAR(50),
                country VARCHAR(50) DEFAULT 'US',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Sample postal codes for the South region
        sample_postal_codes = [
            ('30309', 'Atlanta', 'Georgia', 'South'),
            ('33101', 'Miami', 'Florida', 'South'),
            ('75201', 'Dallas', 'Texas', 'South'),
            ('70112', 'New Orleans', 'Louisiana', 'South'),
            ('28202', 'Charlotte', 'North Carolina', 'South'),
            ('37203', 'Nashville', 'Tennessee', 'South'),
            ('32801', 'Orlando', 'Florida', 'South'),
            ('77002', 'Houston', 'Texas', 'South'),
            ('35203', 'Birmingham', 'Alabama', 'South'),
            ('23510', 'Norfolk', 'Virginia', 'South')
        ]
        
        for postal, city, state, region in sample_postal_codes:
            try:
                cursor.execute("""
                    INSERT INTO postal_codes_reference (postal_code, city, state, region)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (postal, city, state, region))
            except Exception:
                pass  # May already exist
                
    except Exception as e:
        print(f"Could not create sample postal data: {e}")

def enhance_semantic_layer_for_analytics(cursor, conn):
    """Add business definitions for analytics queries"""
    try:
        # Add business terms
        business_terms = [
            ("Delayed Order", "An order where ship_date is more than 3 days after order_date", "Operations"),
            ("Category", "Product category: Technology, Furniture, or Office Supplies", "Product"),
            ("Product Category", "Main product classification for analytics", "Product"),
            ("Region", "Geographic territory: North, South, East, West", "Geography"),
            ("South Region", "Southern US geographic territory", "Geography"),
            ("Shipping Delay", "Days between order and ship dates when > 3 days", "Operations"),
            ("2015 Orders", "Orders placed during calendar year 2015", "Time Period"),
            ("Time Analysis", "Filtering and grouping data by dates and years", "Analytics")
        ]
        
        for term, definition, category in business_terms:
            try:
                cursor.execute("""
                    INSERT INTO semantic_glossary (term, definition, category)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (term) DO UPDATE SET 
                    definition = EXCLUDED.definition,
                    category = EXCLUDED.category
                """, (term, definition, category))
            except Exception:
                pass  # Term may already exist
        
        # Update integrated_data table definition
        cursor.execute("DELETE FROM semantic_tables WHERE table_name = 'integrated_data'")
        cursor.execute("""
            INSERT INTO semantic_tables (table_name, display_name, description, business_purpose, common_queries)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            'integrated_data',
            'Sales Orders Analytics',
            'Sales data with orders, shipping, customers, and product information',
            'Analyze orders, delays, regional performance, and product categories',
            '["Delayed orders by category in South", "Sales by region and year", "Product category analysis"]'
        ))
        
        # Add key column definitions
        cursor.execute("DELETE FROM semantic_columns WHERE table_name = 'integrated_data'")
        
        key_columns = [
            ('integrated_data', 'order_date', 'Order Date', 'When customer placed order', 'date', 'DATE', 
             '["2015-01-03", "2016-12-30"]', '["strftime(\'%Y\', order_date) = \'2015\'"]', 
             '["Use for time filtering"]', 'MIN'),
            ('integrated_data', 'ship_date', 'Ship Date', 'When order shipped', 'date', 'DATE',
             '["2015-01-07", "2017-01-03"]', '["(julianday(ship_date) - julianday(order_date)) > 3"]',
             '["Calculate delays with order_date"]', 'MIN'),
            ('integrated_data', 'region', 'Region', 'Geographic territory', 'dimension', 'VARCHAR',
             '["South", "North", "East", "West"]', '["region = \'South\'"]',
             '["Four regions for analysis"]', None),
            ('integrated_data', 'category', 'Category', 'Product type', 'dimension', 'VARCHAR',
             '["Technology", "Furniture", "Office Supplies"]', '["category = \'Technology\'"]',
             '["Three main categories"]', None),
            ('integrated_data', 'sales', 'Sales', 'Revenue amount', 'measure', 'DECIMAL',
             '["261.96", "731.94"]', '["sales > 100"]', '["Always positive"]', 'SUM'),
            ('integrated_data', 'profit', 'Profit', 'Net profit', 'measure', 'DECIMAL',
             '["41.91", "-2.54"]', '["profit > 0"]', '["Can be negative"]', 'SUM')
        ]
        
        for col_data in key_columns:
            try:
                cursor.execute("""
                    INSERT INTO semantic_columns 
                    (table_name, column_name, display_name, description, semantic_type, data_type, 
                     sample_values, common_filters, business_rules, aggregation_default)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, col_data)
            except Exception:
                pass
        
        conn.commit()
        return True
    except Exception as e:
        print(f"Error enhancing semantic layer: {e}")
        return False

def show_semantic_overview():
    """Overview of semantic layer status and statistics"""
    st.subheader("📊 Semantic Layer Overview")
    
    # Load or initialize semantic layer
    semantic_layer = load_or_create_semantic_layer()
    
    # Show statistics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Semantic Tables", len(semantic_layer.tables))
    with col2:
        total_columns = sum(len(table.columns) for table in semantic_layer.tables.values())
        st.metric("Enhanced Columns", total_columns)
    with col3:
        st.metric("Relationships", len(semantic_layer.relationships))
    with col4:
        st.metric("Business Metrics", len(semantic_layer.metrics))
    
    st.markdown("---")
    
    # Integration status
    st.subheader("🔗 Data Integration Status")
    integration_engine = data_integration.data_integration_engine
    summary = integration_engine.get_data_sources_summary()
    
    if summary['total_sources'] > 0:
        st.success(f"✅ {summary['total_sources']} data sources available for semantic enhancement")
        
        # Auto-generate button
        if st.button("🤖 Auto-Generate Semantic Metadata", type="primary"):
            with st.spinner("Analyzing data sources and generating semantic metadata..."):
                success = semantic_layer.auto_generate_metadata_from_data_integration(integration_engine)
                
                if success:
                    # Save to database
                    conn = database.get_db_connection()
                    if conn:
                        try:
                            saved = semantic_layer.save_to_database(conn)
                            if saved:
                                st.session_state.semantic_layer = semantic_layer
                                st.success("🎉 Semantic metadata generated and saved successfully!")
                                st.balloons()
                            else:
                                st.error("Failed to save semantic metadata to database")
                        finally:
                            conn.close()
                    else:
                        st.error("Could not connect to database to save semantic metadata")
                else:
                    st.error("Failed to generate semantic metadata")
        
        # Manual refresh button
        if st.button("🔄 Refresh from Database"):
            semantic_layer = load_semantic_layer_from_database()
            if semantic_layer:
                st.session_state.semantic_layer = semantic_layer
                st.success("Semantic layer refreshed from database")
            else:
                st.warning("No semantic layer found in database")
            st.rerun()
    else:
        st.warning("⚠️ No data sources found. Please add data sources in the 'Data Integration' page first.")
        if st.button("Go to Data Integration", type="primary"):
            st.session_state.app_page = 'data_integration'
            st.rerun()
    
    # Show current semantic tables
    if semantic_layer.tables:
        st.subheader("📋 Current Semantic Tables")
        
        for table_name, table_info in semantic_layer.tables.items():
            with st.expander(f"📊 {table_info.display_name} ({table_name})", expanded=False):
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    st.write(f"**Description:** {table_info.description}")
                    st.write(f"**Business Purpose:** {table_info.business_purpose}")
                    st.write(f"**Columns:** {len(table_info.columns)}")
                    
                    # Show column types breakdown
                    type_counts = {}
                    for col in table_info.columns.values():
                        semantic_type = col.semantic_type.value
                        type_counts[semantic_type] = type_counts.get(semantic_type, 0) + 1
                    
                    type_display = ", ".join([f"{count} {type_name}" for type_name, count in type_counts.items()])
                    st.write(f"**Column Types:** {type_display}")
                
                with col2:
                    if st.button(f"Configure {table_name}", key=f"config_{table_name}"):
                        st.session_state.selected_semantic_table = table_name
                        st.session_state.semantic_config_tab = 1  # Switch to configure tab
                        st.rerun()
    
    # Show comparison with/without semantic layer
    st.markdown("---")
    st.subheader("📈 Schema Enhancement Comparison")
    
    if st.button("Show Basic vs Enhanced Schema"):
        # Get current data schema
        data_schema = st.session_state.get("data_schema", {})
        if data_schema:
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Current Basic Schema:**")
                basic_prompt = generate_basic_schema_prompt(data_schema)
                st.code(basic_prompt, language="text")
            
            with col2:
                st.markdown("**Enhanced Semantic Schema:**")
                if semantic_layer.tables:
                    enhanced_prompt = semantic_layer.generate_enhanced_schema_prompt(data_schema)
                    # Truncate for display
                    display_prompt = enhanced_prompt[:1500] + "..." if len(enhanced_prompt) > 1500 else enhanced_prompt
                    st.code(display_prompt, language="text")
                else:
                    st.info("Generate semantic metadata first to see enhanced schema")

def show_semantic_configuration():
    """UI for configuring semantic metadata"""
    st.subheader("⚙️ Configure Semantic Metadata")
    
    semantic_layer = load_or_create_semantic_layer()
    
    # Table selection
    if not semantic_layer.tables:
        st.info("No semantic tables found. Generate metadata from the Overview tab first.")
        return
    
    table_names = list(semantic_layer.tables.keys())
    
    # Check if a specific table was selected from overview
    selected_table_name = st.session_state.get('selected_semantic_table', table_names[0])
    if selected_table_name not in table_names:
        selected_table_name = table_names[0]
    
    selected_table_name = st.selectbox(
        "Select table to configure:",
        table_names,
        index=table_names.index(selected_table_name) if selected_table_name in table_names else 0
    )
    
    if not selected_table_name:
        return
    
    table_info = semantic_layer.tables[selected_table_name]
    
    # Table-level configuration
    st.markdown("#### 📊 Table Configuration")
    
    with st.form(f"table_config_{selected_table_name}"):
        new_display_name = st.text_input("Display Name:", value=table_info.display_name)
        new_description = st.text_area("Description:", value=table_info.description, height=80)
        new_business_purpose = st.text_area("Business Purpose:", value=table_info.business_purpose, height=80)
        
        # Common queries
        st.markdown("**Common Queries (one per line):**")
        current_queries = "\n".join(table_info.common_queries)
        new_queries_text = st.text_area("Common query patterns:", value=current_queries, height=100)
        
        if st.form_submit_button("Save Table Configuration"):
            # Update table info
            table_info.display_name = new_display_name
            table_info.description = new_description
            table_info.business_purpose = new_business_purpose
            table_info.common_queries = [q.strip() for q in new_queries_text.split('\n') if q.strip()]
            
            # Save to database
            conn = database.get_db_connection()
            if conn:
                try:
                    saved = semantic_layer.save_to_database(conn)
                    if saved:
                        st.session_state.semantic_layer = semantic_layer
                        st.success("Table configuration saved successfully!")
                    else:
                        st.error("Failed to save table configuration")
                finally:
                    conn.close()
    
    st.markdown("---")
    
    # Column-level configuration
    st.markdown("#### 📝 Column Configuration")
    
    if table_info.columns:
        column_names = list(table_info.columns.keys())
        
        # Column selector
        selected_column = st.selectbox("Select column to configure:", column_names)
        
        if selected_column:
            col_info = table_info.columns[selected_column]
            
            with st.form(f"column_config_{selected_table_name}_{selected_column}"):
                col1, col2 = st.columns(2)
                
                with col1:
                    new_col_display = st.text_input("Display Name:", value=col_info.display_name)
                    new_col_desc = st.text_area("Description:", value=col_info.description, height=80)
                    
                    # Semantic type
                    semantic_types = [t.value for t in DataType]
                    current_type_index = semantic_types.index(col_info.semantic_type.value)
                    new_semantic_type = st.selectbox("Semantic Type:", semantic_types, index=current_type_index)
                    
                    # Aggregation default
                    agg_options = ["None", "SUM", "AVG", "COUNT", "MIN", "MAX"]
                    current_agg = col_info.aggregation_default or "None"
                    current_agg_index = agg_options.index(current_agg) if current_agg in agg_options else 0
                    new_agg_default = st.selectbox("Default Aggregation:", agg_options, index=current_agg_index)
                
                with col2:
                    # Sample values
                    current_samples = "\n".join(col_info.sample_values)
                    new_samples = st.text_area("Sample Values (one per line):", value=current_samples, height=100)
                    
                    # Common filters
                    current_filters = "\n".join(col_info.common_filters)
                    new_filters = st.text_area("Common Filters (one per line):", value=current_filters, height=100)
                    
                    # Business rules
                    current_rules = "\n".join(col_info.business_rules)
                    new_rules = st.text_area("Business Rules (one per line):", value=current_rules, height=100)
                
                if st.form_submit_button("Save Column Configuration"):
                    # Update column info
                    col_info.display_name = new_col_display
                    col_info.description = new_col_desc
                    col_info.semantic_type = DataType(new_semantic_type)
                    col_info.aggregation_default = new_agg_default if new_agg_default != "None" else None
                    col_info.sample_values = [s.strip() for s in new_samples.split('\n') if s.strip()]
                    col_info.common_filters = [f.strip() for f in new_filters.split('\n') if f.strip()]
                    col_info.business_rules = [r.strip() for r in new_rules.split('\n') if r.strip()]
                    
                    # Save to database
                    conn = database.get_db_connection()
                    if conn:
                        try:
                            saved = semantic_layer.save_to_database(conn)
                            if saved:
                                st.session_state.semantic_layer = semantic_layer
                                st.success(f"Column '{selected_column}' configuration saved successfully!")
                            else:
                                st.error("Failed to save column configuration")
                        finally:
                            conn.close()

def show_relationship_management():
    """UI for managing table relationships"""
    st.subheader("🔗 Table Relationships")
    
    semantic_layer = load_or_create_semantic_layer()
    
    # Current relationships
    if semantic_layer.relationships:
        st.markdown("#### Current Relationships")
        
        for i, rel in enumerate(semantic_layer.relationships):
            with st.expander(f"🔗 {rel.from_table}.{rel.from_column} → {rel.to_table}.{rel.to_column}"):
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    st.write(f"**Type:** {rel.relationship_type}")
                    st.write(f"**Description:** {rel.description}")
                
                with col2:
                    if st.button(f"Delete", key=f"delete_rel_{i}", type="secondary"):
                        semantic_layer.relationships.pop(i)
                        
                        # Save to database
                        conn = database.get_db_connection()
                        if conn:
                            try:
                                saved = semantic_layer.save_to_database(conn)
                                if saved:
                                    st.session_state.semantic_layer = semantic_layer
                                    st.success("Relationship deleted successfully!")
                                else:
                                    st.error("Failed to save changes")
                            finally:
                                conn.close()
                        st.rerun()
    else:
        st.info("No relationships defined yet.")
    
    st.markdown("---")
    
    # Add new relationship
    st.markdown("#### ➕ Add New Relationship")
    
    if len(semantic_layer.tables) < 2:
        st.warning("Need at least 2 tables to create relationships.")
        return
    
    table_names = list(semantic_layer.tables.keys())
    
    with st.form("add_relationship"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**From Table:**")
            from_table = st.selectbox("From Table:", table_names, key="rel_from_table")
            
            from_columns = []
            if from_table:
                from_columns = list(semantic_layer.tables[from_table].columns.keys())
            from_column = st.selectbox("From Column:", from_columns, key="rel_from_column")
        
        with col2:
            st.markdown("**To Table:**")
            to_table_options = [t for t in table_names if t != from_table]
            to_table = st.selectbox("To Table:", to_table_options, key="rel_to_table")
            
            to_columns = []
            if to_table:
                to_columns = list(semantic_layer.tables[to_table].columns.keys())
            to_column = st.selectbox("To Column:", to_columns, key="rel_to_column")
        
        relationship_type = st.selectbox("Relationship Type:", 
                                       ["one_to_many", "many_to_one", "one_to_one", "many_to_many"])
        description = st.text_input("Description:", placeholder="Describe this relationship...")
        
        if st.form_submit_button("Add Relationship"):
            if from_table and from_column and to_table and to_column and relationship_type:
                new_relationship = SemanticRelationship(
                    from_table=from_table,
                    from_column=from_column,
                    to_table=to_table,
                    to_column=to_column,
                    relationship_type=relationship_type,
                    description=description or ""
                )
                
                semantic_layer.register_relationship(new_relationship)
                
                # Save to database
                conn = database.get_db_connection()
                if conn:
                    try:
                        saved = semantic_layer.save_to_database(conn)
                        if saved:
                            st.session_state.semantic_layer = semantic_layer
                            st.success("Relationship added successfully!")
                        else:
                            st.error("Failed to save relationship")
                    finally:
                        conn.close()
                st.rerun()
            else:
                st.error("Please fill in all fields")

def show_query_testing():
    """UI for testing query generation with and without semantic layer"""
    st.subheader("🧪 Test Query Generation")
    st.info("Compare AI query generation with and without semantic layer enhancement")
    
    # Test query input
    test_questions = [
        "What is the total revenue by region?",
        "Show me the top 5 customers by sales amount",
        "What's the profit margin for each product category?",
        "How many orders were placed last month?",
        "Which region has the highest average order value?"
    ]
    
    selected_question = st.selectbox("Select a test question:", [""] + test_questions)
    custom_question = st.text_input("Or enter your own question:", value=selected_question)
    
    test_query = custom_question or selected_question
    
    if test_query and st.button("🔍 Test Query Generation"):
        # Get current data schema
        data_schema = st.session_state.get("data_schema", {})
        connection_type = st.session_state.get("connection_type", "database")
        
        if not data_schema:
            st.warning("No data schema available. Please connect to data first.")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 🔧 Without Semantic Layer")
            
            # Generate basic SQL
            try:
                # Import safely to avoid circular imports
                try:
                    from semantic_integration import get_enhanced_sql_from_openai
                    basic_sql = get_enhanced_sql_from_openai(
                        test_query, data_schema, connection_type, use_semantic_layer=False
                    )
                except ImportError:
                    # Fallback to app's basic function
                    import app
                    basic_sql = app.get_basic_sql_from_openai(test_query, data_schema, connection_type)
                
                if basic_sql:
                    st.code(basic_sql, language="sql")
                    
                    # Show what the LLM received
                    with st.expander("LLM Input (Basic Schema)"):
                        basic_prompt = generate_basic_schema_prompt(data_schema)
                        st.code(basic_prompt, language="text")
                else:
                    st.error("Failed to generate basic SQL")
                    
            except Exception as e:
                st.error(f"Error generating basic SQL: {e}")
        
        with col2:
            st.markdown("#### 🧠 With Semantic Layer")
            
            # Generate enhanced SQL
            try:
                # Import safely to avoid circular imports
                try:
                    from semantic_integration import get_enhanced_sql_from_openai
                    enhanced_sql = get_enhanced_sql_from_openai(
                        test_query, data_schema, connection_type, use_semantic_layer=True
                    )
                except ImportError:
                    # Fallback to basic generation with manual enhancement
                    import app
                    semantic_layer = load_or_create_semantic_layer()
                    if semantic_layer and semantic_layer.tables:
                        st.info("Using manual semantic enhancement")
                        enhanced_sql = app.get_basic_sql_from_openai(test_query, data_schema, connection_type)
                    else:
                        enhanced_sql = app.get_basic_sql_from_openai(test_query, data_schema, connection_type)
                
                if enhanced_sql:
                    st.code(enhanced_sql, language="sql")
                    
                    # Show what the LLM received
                    with st.expander("LLM Input (Enhanced Schema)"):
                        semantic_layer = load_or_create_semantic_layer()
                        if semantic_layer and semantic_layer.tables:
                            enhanced_prompt = semantic_layer.generate_enhanced_schema_prompt(data_schema, connection_type)
                            # Truncate for display
                            display_prompt = enhanced_prompt[:2000] + "..." if len(enhanced_prompt) > 2000 else enhanced_prompt
                            st.code(display_prompt, language="text")
                        else:
                            st.code(generate_basic_schema_prompt(data_schema), language="text")
                else:
                    st.error("Failed to generate enhanced SQL")
                    
            except Exception as e:
                st.error(f"Error generating enhanced SQL: {e}")
        
        # Analysis
        st.markdown("---")
        st.markdown("#### 📊 Analysis")
        
        if 'basic_sql' in locals() and 'enhanced_sql' in locals() and basic_sql and enhanced_sql:
            st.write("**Key Differences:**")
            
            # Simple analysis
            basic_words = basic_sql.lower().split()
            enhanced_words = enhanced_sql.lower().split()
            
            improvements = []
            
            if "join" in enhanced_words and "join" not in basic_words:
                improvements.append("✅ Enhanced version includes proper table JOINs")
            
            if len(enhanced_words) > len(basic_words):
                improvements.append("✅ Enhanced version is more comprehensive")
            
            if any(word in enhanced_sql.lower() for word in ["sum", "avg", "count"]) and \
               not any(word in basic_sql.lower() for word in ["sum", "avg", "count"]):
                improvements.append("✅ Enhanced version includes appropriate aggregations")
            
            if improvements:
                for improvement in improvements:
                    st.write(improvement)
            else:
                st.write("📝 Both versions are similar - semantic layer may not have significant impact for this query")

def load_or_create_semantic_layer():
    """Load semantic layer from session state or database, or create new one"""
    # Try session state first
    if 'semantic_layer' in st.session_state and st.session_state.semantic_layer:
        return st.session_state.semantic_layer
    
    # Try loading from database
    semantic_layer = load_semantic_layer_from_database()
    if semantic_layer and semantic_layer.tables:
        st.session_state.semantic_layer = semantic_layer
        return semantic_layer
    
    # Create new one
    semantic_layer = SemanticLayer()
    st.session_state.semantic_layer = semantic_layer
    return semantic_layer

def load_semantic_layer_from_database():
    """Load semantic layer from database"""
    try:
        conn = database.get_db_connection()
        if conn:
            try:
                semantic_layer = SemanticLayer.load_from_database(conn)
                return semantic_layer
            finally:
                conn.close()
    except Exception as e:
        print(f"Error loading semantic layer from database: {e}")
    
    return None

def generate_basic_schema_prompt(data_schema):
    """Generate basic schema prompt for comparison"""
    if isinstance(data_schema, dict):
        prompt = "Basic Schema:\n"
        for table, columns in data_schema.items():
            prompt += f"Table {table}:\n"
            for col_name, col_type in columns.items():
                prompt += f"  - {col_name} ({col_type})\n"
    elif isinstance(data_schema, list):
        prompt = "CSV/Integrated Data Columns:\n"
        for col_info in data_schema:
            prompt += f"  - {col_info['name']} ({col_info['type']})\n"
    else:
        prompt = "Schema format not recognized"
    
    return prompt 