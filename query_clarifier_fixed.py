import streamlit as st
import openai # Assuming this is the OpenAI library being used

# Markers to distinguish LLM response types
SQL_MARKER = "SQL_QUERY:"
CLARIFICATION_MARKER = "CLARIFICATION_QUESTION:"
CANNOT_ANSWER_MARKER = "CANNOT_ANSWER:"

def get_sql_or_clarification(natural_language_query, data_schema, db_type="sqlserver", target_table=None, conversation_history=None):
    """
    Contacts the LLM to get either an SQL query or a clarifying question.

    Args:
        natural_language_query (str): The user's initial query.
        data_schema (dict or list): The schema of the data source.
        db_type (str): The type of database (e.g., "sqlserver", "postgresql", "csv").
        target_table (str, optional): A hint for a specific table to focus on.
        conversation_history (list, optional): A list of previous turns in the conversation for context.
                                             Each item is a dict: {"role": "user/assistant", "content": "..."}

    Returns:
        dict: A dictionary with "type" (sql, clarification, cannot_answer, error) and "content".
    """
    client = st.session_state.get("llm_client_instance")
    if not client:
        return {"type": "error", "content": "LLM client is not initialized. Please configure it in LLM Settings."}

    # Get the configured model name from session state
    model_name_to_use = st.session_state.get("llm_model_name")
    if not model_name_to_use: 
        # Fallback if model name isn't set. For local setup, sqlcoder is a good default.
        # For a more general solution, you might want this to align with app.py's default or raise an error.
        model_name_to_use = "sqlcoder:latest" 
        st.warning(f"LLM model name not found in session state (query_clarifier). Defaulting to {model_name_to_use}")

    # Determine the SQL dialect hint
    sql_dialect = "SQL"
    if db_type == "sqlserver":
        sql_dialect = "Transact-SQL (T-SQL) for SQL Server"
    elif db_type == "postgresql":
        sql_dialect = "PostgreSQL SQL"
    elif db_type == "oracle":
        sql_dialect = "Oracle SQL (PL/SQL)"
    elif db_type == "csv":
        sql_dialect = "SQLite-compatible SQL (querying a table named 'csv_data')"

    # Basic schema representation for the prompt
    schema_prompt_part = "Schema:\n"
    if isinstance(data_schema, dict): # For databases
        for table, columns in data_schema.items():
            schema_prompt_part += f"Table {table}:\n"
            for col_name, col_type in columns.items():
                # Add quotes around column names with spaces for clarity
                if ' ' in col_name:
                    schema_prompt_part += f"  - \"{col_name}\" ({col_type})\n"
                else:
                    schema_prompt_part += f"  - {col_name} ({col_type})\n"
    elif isinstance(data_schema, list): # For CSV
        schema_prompt_part += f"CSV Columns (query this as a table named 'csv_data'):\n"
        for col_info in data_schema:
            col_name = col_info['name']
            col_type = col_info['type']
            # Add quotes around column names with spaces for clarity
            if ' ' in col_name:
                schema_prompt_part += f"  - \"{col_name}\" ({col_type}) -- NOTE: Use quotes because of space\n"
            else:
                schema_prompt_part += f"  - {col_name} ({col_type})\n"
    else:
        schema_prompt_part = "Schema: Not available or in an unrecognized format."

    focus_hint = ""
    if target_table and target_table != "All Tables / Auto-detect":
        focus_hint = f"Consider focusing on the table named '{target_table}' if relevant."

    # System Prompt - Simplified for sqlcoder compatibility
    if "sqlcoder" in model_name_to_use.lower():
        # Use the original sqlcoder training format
        prompt_text = f"""### Instruction:
Your task is to convert a question into a SQL query, given a database schema.
Adhere to these rules:
- **Deliberately go through the question and database schema word by word** to appropriately answer the question
- **Use Table Aliases** to prevent ambiguity. For example, `SELECT table1.col1, table2.col1 FROM table1 JOIN table2 ON table1.id = table2.id`.
- When creating a ratio, always cast the numerator as float
- ALWAYS respond with valid SQL only, do not explain or list the schema
- **IMPORTANT**: Column names with spaces MUST be enclosed in double quotes (e.g., "customer ID", "Order Date")
- Use EXACT column names as shown in the schema - do not convert spaces to underscores

### Input:
{schema_prompt_part}

-- Using valid SQL, answer the following questions for the tables provided above.

-- {natural_language_query}

### Response:
SELECT"""
        
        # For sqlcoder, use just a single user message with everything included
        messages = [{"role": "user", "content": prompt_text}]
        
        # Log the sqlcoder format
        st.session_state.log_openai_prompt_str = f"SQLCoder Format:\n{prompt_text}"
    else:
        # Original complex prompt for other models
        system_prompt_content = f"""
You are an expert AI assistant. Your goal is to convert a user's natural language question about their data into a precise {sql_dialect} query.
First, carefully analyze the user's question and the provided schema: {schema_prompt_part}
{focus_hint}

If the user's question is clear and directly translatable to {sql_dialect} based on the schema, respond ONLY with the SQL query, prefixed by "{SQL_MARKER}".
Example: {SQL_MARKER} SELECT column FROM table WHERE condition;

If the user's question is ambiguous, lacks necessary specifics, or requires information not obviously present in the schema, DO NOT attempt to guess or generate a flawed SQL query.
Instead, ask a single, concise clarifying question to the user to help them refine their request. Prefix your question with "{CLARIFICATION_MARKER}".
Example: {CLARIFICATION_MARKER} Which specific date range are you interested in?

If, after receiving clarification, you still determine that the query cannot be answered with the given schema (e.g., requested data is definitively not available), respond with "{CANNOT_ANSWER_MARKER}" followed by a brief explanation.
Example: {CANNOT_ANSWER_MARKER} The schema does not contain information about customer sentiment.

IMPORTANT: When generating SQL, if a column name contains spaces or special characters, you MUST enclose it in double quotes (e.g., "Customer ID", "Order Value"). This is crucial for compatibility, especially with SQLite when querying CSVs (which are treated as tables named 'csv_data').

Do not include any other explanatory text, preambles, or apologies outside of these markers.
    """

        messages = [{"role": "system", "content": system_prompt_content}]
        
        if conversation_history:
            messages.extend(conversation_history)
        else:
            # If no history, this is the first turn with the initial query
            messages.append({"role": "user", "content": natural_language_query})
        
        # Log the other model format
        if messages[-1]["role"] == "user":
            st.session_state.log_openai_prompt_str = f"System: {system_prompt_content}\nUser (latest): {messages[-1]['content']}"
        else:
            st.session_state.log_openai_prompt_str = f"System: {system_prompt_content}\n(No immediate user message in this turn, check history)"

    print(f"[QueryClarifier DEBUG] About to call LLM with model: {model_name_to_use}")
    print(f"[QueryClarifier DEBUG] Client type: {type(client)}")
    if hasattr(client, 'base_url'):
        print(f"[QueryClarifier DEBUG] Client base_url: {client.base_url}")
    
    # Debug prompt length for sqlcoder
    if "sqlcoder" in model_name_to_use.lower():
        print(f"[QueryClarifier DEBUG] SQLCoder prompt length: {len(prompt_text)} characters")
        print(f"[QueryClarifier DEBUG] Schema entries: {len(data_schema) if isinstance(data_schema, list) else len(data_schema.keys()) if isinstance(data_schema, dict) else 'unknown'}")
    else:
        print(f"[QueryClarifier DEBUG] Other model prompt length: {len(system_prompt_content)} characters")

    try:
        # Make the LLM call
        response = client.chat.completions.create(
            model=model_name_to_use,
            messages=messages,
            temperature=0.1,  # Low temperature for deterministic SQL
            max_tokens=600  # Increased to handle longer responses
        )
        
        response_content = response.choices[0].message.content.strip()
        print(f"[QueryClarifier DEBUG] Successfully got response, length: {len(response_content)}")
        print(f"[QueryClarifier DEBUG] Model: {model_name_to_use}")
        print(f"[QueryClarifier DEBUG] Raw Response: {repr(response_content)}")
        
        # Handle unusual/non-standard responses (like strings of zeros)
        if not response_content or len(response_content) > 1000:
            print(f"[QueryClarifier DEBUG] Unusual response length: {len(response_content)}")
            
            # For sqlcoder, try a different format if we get empty response
            if "sqlcoder" in model_name_to_use.lower() and not response_content:
                print(f"[QueryClarifier DEBUG] Trying alternative format for sqlcoder...")
                
                # Try the most basic sqlcoder format possible
                basic_prompt = f"""### Instruction:
Convert the question to SQL.

### Input:
{schema_prompt_part}

Question: {natural_language_query}

### Response:"""
                try:
                    fallback_response = client.chat.completions.create(
                        model=model_name_to_use,
                        messages=[{"role": "user", "content": basic_prompt}],
                        temperature=0.0,
                        max_tokens=600  # Increased to handle longer schemas
                    )
                    fallback_content = fallback_response.choices[0].message.content.strip()
                    print(f"[QueryClarifier DEBUG] Fallback response: {repr(fallback_content)}")
                    
                    if fallback_content:
                        # If we got a response with fallback, use it
                        response_content = fallback_content
                        print(f"[QueryClarifier DEBUG] Using fallback response")
                    else:
                        return {"type": "error", "content": f"SQLCoder model '{model_name_to_use}' returned empty responses with multiple prompt formats. Please check if the model is properly loaded in Ollama."}
                except Exception as fallback_error:
                    print(f"[QueryClarifier DEBUG] Fallback also failed: {fallback_error}")
                    return {"type": "error", "content": f"SQLCoder model returned empty response and fallback failed: {fallback_error}"}
            else:
                return {"type": "error", "content": "LLM returned an unusually long or empty response"}
        
        # Check for repeated characters (like '000000000')
        if len(set(response_content)) <= 2 and len(response_content) > 10:
            print(f"[QueryClarifier DEBUG] Response appears to be repeated characters")
            return {"type": "error", "content": f"LLM returned an unusual pattern response. Try rephrasing your question or check if the '{model_name_to_use}' model is properly configured."}
        
        # For sqlcoder models, assume any reasonable response is SQL
        if "sqlcoder" in model_name_to_use.lower():
            # Clean the response - remove common prefixes
            cleaned_response = response_content
            for prefix in ["sql query:", "query:", "select", "SELECT"]:
                if cleaned_response.lower().startswith(prefix.lower()):
                    if prefix.lower() in ["select", "select"]:
                        # Don't remove SELECT, just clean
                        break
                    cleaned_response = cleaned_response[len(prefix):].strip()
            
            # Remove markdown formatting if present
            cleaned_response = cleaned_response.replace('```sql', '').replace('```', '').strip()
            
            # Check if sqlcoder returned schema information instead of SQL
            if "CSV Columns" in cleaned_response or "Table" in cleaned_response and ":" in cleaned_response:
                print(f"[QueryClarifier DEBUG] SQLCoder returned schema info instead of SQL")
                return {"type": "error", "content": f"SQLCoder model appears to be confused. It returned schema information instead of SQL. Please try rephrasing your question more specifically."}
            
            # Fix incomplete SQL from SQLCoder (often missing SELECT)
            if cleaned_response and not cleaned_response.upper().startswith('SELECT'):
                # Check if it looks like the rest of a SELECT statement
                if any(keyword in cleaned_response.upper() for keyword in ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'FROM', '*']):
                    cleaned_response = "SELECT " + cleaned_response
                    print(f"[QueryClarifier DEBUG] Added missing SELECT to SQLCoder response")
            
            # If it looks like SQL, return it
            if any(word in cleaned_response.upper() for word in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER']):
                return {"type": "sql_query", "query": cleaned_response}
            else:
                # If it doesn't look like SQL, treat as clarification
                return {"type": "clarification_needed", "question": cleaned_response}
        
        # For other models, use the original marker-based parsing
        if SQL_MARKER in response_content:
            sql_query = response_content.split(SQL_MARKER, 1)[1].strip()
            return {"type": "sql_query", "query": sql_query}
        
        elif CLARIFICATION_MARKER in response_content:
            clarification_question = response_content.split(CLARIFICATION_MARKER, 1)[1].strip()
            return {"type": "clarification_needed", "question": clarification_question}
        
        elif CANNOT_ANSWER_MARKER in response_content:
            reason = response_content.split(CANNOT_ANSWER_MARKER, 1)[1].strip()
            return {"type": "cannot_answer", "content": reason}
        
        else:
            # Response didn't use expected markers - this is the case we're hitting
            print(f"[QueryClarifier DEBUG] Response did not use expected markers: {response_content}")
            
            # Try to salvage: if it looks like SQL, treat it as such
            if any(keyword in response_content.upper() for keyword in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'FROM', 'WHERE']):
                return {
                    "type": "sql_query", 
                    "query": response_content,
                    "warning": f"LLM didn't use expected format markers, but response appears to be SQL"
                }
            else:
                return {
                    "type": "error", 
                    "content": f"LLM response in unexpected format. Raw response: {response_content[:200]}..."
                }

    except Exception as e:
        print(f"[QueryClarifier DEBUG] Exception during LLM call: {e}")
        return {"type": "error", "content": f"Error calling LLM: {str(e)}"}

# Fallback function for direct requests if OpenAI client fails
def make_direct_request(base_url, model_name, messages, api_key=None):
    """Make a direct HTTP request to the LLM API as a fallback"""
    import requests
    import json
    
    try:
        # Ensure URL ends properly
        if not base_url.endswith('/'):
            base_url += '/'
        if not base_url.endswith('chat/completions'):
            base_url += 'chat/completions'
        
        headers = {'Content-Type': 'application/json'}
        if api_key and api_key != "NA":
            headers['Authorization'] = f'Bearer {api_key}'
        
        payload = {
            "model": model_name,
            "messages": messages,
            "temperature": 0.1,
            "max_tokens": 400
        }
        
        print(f"[QueryClarifier DEBUG] Fallback: Making direct request to {base_url}")
        response = requests.post(base_url, headers=headers, json=payload, timeout=60)
        response.raise_for_status()
        
        result = response.json()
        if 'choices' in result and len(result['choices']) > 0:
            return result['choices'][0]['message']['content'].strip()
        else:
            return None
            
    except Exception as e:
        print(f"[QueryClarifier DEBUG] Fallback request also failed: {e}")
        return None

if __name__ == '__main__':
    # Example Usage (for testing query_clarifier.py directly if needed)
    # This requires setting up a mock st.session_state.llm_client_instance
    # or running within a Streamlit context where it's populated.

    # Mock schema for testing
    mock_schema_db = {
        "employees": {"id": "INT", "name": "VARCHAR", "department_id": "INT", "salary": "DECIMAL"},
        "departments": {"id": "INT", "name": "VARCHAR"}
    }
    mock_schema_csv = [{"name": "product_name", "type": "TEXT"}, {"name": "sales_amount", "type": "REAL"}, {"name": "sale_date", "type":"TEXT"}]

    # --- Mock Streamlit and OpenAI client for standalone testing ---
    class MockLLMChoice:
        def __init__(self, content):
            self.message = MockLLMMessage(content)

    class MockLLMMessage:
        def __init__(self, content):
            self.content = content

    class MockLLMResponse:
        def __init__(self, content):
            self.choices = [MockLLMChoice(content)]

    class MockOpenAIClient:
        def __init__(self):
            self.chat = self._Chat()
            self.clarification_mode = False # Test variable

        class _Chat:
            def __init__(self):
                self.completions = self._Completions()
            
            class _Completions:
                def create(self, model, messages, temperature, max_tokens):
                    user_query_for_test = ""
                    if messages and messages[-1]["role"] == "user":
                        user_query_for_test = messages[-1]["content"]
                    
                    print(f"--- Mock LLM called with last user message: {user_query_for_test} ---")
                    # Simulate different LLM responses based on query
                    if "details about sales" in user_query_for_test.lower() and "specific product" not in user_query_for_test.lower():
                        # Set by test harness if it wants to simulate clarification path
                        if hasattr(st.session_state.llm_client_instance, 'clarification_mode') and st.session_state.llm_client_instance.clarification_mode:
                             return MockLLMResponse(f"{CLARIFICATION_MARKER} Which specific product are you interested in for sales details?")
                        else: # If not in clarification_mode, assume it became clear after user response
                             return MockLLMResponse(f"{SQL_MARKER} SELECT * FROM csv_data WHERE product_name = 'Test Product';")

                    elif "highest salary" in user_query_for_test.lower():
                        return MockLLMResponse(f"{SQL_MARKER} SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 1;")
                    elif "non_existent_info" in user_query_for_test.lower():
                        return MockLLMResponse(f"{CANNOT_ANSWER_MARKER} The schema does not contain 'non_existent_info'.")
                    else: # Default to asking a question if not specific
                        return MockLLMResponse(f"{CLARIFICATION_MARKER} Could you please specify which table or columns you are interested in?")

    # Mock st.session_state for testing
    if "llm_client_instance" not in st.session_state:
        st.session_state.llm_client_instance = MockOpenAIClient()
    if "log_openai_prompt_str" not in st.session_state:
        st.session_state.log_openai_prompt_str = ""


    print("\n--- Test 1: Ambiguous query, expecting clarification ---")
    st.session_state.llm_client_instance.clarification_mode = True # Ensure LLM asks a question first
    response1 = get_sql_or_clarification("details about sales", mock_schema_csv, "csv")
    print(f"Response 1: {response1}")
    assert response1['type'] == 'clarification'

    print("\n--- Test 2: Specific query, expecting SQL ---")
    response2 = get_sql_or_clarification("highest salary", mock_schema_db, "postgresql")
    print(f"Response 2: {response2}")
    assert response2['type'] == 'sql' and "SELECT name, salary" in response2['content']
    
    print("\n--- Test 3: Query that cannot be answered ---")
    response3 = get_sql_or_clarification("non_existent_info about employees", mock_schema_db, "postgresql")
    print(f"Response 3: {response3}")
    assert response3['type'] == 'cannot_answer'

    print("\n--- Test 4: Multi-turn conversation for clarification ---")
    # Initial ambiguous query
    st.session_state.llm_client_instance.clarification_mode = True 
    initial_query = "details about sales"
    print(f"User asks: {initial_query}")
    clarif_response = get_sql_or_clarification(initial_query, mock_schema_csv, "csv")
    print(f"LLM responds: {clarif_response}")
    assert clarif_response['type'] == 'clarification'

    # User provides an answer
    user_answer_to_clarification = "Test Product"
    print(f"User answers: {user_answer_to_clarification}")
    
    conversation = [
        {"role": "user", "content": initial_query},
        {"role": "assistant", "content": clarif_response['content']}, # LLM's question
        {"role": "user", "content": user_answer_to_clarification}    # User's answer
    ]
    st.session_state.llm_client_instance.clarification_mode = False # Now LLM should give SQL
    final_response = get_sql_or_clarification(
        initial_query, # The original query might still be useful, or just rely on history
        mock_schema_csv, 
        "csv", 
        conversation_history=conversation
    )
    print(f"LLM responds after clarification: {final_response}")
    assert final_response['type'] == 'sql' and "Test Product" in final_response['content']
    
    print("\nAll direct tests passed for query_clarifier.py!") 