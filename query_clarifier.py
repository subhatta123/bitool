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
                schema_prompt_part += f"  - {col_name} ({col_type})\n"
    elif isinstance(data_schema, list): # For CSV
        schema_prompt_part += f"CSV Columns (query this as a table named 'csv_data'):\n"
        for col_info in data_schema:
            schema_prompt_part += f"  - {col_info['name']} ({col_info['type']})\n"
    else:
        schema_prompt_part = "Schema: Not available or in an unrecognized format."

    focus_hint = ""
    if target_table and target_table != "All Tables / Auto-detect":
        focus_hint = f"Consider focusing on the table named '{target_table}' if relevant."

    # System Prompt
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

Do not include any other explanatory text, preambles, or apologies outside of these markers.
    """

    messages = [{"role": "system", "content": system_prompt_content}]
    
    if conversation_history:
        messages.extend(conversation_history)
    else:
        # If no history, this is the first turn with the initial query
        messages.append({"role": "user", "content": natural_language_query})
    
    # Log the full prompt being sent for this turn (last user message or system + history + last user message)
    # This is helpful for debugging the interaction.
    # For simplicity, we'll log just the final user message content here, but in a real scenario,
    # you might want to log the whole `messages` list structure.
    if messages[-1]["role"] == "user":
        st.session_state.log_openai_prompt_str = f"System: {system_prompt_content}\nUser (latest): {messages[-1]['content']}"
    else: # Should not happen if conversation_history is structured correctly or it's the first turn
        st.session_state.log_openai_prompt_str = f"System: {system_prompt_content}\n(No immediate user message in this turn, check history)"


    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo", # Or your preferred model
            messages=messages,
            temperature=0.1, # Low temperature for more factual/direct responses
            max_tokens=400 # Adjust as needed
        )
        llm_response_content = response.choices[0].message.content.strip()
        
        # Log the raw response
        # In app.py, log_generated_sql_str or a new log var can store this.
        # For now, let query_screen handle what it logs based on parsed type.

        if llm_response_content.startswith(SQL_MARKER):
            sql_query = llm_response_content[len(SQL_MARKER):].strip()
            sql_query = sql_query.replace("```sql", "").replace("```", "").strip() # Clean up potential markdown
            return {"type": "sql", "content": sql_query}
        elif llm_response_content.startswith(CLARIFICATION_MARKER):
            clarifying_question = llm_response_content[len(CLARIFICATION_MARKER):].strip()
            return {"type": "clarification", "content": clarifying_question}
        elif llm_response_content.startswith(CANNOT_ANSWER_MARKER):
            reason = llm_response_content[len(CANNOT_ANSWER_MARKER):].strip()
            return {"type": "cannot_answer", "content": reason}
        else:
            # If no marker, it might be a direct SQL attempt or unexpected format.
            # Try to treat it as SQL as a fallback, but log a warning.
            # print(f"[QueryClarifier] LLM response did not use expected markers: {llm_response_content}") # For server-side logs
            # Fallback: assume it's SQL if no other markers are present.
            # This might be risky if the LLM just chats. A stricter approach might be to return an error here.
            # For now, let's try to be somewhat lenient and assume it might be SQL.
            if "SELECT " in llm_response_content.upper() or "UPDATE " in llm_response_content.upper() or "DELETE " in llm_response_content.upper() or "INSERT " in llm_response_content.upper():
                 cleaned_fallback_sql = llm_response_content.replace("```sql", "").replace("```", "").strip()
                 return {"type": "sql", "content": cleaned_fallback_sql, "warning": "LLM did not use the expected SQL_QUERY marker."}
            return {"type": "error", "content": f"LLM response was in an unexpected format: {llm_response_content}"}

    except openai.APIConnectionError as e:
        return {"type": "error", "content": f"OpenAI API Connection Error: {e}"}
    except openai.RateLimitError as e:
        return {"type": "error", "content": f"OpenAI API Rate Limit Exceeded: {e}"}
    except openai.AuthenticationError as e:
        return {"type": "error", "content": f"OpenAI API Authentication Error: {e}"}
    except openai.APIError as e:
        return {"type": "error", "content": f"OpenAI API Error: {e}"}
    except Exception as e:
        return {"type": "error", "content": f"An unexpected error occurred: {e}"}

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