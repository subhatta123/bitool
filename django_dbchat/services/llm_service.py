"""
LLM Service for ConvaBI Application
Enhanced support for Llama 3.2b and OpenAI with optimized prompts and error handling
"""

import json
import logging
import requests
from typing import Tuple, Dict, Any, Optional, List
from django.conf import settings
import time
import re

logger = logging.getLogger(__name__)

class LLMService:
    """
    Enhanced LLM service with support for Llama 3.2b and OpenAI
    """
    
    def __init__(self):
        # Initialize default values first
        self.openai_api_key = getattr(settings, 'OPENAI_API_KEY', None)
        self.ollama_url = getattr(settings, 'OLLAMA_URL', 'http://ollama:11434')  # Use Docker hostname
        self.preferred_provider = getattr(settings, 'LLM_PROVIDER', 'local')
        self.openai_model = getattr(settings, 'OPENAI_MODEL', 'gpt-4o')
        self.ollama_model = getattr(settings, 'OLLAMA_MODEL', 'llama3.2:3b')
        
        # Enhanced settings
        self.max_retries = 3
        self.retry_delay = 2
        self.request_timeout = 60
        
        # Load configuration from database, overriding defaults
        self._load_database_config()
    
    def generate_sql(self, prompt: str, schema_info: Dict[str, Any], data_source=None) -> Tuple[bool, str]:
        """
        Generate SQL from natural language prompt with enhanced error handling
        
        Args:
            prompt: Natural language query
            schema_info: Database schema information
            data_source: Data source for enhanced context
            
        Returns:
            Tuple of (success, sql_query_or_error)
        """
        try:
            # Get active configuration
            from core.models import LLMConfig
            config = LLMConfig.get_active_config()
            
            if not config:
                return False, "No active LLM configuration found"
            
            # Create enhanced schema context with data format information
            enhanced_schema_context = self._create_enhanced_schema_context(schema_info, data_source)
            
            # Get sample data context for better understanding
            sample_data_context = self._get_sample_data_context(data_source, limit=3)
            
            # Combine contexts for comprehensive LLM guidance
            if sample_data_context:
                full_context = f"{enhanced_schema_context}\n\n{sample_data_context}"
            else:
                full_context = enhanced_schema_context
            
            # Use the optimized prompt format from the config
            optimized_prompt = config.get_optimized_prompt(prompt, full_context)
            
            logger.info(f"Enhanced context length: {len(full_context)} characters")
            
            # Generate SQL based on provider
            if config.provider == 'openai':
                return self._generate_sql_openai_enhanced(optimized_prompt, config)
            elif config.provider == 'local':
                return self._generate_sql_ollama_enhanced(optimized_prompt, config)
            else:
                return False, f"Unsupported provider: {config.provider}"
                
        except Exception as e:
            logger.error(f"Failed to generate SQL: {e}")
            return False, f"SQL generation failed: {str(e)}"
    
    def _generate_sql_openai_enhanced(self, prompt: str, config) -> Tuple[bool, str]:
        """Enhanced OpenAI SQL generation with retry logic"""
        try:
            import openai
            
            # Configure OpenAI client with safe initialization
            client = self._create_openai_client(config.api_key or self.openai_api_key)
            
            # If using optimized prompt, use it as user message
            if config.is_llama32_model():
                # For consistency, use system/user format
                messages = [
                    {"role": "system", "content": config.system_prompt},
                    {"role": "user", "content": prompt}
                ]
            else:
                # Use the optimized prompt directly
                messages = [{"role": "user", "content": prompt}]
            
            for attempt in range(self.max_retries):
                try:
                    response = client.chat.completions.create(
                        model=config.model_name,
                        messages=messages,
                        max_tokens=config.max_tokens,
                        temperature=config.temperature,
                        timeout=self.request_timeout
                    )
                    
                    sql_query = response.choices[0].message.content
                    if sql_query:
                        sql_query = sql_query.strip()
                        
                        # Enhanced SQL cleaning
                        sql_query = self._clean_sql_response_enhanced(sql_query)
                        
                        # Validate SQL
                        if self._validate_sql_basic(sql_query):
                            logger.info(f"Generated SQL using OpenAI ({config.model_name}): {sql_query[:100]}...")
                            return True, sql_query
                        else:
                            logger.warning(f"Generated invalid SQL on attempt {attempt + 1}")
                            if attempt < self.max_retries - 1:
                                time.sleep(self.retry_delay)
                                continue
                    
                    return False, "OpenAI returned empty or invalid response"
                    
                except Exception as e:
                    logger.error(f"OpenAI API attempt {attempt + 1} failed: {e}")
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay)
                        continue
                    return False, f"OpenAI API error after {self.max_retries} attempts: {str(e)}"
            
            return False, "Failed to generate SQL after all retries"
            
        except ImportError:
            logger.error("OpenAI library not installed")
            return False, "OpenAI library not available. Please install: pip install openai"
        except Exception as e:
            logger.error(f"OpenAI API error: {e}")
            return False, f"OpenAI API error: {str(e)}"
    
    def _generate_sql_ollama_enhanced(self, prompt: str, config) -> Tuple[bool, str]:
        """Enhanced Ollama SQL generation with Llama 3.2b optimizations"""
        try:
            # Get additional settings from config
            additional_settings = config.additional_settings or {}
            stop_sequences = additional_settings.get('stop_sequences', [])
            
            # Prepare request payload
            payload = {
                "model": config.model_name,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": config.temperature,
                    "top_p": additional_settings.get('top_p', 0.9),
                    "num_predict": config.max_tokens,
                    "stop": stop_sequences
                }
            }
            
            # Add Llama 3.2 specific optimizations
            if config.is_llama32_model():
                payload["options"].update({
                    "num_ctx": additional_settings.get('context_window', 8192),
                    "repeat_penalty": 1.1,
                    "tfs_z": 1.0,
                    "mirostat": 0,
                    "mirostat_eta": 0.1,
                    "mirostat_tau": 5.0
                })
            
            for attempt in range(self.max_retries):
                try:
                    response = requests.post(
                        f"{config.base_url or self.ollama_url}/api/generate",
                        json=payload,
                        timeout=self.request_timeout
                    )
                    
                    if response.status_code == 200:
                        result = response.json()
                        sql_query = result.get('response', '').strip()
                        
                        if sql_query:
                            # Enhanced SQL cleaning for Llama 3.2
                            sql_query = self._clean_sql_response_enhanced(sql_query)
                            
                            # Validate SQL
                            if self._validate_sql_basic(sql_query):
                                logger.info(f"Generated SQL using Ollama ({config.model_name}): {sql_query[:100]}...")
                                return True, sql_query
                            else:
                                logger.warning(f"Generated invalid SQL on attempt {attempt + 1}")
                                if attempt < self.max_retries - 1:
                                    time.sleep(self.retry_delay)
                                    continue
                        
                        return False, "Ollama returned empty response"
                    else:
                        logger.error(f"Ollama API error: {response.status_code}")
                        if attempt < self.max_retries - 1:
                            time.sleep(self.retry_delay)
                            continue
                        return False, f"Ollama API error: {response.status_code}"
                        
                except requests.exceptions.RequestException as e:
                    logger.error(f"Ollama request attempt {attempt + 1} failed: {e}")
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay)
                        continue
                    return False, f"Ollama connection error after {self.max_retries} attempts: {str(e)}"
            
            return False, "Failed to generate SQL after all retries"
            
        except Exception as e:
            logger.error(f"Ollama error: {e}")
            return False, f"Ollama error: {str(e)}"
    
    def _clean_sql_response_enhanced(self, sql_query: str) -> str:
        """Enhanced SQL response cleaning with Llama 3.2 specific handling"""
        if not sql_query:
            return sql_query
        
        # Remove common prefixes and suffixes
        sql_query = re.sub(r'^(```sql|```|SELECT\s+)?', '', sql_query, flags=re.IGNORECASE)
        sql_query = re.sub(r'```$', '', sql_query)
        
        # Remove explanation text after SQL
        sql_query = re.sub(r'\n\n.*', '', sql_query, flags=re.DOTALL)
        
        # Handle Llama 3.2 specific formatting
        if '<|eot_id|>' in sql_query:
            sql_query = sql_query.split('<|eot_id|>')[0]
        
        # Remove any trailing explanations
        sql_query = re.sub(r'\n\s*(This query|The query|Note:|Explanation:|--.*)', '', sql_query, flags=re.IGNORECASE)
        
        # Ensure SQL starts with SELECT, INSERT, UPDATE, DELETE, or WITH
        sql_query = sql_query.strip()
        if sql_query and not re.match(r'^(SELECT|INSERT|UPDATE|DELETE|WITH|CREATE|DROP|ALTER)\s+', sql_query, re.IGNORECASE):
            # If it doesn't start with a SQL keyword, assume it's a SELECT
            if not sql_query.upper().startswith('SELECT'):
                sql_query = 'SELECT ' + sql_query
        
        # Ensure semicolon at end
        if sql_query and not sql_query.endswith(';'):
            sql_query += ';'
        
        return sql_query.strip()
    
    def _validate_sql_basic(self, sql_query: str) -> bool:
        """Basic SQL validation"""
        if not sql_query or sql_query.strip() == ';':
            return False
        
        # Check for SQL keywords
        sql_upper = sql_query.upper()
        sql_keywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WITH', 'CREATE', 'DROP', 'ALTER']
        
        if not any(keyword in sql_upper for keyword in sql_keywords):
            return False
        
        # Check for balanced parentheses
        if sql_query.count('(') != sql_query.count(')'):
            return False
        
        # Check for basic structure
        if 'SELECT' in sql_upper and 'FROM' not in sql_upper:
            # Allow SELECT without FROM for expressions
            pass
        
        return True
    
    def test_ollama_connection(self, url: Optional[str] = None, model: str = "llama3.2:3b") -> Tuple[bool, str]:
        """Test Ollama connection with enhanced error reporting"""
        try:
            test_url = url or self.ollama_url
            
            # First, check if Ollama is running
            try:
                health_response = requests.get(f"{test_url}/api/tags", timeout=10)
            except requests.exceptions.ConnectionError:
                return False, "Cannot connect to Ollama server. Please ensure Ollama is running."
            
            if health_response.status_code != 200:
                return False, f"Ollama server not responding properly: {health_response.status_code}"
            
            # Check if the model is available
            try:
                tags_data = health_response.json()
                available_models = [model_info['name'] for model_info in tags_data.get('models', [])]
                
                if model not in available_models:
                    return False, f"Model '{model}' not found. Available models: {', '.join(available_models) if available_models else 'None'}"
            except Exception as e:
                return False, f"Error parsing Ollama models: {e}"
            
            # Test generation with simple prompt
            test_prompt = "Generate a simple SELECT query to get all rows from a table called 'users'."
            
            try:
                test_response = requests.post(
                    f"{test_url}/api/generate",
                    json={
                        "model": model,
                        "prompt": test_prompt,
                        "stream": False,
                        "options": {
                            "temperature": 0.1,
                            "num_predict": 100
                        }
                    },
                    timeout=30
                )
                
                if test_response.status_code == 200:
                    result = test_response.json()
                    response_text = result.get('response', '').strip()
                    
                    if response_text:
                        return True, f"✅ Ollama connection successful. Model '{model}' is working. Sample response: {response_text[:50]}..."
                    else:
                        return False, "Ollama responded but returned empty response"
                else:
                    return False, f"Ollama generation test failed: {test_response.status_code}"
                    
            except requests.exceptions.Timeout:
                return False, "Ollama generation test timed out. Model might be loading."
            except Exception as e:
                return False, f"Ollama generation test error: {e}"
                
        except Exception as e:
            return False, f"Ollama connection test failed: {str(e)}"
    
    def get_ollama_models(self, url: Optional[str] = None) -> Tuple[bool, List[Dict]]:
        """Get available Ollama models with enhanced information"""
        try:
            test_url = url or self.ollama_url
            
            response = requests.get(f"{test_url}/api/tags", timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                models = []
                
                for model_info in data.get('models', []):
                    model_name = model_info.get('name', '')
                    models.append({
                        'name': model_name,
                        'display_name': self._get_model_display_name(model_name),
                        'size': model_info.get('size', 0),
                        'modified_at': model_info.get('modified_at', ''),
                        'is_llama32': model_name.startswith('llama3.2:'),
                        'is_recommended': model_name in ['llama3.2:3b', 'llama3.2:1b']
                    })
                
                # Sort by recommendation first, then by name
                models.sort(key=lambda x: (not x['is_recommended'], x['name']))
                
                return True, models
            else:
                return False, []
                
        except Exception as e:
            logger.error(f"Failed to get Ollama models: {e}")
            return False, []
    
    def _get_model_display_name(self, model_name: str) -> str:
        """Get display name for a model"""
        from core.models import LLMConfig
        
        # Check if it's in our configured models
        if model_name in LLMConfig.OLLAMA_MODEL_CONFIGS:
            return LLMConfig.OLLAMA_MODEL_CONFIGS[model_name]['display_name']
        
        # Generate display name for other models
        if model_name.startswith('llama3.2:'):
            return f"Llama 3.2 {model_name.split(':')[1].upper()}"
        elif model_name.startswith('llama3:'):
            return f"Llama 3 {model_name.split(':')[1].upper()}"
        elif model_name.startswith('codellama:'):
            return f"Code Llama {model_name.split(':')[1].upper()}"
        elif model_name.startswith('sqlcoder:'):
            return f"SQLCoder {model_name.split(':')[1].upper()}"
        else:
            return model_name.replace(':', ' ').title()
    
    def switch_to_openai(self, api_key: str, model: str = 'gpt-4o', user=None) -> Tuple[bool, str]:
        """Switch to OpenAI configuration"""
        try:
            from core.models import LLMConfig
            
            # Test the API key first
            test_success, test_message = self.test_openai_connection(api_key)
            if not test_success:
                return False, f"OpenAI connection test failed: {test_message}"
            
            # Create OpenAI configuration
            config = LLMConfig.create_openai_config(model, api_key, user)
            
            # Update service configuration
            self.update_configuration()
            
            return True, f"Successfully switched to OpenAI ({model})"
            
        except Exception as e:
            logger.error(f"Failed to switch to OpenAI: {e}")
            return False, f"Failed to switch to OpenAI: {str(e)}"
    
    def switch_to_llama32(self, model: str = 'llama3.2:3b', user=None) -> Tuple[bool, str]:
        """Switch to Llama 3.2 configuration"""
        try:
            from core.models import LLMConfig
            
            # Test Ollama connection first
            test_success, test_message = self.test_ollama_connection(model=model)
            if not test_success:
                return False, f"Ollama connection test failed: {test_message}"
            
            # Create Llama 3.2 configuration
            config = LLMConfig.create_llama32_config(model, user)
            
            # Update service configuration
            self.update_configuration()
            
            return True, f"Successfully switched to Llama 3.2 ({model})"
            
        except Exception as e:
            logger.error(f"Failed to switch to Llama 3.2: {e}")
            return False, f"Failed to switch to Llama 3.2: {str(e)}"
    
    def test_openai_connection(self, api_key: str) -> Tuple[bool, str]:
        """Test OpenAI connection"""
        try:
            client = self._create_openai_client(api_key)
            
            # Test with a simple request
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": "Hello"}],
                max_tokens=10
            )
            
            if response.choices:
                return True, "OpenAI connection successful"
            else:
                return False, "OpenAI returned empty response"
                
        except ImportError:
            return False, "OpenAI library not installed"
        except Exception as e:
            return False, f"OpenAI connection failed: {str(e)}"
    
    def is_configured(self) -> bool:
        """Check if LLM service is properly configured"""
        try:
            from core.models import LLMConfig
            config = LLMConfig.get_active_config()
            
            if not config:
                return False
            
            if config.provider == 'openai':
                return bool(config.api_key)
            elif config.provider == 'local':
                return self._check_ollama_available()
            
            return False
            
        except Exception:
            return False
    
    def _check_ollama_available(self) -> bool:
        """Check if Ollama is available"""
        try:
            response = requests.get(f"{self.ollama_url}/api/tags", timeout=5)
            return response.status_code == 200
        except:
            return False
    
    def _create_openai_client(self, api_key: str):
        """Create OpenAI client with safe initialization"""
        try:
            from .openai_compatibility_fix import create_openai_client_with_fallback
            
            success, client, error_msg = create_openai_client_with_fallback(api_key)
            
            if success and client:
                return client
            else:
                error_msg = error_msg or "Unknown OpenAI client creation error"
                logger.error(f"Failed to create OpenAI client: {error_msg}")
                raise Exception(f"OpenAI client creation failed: {error_msg}")
                
        except ImportError:
            # Fallback to original method if compatibility fix is not available
            logger.warning("OpenAI compatibility fix not available, using fallback method")
            try:
                import openai
                return openai.OpenAI(api_key=api_key)
            except Exception as e:
                logger.error(f"Fallback OpenAI client creation failed: {e}")
                raise Exception(f"OpenAI client creation failed: {e}")
        except Exception as e:
            logger.error(f"OpenAI client initialization failed: {e}")
            raise Exception(f"OpenAI client creation failed: {e}")
    
    def _load_database_config(self):
        """Load LLM configuration from database"""
        try:
            from core.models import LLMConfig
            
            active_config = LLMConfig.get_active_config()
            if active_config:
                logger.info(f"Loading LLM config from database: {active_config.provider} - {active_config.model_name}")
                
                if active_config.provider == 'openai':
                    self.openai_api_key = active_config.api_key
                    self.openai_model = active_config.model_name
                    self.preferred_provider = 'openai'
                    
                elif active_config.provider == 'local':
                    # This is Ollama configuration
                    self.ollama_url = active_config.base_url or 'http://ollama:11434'  # Use Docker hostname
                    self.ollama_model = active_config.model_name
                    self.preferred_provider = 'ollama'
                    
                # Set common configuration
                self.temperature = active_config.temperature
                self.max_tokens = active_config.max_tokens
                self.system_prompt = active_config.system_prompt
                
                # Load additional settings
                additional = active_config.additional_settings or {}
                self.request_timeout = additional.get('timeout', 60)
                
            else:
                logger.info("No active LLM configuration found in database, using defaults")
                
        except Exception as e:
            logger.warning(f"Failed to load LLM config from database: {e}")
    
    def update_configuration(self):
        """Update service configuration from database"""
        self._load_database_config()
    
    def get_configuration_status(self) -> Dict[str, Any]:
        """Get current LLM configuration status"""
        try:
            from core.models import LLMConfig
            config = LLMConfig.get_active_config()
            
            return {
                'has_active_config': bool(config),
                'provider': config.provider if config else None,
                'model_name': config.model_name if config else None,
                'is_llama32': config.is_llama32_model() if config else False,
                'openai_configured': bool(config and config.provider == 'openai' and config.api_key),
                'ollama_available': self._check_ollama_available(),
                'preferred_provider': self.preferred_provider,
                'ollama_url': self.ollama_url,
                'database_config_loaded': bool(config)
            }
            
        except Exception as e:
            logger.error(f"Error getting configuration status: {e}")
            return {
                'has_active_config': False,
                'error': str(e)
            } 

    def _create_enhanced_schema_context(self, schema_info: Dict[str, Any], data_source=None) -> str:
        """
        Create enhanced schema context with EXACT column names from database
        """
        try:
            context_parts = []
            actual_columns = []
            
            # Get ACTUAL column names from database if data_source is available
            if data_source:
                try:
                    from services.data_service import DataService
                    data_service = DataService()
                    
                    # Get real schema from database
                    real_schema_info = data_service.get_schema_info(
                        data_source.connection_info, data_source
                    )
                    
                    if real_schema_info:
                        if isinstance(real_schema_info, dict) and 'columns' in real_schema_info:
                            actual_columns = [col.get('name') if isinstance(col, dict) else str(col) 
                                            for col in real_schema_info['columns']]
                        elif isinstance(real_schema_info, list):
                            actual_columns = [str(col) for col in real_schema_info]
                        
                        logger.info(f"Got actual column names from database: {actual_columns[:10]}")
                    
                except Exception as db_error:
                    logger.warning(f"Could not get actual column names from database: {db_error}")
            
            # Fallback to schema_info if no actual columns
            if not actual_columns and schema_info and isinstance(schema_info, dict):
                if 'tables' in schema_info:
                    for table_name, table_info in schema_info['tables'].items():
                        context_parts.append(f"TABLE: {table_name}")
                        
                        if 'columns' in table_info:
                            context_parts.append("COLUMNS:")
                            for col in table_info['columns']:
                                if isinstance(col, dict):
                                    col_name = col.get('name', 'Unknown')
                                    col_type = col.get('type', 'string')
                                    context_parts.append(f"  - \"{col_name}\" ({col_type})")
                                    actual_columns.append(col_name)
                        context_parts.append("")
                        
                elif 'columns' in schema_info:
                    context_parts.append("COLUMNS:")
                    for col in schema_info['columns']:
                        if isinstance(col, dict):
                            col_name = col.get('name', 'Unknown')
                            col_type = col.get('type', 'string')
                            context_parts.append(f"  - \"{col_name}\" ({col_type})")
                            actual_columns.append(col_name)
                        elif isinstance(col, str):
                            context_parts.append(f"  - \"{col}\" (string)")
                            actual_columns.append(col)
                    context_parts.append("")
            
            # If we have actual columns, create a comprehensive column reference section
            if actual_columns:
                context_parts.extend([
                    "EXACT COLUMN NAMES (use these exactly as shown):",
                    ""
                ])
                
                for col in actual_columns:
                    # Show both quoted and unquoted versions for clarity
                    if ' ' in col:
                        context_parts.append(f"  - \"{col}\" (use with quotes due to spaces)")
                    else:
                        context_parts.append(f"  - {col} (can use with or without quotes)")
                
                context_parts.append("")
                
                # Add specific guidance for common column naming patterns
                date_columns = [col for col in actual_columns if 'date' in col.lower()]
                if date_columns:
                    context_parts.extend([
                        "DATE COLUMNS IDENTIFIED:",
                        ""
                    ])
                    for date_col in date_columns:
                        context_parts.append(f"  - \"{date_col}\" (DD-MM-YYYY format)")
                    context_parts.append("")
            
            # Add critical data format information with EXACT column references
            context_parts.extend([
                "CRITICAL SQL GENERATION RULES:",
                "",
                "1. COLUMN NAMES - USE EXACTLY AS LISTED ABOVE:",
                "   - If column has spaces: MUST use double quotes: \"Order Date\"",
                "   - NEVER convert spaces to underscores unless that's the actual name",
                "   - NEVER use Order_Date if the actual column is \"Order Date\"",
                "",
                "2. DATE COLUMN HANDLING:",
                "   - Date columns store DD-MM-YYYY format strings (e.g., '26-04-2015')",
                "   - To filter by YEAR: use substr(\"Order Date\", 7, 4) = '2015'",
                "   - To filter by MONTH: use substr(\"Order Date\", 4, 2) = '04'",
                "   - To filter by DAY: use substr(\"Order Date\", 1, 2) = '26'",
                "   - NEVER use LIKE operator for year filtering",
                "",
                "3. SQL SYNTAX REQUIREMENTS:",
                "   - Use exact column names from EXACT COLUMN NAMES section above",
                "   - Use double quotes for column names with spaces",
                "   - Use single quotes for string literals",
                "   - Always end queries with semicolon",
                "",
                "4. AGGREGATION AND GROUPING:",
                "   - Use SUM() for sales, revenue, profit calculations",
                "   - Use COUNT() for counting records",
                "   - Always use GROUP BY with aggregations",
                "   - Use ORDER BY ... DESC LIMIT N for top N queries",
                "",
            ])
            
            # Add practical examples with actual column names if available
            if actual_columns:
                sample_customer_col = next((col for col in actual_columns if 'customer' in col.lower()), 'Customer_Name')
                sample_sales_col = next((col for col in actual_columns if 'sales' in col.lower()), 'Sales')
                sample_region_col = next((col for col in actual_columns if 'region' in col.lower()), 'Region')
                sample_date_col = next((col for col in actual_columns if 'date' in col.lower()), 'Order Date')
                
                context_parts.extend([
                    "SAMPLE QUERIES WITH ACTUAL COLUMN NAMES:",
                    f"• Top customers: SELECT \"{sample_customer_col}\", SUM(\"{sample_sales_col}\") FROM table GROUP BY \"{sample_customer_col}\" ORDER BY SUM(\"{sample_sales_col}\") DESC LIMIT 3;",
                    f"• Sales by year: SELECT substr(\"{sample_date_col}\", 7, 4) as Year, SUM(\"{sample_sales_col}\") FROM table GROUP BY substr(\"{sample_date_col}\", 7, 4);",
                    f"• Regional analysis: SELECT \"{sample_region_col}\", SUM(\"{sample_sales_col}\") FROM table WHERE \"{sample_region_col}\" = 'South' GROUP BY \"{sample_region_col}\";",
                    f"• Year filtering: SELECT * FROM table WHERE substr(\"{sample_date_col}\", 7, 4) = '2015';",
                    ""
                ])
            
            # Add semantic layer information if available
            if data_source:
                try:
                    from datasets.models import SemanticTable, SemanticColumn
                    semantic_tables = SemanticTable.objects.filter(data_source=data_source)
                    if semantic_tables.exists():
                        context_parts.append("BUSINESS CONTEXT:")
                        for table in semantic_tables:
                            context_parts.append(f"  Business Table: {table.table_name}")
                            
                            # Get semantic columns
                            semantic_columns = SemanticColumn.objects.filter(semantic_table=table)
                            if semantic_columns.exists():
                                context_parts.append("  Business Columns:")
                                for col in semantic_columns:
                                    col_type = col.semantic_type or 'dimension'
                                    business_name = col.business_name or col.column_name
                                    context_parts.append(f"    - {business_name} ({col_type})")
                            context_parts.append("")
                except Exception as semantic_error:
                    logger.debug(f"Could not load semantic information: {semantic_error}")
            
            result = "\n".join(context_parts)
            logger.info(f"Enhanced schema context created with {len(actual_columns)} actual columns")
            return result
            
        except Exception as e:
            logger.error(f"Error creating enhanced schema context: {e}")
            return "Error generating schema context"

    def _get_sample_data_context(self, data_source, limit=3) -> str:
        """
        Get sample data to help LLM understand actual data patterns
        """
        try:
            if not data_source:
                return ""
                
            from services.data_service import DataService
            data_service = DataService()
            
            # Get sample data
            success, sample_df, error_msg = data_service.get_sample_data(data_source, limit=limit)
            
            if success and sample_df is not None and not sample_df.empty:
                context_parts = [
                    f"SAMPLE DATA ({limit} rows):",
                    ""
                ]
                
                # Add column headers
                headers = " | ".join([f'"{col}"' for col in sample_df.columns])
                context_parts.append(headers)
                context_parts.append("-" * min(len(headers), 80))
                
                # Add sample rows
                for _, row in sample_df.head(limit).iterrows():
                    row_data = " | ".join([str(val) if val is not None else 'NULL' for val in row.values])
                    if len(row_data) > 80:
                        row_data = row_data[:77] + "..."
                    context_parts.append(row_data)
                
                context_parts.extend([
                    "",
                    "DATA PATTERNS OBSERVED:",
                ])
                
                # Analyze patterns  
                import re
                for col in sample_df.columns:
                    sample_values = sample_df[col].dropna().astype(str).head(3).tolist()
                    if sample_values:
                        if any(re.match(r'\d{2}-\d{2}-\d{4}', val) for val in sample_values):
                            context_parts.append(f"• {col}: Date format DD-MM-YYYY detected")
                        elif all(val.replace('.', '').replace('-', '').isdigit() for val in sample_values if val != 'NULL'):
                            context_parts.append(f"• {col}: Numeric values (use SUM/AVG for aggregation)")
                        else:
                            context_parts.append(f"• {col}: Text values (use for grouping/filtering)")
                
                context_parts.append("")
                return "\n".join(context_parts)
            else:
                return ""
                
        except Exception as e:
            logger.debug(f"Could not get sample data context: {e}")
            return "" 