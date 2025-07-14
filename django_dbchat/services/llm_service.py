"""
LLM Service for ConvaBI Application
Handles OpenAI and Ollama integrations for SQL generation
"""

import json
import logging
import requests
from typing import Tuple, Dict, Any, Optional, List
from django.conf import settings

logger = logging.getLogger(__name__)

class LLMService:
    """
    Service for LLM integrations and SQL generation
    """
    
    def __init__(self):
        # Initialize default values first
        self.openai_api_key = getattr(settings, 'OPENAI_API_KEY', None)
        self.ollama_url = getattr(settings, 'OLLAMA_URL', 'http://localhost:11434')
        self.preferred_provider = getattr(settings, 'LLM_PROVIDER', 'openai')
        self.openai_model = getattr(settings, 'OPENAI_MODEL', 'gpt-3.5-turbo')
        self.ollama_model = getattr(settings, 'OLLAMA_MODEL', 'sqlcoder')
        
        # Load configuration from database, overriding defaults
        self._load_database_config()
    
    def generate_sql(self, prompt: str, schema_info: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Generate SQL from natural language prompt
        
        Args:
            prompt: Natural language query with schema context
            schema_info: Database schema information
            
        Returns:
            Tuple of (success, sql_query_or_error)
        """
        try:
            # Try preferred provider first
            if self.preferred_provider == 'openai' and self.openai_api_key:
                return self._generate_sql_openai(prompt, schema_info)
            elif self.preferred_provider == 'ollama':
                return self._generate_sql_ollama(prompt, schema_info)
            
            # Fallback to available provider
            if self.openai_api_key:
                return self._generate_sql_openai(prompt, schema_info)
            else:
                return self._generate_sql_ollama(prompt, schema_info)
                
        except Exception as e:
            logger.error(f"Failed to generate SQL: {e}")
            return False, f"SQL generation failed: {str(e)}"
    
    def _generate_sql_openai(self, prompt: str, schema_info: Dict[str, Any]) -> Tuple[bool, str]:
        """Generate SQL using OpenAI API"""
        try:
            import openai
            
            # Configure OpenAI client
            client = openai.OpenAI(api_key=self.openai_api_key)
            
            # Create system message with SQL generation context
            system_message = """You are an expert SQL query generator. Your job is to convert natural language questions into accurate SQL queries based on the provided database schema.

Rules:
1. Generate only valid SQL queries
2. Use exact table and column names as provided in the schema
3. Include appropriate WHERE clauses, JOINs, and aggregations as needed
4. Use proper SQL syntax for the database type
5. If the query is ambiguous, ask for clarification by starting your response with "CLARIFICATION_NEEDED:"
6. Otherwise, respond with only the SQL query, no explanations

Focus on creating efficient, accurate queries that directly answer the user's question."""

            # Combine prompt with schema information
            full_prompt = f"{prompt}\n\nSchema Information:\n{json.dumps(schema_info, indent=2)}"
            
            # Call OpenAI API
            response = client.chat.completions.create(
                model=self.openai_model,
                messages=[
                    {"role": "system", "content": system_message},
                    {"role": "user", "content": full_prompt}
                ],
                max_tokens=500,
                temperature=0.1  # Low temperature for consistent results
            )
            
            sql_query = response.choices[0].message.content
            if sql_query:
                sql_query = sql_query.strip()
                
                # Clean up the response
                sql_query = self._clean_sql_response(sql_query)
                
                logger.info(f"Generated SQL using OpenAI: {sql_query[:100]}...")
                return True, sql_query
            else:
                return False, "OpenAI returned empty response"
            
        except ImportError:
            logger.error("OpenAI library not installed")
            return False, "OpenAI library not available. Please install: pip install openai"
        except Exception as e:
            logger.error(f"OpenAI API error: {e}")
            return False, f"OpenAI API error: {str(e)}"
    
    def _generate_sql_ollama(self, prompt: str, schema_info: Dict[str, Any]) -> Tuple[bool, str]:
        """Generate SQL using Ollama local LLM with dynamic data detection"""
        try:
            # Import the dynamic service for data analysis
            from .dynamic_llm_service import DynamicLLMService
            
            # Create dynamic service instance
            dynamic_service = DynamicLLMService()
            
            # Use the dynamic service for SQL generation
            return dynamic_service.generate_sql(prompt)
                
        except requests.exceptions.ConnectionError:
            logger.error("Could not connect to Ollama. Is it running?")
            return False, "Could not connect to Ollama. Please ensure Ollama is running and accessible at " + self.ollama_url
        except requests.exceptions.Timeout:
            logger.error("Ollama request timed out")
            return False, "Ollama request timed out. Please try again."
        except Exception as e:
            error_str = str(e)
            logger.error(f"Ollama error: {e}")
            
            # Handle specific model not found error
            if "404" in error_str and "not found" in error_str:
                return False, f"Model '{self.ollama_model}' not found in Ollama. Please install it by running: ollama pull {self.ollama_model}"
            
            return False, f"Ollama error: {error_str}"
    
    def _clean_sql_response(self, sql_query: str) -> str:
        """Clean up SQL response from LLM"""
        # Remove common prefixes/suffixes
        prefixes_to_remove = [
            "```sql", "```", "SQL:", "Query:", "Answer:", "Here's the SQL:", "Here is the SQL:"
        ]
        
        suffixes_to_remove = [
            "```", ";"
        ]
        
        cleaned = sql_query.strip()
        
        # Remove prefixes
        for prefix in prefixes_to_remove:
            if cleaned.lower().startswith(prefix.lower()):
                cleaned = cleaned[len(prefix):].strip()
        
        # Remove suffixes (except semicolon at the very end)
        for suffix in suffixes_to_remove[:-1]:  # Don't remove semicolon
            if cleaned.lower().endswith(suffix.lower()):
                cleaned = cleaned[:-len(suffix)].strip()
        
        # Ensure query ends with semicolon
        if not cleaned.endswith(';'):
            cleaned += ';'
        
        return cleaned
    
    def test_openai_connection(self, api_key: str, model: str = "gpt-3.5-turbo") -> Tuple[bool, str]:
        """Test OpenAI API connection"""
        try:
            import openai
            
            # Create client with test API key
            client = openai.OpenAI(api_key=api_key)
            
            # Test with a simple request
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "user", "content": "Hello, just testing the connection. Please respond with 'OK'."}
                ],
                max_tokens=10,
                temperature=0
            )
            
            if response and response.choices:
                return True, "OpenAI connection successful"
            else:
                return False, "OpenAI responded but with unexpected format"
                
        except ImportError:
            return False, "OpenAI library not installed. Please install: pip install openai"
        except Exception as e:
            return False, f"OpenAI connection failed: {str(e)}"
    
    def test_ollama_connection(self, url: Optional[str] = None, model: str = "sqlcoder") -> Tuple[bool, str]:
        """Test Ollama connection"""
        try:
            test_url = url or self.ollama_url
            
            # First, check if Ollama is running
            health_response = requests.get(f"{test_url}/api/tags", timeout=5)
            
            if health_response.status_code != 200:
                return False, f"Ollama server not responding: {health_response.status_code}"
            
            # Check if the model is available
            tags_data = health_response.json()
            available_models = [model_info['name'] for model_info in tags_data.get('models', [])]
            
            if model not in available_models:
                return False, f"Model '{model}' not found. Available models: {', '.join(available_models)}"
            
            # Test generation
            test_response = requests.post(
                f"{test_url}/api/generate",
                json={
                    "model": model,
                    "prompt": "Test prompt. Respond with 'OK'.",
                    "stream": False,
                    "options": {"temperature": 0}
                },
                timeout=10
            )
            
            if test_response.status_code == 200:
                result = test_response.json()
                if 'response' in result:
                    return True, f"Ollama connection successful. Model '{model}' is working."
                else:
                    return False, "Ollama responded but with unexpected format"
            else:
                return False, f"Ollama generation test failed: {test_response.status_code}"
                
        except requests.exceptions.ConnectionError:
            return False, "Could not connect to Ollama. Please ensure Ollama is running."
        except requests.exceptions.Timeout:
            return False, "Ollama connection timed out."
        except Exception as e:
            return False, f"Ollama connection test failed: {str(e)}"
    
    def get_ollama_models(self, url: Optional[str] = None) -> Tuple[bool, List[str]]:
        """Get available Ollama models"""
        try:
            test_url = url or self.ollama_url
            
            response = requests.get(f"{test_url}/api/tags", timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                models = [model_info['name'] for model_info in data.get('models', [])]
                return True, models
            else:
                return False, []
                
        except Exception as e:
            logger.error(f"Failed to get Ollama models: {e}")
            return False, []
    
    def is_configured(self) -> bool:
        """Check if LLM service is properly configured"""
        return bool(self.openai_api_key or self._check_ollama_available())
    
    def _check_ollama_available(self) -> bool:
        """Check if Ollama is available"""
        try:
            response = requests.get(f"{self.ollama_url}/api/tags", timeout=2)
            return response.status_code == 200
        except:
            return False
    
    def _load_database_config(self):
        """Load LLM configuration from database"""
        try:
            from core.models import LLMConfig
            
            active_config = LLMConfig.get_active_config()
            if active_config:
                logger.info(f"Loading LLM config from database: {active_config.provider}")
                
                if active_config.provider == 'openai':
                    self.openai_api_key = active_config.api_key
                    self.openai_model = active_config.model_name
                    self.preferred_provider = 'openai'
                    
                elif active_config.provider == 'local':
                    # This is Ollama configuration
                    self.ollama_url = active_config.base_url or 'http://localhost:11434'
                    self.ollama_model = active_config.model_name
                    self.preferred_provider = 'ollama'
                    
                # Set common configuration
                self.temperature = active_config.temperature
                self.max_tokens = active_config.max_tokens
                self.system_prompt = active_config.system_prompt
                
                # Load additional settings
                additional = active_config.additional_settings or {}
                self.timeout = additional.get('timeout', 30)
                
            else:
                logger.info("No active LLM configuration found in database, using settings")
                
        except Exception as e:
            logger.warning(f"Failed to load LLM config from database: {e}")
    
    def update_configuration(self):
        """Update service configuration from database"""
        self._load_database_config()
    
    def get_configuration_status(self) -> Dict[str, Any]:
        """Get current LLM configuration status"""
        return {
            'openai_configured': bool(self.openai_api_key),
            'ollama_available': self._check_ollama_available(),
            'preferred_provider': self.preferred_provider,
            'openai_model': self.openai_model,
            'ollama_model': self.ollama_model,
            'ollama_url': self.ollama_url,
            'database_config_loaded': hasattr(self, 'system_prompt')
        } 