"""
Semantic Service for ConvaBI Application
Provides business-friendly metadata and context to improve LLM query generation
"""

import json
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict, field
from enum import Enum
import logging
from django.db import transaction
from core.models import LLMConfig
from datasets.models import SemanticTable, SemanticColumn, SemanticMetric
from django.core.cache import cache
from utils.type_helpers import validate_semantic_data_type

logger = logging.getLogger(__name__)

class DataType(Enum):
    DIMENSION = "dimension"
    MEASURE = "measure" 
    IDENTIFIER = "identifier"
    DATE = "date"

class SemanticService:
    """
    Semantic service that enriches raw database schema with business context
    """
    
    def __init__(self):
        self.business_glossary: Dict[str, str] = {}
        self._load_business_glossary()
    
    def _load_business_glossary(self):
        """Load universal glossary that works across all business domains"""
        # UNIVERSAL: Generic terms that work with any dataset, any business domain
        self.business_glossary = {
            "identifier": "Unique reference code or ID",
            "name": "Name or title field",
            "amount": "Numeric value or quantity",
            "date": "Date or time information",
            "location": "Geographic or physical location",
            "category": "Classification or grouping",
            "status": "Current state or condition",
            "type": "Classification or category",
            "count": "Number or quantity",
            "total": "Sum or aggregate value",
            "average": "Mean or typical value",
            "maximum": "Highest value in a set",
            "minimum": "Lowest value in a set",
            "percentage": "Proportion expressed as a fraction of 100",
            "ratio": "Relationship between two quantities"
        }
    
    def get_enhanced_sql_from_openai(self, natural_language_query: str, schema_info: Dict[str, Any], 
                                   connection_type: str = "database") -> Tuple[bool, str, Optional[str]]:
        """
        Generate enhanced SQL from natural language using semantic context
        
        Args:
            natural_language_query: User's natural language query
            schema_info: Database schema information
            connection_type: Type of data connection
            
        Returns:
            Tuple of (success, sql_or_clarification, clarification_question)
        """
        try:
            from services.llm_service import LLMService
            
            llm_service = LLMService()
            enhanced_prompt = self.generate_enhanced_schema_prompt(schema_info, connection_type)
            
            # Add semantic context to the query
            semantic_context = self.get_semantic_context_for_query(natural_language_query)
            
            full_prompt = f"""
{enhanced_prompt}

SEMANTIC CONTEXT:
{semantic_context}

BUSINESS GLOSSARY:
{self._format_business_glossary()}

USER QUERY: {natural_language_query}

INSTRUCTIONS:
You are a helpful data analyst assistant. When a user asks a question about their data, you should:

1. **If the query is clear and you can write SQL**: Respond with just the SQL query
2. **If the query is unclear, ambiguous, or missing details**: Ask clarifying questions to help the user

WHEN TO ASK FOR CLARIFICATION:
- If column names or relationships aren't clear
- If aggregation type isn't specified (sum, average, count, etc.)
- If time periods aren't defined
- If filtering criteria are vague
- If sorting preferences aren't mentioned
- If the user asks about entities not clearly present in the schema

HOW TO ASK FOR CLARIFICATION:
Respond with "CLARIFICATION_NEEDED: " followed by helpful questions like:
- "I can help you find top customers! Could you clarify: Do you want them ranked by total sales amount, number of orders, or profit? Also, should I include all time periods or a specific date range?"
- "I see you want sales data for the South region. Would you like to see total sales, average sales per order, or sales by specific time period (monthly/yearly)?"

EXAMPLES:
User: "top customers" → Ask: "How would you like to rank the customers (by sales amount, order count, or profit) and how many top customers should I show?"
User: "sales in south" → Ask: "What would you like to know about sales in the South region? Total sales amount, sales trends over time, or sales by product category?"

Remember: It's better to ask helpful questions than to guess or return empty results!
"""
            
            success, response = llm_service.generate_sql(full_prompt, schema_info)
            
            if success and response:
                if response.startswith("CLARIFICATION_NEEDED:"):
                    clarification = response.replace("CLARIFICATION_NEEDED:", "").strip()
                    return True, "", clarification
                
                # Check for low confidence indicators in the response
                confidence_score = self._assess_query_confidence(natural_language_query, response, schema_info)
                logger.info(f"Query confidence score: {confidence_score}% for query: {natural_language_query}")
                
                # For complex queries, confidence scoring will handle them
                
                if confidence_score < 40:
                    logger.info(f"Low confidence ({confidence_score}%), generating clarification")
                    clarification = self._generate_intelligent_clarification(natural_language_query, schema_info)
                    return True, "", clarification
                else:
                    # Validate the SQL response before returning
                    cleaned_response = response.strip()
                    
                    # Check if response is just a semicolon or empty
                    if not cleaned_response or cleaned_response in [';', ';;']:
                        # Don't ask for clarification on empty SQL - use template generation instead
                        logger.info(f"LLM returned empty SQL for: {natural_language_query}, will try template generation")
                        return False, "Empty SQL returned, attempting template generation", None
                    
                    # If still getting empty SQL, try template-based generation
                    if not any(keyword in cleaned_response.upper() for keyword in ['SELECT', 'FROM', 'WHERE']):
                        template_sql = self._try_template_sql_generation(natural_language_query, schema_info)
                        if template_sql:
                            logger.info(f"Generated SQL using template approach: {template_sql}")
                            return True, template_sql, None
                    
                    # Check if response is too short to be valid SQL
                    if len(cleaned_response) < 10:
                        logger.warning(f"LLM returned very short SQL: '{cleaned_response}' for query: {natural_language_query}")
                        return False, f"Generated SQL appears incomplete: '{cleaned_response}'. Please try rephrasing your question.", None
                    
                    # Check if response contains actual SQL keywords
                    sql_keywords = ['SELECT', 'FROM', 'WHERE', 'GROUP', 'ORDER', 'INSERT', 'UPDATE', 'DELETE']
                    if not any(keyword in cleaned_response.upper() for keyword in sql_keywords):
                        # Instead of failing, generate a helpful clarification question
                        logger.info(f"LLM returned non-SQL response, generating clarification for: {natural_language_query}")
                        clarification = self._generate_intelligent_clarification(natural_language_query, schema_info)
                        return True, "", clarification
                    
                    return True, cleaned_response, None
            else:
                logger.error(f"LLM service failed for query: {natural_language_query}, response: {response}")
                return False, "Failed to generate SQL query. Please check your LLM configuration.", None
                
        except Exception as e:
            logger.error(f"Failed to generate enhanced SQL: {e}")
            return False, f"Error generating SQL: {str(e)}", None
    
    def _generate_intelligent_clarification(self, query: str, schema_info: Dict[str, Any]) -> str:
        """
        Generate intelligent clarification questions based on query analysis and actual data
        """
        try:
            query_lower = query.lower()
            available_columns = self._extract_column_names(schema_info)
            
            # Get original column names for proper casing
            original_columns = []
            if isinstance(schema_info, dict) and 'columns' in schema_info:
                original_columns = [col.get('name', '') for col in schema_info['columns'] if isinstance(col, dict)]
            elif isinstance(schema_info, list):
                original_columns = [col.get('name', '') for col in schema_info if isinstance(col, dict)]
            
            # Map lowercase to original for display
            col_mapping = {col.lower(): col for col in original_columns}
            
            # Analyze the query and generate contextual clarifications
            clarifications = []
            query_type_detected = False
            
            # UNIVERSAL: Check for demographic queries without hardcoded business terms
            if any(word in query_lower for word in ['male', 'female', 'gender', 'sex']):
                query_type_detected = True
                # Use pattern-based column discovery instead of hardcoded terms
                from .universal_schema_service import universal_schema_service
                
                demographic_columns = []
                for col in available_columns:
                    patterns = universal_schema_service.discover_column_patterns(col)
                    if 'categorical' in patterns and any(term in col.lower() for term in ['sex', 'gender']):
                        demographic_columns.append(col_mapping.get(col, col))
                
                if demographic_columns:
                    clarifications.append(f"I found demographic data in column(s): {', '.join(demographic_columns)}. What would you like to know?")
                    clarifications.append("Would you like total counts, percentages, or some other analysis?")
                else:
                    clarifications.append("I don't see a clear demographic column in your data. Could you tell me which column contains this information?")
            
            # Check for counting/total queries
            if any(word in query_lower for word in ['total', 'count', 'number', 'how many']):
                query_type_detected = True
                if not any(word in query_lower for word in ['male', 'female', 'gender']):
                    clarifications.append("What would you like to count? Please specify the category or column.")
                    clarifications.append("Should I show total counts, unique counts, or counts grouped by some category?")
            
            # UNIVERSAL: Check for ranking requests using pattern-based discovery
            if any(word in query_lower for word in ['top', 'best', 'highest', 'largest', 'most']):
                query_type_detected = True
                # Use pattern-based column discovery instead of hardcoded business terms
                from .universal_schema_service import universal_schema_service
                
                ranking_columns = []
                for col in available_columns:
                    patterns = universal_schema_service.discover_column_patterns(col)
                    if 'monetary' in patterns or 'quantity' in patterns:
                        ranking_columns.append(col_mapping.get(col, col))
                
                if ranking_columns:
                    clarifications.append(f"I can rank by these metrics: {', '.join(ranking_columns)}. Which one should I use?")
                clarifications.append("How many top items would you like to see? (e.g., top 5, top 10)")
            
            # UNIVERSAL: Check for category-based queries using pattern discovery
            categorical_keywords = ['category', 'type', 'group', 'class', 'segment']
            if any(keyword in query_lower for keyword in categorical_keywords):
                query_type_detected = True
                # Use pattern-based column discovery instead of hardcoded terms
                from .universal_schema_service import universal_schema_service
                
                category_columns = []
                for col in available_columns:
                    patterns = universal_schema_service.discover_column_patterns(col)
                    if 'categorical' in patterns:
                        category_columns.append(col_mapping.get(col, col))
                
                if category_columns:
                    clarifications.append(f"I found these category columns: {', '.join(category_columns)}. Which one interests you?")
            
            # UNIVERSAL: Check for location-based queries using pattern discovery
            location_keywords = ['location', 'city', 'state', 'country', 'region', 'place']
            if any(keyword in query_lower for keyword in location_keywords):
                query_type_detected = True
                # Use pattern-based column discovery instead of hardcoded terms
                from .universal_schema_service import universal_schema_service
                
                location_columns = []
                for col in available_columns:
                    patterns = universal_schema_service.discover_column_patterns(col)
                    if 'geographic' in patterns:
                        location_columns.append(col_mapping.get(col, col))
                
                if location_columns:
                    clarifications.append(f"I can filter by location using: {', '.join(location_columns)}.")
            
            # Check for time-based queries
            if any(word in query_lower for word in ['date', 'time', 'year', 'month', 'day', 'trend', 'over time', 'when']):
                query_type_detected = True
                date_columns = [col_mapping.get(col, col) for col in available_columns if any(term in col for term in ['date', 'time', 'year', 'month'])]
                if date_columns:
                    clarifications.append(f"I found date/time columns: {', '.join(date_columns)}. What time period interests you?")
                    clarifications.append("Would you like to group by day, month, year, or see trends over time?")
            
            # If no specific pattern detected, provide generic but data-aware help
            if not query_type_detected:
                # Show available columns to help user
                if len(original_columns) <= 10:
                    clarifications.append(f"Available data columns: {', '.join(original_columns[:10])}")
                else:
                    clarifications.append(f"I have {len(original_columns)} columns of data. Some key ones are: {', '.join(original_columns[:5])}...")
                clarifications.append("What specific analysis would you like me to perform with this data?")
            
            # Generate final clarification message
            if clarifications:
                base_message = f"I want to help you with '{query}'. To give you the best results, could you clarify:"
                return base_message + "\n\n" + "\n".join(f"• {q}" for q in clarifications[:3])  # Limit to 3 questions
            else:
                # Fallback with actual data context
                available_cols_str = ', '.join(original_columns[:5]) if original_columns else "your data"
                return f"I'd like to help you analyze '{query}' using {available_cols_str}. Could you be more specific about what you want to see or calculate?"
        
        except Exception as e:
            logger.error(f"Error generating clarification: {e}")
            return f"I'd like to help you with '{query}', but could you provide more specific details about what you want to analyze?"
    
    def _extract_column_names(self, schema_info: Dict[str, Any]) -> List[str]:
        """Extract column names from schema information"""
        try:
            columns = []
            
            if isinstance(schema_info, dict):
                if 'columns' in schema_info:
                    # Single table schema (CSV format)
                    for col in schema_info['columns']:
                        if isinstance(col, dict) and 'name' in col:
                            columns.append(col['name'])
                else:
                    # Multi-table schema
                    for table_name, table_info in schema_info.items():
                        if isinstance(table_info, dict):
                            if 'columns' in table_info:
                                for col in table_info['columns']:
                                    if isinstance(col, dict) and 'name' in col:
                                        columns.append(col['name'])
                            else:
                                # Legacy format
                                columns.extend(table_info.keys())
            elif isinstance(schema_info, list):
                # List format
                for col in schema_info:
                    if isinstance(col, dict) and 'name' in col:
                        columns.append(col['name'])
            
            return [col.lower() for col in columns]
        
        except Exception as e:
            logger.error(f"Error extracting column names: {e}")
            return []
    
    def _try_template_sql_generation(self, query: str, schema_info: Dict[str, Any]) -> Optional[str]:
        """
        Generate SQL using simple templates for common query patterns
        """
        try:
            logger.info(f"Template SQL generation for query: {query}")
            logger.info(f"Schema info type: {type(schema_info)}, keys: {schema_info.keys() if isinstance(schema_info, dict) else 'Not dict'}")
            
            query_lower = query.lower()
            available_columns = self._extract_column_names(schema_info)
            logger.info(f"Available columns: {available_columns}")
            
            # UNIVERSAL: Pattern-based ranking query generation
            if ('top' in query_lower and any(word in query_lower for word in ['name', 'user', 'person', 'entity'])) or \
               (any(word in query_lower for word in ['best', 'highest', 'most']) and any(word in query_lower for word in ['name', 'user', 'person', 'entity'])):
                
                logger.info("Matched customer ranking pattern")
                
                # Extract number
                import re
                numbers = re.findall(r'\d+', query)
                limit = numbers[0] if numbers else '10'
                
                # Get original column names from schema first
                original_columns = []
                if isinstance(schema_info, dict) and 'columns' in schema_info:
                    original_columns = [col.get('name', '') for col in schema_info['columns'] if isinstance(col, dict)]
                elif isinstance(schema_info, list):
                    original_columns = [col.get('name', '') for col in schema_info if isinstance(col, dict)]
                
                # Map lowercase to original
                col_mapping = {col.lower(): col for col in original_columns}
                
                # UNIVERSAL: Determine ranking column using pattern discovery
                from .universal_schema_service import universal_schema_service
                
                # Find the best metric column by pattern
                metric_col = None
                for col in available_columns:
                    patterns = universal_schema_service.discover_column_patterns(col)
                    if 'monetary' in patterns or 'quantity' in patterns:
                        metric_col = col_mapping.get(col, col)
                        break
                
                if not metric_col:
                    metric_col = 'metric_column'  # generic fallback
                
                logger.info(f"Using ranking column: {metric_col}, limit: {limit}")
                
                # Check for region filter (will be updated with actual column name later)
                region_filter = ""
                region_value = None
                for region_name in ['south', 'north', 'east', 'west']:
                    if region_name in query_lower:
                        region_value = region_name.title()
                        break
                
                logger.info(f"Region filter: {region_value}")
                
                # Find actual column names (search both ways - in lowercase list and get original)
                # Get original column names from schema
                original_columns = []
                if isinstance(schema_info, dict) and 'columns' in schema_info:
                    original_columns = [col.get('name', '') for col in schema_info['columns'] if isinstance(col, dict)]
                elif isinstance(schema_info, list):
                    original_columns = [col.get('name', '') for col in schema_info if isinstance(col, dict)]
                
                logger.info(f"Original columns from schema: {original_columns}")
                
                # Map lowercase to original
                col_mapping = {col.lower(): col for col in original_columns}
                
                # Find best matches with enhanced logic
                customer_col = None
                for lower_col in available_columns:
                    if 'customer' in lower_col and 'name' in lower_col:
                        customer_col = col_mapping.get(lower_col, lower_col)
                        break
                if not customer_col:
                    customer_col = next((col_mapping.get(col, col) for col in available_columns if 'customer' in col), 'Customer_Name')
                
                sales_col = metric_col
                region_col = next((col_mapping.get(col, col) for col in available_columns if 'region' in col), 'Region')
                
                logger.info(f"Mapped columns - Customer: {customer_col}, Sales: {sales_col}, Region: {region_col}")
                
                # Determine table name from schema - try multiple approaches
                # Get dynamic table name instead of hardcoded 'csv_data'
                from utils.dynamic_naming import dynamic_naming
                
                available_tables = dynamic_naming.get_available_tables('duckdb')
                if available_tables:
                    table_name = available_tables[0]  # Use first available table
                else:
                    table_name = "csv_data"  # fallback only
                
                # Try to get table name from schema info
                if isinstance(schema_info, dict):
                    if 'table_name' in schema_info:
                        table_name = schema_info['table_name']
                    elif 'name' in schema_info:
                        table_name = schema_info['name']
                    # Check if this is a DuckDB integrated table format
                    elif any(isinstance(key, str) and key.startswith('source_') for key in schema_info.keys()):
                        # Look for source_xxxxx pattern which is common in integrated tables
                        for key in schema_info.keys():
                            if isinstance(key, str) and key.startswith('source_'):
                                table_name = key
                                break
                
                logger.info(f"Using table name: {table_name} for top customers template SQL")
                
                # Build region filter with correct column name
                if region_value:
                    region_filter = f' WHERE "{region_col}" = \'{region_value}\''
                
                sql = f"""
                SELECT "{customer_col}", SUM("{sales_col}") as total_metric
                FROM {table_name}{region_filter}
                GROUP BY "{customer_col}"
                ORDER BY total_metric DESC
                LIMIT {limit}
                """
                
                logger.info(f"Generated top customers SQL: {sql.strip()}")
                return sql.strip()
            
            # Pattern: "[metric] by [dimension] in year [YYYY]" (e.g., "total sales by segment in year 2016") - NEW PATTERN
            elif (any(metric in query_lower for metric in ['profit', 'sales', 'revenue']) and 
                  'by' in query_lower and 
                  any(dimension in query_lower for dimension in ['segment', 'category', 'sub_category']) and
                  ('year' in query_lower or any(year in query for year in ['2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023']))):
                
                logger.info("Matched metric by dimension in year pattern")
                
                # Get original column names from schema
                original_columns = []
                if isinstance(schema_info, dict) and 'columns' in schema_info:
                    original_columns = [col.get('name', '') for col in schema_info['columns'] if isinstance(col, dict)]
                elif isinstance(schema_info, list):
                    original_columns = [col.get('name', '') for col in schema_info if isinstance(col, dict)]
                
                # Map lowercase to original
                col_mapping = {col.lower(): col for col in original_columns}
                
                # Determine metric column - Find actual column name in the schema
                metric_col = None
                if 'profit' in query_lower:
                    metric_col = next((col_mapping.get(col, col) for col in available_columns if 'profit' in col), None)
                elif 'sales' in query_lower:
                    metric_col = next((col_mapping.get(col, col) for col in available_columns if 'sales' in col), None)
                elif 'revenue' in query_lower:
                    metric_col = next((col_mapping.get(col, col) for col in available_columns if 'revenue' in col), None)
                
                # Fallback to any sales-like column
                if not metric_col:
                    metric_col = next((col_mapping.get(col, col) for col in available_columns if 'sales' in col), 'Sales')
                
                # Determine dimension column - Find actual column name
                dimension_col = None
                if 'segment' in query_lower:
                    dimension_col = next((col_mapping.get(col, col) for col in available_columns if 'segment' in col), None)
                elif 'category' in query_lower and 'sub' not in query_lower:
                    dimension_col = next((col_mapping.get(col, col) for col in available_columns if 'category' in col and 'sub' not in col), None)
                elif 'sub_category' in query_lower or 'subcategory' in query_lower:
                    dimension_col = next((col_mapping.get(col, col) for col in available_columns if 'sub_category' in col), None)
                
                # Fallback to any segment-like column  
                if not dimension_col:
                    dimension_col = next((col_mapping.get(col, col) for col in available_columns if 'segment' in col), 'Segment')
                
                # Find year
                import re
                years = [year for year in ['2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023'] if year in query]
                year_value = years[0] if years else '2016'
                
                # Find date column - Look for actual date column
                date_col = next((col_mapping.get(col, col) for col in available_columns if 'order_date' in col or 'date' in col), 'Order_Date')
                
                # Check for region filter
                region_filter = ""
                region_value = None
                for region_name in ['south', 'north', 'east', 'west']:
                    if region_name in query_lower:
                        region_value = region_name.title()
                        break
                
                if region_value:
                    region_col = next((col_mapping.get(col, col) for col in available_columns if 'region' in col), 'Region')
                    region_filter = f' AND "{region_col}" = \'{region_value}\''
                
                # Determine table name from schema
                table_name = "csv_data"
                if isinstance(schema_info, dict) and 'table_name' in schema_info:
                    table_name = schema_info['table_name']
                elif isinstance(schema_info, dict) and 'name' in schema_info:
                    table_name = schema_info['name']
                
                sql = f"""
                SELECT "{dimension_col}", SUM("{metric_col}") as total_{metric_col.lower()}
                FROM {table_name}
                WHERE strftime('%Y', "{date_col}") = '{year_value}'{region_filter}
                GROUP BY "{dimension_col}"
                ORDER BY total_{metric_col.lower()} DESC
                """
                
                logger.info(f"Generated {metric_col.lower()} by {dimension_col.lower()} in year {year_value} SQL: {sql.strip()}")
                return sql.strip()
            
            # Pattern: "[metric] by [dimension] in [region]" (e.g., "profit by segment in south") - MOVED UP FOR PRIORITY
            elif (any(metric in query_lower for metric in ['profit', 'sales', 'revenue']) and 
                  'by' in query_lower and 
                  any(dimension in query_lower for dimension in ['segment', 'category', 'sub_category', 'state', 'city']) and
                  any(region in query_lower for region in ['south', 'north', 'east', 'west'])):
                
                logger.info("Matched metric by dimension in region pattern")
                
                # Get original column names from schema
                original_columns = []
                if isinstance(schema_info, dict) and 'columns' in schema_info:
                    original_columns = [col.get('name', '') for col in schema_info['columns'] if isinstance(col, dict)]
                elif isinstance(schema_info, list):
                    original_columns = [col.get('name', '') for col in schema_info if isinstance(col, dict)]
                
                # Map lowercase to original
                col_mapping = {col.lower(): col for col in original_columns}
                
                # Determine metric column
                if 'profit' in query_lower:
                    metric_col = next((col_mapping.get(col, col) for col in available_columns if 'profit' in col), 'Profit')
                elif 'sales' in query_lower:
                    metric_col = next((col_mapping.get(col, col) for col in available_columns if 'sales' in col), 'Sales')
                elif 'revenue' in query_lower:
                    metric_col = next((col_mapping.get(col, col) for col in available_columns if 'revenue' in col), 'Revenue')
                else:
                    metric_col = 'Sales'  # fallback
                
                # Determine dimension column
                if 'segment' in query_lower:
                    dimension_col = next((col_mapping.get(col, col) for col in available_columns if 'segment' in col), 'Segment')
                elif 'category' in query_lower and 'sub' not in query_lower:
                    dimension_col = next((col_mapping.get(col, col) for col in available_columns if 'category' in col and 'sub' not in col), 'Category')
                elif 'sub_category' in query_lower or 'subcategory' in query_lower:
                    dimension_col = next((col_mapping.get(col, col) for col in available_columns if 'sub_category' in col), 'Sub_Category')
                elif 'state' in query_lower:
                    dimension_col = next((col_mapping.get(col, col) for col in available_columns if 'state' in col), 'State')
                elif 'city' in query_lower:
                    dimension_col = next((col_mapping.get(col, col) for col in available_columns if 'city' in col), 'City')
                else:
                    dimension_col = 'Segment'  # fallback
                
                # Find region column and value
                region_col = next((col_mapping.get(col, col) for col in available_columns if 'region' in col), 'Region')
                region_value = None
                for region_name in ['south', 'north', 'east', 'west']:
                    if region_name in query_lower:
                        region_value = region_name.title()
                        break
                
                # Determine table name from schema
                table_name = "csv_data"
                if isinstance(schema_info, dict) and 'table_name' in schema_info:
                    table_name = schema_info['table_name']
                elif isinstance(schema_info, dict) and 'name' in schema_info:
                    table_name = schema_info['name']
                
                sql = f"""
                SELECT "{dimension_col}", SUM("{metric_col}") as total_{metric_col.lower()}
                FROM {table_name}
                WHERE "{region_col}" = '{region_value}'
                GROUP BY "{dimension_col}"
                ORDER BY total_{metric_col.lower()} DESC
                """
                
                logger.info(f"Generated {metric_col.lower()} by {dimension_col.lower()} in {region_value} SQL: {sql.strip()}")
                return sql.strip()
            
            # Pattern: "sales in [specific region]" or "total sales in [region]" - MOVED DOWN FOR LOWER PRIORITY
            elif ('sales' in query_lower or 'revenue' in query_lower) and any(region in query_lower for region in ['south', 'north', 'east', 'west']):
                logger.info("Matched sales in specific region pattern")
                
                # Get original column names from schema
                original_columns = []
                if isinstance(schema_info, dict) and 'columns' in schema_info:
                    original_columns = [col.get('name', '') for col in schema_info['columns'] if isinstance(col, dict)]
                elif isinstance(schema_info, list):
                    original_columns = [col.get('name', '') for col in schema_info if isinstance(col, dict)]
                
                # Map lowercase to original
                col_mapping = {col.lower(): col for col in original_columns}
                
                sales_col = next((col_mapping.get(col, col) for col in available_columns if 'sales' in col), 'Sales')
                region_col = next((col_mapping.get(col, col) for col in available_columns if 'region' in col), 'Region')
                
                # Find the specific region mentioned
                region_value = None
                for region_name in ['south', 'north', 'east', 'west']:
                    if region_name in query_lower:
                        region_value = region_name.title()
                        break
                
                # Determine table name from schema
                table_name = "csv_data"
                if isinstance(schema_info, dict) and 'table_name' in schema_info:
                    table_name = schema_info['table_name']
                elif isinstance(schema_info, dict) and 'name' in schema_info:
                    table_name = schema_info['name']
                
                # Check if user wants total or breakdown
                if 'total' in query_lower or any(word in query_lower for word in ['sum', 'amount', 'kpi']):
                    # Just return the sum for that region - SINGLE VALUE FOR KPI
                    sql = f"""
                    SELECT SUM("{sales_col}") as total_sales
                    FROM {table_name}
                    WHERE "{region_col}" = '{region_value}'
                    """
                    logger.info(f"Generated KPI query for total sales in {region_value}")
                elif any(word in query_lower for word in ['breakdown', 'by city', 'by state', 'details', 'detailed']):
                    # Return detailed breakdown for that region (e.g., by state or city)
                    # Try to find a more granular geographic column
                    city_col = next((col_mapping.get(col, col) for col in available_columns if 'city' in col), None)
                    state_col = next((col_mapping.get(col, col) for col in available_columns if 'state' in col), None)
                    
                    if city_col:
                        breakdown_col = city_col
                    elif state_col:
                        breakdown_col = state_col
                    else:
                        breakdown_col = region_col
                    
                    sql = f"""
                    SELECT "{breakdown_col}", SUM("{sales_col}") as total_sales
                    FROM {table_name}
                    WHERE "{region_col}" = '{region_value}'
                    GROUP BY "{breakdown_col}"
                    ORDER BY total_sales DESC
                    """
                    logger.info(f"Generated breakdown query for sales in {region_value}")
                else:
                    # DEFAULT: For ambiguous queries like "sales in south", give total (KPI)
                    sql = f"""
                    SELECT SUM("{sales_col}") as total_sales
                    FROM {table_name}
                    WHERE "{region_col}" = '{region_value}'
                    """
                    logger.info(f"Generated default KPI query for sales in {region_value}")
                
                logger.info(f"Generated sales in {region_value} SQL: {sql.strip()}")
                return sql.strip()
            
            # Pattern: "sales by region" or "revenue by region" (general, no specific region mentioned)
            elif ('sales' in query_lower or 'revenue' in query_lower) and 'region' in query_lower and not any(region in query_lower for region in ['south', 'north', 'east', 'west']):
                # Get original column names from schema
                original_columns = []
                if isinstance(schema_info, dict) and 'columns' in schema_info:
                    original_columns = [col.get('name', '') for col in schema_info['columns'] if isinstance(col, dict)]
                elif isinstance(schema_info, list):
                    original_columns = [col.get('name', '') for col in schema_info if isinstance(col, dict)]
                
                # Map lowercase to original
                col_mapping = {col.lower(): col for col in original_columns}
                
                sales_col = next((col_mapping.get(col, col) for col in available_columns if 'sales' in col), 'Sales')
                region_col = next((col_mapping.get(col, col) for col in available_columns if 'region' in col), 'Region')
                
                # Determine table name from schema
                table_name = "csv_data"
                if isinstance(schema_info, dict) and 'table_name' in schema_info:
                    table_name = schema_info['table_name']
                elif isinstance(schema_info, dict) and 'name' in schema_info:
                    table_name = schema_info['name']
                
                sql = f"""
                SELECT "{region_col}", SUM("{sales_col}") as total_sales
                FROM {table_name}
                GROUP BY "{region_col}"
                ORDER BY total_sales DESC
                """
                
                logger.info(f"Generated sales by region SQL: {sql.strip()}")
                return sql.strip()
            
            # Pattern: "top X selling items/products by profit/sales in region in year"
            elif (('top' in query_lower and any(word in query_lower for word in ['item', 'product', 'selling'])) or 
                  ('compare' in query_lower and any(word in query_lower for word in ['item', 'product']))) and \
                 any(word in query_lower for word in ['profit', 'sales']) and \
                 any(region in query_lower for region in ['south', 'north', 'east', 'west']) and \
                 any(year in query for year in ['2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023']):
                
                logger.info("Matched top selling items with year and region pattern")
                
                # Extract components
                import re
                numbers = re.findall(r'\d+', query)
                limit = numbers[0] if numbers else '3'
                
                # Find year
                years = [year for year in ['2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023'] if year in query]
                year_value = years[0] if years else '2015'
                
                # Find region
                region_value = None
                for region_name in ['south', 'north', 'east', 'west']:
                    if region_name in query_lower:
                        region_value = region_name.title()
                        break
                
                # Determine metric (profit vs sales)
                if 'profit' in query_lower:
                    metric_col = 'Profit'
                    order_col = 'Sales'  # Order by sales to get "selling" items
                else:
                    metric_col = 'Sales'
                    order_col = 'Sales'
                
                # Get original column names from schema
                original_columns = []
                if isinstance(schema_info, dict) and 'columns' in schema_info:
                    original_columns = [col.get('name', '') for col in schema_info['columns'] if isinstance(col, dict)]
                
                # Map lowercase to original
                col_mapping = {col.lower(): col for col in original_columns}
                
                product_col = next((col_mapping.get(col, col) for col in available_columns if 'product_name' in col), 'Product_Name')
                metric_col = col_mapping.get(metric_col.lower(), metric_col)
                order_col = col_mapping.get(order_col.lower(), order_col) 
                region_col = next((col_mapping.get(col, col) for col in available_columns if 'region' in col), 'Region')
                date_col = next((col_mapping.get(col, col) for col in available_columns if 'order_date' in col or 'date' in col), 'Order_Date')
                
                # Determine table name
                table_name = "csv_data"
                if isinstance(schema_info, dict) and 'table_name' in schema_info:
                    table_name = schema_info['table_name']
                
                sql = f"""
                SELECT "{product_col}", SUM("{metric_col}") as total_{metric_col.lower()}
                FROM {table_name}
                WHERE "{region_col}" = '{region_value}' 
                AND strftime('%Y', "{date_col}") = '{year_value}'
                GROUP BY "{product_col}"
                ORDER BY SUM("{order_col}") DESC
                LIMIT {limit}
                """
                
                logger.info(f"Generated top selling items SQL: {sql.strip()}")
                return sql.strip()
            
            # Pattern: general "show all" or "list all"
            elif any(phrase in query_lower for phrase in ['show all', 'list all', 'all data']):
                # Determine table name from schema
                table_name = "csv_data"
                if isinstance(schema_info, dict) and 'table_name' in schema_info:
                    table_name = schema_info['table_name']
                elif isinstance(schema_info, dict) and 'name' in schema_info:
                    table_name = schema_info['name']
                
                sql = f"SELECT * FROM {table_name} LIMIT 100"
                logger.info(f"Generated general query SQL: {sql}")
                return sql
            
            return None
            
        except Exception as e:
            logger.error(f"Error in template SQL generation: {e}")
            return None
    
    def handle_postal_code_query(self, query: str, df: pd.DataFrame) -> Tuple[bool, str]:
        """
        Handle postal code related queries with semantic understanding
        """
        try:
            # Check if query is about postal codes
            if any(term in query.lower() for term in ['postal', 'zip', 'postcode', 'zip code']):
                # Look for postal code columns in the data
                postal_columns = []
                for col in df.columns:
                    if any(term in col.lower() for term in ['postal', 'zip', 'postcode']):
                        postal_columns.append(col)
                
                if postal_columns:
                    col_name = postal_columns[0]
                    
                    # Generate appropriate query based on request
                    if 'count' in query.lower() or 'number' in query.lower():
                        sql = f"SELECT COUNT(DISTINCT {col_name}) as unique_postal_codes FROM data"
                    elif 'list' in query.lower() or 'show' in query.lower():
                        sql = f"SELECT DISTINCT {col_name} FROM data ORDER BY {col_name} LIMIT 20"
                    else:
                        sql = f"SELECT {col_name}, COUNT(*) as count FROM data GROUP BY {col_name} ORDER BY count DESC LIMIT 10"
                    
                    return True, sql
            
            return False, "Not a postal code query"
            
        except Exception as e:
            logger.error(f"Failed to handle postal code query: {e}")
            return False, f"Error handling postal code query: {str(e)}"
    
    def get_semantic_context_for_query(self, query: str) -> str:
        """
        Generate semantic context based on the query content
        """
        context_parts = []
        
        # Analyze query for business terms
        query_lower = query.lower()
        
        # Financial metrics context
        if any(term in query_lower for term in ['revenue', 'sales', 'profit', 'income', 'money']):
            context_parts.append("FINANCIAL METRICS: Focus on monetary values, use SUM for totals, consider time periods for trends")
        
        # Customer analysis context
        if any(term in query_lower for term in ['customer', 'client', 'user', 'buyer']):
            context_parts.append("CUSTOMER ANALYSIS: Consider customer segmentation, lifetime value, and behavioral patterns")
        
        # Time-based analysis context
        if any(term in query_lower for term in ['trend', 'over time', 'monthly', 'yearly', 'daily', 'growth']):
            context_parts.append("TIME ANALYSIS: Use date functions, consider seasonality, group by time periods")
        
        # Geographical analysis context
        if any(term in query_lower for term in ['region', 'location', 'city', 'state', 'country', 'postal']):
            context_parts.append("GEOGRAPHICAL ANALYSIS: Consider regional differences, use location-based grouping")
        
        # Product analysis context
        if any(term in query_lower for term in ['product', 'item', 'category', 'brand']):
            context_parts.append("PRODUCT ANALYSIS: Consider product categories, inventory, and performance metrics")
        
        return "\n".join(context_parts) if context_parts else "GENERAL ANALYSIS: Standard data analysis approach"
    
    def enhance_query_with_semantic_context(self, original_query: str, schema_info: Dict[str, Any]) -> str:
        """
        Enhance a SQL query with semantic context and business rules
        """
        try:
            enhanced_query = original_query
            
            # Add common business filters
            if 'WHERE' not in enhanced_query.upper() and 'revenue' in enhanced_query.lower():
                # Add filter to exclude negative revenue
                enhanced_query = enhanced_query.replace(';', ' WHERE revenue > 0;')
            
            # Add meaningful column aliases
            alias_mappings = {
                'SUM(revenue)': 'SUM(revenue) as total_revenue',
                'SUM(profit)': 'SUM(profit) as total_profit',
                'COUNT(*)': 'COUNT(*) as record_count',
                'AVG(': 'AVG(',  # Keep as is, but could enhance
            }
            
            for original, enhanced in alias_mappings.items():
                if original in enhanced_query and ' as ' not in enhanced_query.lower():
                    enhanced_query = enhanced_query.replace(original, enhanced)
            
            return enhanced_query
            
        except Exception as e:
            logger.error(f"Failed to enhance query with semantic context: {e}")
            return original_query
    
    def generate_enhanced_schema_prompt(self, raw_schema: Dict[str, Any], 
                                      connection_type: str = "database") -> str:
        """
        Generate an enhanced schema prompt with business context for LLM
        """
        try:
            prompt_parts = ["=== ENHANCED DATA SCHEMA WITH BUSINESS CONTEXT ===\n"]
            
            # Add business glossary context
            if self.business_glossary:
                prompt_parts.append("BUSINESS GLOSSARY:")
                for term, definition in self.business_glossary.items():
                    prompt_parts.append(f"- {term}: {definition}")
                prompt_parts.append("")
            
            # Add business metrics from database
            metrics = self._get_common_metrics()
            if metrics:
                prompt_parts.append("COMMON BUSINESS METRICS:")
                for metric in metrics:
                    prompt_parts.append(f"- {metric['display_name']}: {metric['formula']}")
                    if metric.get('description'):
                        prompt_parts.append(f"  Description: {metric['description']}")
                prompt_parts.append("")
            
            # Enhanced table information
            if isinstance(raw_schema, dict):
                # Check if this is a database schema format or a single table schema
                if 'tables' in raw_schema:
                    # Database schema with tables
                    for table_name, table_info in raw_schema['tables'].items():
                        columns = table_info.get('columns', [])
                        self._generate_table_prompt(table_name, columns, prompt_parts)
                elif 'columns' in raw_schema:
                    # Single table schema (like CSV) - use actual table name if available
                    if 'table_name' in raw_schema and raw_schema['table_name']:
                        table_name = raw_schema['table_name']
                    elif 'name' in raw_schema and raw_schema['name']:
                        table_name = raw_schema['name']
                    else:
                        table_name = "integrated_data" if connection_type == "integrated" else "csv_data"
                    columns = raw_schema['columns']
                    self._generate_table_prompt(table_name, columns, prompt_parts)
                else:
                    # Legacy database schema format
                    for table_name, columns in raw_schema.items():
                        # Skip non-table items
                        if isinstance(columns, dict) and all(isinstance(v, (str, dict)) for v in columns.values()):
                            self._generate_table_prompt(table_name, columns, prompt_parts)
            elif isinstance(raw_schema, list):
                # CSV/API/Integrated data format as list
                table_name = "integrated_data" if connection_type == "integrated" else "csv_data"
                columns = raw_schema
                self._generate_table_prompt(table_name, columns, prompt_parts)
            
            # Add relationship summary
            relationships = self._get_table_relationships()
            if relationships:
                prompt_parts.append("TABLE RELATIONSHIPS:")
                for rel in relationships:
                    prompt_parts.append(f"- {rel['from_table']}.{rel['from_column']} → {rel['to_table']}.{rel['to_column']} ({rel['relationship_type']})")
                    if rel.get('description'):
                        prompt_parts.append(f"  {rel['description']}")
                prompt_parts.append("")
            
            prompt_parts.append("=== QUERY GENERATION GUIDELINES ===")
            prompt_parts.append("1. Use exact column names as shown (with quotes for spaces)")
            prompt_parts.append("2. Follow the relationship patterns for JOINs")
            prompt_parts.append("3. Use recommended aggregations for measures")
            prompt_parts.append("4. Apply business rules and common filters")
            prompt_parts.append("5. Consider business context when interpreting questions")
            prompt_parts.append("")
            
            return "\n".join(prompt_parts)
            
        except Exception as e:
            logger.error(f"Failed to generate enhanced schema prompt: {e}")
            return f"Error generating schema prompt: {str(e)}"
    
    def _generate_table_prompt(self, table_name: str, columns: Any, prompt_parts: List[str]):
        """Generate prompt section for a single table"""
        try:
            # Get semantic table if exists
            semantic_table = self._get_semantic_table(table_name)
            
            if semantic_table:
                prompt_parts.append(f"TABLE: {semantic_table.display_name} ({table_name})")
                prompt_parts.append(f"Purpose: {semantic_table.description}")
                if semantic_table.business_purpose:
                    prompt_parts.append(f"Business Purpose: {semantic_table.business_purpose}")
            else:
                prompt_parts.append(f"TABLE: {table_name}")
                
            prompt_parts.append("COLUMNS:")
            
            # Handle different column formats
            if isinstance(columns, dict):
                # Standard dictionary format
                column_items = columns.items()
            elif isinstance(columns, list):
                # List format from CSV schema
                column_items = [(col['name'], col['type']) for col in columns if isinstance(col, dict) and 'name' in col and 'type' in col]
            else:
                logger.error(f"Unsupported columns format for table {table_name}: {type(columns)}")
                prompt_parts.append(f"  Error: Unsupported schema format")
                prompt_parts.append("")
                return
            
            # Enhanced column information
            for col_name, col_type in column_items:
                semantic_col = self._get_semantic_column(table_name, col_name) if semantic_table else None
                
                if semantic_col:
                    prompt_parts.append(f"  • {semantic_col.display_name} ({col_name})")
                    prompt_parts.append(f"    Type: {col_type} | Semantic: {semantic_col.semantic_type}")
                    prompt_parts.append(f"    Description: {semantic_col.description}")
                    
                    if semantic_col.sample_values:
                        sample_values = json.loads(semantic_col.sample_values) if isinstance(semantic_col.sample_values, str) else semantic_col.sample_values
                        prompt_parts.append(f"    Sample Values: {', '.join(map(str, sample_values[:5]))}")
                    
                    if semantic_col.common_filters:
                        common_filters = json.loads(semantic_col.common_filters) if isinstance(semantic_col.common_filters, str) else semantic_col.common_filters
                        prompt_parts.append(f"    Common Filters: {', '.join(common_filters)}")
                        
                    if semantic_col.aggregation_default:
                        prompt_parts.append(f"    Default Aggregation: {semantic_col.aggregation_default}")
                        
                    if semantic_col.business_rules:
                        business_rules = json.loads(semantic_col.business_rules) if isinstance(semantic_col.business_rules, str) else semantic_col.business_rules
                        prompt_parts.append(f"    Business Rules: {'; '.join(business_rules)}")
                else:
                    # Fallback for columns without semantic metadata
                    prompt_parts.append(f"  • {col_name}: {col_type}")
            
            prompt_parts.append("")
            
        except Exception as e:
            logger.error(f"Failed to generate table prompt for {table_name}: {e}")
            prompt_parts.append(f"TABLE: {table_name} (error loading metadata)")
            prompt_parts.append("")
    
    def _get_semantic_table(self, table_name: str) -> Optional[SemanticTable]:
        """Get semantic table metadata from database"""
        try:
            return SemanticTable.objects.filter(name=table_name).first()
        except:
            return None
    
    def _get_semantic_column(self, table_name: str, column_name: str) -> Optional[SemanticColumn]:
        """Get semantic column metadata from database"""
        try:
            semantic_table = self._get_semantic_table(table_name)
            if semantic_table:
                return SemanticColumn.objects.filter(
                    semantic_table=semantic_table,
                    name=column_name
                ).first()
            return None
        except:
            return None
    
    def _get_common_metrics(self) -> List[Dict[str, Any]]:
        """Get common business metrics from database with enhanced metadata"""
        try:
            from services.business_metrics_service import BusinessMetricsService
            
            # Get metrics from enhanced service
            metrics_service = BusinessMetricsService()
            metrics = metrics_service.get_metrics_for_llm()
            
            # If enhanced service returns empty, fall back to database
            if not metrics:
                from datasets.models import SemanticMetric
                db_metrics = SemanticMetric.objects.filter(is_active=True)
                metrics = [
                    {
                        'name': metric.name,
                        'display_name': metric.display_name,
                        'description': metric.description or '',
                        'formula': metric.calculation,
                        'category': metric.metric_type,
                        'unit': metric.unit or '',
                        'table': metric.base_table.name if metric.base_table else ''
                    }
                    for metric in db_metrics
                ]
            
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to get common metrics: {e}")
            return []
    
    def _get_table_relationships(self) -> List[Dict[str, str]]:
        """Get table relationships from database - DISABLED (relationships removed)"""
        # Relationships functionality has been disabled
        return []
    
    def _format_business_glossary(self) -> str:
        """Format business glossary for prompt with caching"""
        cache_key = 'business_glossary_formatted'
        formatted = cache.get(cache_key)
        
        if not formatted:
            formatted_list = []
            for term, definition in self.business_glossary.items():
                formatted_list.append(f"- {term}: {definition}")
            formatted = "\n".join(formatted_list)
            cache.set(cache_key, formatted, timeout=3600)  # Cache for 1 hour
        
        return formatted
    
    def validate_metric_formula(self, formula: str) -> Tuple[bool, str]:
        """Validate business metric formulas"""
        try:
            import re
            
            # Basic SQL function validation
            allowed_functions = ['SUM', 'AVG', 'COUNT', 'MIN', 'MAX', 'DISTINCT']
            allowed_operators = ['+', '-', '*', '/', '(', ')', '=', '<', '>', '<=', '>=']
            
            # Check for dangerous keywords
            dangerous_keywords = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE']
            formula_upper = formula.upper()
            
            for keyword in dangerous_keywords:
                if keyword in formula_upper:
                    return False, f"Dangerous keyword '{keyword}' not allowed in formulas"
            
            # Check if formula contains only allowed functions and operators
            # This is a basic check - more sophisticated validation could be added
            if re.search(r'[^a-zA-Z0-9_\s\+\-\*\/\(\)\=\<\>\.\,]', formula):
                return False, "Formula contains invalid characters"
            
            return True, "Formula is valid"
            
        except Exception as e:
            return False, f"Formula validation error: {str(e)}"
    
    def create_semantic_metric(self, name: str, display_name: str, calculation: str, 
                             metric_type: str, description: str = "", 
                             base_table_id: Optional[str] = None, 
                             user_id: Optional[int] = None) -> Optional[str]:
        """Create a new semantic metric with validation"""
        try:
            # Validate formula
            is_valid, validation_msg = self.validate_metric_formula(calculation)
            if not is_valid:
                logger.error(f"Invalid metric formula: {validation_msg}")
                return None
            
            # Get base table if provided
            base_table = None
            if base_table_id:
                try:
                    base_table = SemanticTable.objects.get(id=base_table_id)
                except SemanticTable.DoesNotExist:
                    logger.error(f"Base table {base_table_id} not found")
                    return None
            
            # Create metric with correct field names
            metric = SemanticMetric.objects.create(
                name=name,
                display_name=display_name,
                description=description,
                metric_type=metric_type,  # Fixed: use 'metric_type' not 'category'
                calculation=calculation,  # Fixed: use 'calculation' not 'formula'
                base_table=base_table,
                created_by_id=user_id or 1,
                is_active=True
            )
            
            # Clear cache to refresh metrics
            cache.delete('business_metrics_cache')
            
            logger.info(f"Created semantic metric: {name}")
            return str(metric.pk)
            
        except Exception as e:
            logger.error(f"Failed to create semantic metric: {e}")
            return None
    
    def get_cached_semantic_metadata(self, cache_key: str, fetch_func, timeout: int = 1800) -> Any:
        """Generic caching utility for semantic metadata"""
        cached_data = cache.get(cache_key)
        
        if cached_data is None:
            try:
                cached_data = fetch_func()
                cache.set(cache_key, cached_data, timeout=timeout)
                logger.debug(f"Cached data for key: {cache_key}")
            except Exception as e:
                logger.error(f"Failed to fetch data for cache key {cache_key}: {e}")
                return None
        
        return cached_data
    
    def _add_common_metrics(self):
        """Add common business metrics with enhanced definitions and error handling"""
        # FIXED: Remove hardcoded metrics and generate from actual data
        try:
            # Check if semantic_metrics table exists and has required columns
            from django.db import connection
            with connection.cursor() as cursor:
                cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='semantic_metrics'")
                table_info = cursor.fetchone()
                if not table_info:
                    logger.warning("Semantic metrics table does not exist. Skipping metric creation.")
                    return
                
                # Check for business_owner column
                if 'business_owner' not in table_info[0]:
                    logger.warning("Business_owner column missing from semantic_metrics table. Some metrics may fail.")
        except Exception as schema_error:
            logger.warning(f"Schema validation failed, proceeding with metric creation: {schema_error}")
        
        # ENHANCED: Generate metrics dynamically from actual semantic tables
        self._generate_dynamic_business_metrics()
        
        # Clear cache after adding metrics
        try:
            cache.delete('business_metrics_cache')
        except Exception as cache_error:
            logger.warning(f"Failed to clear metrics cache: {cache_error}")
    
    def _generate_dynamic_business_metrics(self):
        """Generate business metrics dynamically from actual semantic tables and columns - ZERO HARDCODING"""
        try:
            # Get all active semantic tables with their columns
            semantic_tables = SemanticTable.objects.all()
            
            if not semantic_tables:
                logger.info("No semantic tables found, skipping metric generation")
                return
            
            logger.info(f"Generating dynamic business metrics from {semantic_tables.count()} semantic tables")
            
            metrics_created = 0
            
            for semantic_table in semantic_tables:
                try:
                    # Get columns for this table
                    columns = SemanticColumn.objects.filter(semantic_table=semantic_table)
                    
                    if not columns.exists():
                        logger.warning(f"No columns found for table {semantic_table.name}")
                        continue
                    
                    # Categorize columns dynamically
                    numeric_columns = []
                    identifier_columns = []
                    date_columns = []
                    text_columns = []
                    
                    for column in columns:
                        # Categorize by actual data type and semantic type
                        if column.data_type in ['integer', 'float', 'decimal', 'number'] and column.is_measure:
                            numeric_columns.append(column)
                        elif column.semantic_type == 'identifier' or 'id' in column.name.lower():
                            identifier_columns.append(column)
                        elif column.data_type in ['date', 'datetime', 'timestamp'] or 'date' in column.name.lower():
                            date_columns.append(column)
                        elif column.data_type in ['string', 'text', 'varchar'] and column.semantic_type == 'dimension':
                            text_columns.append(column)
                    
                    logger.info(f"Table {semantic_table.name}: {len(numeric_columns)} numeric, {len(identifier_columns)} identifiers, {len(date_columns)} dates, {len(text_columns)} dimensions")
                    
                    # Generate metrics based on actual columns found
                    table_metrics_created = 0
                    
                    # Basic count metric (always create)
                    table_metrics_created += self._create_basic_count_metric(semantic_table)
                    
                    # Generate metrics for numeric measure columns
                    for column in numeric_columns:
                        table_metrics_created += self._create_numeric_metrics(semantic_table, column)
                    
                    # Generate distinct count metrics for identifier/dimension columns
                    for column in identifier_columns + text_columns:
                        table_metrics_created += self._create_distinct_count_metrics(semantic_table, column)
                    
                    # Generate time-based metrics if date columns exist with numeric measures
                    if date_columns and numeric_columns:
                        table_metrics_created += self._create_time_based_metrics(semantic_table, numeric_columns, date_columns)
                    
                    # Generate ratio metrics if multiple numeric columns exist
                    if len(numeric_columns) >= 2:
                        table_metrics_created += self._create_ratio_metrics(semantic_table, numeric_columns)
                    
                    metrics_created += table_metrics_created
                    logger.info(f"Created {table_metrics_created} metrics for table {semantic_table.name}")
                
                except Exception as table_error:
                    logger.error(f"Error generating metrics for table {semantic_table.name}: {table_error}")
                    continue
            
            logger.info(f"Successfully generated {metrics_created} dynamic business metrics total")
            
        except Exception as e:
            logger.error(f"Error generating dynamic business metrics: {e}")
            logger.info("Skipping metric generation due to errors")
    
    def _create_basic_count_metric(self, semantic_table: SemanticTable) -> int:
        """Create basic record count metric for any table"""
        try:
            metric_name = f"{semantic_table.name}_record_count"
            display_name = f"{semantic_table.display_name} Record Count"
            
            # Check if metric already exists
            if SemanticMetric.objects.filter(name=metric_name, base_table=semantic_table).exists():
                return 0
            
            SemanticMetric.objects.create(
                name=metric_name,
                display_name=display_name,
                description=f'Total number of records in {semantic_table.display_name}',
                metric_type='simple',
                calculation='COUNT(*)',
                base_table=semantic_table,
                created_by_id=1,
                unit='count',
                is_active=True
            )
            return 1
        except Exception as e:
            logger.error(f"Error creating basic count metric for {semantic_table.name}: {e}")
            return 0
    
    def _create_numeric_metrics(self, semantic_table: SemanticTable, column: SemanticColumn) -> int:
        """Create metrics for numeric columns"""
        metrics_created = 0
        table_name = semantic_table.name
        column_name = column.name
        
        # Define metric templates for numeric columns
        metric_templates = [
            {
                'name_suffix': '_total',
                'display_suffix': ' Total',
                'description_template': f'Total sum of {column.display_name}',
                'calculation_template': f'SUM("{column_name}")',
                'metric_type': 'simple'
            },
            {
                'name_suffix': '_average',
                'display_suffix': ' Average',
                'description_template': f'Average value of {column.display_name}',
                'calculation_template': f'AVG("{column_name}")',
                'metric_type': 'simple'
            },
            {
                'name_suffix': '_max',
                'display_suffix': ' Maximum',
                'description_template': f'Maximum value of {column.display_name}',
                'calculation_template': f'MAX("{column_name}")',
                'metric_type': 'simple'
            }
        ]
        
        for template in metric_templates:
            try:
                metric_name = f"{table_name}_{column_name}{template['name_suffix']}"
                
                # Check if metric already exists
                existing = SemanticMetric.objects.filter(name=metric_name).first()
                if not existing:
                    SemanticMetric.objects.create(
                        name=metric_name,
                        display_name=f"{column.display_name}{template['display_suffix']}",
                        description=template['description_template'],
                        metric_type=template['metric_type'],
                        calculation=template['calculation_template'],
                        base_table=semantic_table,  # FIXED: Link to semantic table
                        created_by_id=1,  # System user
                        is_active=True
                    )
                    # Link to the column
                    metric = SemanticMetric.objects.get(name=metric_name)
                    metric.dependent_columns.add(column)
                    
                    metrics_created += 1
                    logger.info(f"Created numeric metric: {metric_name}")
                
            except Exception as metric_error:
                logger.warning(f"Failed to create numeric metric for {column_name}: {metric_error}")
                continue
        
        return metrics_created
    
    def _create_count_metrics(self, semantic_table: SemanticTable, column: SemanticColumn) -> int:
        """Create count metrics for identifier columns"""
        metrics_created = 0
        table_name = semantic_table.name
        column_name = column.name
        
        try:
            metric_name = f"{table_name}_{column_name}_count"
            
            # Check if metric already exists
            existing = SemanticMetric.objects.filter(name=metric_name).first()
            if not existing:
                SemanticMetric.objects.create(
                    name=metric_name,
                    display_name=f"{column.display_name} Count",
                    description=f'Total count of {column.display_name}',
                    metric_type='simple',
                    calculation=f'COUNT(DISTINCT "{column_name}")',
                    base_table=semantic_table,  # FIXED: Link to semantic table
                    created_by_id=1,  # System user
                    is_active=True
                )
                # Link to the column
                metric = SemanticMetric.objects.get(name=metric_name)
                metric.dependent_columns.add(column)
                
                metrics_created += 1
                logger.info(f"Created count metric: {metric_name}")
        
        except Exception as metric_error:
            logger.warning(f"Failed to create count metric for {column_name}: {metric_error}")
        
        return metrics_created
    
    def _create_distinct_count_metrics(self, semantic_table: SemanticTable, column: SemanticColumn) -> int:
        """Create distinct count metrics for identifier/dimension columns - ZERO HARDCODING"""
        try:
            # Distinct count metric
            metric_name = f"{semantic_table.name}_{column.name}_distinct_count"
            display_name = f"{column.display_name} Distinct Count"
            
            # Check if metric already exists
            if SemanticMetric.objects.filter(name=metric_name, base_table=semantic_table).exists():
                return 0
            
            SemanticMetric.objects.create(
                name=metric_name,
                display_name=display_name,
                description=f'Number of unique {column.display_name} values',
                metric_type='simple',
                calculation=f'COUNT(DISTINCT "{column.name}")',
                base_table=semantic_table,
                created_by_id=1,
                unit='count',
                is_active=True
            )
            return 1
            
        except Exception as e:
            logger.error(f"Error creating distinct count metric for {column.name}: {e}")
            return 0
    
    def _create_ratio_metrics(self, semantic_table: SemanticTable, numeric_columns: list) -> int:
        """Create ratio metrics between numeric columns - ZERO HARDCODING"""
        metrics_created = 0
        
        try:
            # Only create ratios for the first few numeric columns to avoid explosion
            if len(numeric_columns) >= 2:
                col1 = numeric_columns[0]
                col2 = numeric_columns[1]
                
                # Create a ratio metric
                metric_name = f"{semantic_table.name}_{col1.name}_to_{col2.name}_ratio"
                display_name = f"{col1.display_name} to {col2.display_name} Ratio"
                
                # Check if metric already exists
                if SemanticMetric.objects.filter(name=metric_name, base_table=semantic_table).exists():
                    return 0
                
                SemanticMetric.objects.create(
                    name=metric_name,
                    display_name=display_name,
                    description=f'Ratio of {col1.display_name} to {col2.display_name}',
                    metric_type='ratio',
                    calculation=f'SUM("{col1.name}") / NULLIF(SUM("{col2.name}"), 0)',
                    base_table=semantic_table,
                    created_by_id=1,
                    unit='ratio',
                    is_active=True
                )
                metrics_created += 1
                
        except Exception as e:
            logger.error(f"Error creating ratio metrics: {e}")
        
        return metrics_created
    
    def _create_time_based_metrics(self, semantic_table: SemanticTable, numeric_columns: list, date_columns: list) -> int:
        """Create time-based metrics combining numeric and date columns"""
        metrics_created = 0
        table_name = semantic_table.name
        
        try:
            # Create monthly and yearly aggregations for the first numeric column and first date column
            if numeric_columns and date_columns:
                numeric_col = numeric_columns[0]
                date_col = date_columns[0]
                
                time_metrics = [
                    {
                        'name_suffix': '_monthly_total',
                        'display_suffix': ' Monthly Total',
                        'description': f'Monthly total of {numeric_col.display_name}',
                        'calculation': f'SUM("{numeric_col.name}") GROUP BY EXTRACT(YEAR FROM "{date_col.name}"), EXTRACT(MONTH FROM "{date_col.name}")',
                        'metric_type': 'calculated'
                    },
                    {
                        'name_suffix': '_yearly_total',
                        'display_suffix': ' Yearly Total',
                        'description': f'Yearly total of {numeric_col.display_name}',
                        'calculation': f'SUM("{numeric_col.name}") GROUP BY EXTRACT(YEAR FROM "{date_col.name}")',
                        'metric_type': 'calculated'
                    }
                ]
                
                for template in time_metrics:
                    try:
                        metric_name = f"{table_name}_{numeric_col.name}{template['name_suffix']}"
                        
                        # Check if metric already exists
                        existing = SemanticMetric.objects.filter(name=metric_name).first()
                        if not existing:
                            SemanticMetric.objects.create(
                                name=metric_name,
                                display_name=f"{numeric_col.display_name}{template['display_suffix']}",
                                description=template['description'],
                                metric_type=template['metric_type'],
                                calculation=template['calculation'],
                                base_table=semantic_table,  # FIXED: Link to semantic table
                                created_by_id=1,  # System user
                                is_active=True
                            )
                            # Link to both columns
                            metric = SemanticMetric.objects.get(name=metric_name)
                            metric.dependent_columns.add(numeric_col, date_col)
                            
                            metrics_created += 1
                            logger.info(f"Created time-based metric: {metric_name}")
                    
                    except Exception as metric_error:
                        logger.warning(f"Failed to create time-based metric: {metric_error}")
                        continue
        
        except Exception as e:
            logger.error(f"Error creating time-based metrics: {e}")
        
        return metrics_created
    
    def _create_sample_metrics(self):
        """Create sample metrics as fallback when no semantic tables exist"""
        try:
            sample_metrics = [
                {
                    'name': 'total_records',
                    'display_name': 'Total Records',
                    'description': 'Total number of records across all tables',
                    'calculation': 'COUNT(*)',
                    'metric_type': 'simple'
                },
                {
                    'name': 'data_quality_score',
                    'display_name': 'Data Quality Score',
                    'description': 'Overall data quality percentage',
                    'calculation': '(COUNT(*) - COUNT(NULL)) / COUNT(*) * 100',
                    'metric_type': 'calculated'
                }
            ]
            
            for metric_def in sample_metrics:
                try:
                    # Check if metric already exists
                    existing = SemanticMetric.objects.filter(name=metric_def['name']).first()
                    if not existing:
                        SemanticMetric.objects.create(
                            name=metric_def['name'],
                            display_name=metric_def['display_name'],
                            description=metric_def['description'],
                            metric_type=metric_def['metric_type'],
                            calculation=metric_def['calculation'],
                            created_by_id=1,  # System user
                            is_active=True
                        )
                        logger.info(f"Created sample metric: {metric_def['name']}")
                except Exception as metric_error:
                    logger.warning(f"Failed to create sample metric {metric_def['name']}: {metric_error}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error creating sample metrics: {e}")
    
    def regenerate_business_metrics_for_table(self, table_name: str) -> bool:
        """Regenerate business metrics for a specific table"""
        try:
            logger.info(f"Regenerating business metrics for table: {table_name}")
            
            # Find the semantic table
            semantic_table = SemanticTable.objects.filter(name=table_name).first()
            if not semantic_table:
                logger.warning(f"Semantic table not found: {table_name}")
                return False
            
            # Delete existing metrics for this table
            deleted_count = SemanticMetric.objects.filter(base_table=semantic_table).count()
            SemanticMetric.objects.filter(base_table=semantic_table).delete()
            logger.info(f"Deleted {deleted_count} existing metrics for table {table_name}")
            
            # Get columns for this table
            columns = SemanticColumn.objects.filter(semantic_table=semantic_table)
            numeric_columns = [col for col in columns if col.data_type in ['integer', 'float'] and col.is_measure]
            count_columns = [col for col in columns if col.semantic_type == 'identifier']
            date_columns = [col for col in columns if col.data_type in ['date', 'datetime']]
            
            metrics_created = 0
            
            # Generate new metrics
            for column in numeric_columns:
                metrics_created += self._create_numeric_metrics(semantic_table, column)
            
            for column in count_columns:
                metrics_created += self._create_count_metrics(semantic_table, column)
            
            if date_columns and numeric_columns:
                metrics_created += self._create_time_based_metrics(semantic_table, numeric_columns, date_columns)
            
            logger.info(f"Successfully regenerated {metrics_created} business metrics for table {table_name}")
            
            # Clear cache
            cache.delete('business_metrics_cache')
            
            return metrics_created > 0
            
        except Exception as e:
            logger.error(f"Error regenerating business metrics for table {table_name}: {e}")
            return False
    
    def enhance_query_with_business_context(self, query: str, available_metrics: List[Dict]) -> str:
        """Enhance SQL query with business context and metric definitions"""
        enhanced_query = query
        
        try:
            # Add business metric context
            if available_metrics:
                metric_context = "\n-- Available Business Metrics:\n"
                for metric in available_metrics[:5]:  # Limit to top 5 metrics
                    metric_context += f"-- {metric['display_name']}: {metric['calculation']}\n"
                enhanced_query = metric_context + enhanced_query
            
            # Add common business filters based on query content
            query_lower = query.lower()
            
            # Add revenue filters
            if 'revenue' in query_lower and 'where' not in query_lower:
                enhanced_query = enhanced_query.replace(';', ' WHERE revenue > 0;')
            
            # Add date filters for time-based queries
            if any(word in query_lower for word in ['trend', 'over time', 'monthly', 'yearly']) and 'order by' not in query_lower:
                if 'date' in query_lower:
                    enhanced_query = enhanced_query.replace(';', ' ORDER BY date;')
                elif 'created_at' in query_lower:
                    enhanced_query = enhanced_query.replace(';', ' ORDER BY created_at;')
            
            return enhanced_query
            
        except Exception as e:
            logger.error(f"Failed to enhance query with business context: {e}")
            return query
    
    def get_business_glossary_suggestions(self, query_text: str) -> List[str]:
        """Get relevant business glossary suggestions based on query text"""
        try:
            suggestions = []
            query_lower = query_text.lower()
            
            for term, definition in self.business_glossary.items():
                if any(word in query_lower for word in term.lower().split()):
                    suggestions.append(f"{term}: {definition}")
            
            return suggestions[:3]  # Return top 3 suggestions
            
        except Exception as e:
            logger.error(f"Failed to get business glossary suggestions: {e}")
            return []
    
    def validate_semantic_metadata(self, metadata: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate semantic metadata for consistency and completeness"""
        errors = []
        
        try:
            # Check required fields
            required_fields = ['name', 'display_name', 'description', 'data_type']
            for field in required_fields:
                if not metadata.get(field):
                    errors.append(f"Missing required field: {field}")
            
            # Validate data types
            valid_data_types = ['string', 'integer', 'float', 'boolean', 'date', 'datetime']
            if metadata.get('data_type') not in valid_data_types:
                errors.append(f"Invalid data type: {metadata.get('data_type')}")
            
            # Validate aggregation type if provided
            if metadata.get('aggregation_default'):
                valid_aggregations = ['sum', 'avg', 'count', 'min', 'max', 'count_distinct']
                if metadata.get('aggregation_default') not in valid_aggregations:
                    errors.append(f"Invalid aggregation type: {metadata.get('aggregation_default')}")
            
            return len(errors) == 0, errors
            
        except Exception as e:
            errors.append(f"Validation error: {str(e)}")
            return False, errors
    
    def get_semantic_metadata_version(self) -> str:
        """Get version information for semantic metadata"""
        try:
            # This could be enhanced to track actual versioning
            from django.utils import timezone
            return f"v1.0_{timezone.now().strftime('%Y%m%d')}"
        except Exception:
            return "v1.0_unknown"
    
    def auto_generate_metadata_from_data_integration(self, integration_service) -> bool:
        """
        Auto-generate semantic metadata from data integration sources with validation
        """
        try:
            summary = integration_service.get_data_sources_summary()
            
            if summary['total_sources'] == 0:
                logger.warning("No data sources available for semantic metadata generation")
                return False
            
            logger.info(f"Starting semantic metadata generation for {summary['total_sources']} data sources")
            
            with transaction.atomic():
                tables_created = 0
                metrics_created = 0
                
                # Process each data source
                for source_info in summary['sources']:
                    try:
                        source_id = source_info['id']
                        
                        # ENHANCED: Use proper table name resolution
                        from utils.table_name_helper import get_integrated_table_name
                        from datasets.models import DataSource
                        
                        # Get the actual DataSource object to use proper table naming
                        try:
                            data_source = DataSource.objects.get(id=source_id)
                            table_name = get_integrated_table_name(data_source)
                            logger.info(f"Resolved table name for source {source_id}: {table_name}")
                        except DataSource.DoesNotExist:
                            logger.warning(f"DataSource {source_id} not found, using fallback table name")
                            table_name = f"source_{source_id}"
                        
                        # Table existence pre-check before attempting to retrieve data
                        if not integration_service.check_table_exists(table_name):
                            logger.warning(f"Table {table_name} does not exist in integrated database, skipping semantic generation for source {source_id}")
                            continue
                        
                        # Get integrated data with proper error handling
                        try:
                            table_data = integration_service.get_integrated_data(table_name)
                            if table_data.empty:
                                logger.warning(f"No data found for table {table_name}, skipping semantic generation")
                                continue
                            
                            logger.info(f"Retrieved {len(table_data)} rows for semantic processing from {table_name}")
                        except Exception as data_error:
                            logger.error(f"Failed to retrieve data for table {table_name}: {data_error}")
                            continue
                        
                        # Validate schema_info for object types
                        if hasattr(source_info, 'schema_info') and source_info.get('schema_info'):
                            schema_info = source_info['schema_info']
                            if 'tables' in schema_info:
                                for table_data_info in schema_info['tables'].values():
                                    if 'columns' in table_data_info:
                                        for col_info in table_data_info['columns'].values():
                                            if isinstance(col_info, dict) and col_info.get('type') == 'object':
                                                logger.warning(f"Found object type in schema for {source_info['name']}, defaulting to string")
                                                col_info['type'] = 'string'
                        
                        # Data validation before proceeding with semantic table creation
                        if len(table_data.columns) == 0:
                            logger.warning(f"Table {table_name} has no columns, skipping semantic generation")
                            continue
                        
                        # Generate semantic metadata for this table with enhanced error handling
                        try:
                            success = self._create_semantic_table_from_dataframe(
                                table_name, source_info['name'], table_data, source_info['type'], source_id
                            )
                            if success:
                                tables_created += 1
                                logger.info(f"Successfully created semantic table for {source_info['name']}")
                            else:
                                logger.warning(f"Semantic table creation returned false for {source_info['name']} - no semantic objects were created")
                        except Exception as table_error:
                            logger.error(f"Exception creating semantic table for {source_info['name']}: {table_error}")
                            continue
                        
                    except Exception as source_error:
                        logger.error(f"Error processing source {source_info.get('name', 'unknown')}: {source_error}")
                        continue
                
                # Add common metrics with error tracking
                try:
                    metrics_before = self._count_existing_metrics()
                    self._add_common_metrics()
                    metrics_after = self._count_existing_metrics()
                    metrics_created = metrics_after - metrics_before
                    logger.info(f"Added {metrics_created} common metrics")
                except Exception as metrics_error:
                    logger.error(f"Error adding common metrics: {metrics_error}")
                
                # Enhanced result reporting
                skipped_sources = summary['total_sources'] - tables_created
                logger.info(f"Semantic metadata generation completed: {tables_created} tables created, {metrics_created} metrics created, {skipped_sources} sources skipped")
                
                if skipped_sources > 0:
                    logger.info(f"Sources were skipped due to missing data - check that ETL processes have completed")
                
                # Return True only if we created at least some semantic objects
                success = tables_created > 0 or metrics_created > 0
                if not success:
                    logger.warning("No semantic objects were created - this may indicate data integration issues")
                
                return success
            
        except Exception as e:
            logger.error(f"Error auto-generating semantic metadata: {e}")
            return False
    
    def _create_semantic_table_from_dataframe(self, table_name: str, display_name: str, 
                                            df: pd.DataFrame, source_type: str, data_source_id: Optional[str] = None) -> bool:
        """Create semantic table metadata from DataFrame with enhanced error handling"""
        try:
            logger.info(f"Creating semantic table: {table_name} ({display_name})")
            logger.info(f"Data types for {table_name}: {df.dtypes.to_dict()}")
            if all(dtype == 'object' for dtype in df.dtypes):
                logger.warning(f"All columns in {table_name} are 'object' dtype. ETL type conversion may be missing!")
            # ENHANCED: Better validation and error handling
            if df.empty:
                logger.warning(f"DataFrame is empty for table {table_name}")
                return False
            if len(df.columns) == 0:
                logger.warning(f"DataFrame has no columns for table {table_name}")
                return False
            
            # FIXED: Get the DataSource object - this was the missing piece!
            from datasets.models import DataSource
            data_source = None
            if data_source_id:
                try:
                    data_source = DataSource.objects.get(id=data_source_id)
                except DataSource.DoesNotExist:
                    logger.warning(f"DataSource {data_source_id} not found")
            
            if not data_source:
                # Try to find by display name as fallback
                try:
                    data_source = DataSource.objects.filter(name=display_name).first()
                    if not data_source:
                        logger.error(f"Cannot create semantic table without valid data_source for {display_name}")
                        return False
                except Exception as e:
                    logger.error(f"Failed to find DataSource: {e}")
                    return False
            
            # Create or update semantic table with transaction safety
            semantic_table, created = SemanticTable.objects.get_or_create(
                data_source=data_source,  # FIXED: Add required data_source field
                name=table_name,
                defaults={
                    'display_name': display_name,
                    'description': f"Data from {source_type} source: {display_name}",
                    'business_purpose': self._infer_business_purpose(table_name, df.columns.tolist(), source_type)
                }
            )
            
            if created:
                logger.info(f"Created new semantic table: {table_name}")
            else:
                logger.info(f"Updated existing semantic table: {table_name}")
            
            # Create semantic columns with detailed error tracking
            columns_created = 0
            columns_failed = 0
            
            for col_name in df.columns:
                try:
                    col_data = df[col_name]
                    data_type = str(col_data.dtype)
                    
                    # Infer semantic type
                    semantic_type = self._infer_semantic_type(col_name, col_data)
                    
                    # Generate description
                    description = self._generate_column_description(col_name, semantic_type, data_type)
                    
                    # Get sample values
                    sample_values = self._get_sample_values(col_data)
                    
                    # Generate common filters
                    common_filters = self._generate_common_filters(col_name, col_data, semantic_type)
                    
                    # Generate business rules
                    business_rules = self._generate_business_rules(col_name, col_data, semantic_type)
                    
                    # Default aggregation
                    aggregation_default = self._get_default_aggregation(semantic_type, data_type)
                    
                    # Map data type to valid semantic data type
                    semantic_data_type = data_type
                    if data_type == 'object':
                        semantic_data_type = 'string'
                        logger.debug(f"Converted object data type to string for column {col_name}")
                    elif data_type == 'datetime64[ns]':
                        semantic_data_type = 'datetime'
                        logger.debug(f"Converted datetime64[ns] data type to datetime for column {col_name}")
                    elif not self._validate_semantic_data_type(data_type):
                        semantic_data_type = 'string'
                        logger.warning(f"Invalid data type '{data_type}' for column {col_name}, defaulting to 'string'")
                    
                    # Ensure aggregation_default is either a valid choice or None
                    if aggregation_default and aggregation_default not in ['sum', 'avg', 'count', 'min', 'max', 'count_distinct']:
                        logger.warning(f"Invalid aggregation default '{aggregation_default}' for column {col_name}, setting to None")
                        aggregation_default = None
                    
                    # ENHANCED: Check if this column has ETL transformations applied
                    etl_enriched = self._check_etl_transformation(table_name, col_name)
                    
                    # Create or update semantic column with enhanced error handling
                    try:
                        semantic_column, col_created = SemanticColumn.objects.update_or_create(
                            semantic_table=semantic_table,
                            name=col_name,
                            defaults={
                                'display_name': col_name.replace("_", " ").title(),
                                'description': description,
                                'data_type': semantic_data_type,
                                'semantic_type': semantic_type.value,
                                'sample_values': json.dumps(sample_values),
                                'common_filters': json.dumps(common_filters),
                                'business_rules': json.dumps(business_rules),
                                'aggregation_default': aggregation_default,
                                'is_nullable': bool(col_data.isnull().any()),
                                'etl_enriched': etl_enriched  # ENHANCED: Set ETL enrichment flag
                            }
                        )
                    except Exception as db_error:
                        # Specific handling for database constraint errors
                        error_str = str(db_error).lower()
                        if 'not null constraint failed' in error_str and 'aggregation_default' in error_str:
                            logger.error(f"NOT NULL constraint failed for aggregation_default in column '{col_name}'. This should not happen after migration. Error: {db_error}")
                            # Retry with explicit None
                            semantic_column, col_created = SemanticColumn.objects.update_or_create(
                                semantic_table=semantic_table,
                                name=col_name,
                                defaults={
                                    'display_name': col_name.replace("_", " ").title(),
                                    'description': description,
                                    'data_type': semantic_data_type,
                                    'semantic_type': semantic_type.value,
                                    'sample_values': json.dumps(sample_values),
                                    'common_filters': json.dumps(common_filters),
                                    'business_rules': json.dumps(business_rules),
                                    'aggregation_default': None,  # Explicitly set to None
                                    'is_nullable': bool(col_data.isnull().any())
                                }
                            )
                        else:
                            raise  # Re-raise if it's a different error
                    
                    columns_created += 1
                    if col_created:
                        logger.debug(f"Created semantic column: {col_name}")
                    else:
                        logger.debug(f"Updated semantic column: {col_name}")
                    
                except Exception as col_error:
                    columns_failed += 1
                    logger.error(f"Failed to create semantic column '{col_name}': {col_error}")
                    continue
            
            logger.info(f"Semantic table creation completed for {table_name}: {columns_created} columns created, {columns_failed} failed")
            
            # Return True if we created the table and at least some columns
            return columns_created > 0
            
        except Exception as e:
            logger.error(f"Failed to create semantic table metadata for {table_name}: {e}")
            return False
    
    def _validate_semantic_data_type(self, data_type: str) -> bool:
        """Validate if a data type is acceptable for semantic columns"""
        valid_types = [
            'string', 'integer', 'float', 'boolean', 'date', 'datetime',
            'int64', 'float64', 'object', 'bool', 'datetime64', 'datetime64[ns]'
        ]
        return data_type.lower() in [t.lower() for t in valid_types]
    
    def _count_existing_metrics(self) -> int:
        """Count existing semantic metrics"""
        try:
            return SemanticMetric.objects.count()
        except Exception:
            return 0
    
    def _count_existing_tables(self) -> int:
        """Count existing semantic tables"""
        try:
            return SemanticTable.objects.count()
        except Exception:
            return 0
    
    def _infer_semantic_type(self, col_name: str, col_data: pd.Series) -> DataType:
        """Infer semantic type from column name and data"""
        col_name_lower = col_name.lower()
        
        # Identifier patterns
        if "id" in col_name_lower or col_name_lower.endswith("_key"):
            return DataType.IDENTIFIER
        
        # Date patterns
        if any(word in col_name_lower for word in ["date", "time", "created", "updated", "timestamp"]):
            return DataType.DATE
        
        # Measure patterns
        if any(word in col_name_lower for word in ["amount", "price", "cost", "revenue", "sales", "profit", "total", "sum", "count", "value"]):
            return DataType.MEASURE
        
        # Check data type
        if pd.api.types.is_numeric_dtype(col_data) and not col_name_lower.endswith("_id"):
            return DataType.MEASURE
        
        # Default to dimension
        return DataType.DIMENSION
    
    def _generate_column_description(self, col_name: str, semantic_type: DataType, data_type: str) -> str:
        """Generate a description for a column"""
        base_name = col_name.replace("_", " ").lower()
        
        if semantic_type == DataType.IDENTIFIER:
            return f"Unique identifier for {base_name.replace(' id', '')}"
        elif semantic_type == DataType.DATE:
            return f"Date/time when {base_name.replace(' date', '').replace(' time', '')} occurred"
        elif semantic_type == DataType.MEASURE:
            return f"Numerical value representing {base_name}"
        else:
            return f"Descriptive attribute for {base_name}"
    
    def _get_sample_values(self, col_data: pd.Series, max_samples: int = 5) -> List[str]:
        """Get sample values from column data"""
        try:
            # Get unique non-null values
            unique_values = col_data.dropna().unique()
            
            # Convert to strings and take first few
            sample_values = []
            for val in unique_values[:max_samples]:
                sample_values.append(str(val))
            
            return sample_values
        except:
            return []
    
    def _generate_common_filters(self, col_name: str, col_data: pd.Series, semantic_type: DataType) -> List[str]:
        """Generate common filter patterns for a column"""
        filters = []
        
        if semantic_type == DataType.DATE:
            filters.extend([
                f"YEAR({col_name}) = 2023",
                f"{col_name} >= '2023-01-01'",
                f"{col_name} < CURRENT_DATE"
            ])
        elif semantic_type == DataType.DIMENSION:
            # Get common values for filter suggestions
            try:
                top_values = col_data.value_counts().head(3).index.tolist()
                for value in top_values:
                    if isinstance(value, str) and len(value) < 20:
                        filters.append(f"{col_name} = '{value}'")
            except:
                pass
            
            filters.append(f"{col_name} IS NOT NULL")
            
        elif semantic_type == DataType.MEASURE:
            filters.extend([
                f"{col_name} > 0",
                f"{col_name} IS NOT NULL"
            ])
        
        return filters[:3]  # Limit to 3 filters
    
    def _generate_business_rules(self, col_name: str, col_data: pd.Series, semantic_type: DataType) -> List[str]:
        """Generate business rules for a column"""
        rules = []
        
        # Check for nulls
        if col_data.isnull().any():
            rules.append("Can contain null values")
        else:
            rules.append("Cannot be null")
        
        # Type-specific rules
        if semantic_type == DataType.IDENTIFIER:
            rules.append("Should be unique")
            if pd.api.types.is_numeric_dtype(col_data):
                rules.append("Always positive")
        elif semantic_type == DataType.MEASURE:
            if pd.api.types.is_numeric_dtype(col_data):
                min_val = col_data.min()
                if min_val >= 0:
                    rules.append("Always positive or zero")
                rules.append("Numeric values only")
        elif semantic_type == DataType.DATE:
            rules.append("Valid date format required")
        
        return rules
    
    def _get_default_aggregation(self, semantic_type: DataType, data_type: str) -> Optional[str]:
        """Get default aggregation for a column"""
        if semantic_type == DataType.MEASURE:
            return "SUM"
        elif semantic_type == DataType.IDENTIFIER:
            return "COUNT"
        elif semantic_type == DataType.DATE:
            return "MIN"
        
        return None
    
    def _infer_business_purpose(self, table_name: str, columns: List[str], source_type: str) -> str:
        """Infer the business purpose of a table"""
        col_names = [col.lower() for col in columns]
        
        # Pattern matching for business purposes
        if any(word in " ".join(col_names) for word in ["customer", "client"]):
            return "Customer relationship and contact management"
        elif any(word in " ".join(col_names) for word in ["order", "sale", "revenue", "transaction"]):
            return "Sales and transaction tracking"
        elif any(word in " ".join(col_names) for word in ["product", "item", "inventory"]):
            return "Product and inventory management"
        elif any(word in " ".join(col_names) for word in ["employee", "staff", "user"]):
            return "Human resources and user management"
        else:
            return f"Business data from {source_type} source"
    
    def _is_complex_query(self, query: str) -> bool:
        """
        Detect if a query is too complex for Ollama to handle reliably
        """
        query_lower = query.lower()
        
        # Complex query indicators
        complex_patterns = [
            ('compare' in query_lower and any(word in query_lower for word in ['vs', 'to', 'with'])),
            (len([year for year in ['2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023'] if year in query]) > 1),
            ('top' in query_lower and 'profit' in query_lower and any(region in query_lower for region in ['south', 'north', 'east', 'west']) and any(year in query for year in ['2015', '2016', '2017', '2018', '2019'])),
            (query_lower.count('and') > 2),
            (query_lower.count('in') > 2),
            ('breakdown' in query_lower or 'detailed' in query_lower),
            ('correlation' in query_lower or 'relationship' in query_lower),
        ]
        
        return sum(complex_patterns) >= 2  # 2 or more complexity indicators
    
    def _generate_query_breakdown(self, query: str) -> str:
        """
        Generate a helpful breakdown for complex queries
        """
        try:
            query_lower = query.lower()
            
            breakdown = f"This query appears complex. Let me help you break it down into simpler parts:\n\n"
            
            # Detect what the user wants
            if 'compare' in query_lower:
                breakdown += "For comparison queries, try asking:\n"
                breakdown += "1. First get data for one condition: \"top 3 selling items in south in 2015\"\n"
                breakdown += "2. Then get data for another condition: \"top 3 selling items in south in 2016\"\n"
                breakdown += "3. You can then compare the results\n\n"
                
            elif 'top' in query_lower and any(year in query for year in ['2015', '2016', '2017', '2018']):
                breakdown += "For top items with year filters, try:\n"
                breakdown += "\"Show me the top 3 products by sales in South region in 2015\"\n\n"
                
            else:
                breakdown += "Try breaking this into smaller questions:\n"
                breakdown += "• Ask about one metric at a time\n"
                breakdown += "• Focus on one region or time period\n"
                breakdown += "• Start with simpler queries and build up\n\n"
            
            breakdown += "**Suggested simpler alternatives:**\n"
            
            # Extract components and suggest alternatives
            if 'profit' in query_lower:
                breakdown += "• \"total profit in south region\"\n"
                breakdown += "• \"top 5 products by profit\"\n"
                
            if 'sales' in query_lower:
                breakdown += "• \"total sales in 2015\"\n"
                breakdown += "• \"sales by region\"\n"
                
            if any(region in query_lower for region in ['south', 'north', 'east', 'west']):
                region = next(region for region in ['south', 'north', 'east', 'west'] if region in query_lower)
                breakdown += f"• \"sales in {region} region\"\n"
                
            breakdown += "\nOnce you get individual results, you can analyze them together!"
            
            return breakdown
            
        except Exception as e:
            logger.error(f"Error generating query breakdown: {e}")
            return "This query is quite complex. Try breaking it into simpler parts, such as asking about one metric or region at a time."

    def get_semantic_schema_for_table(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get semantic schema with ETL-transformed types for a specific table
        CRITICAL: This provides the ETL context that LLM needs
        FIXED: Now properly maps DuckDB table names to semantic table names
        """
        try:
            from datasets.models import SemanticTable, SemanticColumn
            
            # FIXED: Handle DuckDB table name to semantic table name mapping
            semantic_table = None
            
            # First, try direct lookup
            semantic_table = SemanticTable.objects.filter(name=table_name).first()
            
            if not semantic_table:
                # Extract UUID from DuckDB table name (e.g., "ds_1b06f79ff18940fca287183c7d3b73c4")
                # and look for semantic table with format "semantic_<uuid>"
                if table_name.startswith('ds_'):
                    uuid_part = table_name[3:]  # Remove 'ds_' prefix
                    semantic_table_name = f"semantic_{uuid_part}"
                    semantic_table = SemanticTable.objects.filter(name=semantic_table_name).first()
                    logger.info(f"Mapped DuckDB table '{table_name}' to semantic table '{semantic_table_name}'")
                
                # Also try mapping from data source table name
                if not semantic_table:
                    from datasets.models import DataSource
                    # Find data source by table name patterns
                    for ds in DataSource.objects.all():
                        if hasattr(ds, 'table_name') and ds.table_name:
                            # Extract UUID from data source table name
                            if '_' in ds.table_name:
                                uuid_candidates = ds.table_name.split('_')
                                for uuid_part in uuid_candidates:
                                    if len(uuid_part) == 32:  # UUID without hyphens
                                        semantic_table_name = f"semantic_{uuid_part}"
                                        semantic_table = SemanticTable.objects.filter(name=semantic_table_name).first()
                                        if semantic_table:
                                            logger.info(f"Found semantic table via data source mapping: {semantic_table_name}")
                                            break
                            if semantic_table:
                                break
            
            if not semantic_table:
                logger.warning(f"No semantic table found for DuckDB table: {table_name}")
                return None
            
            # Get all columns with semantic metadata
            columns = SemanticColumn.objects.filter(semantic_table=semantic_table)
            
            schema = {
                'table_name': table_name,  # Keep original DuckDB table name for SQL generation
                'semantic_table_name': semantic_table.name,  # Add semantic table name for reference
                'display_name': semantic_table.display_name,
                'description': semantic_table.description,
                'business_purpose': semantic_table.business_purpose,
                'columns': []
            }
            
            for col in columns:
                column_info = {
                    'name': col.name,
                    'display_name': col.display_name,
                    'description': col.description,
                    'data_type': col.data_type,
                    'semantic_type': col.semantic_type,
                    'is_measure': col.is_measure,
                    'is_dimension': col.is_dimension,
                    'sample_values': col.sample_values,
                    'business_rules': col.business_rules,
                    'etl_enriched': col.etl_enriched
                }
                schema['columns'].append(column_info)
            
            logger.info(f"Successfully retrieved semantic schema for table: {table_name} (semantic: {semantic_table.name})")
            return schema
            
        except Exception as e:
            logger.error(f"Error getting semantic schema for {table_name}: {e}")
            return None
    
    def _format_etl_type_context(self, semantic_schema: Dict[str, Any]) -> str:
        """
        Format ETL-transformed data type information for LLM context
        CRITICAL: This tells LLM about proper data types after ETL transformations
        """
        etl_info = []
        
        for col in semantic_schema.get('columns', []):
            if col.get('etl_enriched', False):
                etl_info.append(f"- {col['name']}: {col['data_type']} (ETL-transformed from original)")
            else:
                etl_info.append(f"- {col['name']}: {col['data_type']} (original type)")
        
        if etl_info:
            return "ETL-Transformed Data Types:\n" + "\n".join(etl_info)
        else:
            return "No ETL transformations applied to this table."
    
    def _format_column_descriptions(self, semantic_schema: Dict[str, Any]) -> str:
        """
        Format semantic column descriptions for LLM context
        CRITICAL: This provides business context for each column
        """
        descriptions = []
        
        for col in semantic_schema.get('columns', []):
            desc_parts = [f'"{col["name"]}"']
            
            if col.get('display_name') != col.get('name'):
                desc_parts.append(f"(Business Name: {col['display_name']})")
            
            if col.get('description'):
                desc_parts.append(f"- {col['description']}")
            
            if col.get('semantic_type'):
                desc_parts.append(f"[{col['semantic_type']}]")
            
            if col.get('is_measure'):
                desc_parts.append("(Measure)")
            elif col.get('is_dimension'):
                desc_parts.append("(Dimension)")
            
            descriptions.append(" ".join(desc_parts))
        
        return "Semantic Column Context:\n" + "\n".join(descriptions)
    
    def auto_generate_metadata_from_table(self, table_name: str) -> bool:
        """
        Auto-generate semantic layer metadata for a table if missing
        CRITICAL: Ensures semantic layer exists before LLM queries
        """
        try:
            from services.integration_service import DataIntegrationService
            integration_service = DataIntegrationService()
            
            # Generate semantic layer for this table
            success = self.auto_generate_metadata_from_data_integration(integration_service)
            
            if success:
                logger.info(f"Successfully generated semantic layer for {table_name}")
                return True
            else:
                logger.warning(f"Failed to generate semantic layer for {table_name}")
                return False
                
        except Exception as e:
            logger.error(f"Error auto-generating semantic metadata for {table_name}: {e}")
            return False
    
    def clear_semantic_layer_for_table(self, table_name: str) -> bool:
        """
        Clear existing semantic layer for a table before regeneration
        CRITICAL: Ensures fresh semantic layer after ETL transformations
        """
        try:
            from datasets.models import SemanticTable, SemanticColumn
            
            # Find and delete existing semantic table and columns
            semantic_table = SemanticTable.objects.filter(name=table_name).first()
            if semantic_table:
                # Delete columns first (foreign key constraint)
                SemanticColumn.objects.filter(semantic_table=semantic_table).delete()
                # Delete table
                semantic_table.delete()
                logger.info(f"Cleared existing semantic layer for table: {table_name}")
                return True
            else:
                logger.info(f"No existing semantic layer found for table: {table_name}")
                return True
                
        except Exception as e:
            logger.error(f"Error clearing semantic layer for {table_name}: {e}")
            return False

    def _assess_query_confidence(self, natural_query: str, generated_sql: str, schema_info: Dict[str, Any]) -> int:
        """
        Assess confidence level of query understanding (0-100%)
        """
        try:
            confidence_score = 100  # Start with full confidence
            query_lower = natural_query.lower()
            available_columns = self._extract_column_names(schema_info)
            
            # Factor 1: Query complexity and specificity
            if len(query_lower.split()) < 3:
                confidence_score -= 30  # Very short queries are ambiguous
            elif len(query_lower.split()) < 5:
                confidence_score -= 15  # Short queries might be ambiguous
            
            # Factor 2: Presence of specific business terms
            business_terms = ['customer', 'sales', 'revenue', 'profit', 'order', 'product', 'region']
            matched_terms = sum(1 for term in business_terms if term in query_lower)
            if matched_terms == 0:
                confidence_score -= 25  # No business context
            elif matched_terms == 1:
                confidence_score -= 10  # Limited business context
            
            # Factor 3: Specificity of metrics and aggregations
            vague_terms = ['top', 'best', 'good', 'high', 'low', 'some', 'many']
            vague_count = sum(1 for term in vague_terms if term in query_lower)
            confidence_score -= vague_count * 8  # Deduct for each vague term
            
            # Factor 4: Presence of numbers/quantities
            import re
            numbers = re.findall(r'\d+', natural_query)
            if not numbers:
                confidence_score -= 15  # No specific quantity mentioned
            
            # Factor 5: Column name recognition
            recognized_columns = 0
            for col in available_columns:
                if col in query_lower:
                    recognized_columns += 1
            
            if recognized_columns == 0:
                confidence_score -= 20  # No column names recognized
            elif recognized_columns >= 2:
                confidence_score += 10  # Multiple columns recognized - bonus
            
            # Factor 6: Question clarity (has clear subject and predicate)
            clarity_indicators = ['show', 'list', 'find', 'get', 'what', 'how many', 'which']
            has_clear_action = any(indicator in query_lower for indicator in clarity_indicators)
            if not has_clear_action:
                confidence_score -= 15
            
            # Factor 7: Regional/temporal specificity
            regions = ['north', 'south', 'east', 'west']
            times = ['month', 'year', 'quarter', 'week', 'today', 'yesterday']
            if any(region in query_lower for region in regions):
                confidence_score += 5  # Regional specificity is good
            if any(time in query_lower for time in times):
                confidence_score += 5  # Temporal specificity is good
            
            # Factor 8: Generated SQL quality
            if generated_sql:
                sql_lower = generated_sql.lower()
                if 'select' not in sql_lower or 'from' not in sql_lower:
                    confidence_score -= 30  # Invalid SQL structure
                elif len(generated_sql) < 20:
                    confidence_score -= 20  # Too simple SQL
                elif 'where' in sql_lower and 'group by' in sql_lower:
                    confidence_score += 10  # Complex SQL with filtering and grouping
            
            # Ensure confidence is within bounds
            confidence_score = max(0, min(100, confidence_score))
            
            return confidence_score
            
        except Exception as e:
            logger.error(f"Error assessing query confidence: {e}")
            return 50  # Default moderate confidence 

    def _check_etl_transformation(self, table_name: str, col_name: str) -> bool:
        """
        Check if a column has ETL transformations applied by querying transformation metadata
        """
        try:
            from services.integration_service import DataIntegrationService
            
            # Get the integration service to access DuckDB
            integration_service = DataIntegrationService()
            if not integration_service.integrated_db or not hasattr(integration_service.integrated_db, 'execute'):
                return False
            
            # Query transformation metadata
            result = integration_service.integrated_db.execute("""
                SELECT transformation_applied 
                FROM transformation_metadata 
                WHERE table_name = ? AND column_name = ?
                LIMIT 1
            """, (table_name, col_name)).fetchone()
            
            if result:
                return bool(result[0])
            else:
                return False
                
        except Exception as e:
            logger.warning(f"Could not check ETL transformation for {table_name}.{col_name}: {e}")
            return False