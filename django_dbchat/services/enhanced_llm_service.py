#!/usr/bin/env python3
"""
Enhanced LLM Service with Dynamic Schema Detection
Automatically detects dataset schemas and generates appropriate SQL queries
for any dataset with any column names or data types.
"""

import duckdb
import json
import re
from typing import Dict, List, Tuple, Optional

class EnhancedLLMService:
    """Enhanced LLM service that works with any dataset dynamically"""
    
    def __init__(self, duckdb_path='data/integrated.duckdb'):
        self.duckdb_path = duckdb_path
        self.conn = duckdb.connect(duckdb_path)
        self.schema_cache = {}
    
    def get_all_available_schemas(self) -> Dict[str, List[Tuple[str, str]]]:
        """Get schemas for all available tables and views"""
        
        if self.schema_cache:
            return self.schema_cache
        
        try:
            # Get all tables and views
            tables = self.conn.execute("SHOW TABLES").fetchall()
            
            schemas = {}
            
            for table_row in tables:
                table_name = table_row[0]
                try:
                    schema = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
                    schemas[table_name] = [(row[0], row[1]) for row in schema]
                except:
                    continue
            
            self.schema_cache = schemas
            return schemas
            
        except Exception as e:
            print(f"❌ Error getting schemas: {e}")
            return {}
    
    def find_best_table_for_query(self, user_query: str) -> Optional[str]:
        """Analyze user query and find the most appropriate table"""
        
        schemas = self.get_all_available_schemas()
        if not schemas:
            return None
        
        user_query_lower = user_query.lower()
        
        # Score tables based on relevance
        table_scores = {}
        
        for table_name, columns in schemas.items():
            score = 0
            
            # Check if table name is mentioned
            if table_name.lower() in user_query_lower:
                score += 10
            
            # Check if column names are mentioned
            column_names = [col[0].lower() for col in columns]
            for col_name in column_names:
                if col_name in user_query_lower:
                    score += 5
                
                # Check for partial matches
                for word in user_query_lower.split():
                    if word in col_name or col_name in word:
                        score += 2
            
            # Prefer mapping views for consistency
            if table_name.endswith('_mapping'):
                score += 3
            elif 'universal_mapping' in table_name:
                score += 5
            
            # Prefer larger tables (more data = more useful)
            try:
                count_result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                if count_result and count_result[0]:
                    count = count_result[0]
                    score += min(count / 1000, 10)  # Cap at 10 points
            except:
                pass
            
            table_scores[table_name] = score
        
        # Return the highest scoring table
        if table_scores:
            best_table = max(table_scores.keys(), key=lambda x: table_scores[x])
            return best_table if table_scores[best_table] > 0 else None
        
        return None
    
    def generate_sql_with_smart_mapping(self, user_query: str, target_table: Optional[str] = None) -> Optional[str]:
        """Generate SQL query with intelligent column mapping"""
        
        if not target_table:
            target_table = self.find_best_table_for_query(user_query)
        
        if not target_table:
            # Fallback to universal mapping
            target_table = "universal_mapping"
        
        schemas = self.get_all_available_schemas()
        if target_table not in schemas:
            return None
        
        table_schema = schemas[target_table]
        column_names = [col[0] for col in table_schema]
        
        # Create a flexible SQL template based on common query patterns
        user_query_lower = user_query.lower()
        
        # Detect query type and generate appropriate SQL
        if any(word in user_query_lower for word in ['count', 'how many', 'number of']):
            return self._generate_count_query(user_query, target_table, column_names)
        
        elif any(word in user_query_lower for word in ['sum', 'total', 'add up']):
            return self._generate_sum_query(user_query, target_table, column_names)
        
        elif any(word in user_query_lower for word in ['average', 'avg', 'mean']):
            return self._generate_avg_query(user_query, target_table, column_names)
        
        elif any(word in user_query_lower for word in ['group by', 'by region', 'by category', 'per']):
            return self._generate_group_query(user_query, target_table, column_names)
        
        else:
            return self._generate_select_query(user_query, target_table, column_names)
    
    def _find_column_matches(self, user_query: str, column_names: List[str]) -> List[str]:
        """Find column names that match terms in the user query"""
        
        user_words = user_query.lower().split()
        matches = []
        
        for col_name in column_names:
            col_lower = col_name.lower()
            
            # Exact match
            if col_lower in user_query.lower():
                matches.append(col_name)
                continue
            
            # Partial match
            for word in user_words:
                if word in col_lower or col_lower in word:
                    if col_name not in matches:
                        matches.append(col_name)
                    break
        
        return matches
    
    def _generate_count_query(self, user_query: str, table: str, columns: List[str]) -> str:
        """Generate COUNT query"""
        
        matched_cols = self._find_column_matches(user_query, columns)
        
        if matched_cols:
            # Group by the matched columns
            group_cols = ', '.join(f'"{col}"' for col in matched_cols[:2])  # Limit to 2 columns
            return f'SELECT {group_cols}, COUNT(*) as count FROM {table} GROUP BY {group_cols} ORDER BY count DESC LIMIT 10'
        else:
            return f'SELECT COUNT(*) as total_records FROM {table}'
    
    def _generate_sum_query(self, user_query: str, table: str, columns: List[str]) -> str:
        """Generate SUM query"""
        
        # Find numeric columns for summing
        numeric_keywords = ['sales', 'amount', 'price', 'cost', 'revenue', 'profit', 'quantity']
        numeric_cols = []
        
        for col in columns:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in numeric_keywords):
                numeric_cols.append(col)
        
        # Find grouping columns
        group_cols = self._find_column_matches(user_query, columns)
        group_cols = [col for col in group_cols if col not in numeric_cols]
        
        if numeric_cols and group_cols:
            sum_col = numeric_cols[0]
            group_col = group_cols[0]
            return f'SELECT "{group_col}", SUM(CAST("{sum_col}" AS DOUBLE)) as total_{sum_col.lower()} FROM {table} GROUP BY "{group_col}" ORDER BY total_{sum_col.lower()} DESC LIMIT 10'
        elif numeric_cols:
            sum_col = numeric_cols[0]
            return f'SELECT SUM(CAST("{sum_col}" AS DOUBLE)) as total_{sum_col.lower()} FROM {table}'
        else:
            return f'SELECT COUNT(*) as count FROM {table}'
    
    def _generate_avg_query(self, user_query: str, table: str, columns: List[str]) -> str:
        """Generate AVG query"""
        
        # Similar to sum but with AVG
        numeric_keywords = ['sales', 'amount', 'price', 'cost', 'revenue', 'profit', 'quantity']
        numeric_cols = [col for col in columns if any(keyword in col.lower() for keyword in numeric_keywords)]
        
        if numeric_cols:
            avg_col = numeric_cols[0]
            return f'SELECT AVG(CAST("{avg_col}" AS DOUBLE)) as avg_{avg_col.lower()} FROM {table}'
        else:
            return f'SELECT COUNT(*) as count FROM {table}'
    
    def _generate_group_query(self, user_query: str, table: str, columns: List[str]) -> str:
        """Generate GROUP BY query"""
        
        matched_cols = self._find_column_matches(user_query, columns)
        
        if matched_cols:
            group_col = matched_cols[0]
            return f'SELECT "{group_col}", COUNT(*) as count FROM {table} GROUP BY "{group_col}" ORDER BY count DESC LIMIT 10'
        else:
            return f'SELECT * FROM {table} LIMIT 10'
    
    def _generate_select_query(self, user_query: str, table: str, columns: List[str]) -> str:
        """Generate basic SELECT query"""
        
        matched_cols = self._find_column_matches(user_query, columns)
        
        if matched_cols:
            select_cols = ', '.join(f'"{col}"' for col in matched_cols[:5])  # Limit to 5 columns
            return f'SELECT {select_cols} FROM {table} LIMIT 10'
        else:
            # Show first few columns
            first_cols = ', '.join(f'"{col}"' for col in columns[:5])
            return f'SELECT {first_cols} FROM {table} LIMIT 10'
    
    def execute_smart_query(self, user_query: str) -> Dict:
        """Execute a query with intelligent table and column detection"""
        
        try:
            # Step 1: Find best table
            best_table = self.find_best_table_for_query(user_query)
            
            # Step 2: Generate appropriate SQL
            sql_query = self.generate_sql_with_smart_mapping(user_query, best_table)
            
            if not sql_query:
                return {
                    'success': False,
                    'error': 'Could not generate appropriate SQL query',
                    'user_query': user_query
                }
            
            # Step 3: Execute the query
            result = self.conn.execute(sql_query).fetchall()
            
            # Step 4: Get column names for the result
            query_result = self.conn.execute(sql_query)
            description = query_result.description
            column_names = [desc[0] for desc in description] if description else []
            
            return {
                'success': True,
                'sql_query': sql_query,
                'table_used': best_table or "unknown",
                'results': result,
                'column_names': column_names,
                'row_count': len(result),
                'user_query': user_query
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'user_query': user_query,
                'sql_query': sql_query if 'sql_query' in locals() else None
            }
    
    def get_schema_info(self, table_name: str = None) -> Dict:
        """Get comprehensive schema information for debugging"""
        
        schemas = self.get_all_available_schemas()
        
        if table_name and table_name in schemas:
            return {
                'table': table_name,
                'columns': schemas[table_name],
                'column_count': len(schemas[table_name])
            }
        else:
            return {
                'all_tables': list(schemas.keys()),
                'table_count': len(schemas),
                'schemas': {table: len(cols) for table, cols in schemas.items()}
            }
    
    def close(self):
        """Close the DuckDB connection"""
        if hasattr(self, 'conn'):
            self.conn.close()

def test_enhanced_llm_service():
    """Test the enhanced LLM service with various query types"""
    
    print("🧪 Testing Enhanced LLM Service")
    print("=" * 60)
    
    service = EnhancedLLMService()
    
    try:
        # Test queries for different patterns
        test_queries = [
            "How many customers are there?",
            "What is the total sales by region?",
            "Show me customers and their sales",
            "Average sales amount",
            "Count by category",
            "List all products"
        ]
        
        for query in test_queries:
            print(f"\n🔍 Testing: {query}")
            result = service.execute_smart_query(query)
            
            if result['success']:
                print(f"   ✅ Success: {result['row_count']} rows")
                print(f"   📊 Table: {result['table_used']}")
                print(f"   🗂️ SQL: {result['sql_query'][:60]}...")
                if result['results']:
                    print(f"   📋 Sample: {result['results'][0]}")
            else:
                print(f"   ❌ Failed: {result['error']}")
        
        # Show schema info
        schema_info = service.get_schema_info()
        print(f"\n📊 Available Tables: {len(schema_info['all_tables'])}")
        for table in schema_info['all_tables'][:5]:
            print(f"   - {table} ({schema_info['schemas'][table]} columns)")
        
    finally:
        service.close()

if __name__ == "__main__":
    test_enhanced_llm_service() 