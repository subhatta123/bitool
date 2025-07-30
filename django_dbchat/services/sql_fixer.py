#!/usr/bin/env python3
"""
SQL Fixer Service - Comprehensive SQL Syntax Repair
Fixes common SQL syntax issues including malformed ORDER BY clauses
"""

import re
import logging
from typing import Tuple

logger = logging.getLogger(__name__)

class SQLFixer:
    """
    Comprehensive SQL syntax fixer that handles common issues
    """
    
    @staticmethod
    def fix_sql_syntax(sql_query: str) -> str:
        """
        Fix common SQL syntax issues
        """
        try:
            fixed_sql = sql_query
            
            # Step 1: Fix backticks
            fixed_sql = SQLFixer._fix_backticks(fixed_sql)
            # Step 2: Fix column references
            fixed_sql = SQLFixer._fix_column_references(fixed_sql)
            # Step 3: Fix table references
            fixed_sql = SQLFixer._fix_table_references(fixed_sql)
            # Step 4: Fix semicolons
            fixed_sql = SQLFixer._fix_semicolons(fixed_sql)
            # Step 5: Robust ORDER BY clause fix
            fixed_sql = SQLFixer._fix_order_by_clauses(fixed_sql)
            # Step 6: Fix double-quoted identifiers
            fixed_sql = SQLFixer._fix_double_quotes(fixed_sql)
            # Step 7: Fix malformed aliases
            fixed_sql = SQLFixer._fix_aliases(fixed_sql)
            # Step 8: Fix ORDER BY column references
            fixed_sql = SQLFixer._fix_order_by_references(fixed_sql)
            # Step 9: Fix column name spaces to underscores (LLM vs actual table mismatch)
            fixed_sql = SQLFixer._fix_column_name_spaces(fixed_sql)
            # Step 10: Fix date function type casting (NEW)
            fixed_sql = SQLFixer._fix_date_functions(fixed_sql)
            # Step 11: Fix date year filters (NEW)
            fixed_sql = SQLFixer._fix_date_year_filters(fixed_sql)
            
            # Log if any changes were made
            if fixed_sql != sql_query:
                logger.info(f"SQL FIXER: Fixed syntax issues")
                logger.info(f"  Original: {sql_query}")
                logger.info(f"  Fixed:    {fixed_sql}")
            
            return fixed_sql
            
        except Exception as e:
            logger.error(f"Error fixing SQL syntax: {e}")
            return sql_query
    
    @staticmethod
    def _fix_order_by_clauses(sql: str) -> str:
        """
        Robustly fix malformed ORDER BY clauses:
        - Only quote the column name
        - Move ASC/DESC outside quotes
        - Remove line breaks/semicolons from inside quotes
        - Extract and preserve LIMIT/OFFSET if present
        """
        # Pattern: ORDER BY "col DESC\nLIMIT 3;" -> ORDER BY "col" DESC LIMIT 3;
        def order_by_repl(match):
            inner = match.group(1)
            # Split by whitespace, look for ASC/DESC
            parts = re.split(r'\s+', inner)
            col_parts = []
            direction = None
            limit = ''
            for part in parts:
                if part.upper() in ('ASC', 'DESC'):
                    direction = part.upper()
                elif part.upper() == 'LIMIT':
                    limit = ' LIMIT ' + ' '.join(parts[parts.index(part)+1:])
                    break
                else:
                    col_parts.append(part)
            col = ' '.join(col_parts).replace(';','').replace('\n','').strip()
            result = f'ORDER BY "{col}"'
            if direction:
                result += f' {direction}'
            if limit:
                result += limit
            result += ';'
            return result
        # Replace malformed ORDER BY
        sql = re.sub(r'ORDER\s+BY\s+"([^"]+)"', order_by_repl, sql, flags=re.IGNORECASE)
        # Remove any duplicate semicolons
        sql = re.sub(r';{2,}', ';', sql)
        return sql
    
    @staticmethod
    def _fix_column_references(sql: str) -> str:
        """
        Fix column references that need proper quoting
        """
        def quote_column_if_needed(match):
            keyword = match.group(1)
            column_name = match.group(2)
            if ' ' in column_name and not (column_name.startswith('"') and column_name.endswith('"')):
                return f'{keyword}"{column_name}"'
            return match.group(0)
        sql_patterns = [
            r'(SELECT\s+)([^",\s][^",]*[^",\s](?=\s*,|\s*FROM|\s*$))',
            r'(WHERE\s+)([^",\s][^",]*[^",\s](?=\s*=|\s*<|\s*>|\s*LIKE))',
            r'(GROUP\s+BY\s+)([^",\s][^",]*[^",\s](?=\s*,|\s*ORDER|\s*HAVING|\s*$))',
        ]
        for pattern in sql_patterns:
            sql = re.sub(pattern, quote_column_if_needed, sql, flags=re.IGNORECASE)
        return sql
    
    @staticmethod
    def _fix_table_references(sql: str) -> str:
        """
        Fix table references that need proper quoting and remove double quotes
        """
        # Fix cases like FROM ""table_name"" -> FROM "table_name"
        sql = re.sub(r'FROM\s+""([^\"]+)""', r'FROM "\1"', sql, flags=re.IGNORECASE)
        def quote_table_if_needed(match):
            full_match = match.group(0)
            table_name = match.group(1)
            if ('_' in table_name or '-' in table_name or table_name[0].isdigit()) and not (table_name.startswith('"') and table_name.endswith('"')):
                return full_match.replace(table_name, f'"{table_name}"')
            return full_match
        sql = re.sub(r'(FROM\s+)([^\s,)"]+)', quote_table_if_needed, sql, flags=re.IGNORECASE)
        return sql
    
    @staticmethod
    def _fix_backticks(sql: str) -> str:
        """
        Convert MySQL backticks to DuckDB double quotes
        """
        sql = re.sub(r'`([^`]+)`', r'"\1"', sql)
        return sql
    
    @staticmethod
    def _fix_semicolons(sql: str) -> str:
        """
        Fix semicolon placement
        """
        sql = re.sub(r';\s*(LIMIT|OFFSET)', r' \1', sql, flags=re.IGNORECASE)
        if not sql.strip().endswith(';'):
            sql = sql.strip() + ';'
        return sql
    
    @staticmethod
    def _fix_double_quotes(sql: str) -> str:
        """
        Fix double-quoted identifiers: ""column"" -> "column"
        """
        # Replace ""column"" with "column"
        sql = re.sub(r'""([^"]+)""', r'"\1"', sql)
        return sql
    
    @staticmethod
    def _fix_aliases(sql: str) -> str:
        """
        Fix malformed aliases like as Total"Sales" -> as "Total_Sales"
        """
        # Fix various malformed alias patterns
        # Pattern 1: as Total"Sales" -> as "Total_Sales"
        sql = re.sub(r'as\s+([A-Za-z0-9_]+)"([A-Za-z0-9_]+)"', r'as "\1_\2"', sql, flags=re.IGNORECASE)
        
        # Pattern 2: as "Total"Sales"" -> as "Total_Sales"
        sql = re.sub(r'as\s+"([A-Za-z0-9_]+)""([A-Za-z0-9_]+)""', r'as "\1_\2"', sql, flags=re.IGNORECASE)
        
        # Pattern 3: as Total"Sales (without closing quote) -> as "Total_Sales"
        sql = re.sub(r'as\s+([A-Za-z0-9_]+)"([A-Za-z0-9_]+)(?=\s|$|,|;)', r'as "\1_\2"', sql, flags=re.IGNORECASE)
        
        # Pattern 4: as "Total"Sales (without closing quote) -> as "Total_Sales"
        sql = re.sub(r'as\s+"([A-Za-z0-9_]+)"([A-Za-z0-9_]+)(?=\s|$|,|;)', r'as "\1_\2"', sql, flags=re.IGNORECASE)
        
        # NEW: Fix double underscore aliases
        sql = re.sub(r'as\s+"([A-Za-z0-9_]+)__([A-Za-z0-9_]+)"', r'as "\1_\2"', sql, flags=re.IGNORECASE)
        
        # NEW: Fix unquoted aliases with underscores
        sql = re.sub(r'as\s+([A-Za-z0-9_]+)_"([A-Za-z0-9_]+)"', r'as "\1_\2"', sql, flags=re.IGNORECASE)
        
        return sql
    
    @staticmethod
    def _fix_order_by_references(sql: str) -> str:
        """
        Fix malformed ORDER BY column references
        CRITICAL: Fix issues like ORDER BY Total"Sales" DESC
        """
        # Fix unquoted column references in ORDER BY
        # Pattern: ORDER BY Total_"Sales" -> ORDER BY "Total_Sales"
        sql = re.sub(r'ORDER\s+BY\s+([A-Za-z0-9_]+)_"([A-Za-z0-9_]+)"', r'ORDER BY "\1_\2"', sql, flags=re.IGNORECASE)
        
        # CRITICAL FIX: Pattern: ORDER BY Total"Sales" -> ORDER BY "Total_Sales"
        sql = re.sub(r'ORDER\s+BY\s+([A-Za-z0-9_]+)"([A-Za-z0-9_]+)"', r'ORDER BY "\1_\2"', sql, flags=re.IGNORECASE)
        
        # Fix unquoted column references without underscores
        # Pattern: ORDER BY TotalSales -> ORDER BY "TotalSales"
        sql = re.sub(r'ORDER\s+BY\s+([A-Za-z0-9_]+)(?=\s+DESC|\s+ASC|\s*$|\s*LIMIT)', r'ORDER BY "\1"', sql, flags=re.IGNORECASE)
        
        return sql
    
    @staticmethod
    def _fix_column_name_spaces(sql: str) -> str:
        """
        Fix column names with spaces to underscores for table compatibility
        CRITICAL: LLM generates "Customer Name" but table has Customer_Name
        """
        try:
            # Common column name mappings that cause issues
            column_mappings = {
                '"Customer Name"': '"Customer_Name"',
                '"Order Date"': '"Order_Date"',
                '"Ship Date"': '"Ship_Date"',
                '"Ship Mode"': '"Ship_Mode"',
                '"Customer ID"': '"Customer_ID"',
                '"Product ID"': '"Product_ID"',
                '"Product Name"': '"Product_Name"',
                '"Sub Category"': '"Sub_Category"',
                '"Postal Code"': '"Postal_Code"',
                '"Row ID"': '"Row_ID"',
                '"Order ID"': '"Order_ID"',
            }
            
            # Apply all mappings
            fixed_sql = sql
            for space_name, underscore_name in column_mappings.items():
                if space_name in fixed_sql:
                    fixed_sql = fixed_sql.replace(space_name, underscore_name)
                    logger.info(f"COLUMN FIX: {space_name} -> {underscore_name}")
            
            # Generic pattern: only fix column/table identifiers, not SQL syntax
            def replace_spaces_in_column_quotes(match):
                quoted_content = match.group(1)
                # Only fix if it looks like a column/table name (not SQL syntax)
                if (' ' in quoted_content and 
                    not any(sql_keyword in quoted_content.upper() for sql_keyword in 
                    ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'AS', 'SUM(', ')', '=', '<', '>', 'LIMIT'])):
                    fixed_content = quoted_content.replace(' ', '_')
                    logger.info(f"GENERIC COLUMN FIX: \"{quoted_content}\" -> \"{fixed_content}\"")
                    return f'"{fixed_content}"'
                return match.group(0)
            
            # Apply generic pattern only to column/table identifiers, not SQL syntax
            fixed_sql = re.sub(r'"([^"]+)"', replace_spaces_in_column_quotes, fixed_sql)
            
            return fixed_sql
            
        except Exception as e:
            logger.error(f"Error fixing column name spaces: {e}")
            return sql
    

    @staticmethod
    def _fix_date_year_filters(sql: str) -> str:
        """
        Fix year-based filtering for DD-MM-YYYY format dates
        Convert: WHERE "Order_Date" LIKE '2015%' 
        To: WHERE substr("Order_Date", 7, 4) = '2015'
        """
        try:
            # Pattern for year filtering with LIKE
            year_like_pattern = r'(WHERE|AND)\s+("[^"]+"|[A-Za-z_][A-Za-z0-9_]*)\s+LIKE\s+'(\d{4})%''
            
            def fix_year_like(match):
                where_and = match.group(1)
                column_name = match.group(2)
                year = match.group(3)
                
                # Check if this looks like a date column
                if any(date_term in column_name.lower() for date_term in ['date', 'time', 'created', 'updated']):
                    # Use substr to extract year from DD-MM-YYYY format (positions 7-10)
                    fixed = f'{where_and} substr({column_name}, 7, 4) = '{year}''
                    logger.info(f"DATE FILTER FIX: {column_name} LIKE '{year}%' -> substr({column_name}, 7, 4) = '{year}'")
                    return fixed
                
                return match.group(0)
            
            sql = re.sub(year_like_pattern, fix_year_like, sql, flags=re.IGNORECASE)
            
            # Also fix EXTRACT year functions for string dates
            extract_year_pattern = r'EXTRACT\s*\(\s*YEAR\s+FROM\s+("[^"]+"|[A-Za-z_][A-Za-z0-9_]*)\s*\)'
            
            def fix_extract_year(match):
                column_name = match.group(1)
                
                # Check if this looks like a date column
                if any(date_term in column_name.lower() for date_term in ['date', 'time', 'created', 'updated']):
                    # Use substr to extract year from DD-MM-YYYY format
                    fixed = f'substr({column_name}, 7, 4)'
                    logger.info(f"EXTRACT YEAR FIX: EXTRACT(YEAR FROM {column_name}) -> substr({column_name}, 7, 4)")
                    return fixed
                
                return match.group(0)
            
            sql = re.sub(extract_year_pattern, fix_extract_year, sql, flags=re.IGNORECASE)
            
            # Mark that we've added this function
            sql = sql + " -- strftime_year_extraction"
            
            return sql
            
        except Exception as e:
            logger.error(f"Error fixing date year filters: {e}")
            return sql

    @staticmethod
    def _fix_date_functions(sql: str) -> str:
        """
        Fix date function type casting for VARCHAR date columns
        CRITICAL: Convert EXTRACT(YEAR FROM "Order_Date") to EXTRACT(YEAR FROM STRPTIME("Order_Date", '%d-%m-%Y'))
        ENHANCED: Use STRPTIME to handle DD-MM-YYYY format properly
        """
        try:
            # Known date columns that are stored as VARCHAR but need date conversion
            date_columns = [
                '"Order_Date"', '"Ship_Date"', '"order_date"', '"ship_date"',
                '"Order Date"', '"Ship Date"', '"date"', '"Date"',
                '"created_at"', '"updated_at"', '"timestamp"'
            ]
            
            # Fix EXTRACT function calls
            # Pattern: EXTRACT(YEAR FROM "Order_Date") -> EXTRACT(YEAR FROM STRPTIME("Order_Date", '%d-%m-%Y'))
            def fix_extract(match):
                time_part = match.group(1)  # YEAR, MONTH, DAY, etc.
                column_name = match.group(2)  # "Order_Date"
                
                # Check if this looks like a date column
                if any(date_col.lower() in column_name.lower() for date_col in date_columns):
                    # Use STRPTIME for DD-MM-YYYY format conversion
                    fixed = f'EXTRACT({time_part} FROM STRPTIME({column_name}, \'%d-%m-%Y\'))'
                    logger.info(f"DATE FIX: EXTRACT({time_part} FROM {column_name}) -> {fixed}")
                    return fixed
                return match.group(0)
            
            # Apply EXTRACT fix
            sql = re.sub(r'EXTRACT\s*\(\s*(\w+)\s+FROM\s+("[^"]+"|[A-Za-z_][A-Za-z0-9_]*)\s*\)', 
                        fix_extract, sql, flags=re.IGNORECASE)
            
            # Fix DATE_PART function calls
            # Pattern: DATE_PART('year', "Order_Date") -> DATE_PART('year', STRPTIME("Order_Date", '%d-%m-%Y'))
            def fix_date_part(match):
                time_part = match.group(1)  # 'year', 'month', etc.
                column_name = match.group(2)  # "Order_Date"
                
                # Check if this looks like a date column
                if any(date_col.lower() in column_name.lower() for date_col in date_columns):
                    # Use STRPTIME for DD-MM-YYYY format conversion
                    fixed = f'DATE_PART({time_part}, STRPTIME({column_name}, \'%d-%m-%Y\'))'
                    logger.info(f"DATE FIX: DATE_PART({time_part}, {column_name}) -> {fixed}")
                    return fixed
                return match.group(0)
            
            # Apply DATE_PART fix
            sql = re.sub(r'DATE_PART\s*\(\s*(\'[^\']+\'|"[^"]+"|[A-Za-z_][A-Za-z0-9_]*)\s*,\s*("[^"]+"|[A-Za-z_][A-Za-z0-9_]*)\s*\)', 
                        fix_date_part, sql, flags=re.IGNORECASE)
            
            # Fix YEAR function calls
            # Pattern: YEAR("Order_Date") -> YEAR(STRPTIME("Order_Date", '%d-%m-%Y'))
            def fix_year_function(match):
                column_name = match.group(1)  # "Order_Date"
                
                # Check if this looks like a date column
                if any(date_col.lower() in column_name.lower() for date_col in date_columns):
                    # Use STRPTIME for DD-MM-YYYY format conversion
                    fixed = f'YEAR(STRPTIME({column_name}, \'%d-%m-%Y\'))'
                    logger.info(f"DATE FIX: YEAR({column_name}) -> {fixed}")
                    return fixed
                return match.group(0)
            
            # Apply YEAR function fix
            sql = re.sub(r'YEAR\s*\(\s*("[^"]+"|[A-Za-z_][A-Za-z0-9_]*)\s*\)', 
                        fix_year_function, sql, flags=re.IGNORECASE)
            
            # Fix MONTH, DAY functions similarly
            for func_name in ['MONTH', 'DAY']:
                def fix_date_func(match):
                    column_name = match.group(1)
                    if any(date_col.lower() in column_name.lower() for date_col in date_columns):
                        # Use STRPTIME for DD-MM-YYYY format conversion
                        fixed = f'{func_name}(STRPTIME({column_name}, \'%d-%m-%Y\'))'
                        logger.info(f"DATE FIX: {func_name}({column_name}) -> {fixed}")
                        return fixed
                    return match.group(0)
                
                sql = re.sub(f'{func_name}\\s*\\(\\s*("[^"]+"|[A-Za-z_][A-Za-z0-9_]*)\\s*\\)', 
                           fix_date_func, sql, flags=re.IGNORECASE)
            
            return sql
            
        except Exception as e:
            logger.error(f"Error fixing date functions: {e}")
            return sql
    
    @staticmethod
    def validate_sql_syntax(sql: str) -> Tuple[bool, str]:
        try:
            issues = []
            if 'ORDER BY' in sql:
                order_by_pattern = r'ORDER\s+BY\s+"([^"]+)"\s+([A-Z]+)'
                if not re.search(order_by_pattern, sql, re.IGNORECASE):
                    issues.append("Malformed ORDER BY clause")
            quote_count = sql.count('"')
            if quote_count % 2 != 0:
                issues.append("Unclosed quotes")
            if not re.search(r'\bSELECT\b', sql, re.IGNORECASE):
                issues.append("Missing SELECT clause")
            if not re.search(r'\bFROM\b', sql, re.IGNORECASE):
                issues.append("Missing FROM clause")
            if issues:
                return False, f"SQL syntax issues: {', '.join(issues)}"
            return True, "SQL syntax is valid"
        except Exception as e:
            return False, f"Error validating SQL: {str(e)}"

# Global instance
sql_fixer = SQLFixer() 