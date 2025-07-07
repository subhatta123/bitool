"""
Semantic Layer Demo for ConvaBI Application
Shows the difference between raw schema and semantic-enhanced schema
"""

import json
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field

@dataclass
class SemanticColumn:
    name: str
    display_name: str
    description: str
    data_type: str
    sample_values: List[str] = field(default_factory=list)
    common_filters: List[str] = field(default_factory=list)
    business_rules: List[str] = field(default_factory=list)
    aggregation_default: Optional[str] = None

class SemanticLayer:
    def __init__(self):
        self.tables = {}
        self.relationships = []
        self.business_metrics = {}
    
    def generate_enhanced_prompt(self, raw_schema: Dict) -> str:
        """Generate enhanced schema prompt with business context"""
        
        prompt_parts = ["=== BUSINESS-ENHANCED DATA SCHEMA ===\n"]
        
        # Add business context
        prompt_parts.append("BUSINESS CONTEXT:")
        prompt_parts.append("- This is sales data for a retail company")
        prompt_parts.append("- Revenue = sales_amount, Profit = profit_amount")
        prompt_parts.append("- Regions: North, South, East, West (geographic territories)")
        prompt_parts.append("- All amounts in USD currency")
        prompt_parts.append("")
        
        # Add common metrics
        prompt_parts.append("COMMON BUSINESS METRICS:")
        prompt_parts.append("- Total Revenue: SUM(sales_amount)")
        prompt_parts.append("- Profit Margin: (SUM(profit_amount) / SUM(sales_amount)) * 100")
        prompt_parts.append("- Customer Count: COUNT(DISTINCT customer_id)")
        prompt_parts.append("- Average Order Value: AVG(sales_amount)")
        prompt_parts.append("")
        
        # Enhanced table descriptions
        for table_name, columns in raw_schema.items():
            if table_name == "customers":
                prompt_parts.append("TABLE: Customer Directory (customers)")
                prompt_parts.append("Purpose: Master list of all customers with geographic assignments")
                prompt_parts.append("COLUMNS:")
                prompt_parts.append("  • Customer ID (customer_id): integer - Unique customer identifier")
                prompt_parts.append("    Business Rule: Always positive, never null")
                prompt_parts.append("  • Customer Name (customer_name): varchar - Full legal customer name")
                prompt_parts.append("    Sample Values: 'John Smith', 'ABC Corp', 'Jane Doe'")
                prompt_parts.append("  • Sales Region (region): varchar - Geographic territory assignment")
                prompt_parts.append("    Valid Values: 'North', 'South', 'East', 'West'")
                prompt_parts.append("    Common Filters: region = 'North', region IN ('North', 'South')")
                
            elif table_name == "orders":
                prompt_parts.append("TABLE: Sales Orders (orders)")  
                prompt_parts.append("Purpose: Transactional sales data with revenue and profit")
                prompt_parts.append("COLUMNS:")
                prompt_parts.append("  • Order ID (order_id): integer - Unique order identifier")
                prompt_parts.append("  • Customer ID (customer_id): integer - Links to customers.customer_id")
                prompt_parts.append("    Relationship: orders.customer_id → customers.customer_id (many-to-one)")
                prompt_parts.append("  • Order Date (order_date): date - When order was placed")
                prompt_parts.append("    Common Filters: order_date >= '2023-01-01', YEAR(order_date) = 2023")
                prompt_parts.append("  • Sales Amount (sales_amount): decimal - Total revenue (before costs)")
                prompt_parts.append("    Aggregation: Usually SUM() for totals, AVG() for averages")
                prompt_parts.append("    Business Rule: Always positive, in USD")
                prompt_parts.append("  • Profit Amount (profit_amount): decimal - Net profit (revenue - costs)")
                prompt_parts.append("    Aggregation: Usually SUM() for totals")
                prompt_parts.append("    Business Rule: Can be negative, in USD")
                prompt_parts.append("    Related: Use with sales_amount for profit margin calculations")
            
            prompt_parts.append("")
        
        # Add relationship summary
        prompt_parts.append("RELATIONSHIPS:")
        prompt_parts.append("- customers.customer_id → orders.customer_id (one customer has many orders)")
        prompt_parts.append("")
        
        # Add query patterns
        prompt_parts.append("COMMON QUERY PATTERNS:")
        prompt_parts.append("- Revenue by region: JOIN customers and orders, GROUP BY region, SUM(sales_amount)")
        prompt_parts.append("- Top customers: GROUP BY customer, SUM(sales_amount), ORDER BY total DESC")
        prompt_parts.append("- Monthly trends: GROUP BY MONTH(order_date), SUM(sales_amount)")
        prompt_parts.append("- Profit margin: (SUM(profit_amount) / SUM(sales_amount)) * 100")
        prompt_parts.append("")
        
        return "\n".join(prompt_parts)

# Demo comparison
def demo_schema_comparison():
    """Show the difference between current and semantic approach"""
    
    # Simulated raw schema (what you currently send to LLM)
    raw_schema = {
        "customers": {
            "customer_id": "integer",
            "customer_name": "varchar", 
            "region": "varchar"
        },
        "orders": {
            "order_id": "integer",
            "customer_id": "integer",
            "order_date": "date", 
            "sales_amount": "decimal",
            "profit_amount": "decimal"
        }
    }
    
    # Current basic prompt
    current_prompt = "Schema:\n"
    for table, columns in raw_schema.items():
        current_prompt += f"Table {table}:\n"
        for col_name, col_type in columns.items():
            current_prompt += f"  - {col_name} ({col_type})\n"
    
    # Enhanced semantic prompt
    semantic_layer = SemanticLayer()
    enhanced_prompt = semantic_layer.generate_enhanced_prompt(raw_schema)
    
    print("=== CURRENT BASIC PROMPT ===")
    print(current_prompt)
    print("\n" + "="*50 + "\n")
    print("=== ENHANCED SEMANTIC PROMPT ===")
    print(enhanced_prompt)

if __name__ == "__main__":
    demo_schema_comparison() 