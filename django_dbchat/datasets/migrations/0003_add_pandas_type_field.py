"""
Schema migration to add an optional 'pandas_type' field to the DataSource model's schema_info JSON structure.
This field will store the original pandas dtype for debugging and traceability purposes.
"""

from django.db import migrations


class Migration(migrations.Migration):
    """
    Migration to support pandas_type field in schema_info JSON structure.
    
    This migration is non-destructive since it's adding an optional field to a JSON structure.
    The pandas_type field will store the original pandas dtype alongside the semantic 'type' field
    for better debugging and traceability in the ETL pipeline.
    
    Usage:
    - 'type': semantic type ('string', 'integer', 'float', etc.) - used for business logic
    - 'pandas_type': original pandas dtype ('object', 'int64', 'float64', etc.) - for debugging
    """
    
    dependencies = [
        ('datasets', '0002_fix_object_data_types'),
    ]
    
    operations = [
        # No actual database schema changes needed since we're working with JSONField
        # The enhanced schema analysis will automatically populate the pandas_type field
        # This migration serves as documentation and version tracking
    ] 