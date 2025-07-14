"""
Data migration to fix existing SemanticColumn records that have 'object' as their data_type.
Updates them to 'string' type to ensure consistency with the new type mapping system.
"""

from django.db import migrations
import logging

logger = logging.getLogger(__name__)


def fix_object_data_types(apps, schema_editor):
    """Fix SemanticColumn records with 'object' data_type"""
    SemanticColumn = apps.get_model('datasets', 'SemanticColumn')
    
    # Find all columns with object data type
    object_columns = SemanticColumn.objects.filter(data_type='object')
    count = object_columns.count()
    
    if count > 0:
        logger.info(f"Found {count} SemanticColumn records with 'object' data_type")
        
        # Update them to 'string'
        updated_count = object_columns.update(data_type='string')
        logger.info(f"Updated {updated_count} SemanticColumn records from 'object' to 'string'")
    else:
        logger.info("No SemanticColumn records with 'object' data_type found")


def reverse_fix_object_data_types(apps, schema_editor):
    """Reverse migration - note: this is not ideal but provided for completeness"""
    # We don't really want to reverse this change since 'object' types are problematic
    # But we can log that this migration was reversed
    logger.warning("Reverse migration for fix_object_data_types executed - no changes made")


class Migration(migrations.Migration):
    
    dependencies = [
        ('datasets', '0001_initial'),
    ]
    
    operations = [
        migrations.RunPython(
            fix_object_data_types,
            reverse_fix_object_data_types,
        ),
    ] 