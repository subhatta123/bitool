# Generated migration for adding table_name field to DataSource model

from django.db import migrations, models
import re


def populate_table_names(apps, schema_editor):
    """Populate table_name field for existing DataSource records"""
    DataSource = apps.get_model('datasets', 'DataSource')
    
    def get_safe_table_name(source_id: str) -> str:
        """Generate safe table name from source ID"""
        # Replace hyphens with underscores and ensure it starts with a letter
        safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', str(source_id))
        if safe_name and safe_name[0].isdigit():
            safe_name = f"source_{safe_name}"
        elif not safe_name.startswith('source_'):
            safe_name = f"source_{safe_name}"
        return safe_name
    
    # Update existing records
    for data_source in DataSource.objects.all():
        data_source.table_name = get_safe_table_name(str(data_source.id))
        data_source.save(update_fields=['table_name'])


def reverse_populate_table_names(apps, schema_editor):
    """Reverse migration - clear table_name field"""
    DataSource = apps.get_model('datasets', 'DataSource')
    DataSource.objects.update(table_name='')


class Migration(migrations.Migration):

    dependencies = [
        ('datasets', '0006_add_semantic_business_owner'),
    ]

    operations = [
        migrations.AddField(
            model_name='datasource',
            name='table_name',
            field=models.CharField(blank=True, db_index=True, help_text='Sanitized table name used in DuckDB', max_length=200),
        ),
        migrations.RunPython(populate_table_names, reverse_populate_table_names),
    ] 