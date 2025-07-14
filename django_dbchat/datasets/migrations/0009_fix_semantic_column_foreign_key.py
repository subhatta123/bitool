# Generated manually to fix semantic column foreign key issue

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('datasets', '0008_fix_aggregation_default_null'),
    ]

    operations = [
        # Rename table_id to semantic_table_id to match Django model
        migrations.RunSQL(
            sql="ALTER TABLE semantic_columns RENAME COLUMN table_id TO semantic_table_id;",
            reverse_sql="ALTER TABLE semantic_columns RENAME COLUMN semantic_table_id TO table_id;"
        ),
    ] 