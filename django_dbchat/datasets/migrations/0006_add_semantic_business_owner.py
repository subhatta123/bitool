# Generated migration for adding business_owner field to SemanticMetric model

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('datasets', '0005_alter_etloperation_data_lineage_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='semanticmetric',
            name='business_owner',
            field=models.CharField(blank=True, help_text='Business owner of the metric', max_length=200),
        ),
    ] 