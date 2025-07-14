"""
Django management command to fix data type issues in existing data sources.
Scans DataSource records and updates 'object' types to appropriate semantic types.
"""

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from datasets.models import DataSource, SemanticColumn
from utils.type_helpers import (
    map_pandas_dtype_to_standard,
    validate_semantic_data_type,
    ensure_no_object_types
)
import json
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = """
    Fix data type issues in existing data sources by converting 'object' types to semantic types.
    
    This command scans all DataSource records and their schema_info for 'object' types,
    then updates them to appropriate semantic types using the centralized type mapping utilities.
    
    Options:
        --dry-run: Preview changes without applying them
        --verbose: Show detailed progress information
        --data-source-id: Fix only a specific data source by ID
    
    Examples:
        python manage.py fix_data_types --dry-run
        python manage.py fix_data_types --verbose
        python manage.py fix_data_types --data-source-id=123
    """
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Preview changes without applying them',
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Show detailed progress information',
        )
        parser.add_argument(
            '--data-source-id',
            type=str,
            help='Fix only a specific data source by ID',
        )
    
    def handle(self, *args, **options):
        """Execute the command"""
        dry_run = options['dry_run']
        verbose = options['verbose']
        data_source_id = options.get('data_source_id')
        
        if dry_run:
            self.stdout.write(
                self.style.WARNING('DRY RUN MODE - No changes will be saved')
            )
        
        if verbose:
            self.stdout.write('Starting data type fix process...')
        
        try:
            # Get data sources to process
            if data_source_id:
                data_sources = DataSource.objects.filter(id=data_source_id)
                if not data_sources.exists():
                    raise CommandError(f'DataSource with ID {data_source_id} not found')
            else:
                data_sources = DataSource.objects.all()
            
            total_sources = data_sources.count()
            if verbose:
                self.stdout.write(f'Found {total_sources} data sources to process')
            
            # Process data sources
            fixed_sources = 0
            fixed_columns = 0
            
            with transaction.atomic():
                for i, data_source in enumerate(data_sources, 1):
                    if verbose:
                        self.stdout.write(f'Processing {i}/{total_sources}: {data_source.name}')
                    
                    # Fix schema_info
                    schema_fixed, schema_changes = self._fix_schema_info(data_source)
                    
                    # Fix semantic columns
                    columns_fixed, column_changes = self._fix_semantic_columns(data_source)
                    
                    if schema_fixed or columns_fixed:
                        fixed_sources += 1
                        fixed_columns += column_changes
                        
                        if not dry_run:
                            if schema_fixed:
                                data_source.save()
                        
                        if verbose or dry_run:
                            self._report_changes(data_source, schema_changes, column_changes)
                
                if dry_run:
                    # Rollback in dry run mode
                    transaction.set_rollback(True)
            
            # Summary
            self.stdout.write(
                self.style.SUCCESS(
                    f'Process completed. '
                    f'Fixed {fixed_sources} data sources and {fixed_columns} columns'
                )
            )
            
            if dry_run:
                self.stdout.write(
                    self.style.WARNING('No changes were saved (dry run mode)')
                )
        
        except Exception as e:
            logger.error(f'Error in fix_data_types command: {e}')
            raise CommandError(f'Command failed: {e}')
    
    def _fix_schema_info(self, data_source):
        """Fix object types in schema_info"""
        if not data_source.schema_info:
            return False, 0
        
        original_schema = data_source.schema_info
        fixed_schema = ensure_no_object_types(original_schema)
        
        # Count changes
        changes = self._count_schema_changes(original_schema, fixed_schema)
        
        if changes > 0:
            data_source.schema_info = fixed_schema
            return True, changes
        
        return False, 0
    
    def _fix_semantic_columns(self, data_source):
        """Fix object types in SemanticColumn records"""
        from datasets.models import SemanticTable
        
        try:
            # Get semantic tables for this data source
            semantic_tables = SemanticTable.objects.filter(data_source=data_source)
            if not semantic_tables.exists():
                return False, 0
            
            total_fixed = 0
            
            for semantic_table in semantic_tables:
                # Find columns with object data type
                object_columns = SemanticColumn.objects.filter(
                    semantic_table=semantic_table,
                    data_type='object'
                )
                
                count = object_columns.count()
                if count > 0:
                    # Update to string type
                    object_columns.update(data_type='string')
                    total_fixed += count
                    
                    if hasattr(self, 'verbose') and self.verbose:
                        self.stdout.write(
                            f'  Fixed {count} semantic columns in table {semantic_table.name}'
                        )
            
            return total_fixed > 0, total_fixed
            
        except Exception as e:
            logger.warning(f'Error fixing semantic columns for {data_source.name}: {e}')
            return False, 0
    
    def _count_schema_changes(self, original, fixed):
        """Count the number of changes made to schema"""
        changes = 0
        
        if not isinstance(original, dict) or not isinstance(fixed, dict):
            return changes
        
        # Check tables
        if 'tables' in original and 'tables' in fixed:
            for table_name, table_info in original['tables'].items():
                if table_name in fixed['tables']:
                    fixed_table = fixed['tables'][table_name]
                    
                    if 'columns' in table_info and 'columns' in fixed_table:
                        for col_name, col_info in table_info['columns'].items():
                            if col_name in fixed_table['columns']:
                                original_type = col_info.get('type')
                                fixed_type = fixed_table['columns'][col_name].get('type')
                                
                                if original_type == 'object' and fixed_type == 'string':
                                    changes += 1
        
        return changes
    
    def _report_changes(self, data_source, schema_changes, column_changes):
        """Report changes made to a data source"""
        if schema_changes > 0:
            self.stdout.write(
                f'  Schema: Fixed {schema_changes} object types → string'
            )
        
        if column_changes > 0:
            self.stdout.write(
                f'  Columns: Fixed {column_changes} object types → string'
            )
        
        if schema_changes == 0 and column_changes == 0:
            self.stdout.write('  No object types found')
    
    def _validate_semantic_types(self, data_source):
        """Validate that all semantic types are valid"""
        issues = []
        
        if data_source.schema_info and 'tables' in data_source.schema_info:
            for table_name, table_info in data_source.schema_info['tables'].items():
                if 'columns' in table_info:
                    for col_name, col_info in table_info['columns'].items():
                        col_type = col_info.get('type')
                        if col_type and not validate_semantic_data_type(col_type):
                            issues.append(f'{table_name}.{col_name}: invalid type "{col_type}"')
        
        return issues 