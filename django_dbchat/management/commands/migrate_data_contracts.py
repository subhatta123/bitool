"""
Django management command to migrate existing data to new contract formats
and fix field naming inconsistencies
"""

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone
import json
import logging

from datasets.models import DataSource, ETLOperation, SemanticMetric, SemanticTable, SemanticColumn
from utils.data_contracts import validate_connection_info, validate_workflow_status
from utils.security import security_manager
from utils.workflow_manager import WorkflowManager

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Migrate existing data to new contract formats and fix field naming inconsistencies'
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be migrated without making changes',
        )
        parser.add_argument(
            '--backup',
            action='store_true',
            help='Create backup of existing data before migration',
        )
        parser.add_argument(
            '--encrypt-passwords',
            action='store_true',
            help='Encrypt existing passwords in connection info',
        )
        parser.add_argument(
            '--fix-field-names',
            action='store_true',
            help='Fix field naming inconsistencies',
        )
        parser.add_argument(
            '--update-workflow-status',
            action='store_true',
            help='Update workflow status to new format',
        )
    
    def handle(self, *args, **options):
        """Main command handler"""
        self.dry_run = options['dry_run']
        self.create_backup = options['backup']
        
        self.stdout.write(
            self.style.SUCCESS('Starting data migration to new contract formats...')
        )
        
        try:
            if self.create_backup:
                self._create_backup()
            
            with transaction.atomic():
                migration_stats = {
                    'data_sources_migrated': 0,
                    'etl_operations_migrated': 0,
                    'semantic_metrics_migrated': 0,
                    'passwords_encrypted': 0,
                    'field_names_fixed': 0,
                    'workflow_statuses_updated': 0
                }
                
                if options['encrypt_passwords']:
                    migration_stats['passwords_encrypted'] = self._encrypt_connection_passwords()
                
                if options['fix_field_names']:
                    migration_stats['field_names_fixed'] = self._fix_field_naming_inconsistencies()
                
                if options['update_workflow_status']:
                    migration_stats['workflow_statuses_updated'] = self._update_workflow_status()
                
                # Migrate data sources
                migration_stats['data_sources_migrated'] = self._migrate_data_sources()
                
                # Migrate ETL operations
                migration_stats['etl_operations_migrated'] = self._migrate_etl_operations()
                
                # Migrate semantic metrics
                migration_stats['semantic_metrics_migrated'] = self._migrate_semantic_metrics()
                
                if self.dry_run:
                    self.stdout.write(
                        self.style.WARNING('DRY RUN - No changes were made')
                    )
                    # Rollback transaction in dry run mode
                    transaction.set_rollback(True)
                
                self._print_migration_summary(migration_stats)
                
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            raise CommandError(f"Migration failed: {e}")
    
    def _create_backup(self):
        """Create backup of existing data"""
        self.stdout.write('Creating backup of existing data...')
        
        try:
            backup_data = {
                'timestamp': timezone.now().isoformat(),
                'data_sources': list(DataSource.objects.values()),
                'etl_operations': list(ETLOperation.objects.values()),
                'semantic_metrics': list(SemanticMetric.objects.values()),
                'semantic_tables': list(SemanticTable.objects.values()),
                'semantic_columns': list(SemanticColumn.objects.values())
            }
            
            backup_filename = f"dbchat_backup_{timezone.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            if not self.dry_run:
                with open(backup_filename, 'w') as f:
                    json.dump(backup_data, f, indent=2, default=str)
                
                self.stdout.write(
                    self.style.SUCCESS(f'Backup created: {backup_filename}')
                )
            else:
                self.stdout.write(
                    self.style.WARNING(f'Would create backup: {backup_filename}')
                )
        
        except Exception as e:
            logger.error(f"Backup creation failed: {e}")
            raise CommandError(f"Backup creation failed: {e}")
    
    def _encrypt_connection_passwords(self):
        """Encrypt existing passwords in connection info"""
        self.stdout.write('Encrypting connection passwords...')
        
        encrypted_count = 0
        
        for data_source in DataSource.objects.all():
            try:
                connection_info = data_source.connection_info
                
                if isinstance(connection_info, dict) and 'password' in connection_info:
                    password = connection_info.get('password')
                    
                    if password and not connection_info.get('password_encrypted', False):
                        if not self.dry_run:
                            encrypted_info = security_manager.encrypt_connection_info(connection_info)
                            data_source.connection_info = encrypted_info
                            data_source.save()
                        
                        encrypted_count += 1
                        self.stdout.write(f'  Encrypted password for data source: {data_source.name}')
            
            except Exception as e:
                logger.error(f"Failed to encrypt password for data source {data_source.id}: {e}")
                continue
        
        return encrypted_count
    
    def _fix_field_naming_inconsistencies(self):
        """Fix field naming inconsistencies across models"""
        self.stdout.write('Fixing field naming inconsistencies...')
        
        fixed_count = 0
        
        # The field names have already been fixed in the models, but we might need to
        # handle any data that was created with the old field names
        
        # Check for any semantic metrics that might have old field names in JSON data
        for metric in SemanticMetric.objects.all():
            try:
                needs_update = False
                
                # Check if there are any references to old field names in description or other text fields
                if hasattr(metric, 'metadata') and isinstance(metric.metadata, dict):
                    metadata = metric.metadata.copy()
                    
                    # Fix any references to old field names
                    if 'formula' in metadata:
                        metadata['calculation'] = metadata.pop('formula')
                        needs_update = True
                    
                    if 'category' in metadata:
                        metadata['metric_type'] = metadata.pop('category')
                        needs_update = True
                    
                    if needs_update and not self.dry_run:
                        metric.metadata = metadata
                        metric.save()
                        fixed_count += 1
            
            except Exception as e:
                logger.error(f"Failed to fix field names for metric {metric.id}: {e}")
                continue
        
        return fixed_count
    
    def _update_workflow_status(self):
        """Update workflow status to new standardized format"""
        self.stdout.write('Updating workflow status to new format...')
        
        updated_count = 0
        
        for data_source in DataSource.objects.all():
            try:
                current_status = data_source.workflow_status or {}
                
                # Convert old format to new format if necessary
                if not isinstance(current_status, dict):
                    current_status = {}
                
                # Ensure all required fields are present
                default_status = WorkflowManager.get_default_status()
                updated_status = default_status.copy()
                updated_status.update(current_status)
                
                # Validate against contract
                try:
                    validated_status = validate_workflow_status(updated_status)
                    
                    if not self.dry_run:
                        data_source.workflow_status = updated_status
                        data_source.save()
                    
                    updated_count += 1
                    self.stdout.write(f'  Updated workflow status for: {data_source.name}')
                
                except Exception as validation_error:
                    logger.warning(f"Workflow status validation failed for {data_source.id}: {validation_error}")
                    
                    # Use default status if validation fails
                    if not self.dry_run:
                        data_source.workflow_status = default_status
                        data_source.save()
                    
                    updated_count += 1
            
            except Exception as e:
                logger.error(f"Failed to update workflow status for data source {data_source.id}: {e}")
                continue
        
        return updated_count
    
    def _migrate_data_sources(self):
        """Migrate DataSource records to use new connection_info format"""
        self.stdout.write('Migrating data sources...')
        
        migrated_count = 0
        
        for data_source in DataSource.objects.all():
            try:
                # Check if connection_info needs validation/migration
                connection_info = data_source.connection_info
                
                if connection_info:
                    try:
                        # Validate against contract
                        validated_info = validate_connection_info(connection_info)
                        
                        # If validation passes, the data is already in correct format
                        migrated_count += 1
                        self.stdout.write(f'  Validated data source: {data_source.name}')
                    
                    except Exception as e:
                        logger.warning(f"Connection info validation failed for {data_source.id}: {e}")
                        
                        # Try to fix common issues
                        fixed_info = self._fix_connection_info(connection_info)
                        
                        if fixed_info and not self.dry_run:
                            data_source.connection_info = fixed_info
                            data_source.save()
                            migrated_count += 1
                            self.stdout.write(f'  Fixed data source: {data_source.name}')
            
            except Exception as e:
                logger.error(f"Failed to migrate data source {data_source.id}: {e}")
                continue
        
        return migrated_count
    
    def _migrate_etl_operations(self):
        """Migrate ETLOperation records to use result_summary field"""
        self.stdout.write('Migrating ETL operations...')
        
        migrated_count = 0
        
        for etl_operation in ETLOperation.objects.all():
            try:
                needs_update = False
                
                # Check if result_summary field is properly formatted
                if not etl_operation.result_summary:
                    # Initialize with empty dict if None
                    etl_operation.result_summary = {}
                    needs_update = True
                
                # Add any missing standard fields
                standard_fields = {
                    'status': etl_operation.status,
                    'execution_time': etl_operation.execution_time,
                    'row_count': etl_operation.row_count,
                    'created_at': etl_operation.created_at.isoformat() if etl_operation.created_at else None
                }
                
                for field, value in standard_fields.items():
                    if field not in etl_operation.result_summary and value is not None:
                        etl_operation.result_summary[field] = value
                        needs_update = True
                
                if needs_update and not self.dry_run:
                    etl_operation.save()
                    migrated_count += 1
                    self.stdout.write(f'  Migrated ETL operation: {etl_operation.name}')
            
            except Exception as e:
                logger.error(f"Failed to migrate ETL operation {etl_operation.id}: {e}")
                continue
        
        return migrated_count
    
    def _migrate_semantic_metrics(self):
        """Migrate SemanticMetric records to ensure correct field usage"""
        self.stdout.write('Migrating semantic metrics...')
        
        migrated_count = 0
        
        for metric in SemanticMetric.objects.all():
            try:
                needs_update = False
                
                # Ensure calculation field has content (it should based on model definition)
                if not metric.calculation:
                    logger.warning(f"Metric {metric.id} has empty calculation field")
                    continue
                
                # Ensure metric_type field has content
                if not metric.metric_type:
                    # Set default metric type
                    metric.metric_type = 'simple'
                    needs_update = True
                
                # Validate metric_type against allowed choices
                valid_types = ['simple', 'calculated', 'ratio', 'growth']
                if metric.metric_type not in valid_types:
                    metric.metric_type = 'simple'
                    needs_update = True
                
                if needs_update and not self.dry_run:
                    metric.save()
                    migrated_count += 1
                    self.stdout.write(f'  Migrated semantic metric: {metric.name}')
            
            except Exception as e:
                logger.error(f"Failed to migrate semantic metric {metric.id}: {e}")
                continue
        
        return migrated_count
    
    def _fix_connection_info(self, connection_info):
        """Fix common issues in connection info"""
        try:
            fixed_info = connection_info.copy()
            
            # Ensure 'type' field exists
            if 'type' not in fixed_info:
                # Try to infer from other fields
                if 'file_path' in fixed_info:
                    fixed_info['type'] = 'csv'
                elif 'host' in fixed_info:
                    fixed_info['type'] = 'postgresql'  # Default database type
                else:
                    return None
            
            # Fix port type if it's a string
            if 'port' in fixed_info and isinstance(fixed_info['port'], str):
                try:
                    fixed_info['port'] = int(fixed_info['port'])
                except ValueError:
                    del fixed_info['port']  # Remove invalid port
            
            return fixed_info
        
        except Exception as e:
            logger.error(f"Failed to fix connection info: {e}")
            return None
    
    def _print_migration_summary(self, stats):
        """Print migration summary"""
        self.stdout.write('\n' + '='*50)
        self.stdout.write(self.style.SUCCESS('MIGRATION SUMMARY'))
        self.stdout.write('='*50)
        
        for key, value in stats.items():
            formatted_key = key.replace('_', ' ').title()
            self.stdout.write(f'{formatted_key}: {value}')
        
        self.stdout.write('\n' + self.style.SUCCESS('Migration completed successfully!'))
        
        if self.dry_run:
            self.stdout.write(
                self.style.WARNING('\nThis was a dry run. To apply changes, run without --dry-run flag.')
            ) 