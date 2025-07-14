"""
Django management command for inspecting the integrated database
Helps with troubleshooting data integration pipeline issues
"""

from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from services.integration_service import DataIntegrationService
from datasets.models import DataSource
from utils.table_name_helper import TableNameManager, get_integrated_table_name
import json
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    """
    Django management command that can be run with: python manage.py inspect_integrated_db
    Provides detailed information about the integrated database state
    """
    
    help = 'Inspect the integrated database for troubleshooting data integration issues'
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--table-name',
            type=str,
            help='Inspect a specific table by name',
        )
        parser.add_argument(
            '--data-source-id',
            type=str,
            help='Inspect a specific data source by ID',
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Show detailed information including sample data',
        )
        parser.add_argument(
            '--format',
            choices=['text', 'json'],
            default='text',
            help='Output format (text or json)',
        )
    
    def handle(self, *args, **options):
        try:
            integration_service = DataIntegrationService()
            
            if not integration_service.integrated_db:
                raise CommandError("Integrated database is not available")
            
            # Get basic connection info
            connection_info = self._get_connection_info(integration_service)
            
            if options['table_name']:
                # Inspect specific table
                result = self._inspect_table(integration_service, options['table_name'], options['verbose'])
            elif options['data_source_id']:
                # Inspect specific data source
                result = self._inspect_data_source(integration_service, options['data_source_id'], options['verbose'])
            else:
                # Full database inspection
                result = self._inspect_full_database(integration_service, options['verbose'])
            
            # Add connection info to result
            result['connection_info'] = connection_info
            
            # Output results
            if options['format'] == 'json':
                self.stdout.write(json.dumps(result, indent=2, default=str))
            else:
                self._output_text_format(result)
                
        except Exception as e:
            raise CommandError(f'Error inspecting integrated database: {e}')
    
    def _get_connection_info(self, integration_service):
        """Get database connection information"""
        try:
            connection_type = TableNameManager.get_connection_type(integration_service.integrated_db)
            
            db_path = getattr(settings, 'INTEGRATED_DB_PATH', 'Unknown')
            
            return {
                'connection_type': connection_type,
                'database_path': db_path,
                'is_memory_db': db_path == ':memory:',
                'settings_path': getattr(settings, 'DUCKDB_PATH', 'Not set')
            }
        except Exception as e:
            return {'error': str(e)}
    
    def _inspect_table(self, integration_service, table_name, verbose=False):
        """Inspect a specific table"""
        try:
            exists = integration_service.check_table_exists(table_name)
            
            result = {
                'table_name': table_name,
                'exists': exists
            }
            
            if exists:
                table_info = TableNameManager.get_table_info(integration_service.integrated_db, table_name)
                result.update(table_info)
                
                if verbose:
                    # Get sample data
                    try:
                        sample_data = integration_service.get_integrated_data(table_name)
                        if not sample_data.empty:
                            result['sample_data'] = sample_data.head(5).to_dict('records')
                            result['column_types'] = sample_data.dtypes.to_dict()
                        else:
                            result['sample_data'] = []
                    except Exception as sample_error:
                        result['sample_data_error'] = str(sample_error)
            
            return result
            
        except Exception as e:
            return {'error': str(e), 'table_name': table_name}
    
    def _inspect_data_source(self, integration_service, data_source_id, verbose=False):
        """Inspect a specific data source and its corresponding table"""
        try:
            # Get data source from Django
            data_source = DataSource.objects.get(id=data_source_id)
            
            # Get expected table name
            table_name = get_integrated_table_name(data_source)
            
            result = {
                'data_source_id': data_source_id,
                'data_source_name': data_source.name,
                'source_type': data_source.source_type,
                'expected_table_name': table_name,
                'workflow_status': data_source.workflow_status or {},
                'django_table_name': getattr(data_source, 'table_name', None)
            }
            
            # Check if table exists in integrated DB
            table_info = self._inspect_table(integration_service, table_name, verbose)
            result['integrated_table_info'] = table_info
            
            # Check for workflow inconsistencies
            inconsistencies = []
            
            etl_completed = result['workflow_status'].get('etl_completed', False)
            table_exists = table_info.get('exists', False)
            
            if etl_completed and not table_exists:
                inconsistencies.append("ETL marked as completed but table doesn't exist")
            elif not etl_completed and table_exists:
                inconsistencies.append("Table exists but ETL not marked as completed")
            
            if data_source.source_type == 'csv' and not data_source.table_name:
                inconsistencies.append("CSV data source missing table_name field")
            
            result['inconsistencies'] = inconsistencies
            
            return result
            
        except DataSource.DoesNotExist:
            return {'error': f'Data source {data_source_id} not found'}
        except Exception as e:
            return {'error': str(e), 'data_source_id': data_source_id}
    
    def _inspect_full_database(self, integration_service, verbose=False):
        """Perform full database inspection"""
        try:
            # Get debugging info from TableNameManager
            debug_info = TableNameManager.get_debugging_info(integration_service.integrated_db)
            
            # Get all Django data sources
            data_sources = DataSource.objects.filter(status='active')
            
            # Map data sources to tables
            data_source_mapping = []
            missing_tables = []
            orphaned_tables = list(debug_info.get('tables', []))
            
            for ds in data_sources:
                expected_table = get_integrated_table_name(ds)
                table_exists = integration_service.check_table_exists(expected_table)
                
                ds_info = {
                    'id': str(ds.id),
                    'name': ds.name,
                    'source_type': ds.source_type,
                    'expected_table': expected_table,
                    'table_exists': table_exists,
                    'etl_completed': ds.workflow_status.get('etl_completed', False) if ds.workflow_status else False
                }
                
                data_source_mapping.append(ds_info)
                
                if not table_exists:
                    missing_tables.append(expected_table)
                elif expected_table in orphaned_tables:
                    orphaned_tables.remove(expected_table)
            
            # Check for common issues
            issues = []
            
            if missing_tables:
                issues.append(f"{len(missing_tables)} data sources have missing tables")
            
            if orphaned_tables:
                issues.append(f"{len(orphaned_tables)} tables exist without corresponding data sources")
            
            etl_incomplete = sum(1 for ds in data_source_mapping if not ds['etl_completed'])
            if etl_incomplete > 0:
                issues.append(f"{etl_incomplete} data sources have incomplete ETL status")
            
            # Validation report
            validation_issues = []
            for ds_info in data_source_mapping:
                if ds_info['etl_completed'] and not ds_info['table_exists']:
                    validation_issues.append(f"Data source '{ds_info['name']}' marked ETL complete but table missing")
                elif not ds_info['etl_completed'] and ds_info['table_exists']:
                    validation_issues.append(f"Data source '{ds_info['name']}' has table but ETL not marked complete")
            
            result = {
                'summary': {
                    'total_data_sources': len(data_sources),
                    'total_tables_in_db': debug_info.get('total_tables', 0),
                    'missing_tables': len(missing_tables),
                    'orphaned_tables': len(orphaned_tables),
                    'total_issues': len(issues) + len(validation_issues)
                },
                'database_info': debug_info,
                'data_source_mapping': data_source_mapping,
                'missing_tables': missing_tables,
                'orphaned_tables': orphaned_tables,
                'issues': issues,
                'validation_issues': validation_issues
            }
            
            if verbose:
                # Add detailed table information
                result['detailed_table_info'] = debug_info.get('table_details', {})
            
            return result
            
        except Exception as e:
            return {'error': str(e)}
    
    def _output_text_format(self, result):
        """Output results in human-readable text format"""
        if 'error' in result:
            self.stdout.write(self.style.ERROR(f"Error: {result['error']}"))
            return
        
        # Connection info
        conn_info = result.get('connection_info', {})
        self.stdout.write(self.style.SUCCESS("=== Database Connection Info ==="))
        self.stdout.write(f"Connection Type: {conn_info.get('connection_type', 'Unknown')}")
        self.stdout.write(f"Database Path: {conn_info.get('database_path', 'Unknown')}")
        self.stdout.write(f"Is Memory DB: {conn_info.get('is_memory_db', 'Unknown')}")
        self.stdout.write("")
        
        if 'summary' in result:
            # Full database inspection
            summary = result['summary']
            self.stdout.write(self.style.SUCCESS("=== Database Summary ==="))
            self.stdout.write(f"Data Sources: {summary['total_data_sources']}")
            self.stdout.write(f"Tables in DB: {summary['total_tables_in_db']}")
            self.stdout.write(f"Missing Tables: {summary['missing_tables']}")
            self.stdout.write(f"Orphaned Tables: {summary['orphaned_tables']}")
            self.stdout.write(f"Total Issues: {summary['total_issues']}")
            self.stdout.write("")
            
            # Issues
            if result.get('issues'):
                self.stdout.write(self.style.WARNING("=== Issues Found ==="))
                for issue in result['issues']:
                    self.stdout.write(f"• {issue}")
                self.stdout.write("")
            
            if result.get('validation_issues'):
                self.stdout.write(self.style.ERROR("=== Validation Issues ==="))
                for issue in result['validation_issues']:
                    self.stdout.write(f"• {issue}")
                self.stdout.write("")
            
            # Data source mapping
            self.stdout.write(self.style.SUCCESS("=== Data Source Mapping ==="))
            for ds in result.get('data_source_mapping', []):
                status = "✓" if ds['table_exists'] else "✗"
                etl_status = "Complete" if ds['etl_completed'] else "Incomplete"
                self.stdout.write(f"{status} {ds['name']} ({ds['source_type']}) -> {ds['expected_table']} [ETL: {etl_status}]")
            
            if result.get('orphaned_tables'):
                self.stdout.write("")
                self.stdout.write(self.style.WARNING("=== Orphaned Tables ==="))
                for table in result['orphaned_tables']:
                    self.stdout.write(f"• {table}")
        
        elif 'table_name' in result:
            # Single table inspection
            self.stdout.write(self.style.SUCCESS(f"=== Table: {result['table_name']} ==="))
            self.stdout.write(f"Exists: {result.get('exists', False)}")
            
            if result.get('exists'):
                self.stdout.write(f"Columns: {len(result.get('columns', []))}")
                self.stdout.write(f"Row Count: {result.get('row_count', 'Unknown')}")
                
                if result.get('columns'):
                    self.stdout.write("Column Types:")
                    for col, col_type in result.get('types', {}).items():
                        self.stdout.write(f"  {col}: {col_type}")
        
        elif 'data_source_id' in result:
            # Single data source inspection
            self.stdout.write(self.style.SUCCESS(f"=== Data Source: {result.get('data_source_name', 'Unknown')} ==="))
            self.stdout.write(f"ID: {result['data_source_id']}")
            self.stdout.write(f"Type: {result.get('source_type', 'Unknown')}")
            self.stdout.write(f"Expected Table: {result.get('expected_table_name', 'Unknown')}")
            
            table_info = result.get('integrated_table_info', {})
            self.stdout.write(f"Table Exists: {table_info.get('exists', False)}")
            
            if result.get('inconsistencies'):
                self.stdout.write(self.style.WARNING("Inconsistencies:"))
                for inconsistency in result['inconsistencies']:
                    self.stdout.write(f"  • {inconsistency}") 