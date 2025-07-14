"""
Django management command to clean up orphaned data after data sources are deleted
"""

from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model
from datasets.signals import cleanup_orphaned_data_for_user

User = get_user_model()


class Command(BaseCommand):
    help = 'Clean up orphaned ETL operations and query logs for users who have deleted data sources'

    def add_arguments(self, parser):
        parser.add_argument(
            '--user',
            type=str,
            help='Username to clean up (optional, if not provided will clean up all users)',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be cleaned up without actually deleting anything',
        )

    def handle(self, *args, **options):
        username = options.get('user')
        dry_run = options.get('dry_run')
        
        if dry_run:
            self.stdout.write(
                self.style.WARNING('DRY RUN MODE - No data will be deleted')
            )
        
        if username:
            try:
                user = User.objects.get(username=username)
                users_to_process = [user]
                self.stdout.write(f'Processing user: {username}')
            except User.DoesNotExist:
                self.stdout.write(
                    self.style.ERROR(f'User "{username}" not found')
                )
                return
        else:
            users_to_process = User.objects.all()
            self.stdout.write(f'Processing all {users_to_process.count()} users')
        
        total_cleaned = {
            'etl_operations': 0,
            'query_logs': 0,
            'dashboard_items': 0
        }
        
        for user in users_to_process:
            self.stdout.write(f'\n--- Processing user: {user.username} ---')
            
            if dry_run:
                # For dry run, just count what would be cleaned
                from datasets.models import DataSource, ETLOperation
                from core.models import QueryLog
                
                user_data_sources = DataSource.objects.filter(created_by=user)
                etl_count = ETLOperation.objects.filter(created_by=user).count()
                query_count = QueryLog.objects.filter(user=user).count()
                
                self.stdout.write(f'  Data Sources: {user_data_sources.count()}')
                self.stdout.write(f'  ETL Operations: {etl_count}')
                self.stdout.write(f'  Query Logs: {query_count}')
                
                if user_data_sources.count() == 0 and (etl_count > 0 or query_count > 0):
                    self.stdout.write(
                        self.style.WARNING(f'  Would clean up {etl_count} ETL operations and {query_count} query logs')
                    )
                else:
                    self.stdout.write('  No cleanup needed')
            else:
                # Actually perform cleanup
                cleanup_result = cleanup_orphaned_data_for_user(user)
                
                if 'error' in cleanup_result:
                    self.stdout.write(
                        self.style.ERROR(f'  Error: {cleanup_result["error"]}')
                    )
                else:
                    cleaned_etl = int(cleanup_result.get('etl_operations', 0))
                    cleaned_queries = int(cleanup_result.get('query_logs', 0))
                    cleaned_dashboards = int(cleanup_result.get('dashboard_items', 0))
                    
                    total_cleaned['etl_operations'] += cleaned_etl
                    total_cleaned['query_logs'] += cleaned_queries
                    total_cleaned['dashboard_items'] += cleaned_dashboards
                    
                    if any([cleaned_etl, cleaned_queries, cleaned_dashboards]):
                        self.stdout.write(
                            self.style.SUCCESS(
                                f'  Cleaned: {cleaned_etl} ETL operations, '
                                f'{cleaned_queries} query logs, '
                                f'{cleaned_dashboards} dashboard items'
                            )
                        )
                    else:
                        self.stdout.write('  No cleanup needed')
        
        if not dry_run:
            self.stdout.write(
                self.style.SUCCESS(
                    f'\nTotal cleanup completed:\n'
                    f'  ETL Operations: {total_cleaned["etl_operations"]}\n'
                    f'  Query Logs: {total_cleaned["query_logs"]}\n'
                    f'  Dashboard Items: {total_cleaned["dashboard_items"]}'
                )
            )
        else:
            self.stdout.write(
                self.style.WARNING('\nDry run completed - no data was deleted')
            ) 