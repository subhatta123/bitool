"""
Django management command to clean up duplicate data sources.
This command should be run before applying the migration that adds unique constraint.
"""

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone
from datasets.models import DataSource
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Clean up duplicate data sources before applying unique constraint migration'
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be cleaned up without making changes'
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Verbose output showing detailed information'
        )
    
    def handle(self, *args, **options):
        dry_run = options['dry_run']
        verbose = options['verbose']
        
        self.stdout.write(self.style.SUCCESS('Starting duplicate data source cleanup...'))
        
        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN MODE: No changes will be made'))
        
        # Find duplicate data sources
        duplicates = self.find_duplicates(verbose)
        
        if not duplicates:
            self.stdout.write(self.style.SUCCESS('No duplicate data sources found!'))
            return
        
        self.stdout.write(f'Found {len(duplicates)} sets of duplicate data sources')
        
        # Process duplicates
        total_deleted = 0
        total_preserved = 0
        
        for (user_id, name), sources in duplicates.items():
            deleted_count = self.process_duplicate_set(sources, dry_run, verbose)
            total_deleted += deleted_count
            total_preserved += 1  # One source preserved per set
        
        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    f'DRY RUN SUMMARY: Would delete {total_deleted} duplicate sources, '
                    f'preserving {total_preserved} sources'
                )
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(
                    f'CLEANUP COMPLETE: Deleted {total_deleted} duplicate sources, '
                    f'preserved {total_preserved} sources'
                )
            )
    
    def find_duplicates(self, verbose):
        """Find duplicate data sources grouped by (created_by, name)"""
        # Group data sources by (user, name)
        grouped_sources = defaultdict(list)
        
        # Only look at non-deleted sources
        all_sources = DataSource.objects.filter(is_deleted=False).order_by('created_at')
        
        for source in all_sources:
            key = (source.created_by.pk, source.name)
            grouped_sources[key].append(source)
        
        # Filter to only groups with duplicates
        duplicates = {
            key: sources for key, sources in grouped_sources.items()
            if len(sources) > 1
        }
        
        if verbose:
            for (user_id, name), sources in duplicates.items():
                self.stdout.write(f'User {user_id}, Name "{name}": {len(sources)} duplicates')
                for source in sources:
                    self.stdout.write(f'  - ID: {source.id}, Created: {source.created_at}')
        
        return duplicates
    
    def process_duplicate_set(self, sources, dry_run, verbose):
        """Process a set of duplicate sources, keeping the most recent one"""
        if len(sources) <= 1:
            return 0
        
        # Sort by created_at descending to get most recent first
        sources_sorted = sorted(sources, key=lambda x: x.created_at, reverse=True)
        
        # Keep the most recent one
        to_keep = sources_sorted[0]
        to_delete = sources_sorted[1:]
        
        if verbose:
            self.stdout.write(f'Processing duplicates for "{to_keep.name}" (User: {to_keep.created_by.pk})')
            self.stdout.write(f'  Keeping: {to_keep.id} (created {to_keep.created_at})')
            for source in to_delete:
                self.stdout.write(f'  Deleting: {source.id} (created {source.created_at})')
        
        # Check for dependencies before deletion
        dependencies_found = False
        for source in to_delete:
            deps = self.check_dependencies(source, verbose)
            if deps:
                dependencies_found = True
                if not dry_run:
                    self.migrate_dependencies(source, to_keep, verbose)
        
        # Perform deletions
        deleted_count = 0
        if not dry_run:
            with transaction.atomic():
                for source in to_delete:
                    try:
                        # Use soft delete to preserve data integrity
                        source.soft_delete()
                        deleted_count += 1
                        if verbose:
                            self.stdout.write(f'    Soft deleted: {source.id}')
                    except Exception as e:
                        self.stdout.write(
                            self.style.ERROR(f'Error deleting source {source.id}: {e}')
                        )
        else:
            deleted_count = len(to_delete)
        
        return deleted_count
    
    def check_dependencies(self, source, verbose):
        """Check if a data source has dependent objects"""
        dependencies = []
        
        # Check for semantic tables
        semantic_tables = source.semantic_tables.all()
        if semantic_tables.exists():
            dependencies.append(f'{semantic_tables.count()} semantic tables')
        
        # Check for ETL operations (if they reference this source)
        try:
            from datasets.models import ETLOperation
            # ETL operations might reference this source in their parameters
            etl_operations = ETLOperation.objects.filter(
                source_tables__contains=str(source.id)
            )
            if etl_operations.exists():
                dependencies.append(f'{etl_operations.count()} ETL operations')
        except:
            pass  # ETL model might not exist or be different
        
        if dependencies and verbose:
            self.stdout.write(f'    Dependencies for {source.id}: {", ".join(dependencies)}')
        
        return dependencies
    
    def migrate_dependencies(self, from_source, to_source, verbose):
        """Migrate dependencies from one source to another"""
        try:
            # Migrate semantic tables
            semantic_tables = from_source.semantic_tables.all()
            if semantic_tables.exists():
                for table in semantic_tables:
                    # Check if target already has a table with this name
                    existing = to_source.semantic_tables.filter(name=table.name).first()
                    if not existing:
                        table.data_source = to_source
                        table.save()
                        if verbose:
                            self.stdout.write(f'      Migrated semantic table: {table.name}')
                    else:
                        if verbose:
                            self.stdout.write(f'      Skipped semantic table (already exists): {table.name}')
            
            # Note: ETL operations are more complex to migrate and might be left as-is
            # since they contain source IDs in JSON fields
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Error migrating dependencies: {e}')
            ) 