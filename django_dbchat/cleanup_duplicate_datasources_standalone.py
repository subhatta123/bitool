import os
import django
from collections import defaultdict

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from datasets.models import DataSource


def cleanup_duplicates():
    print('Starting duplicate data source cleanup...')
    grouped = defaultdict(list)
    all_sources = DataSource.objects.filter(is_deleted=False).order_by('created_at')
    for ds in all_sources:
        grouped[(ds.created_by.pk, ds.name)].append(ds)
    total_deleted = 0
    total_preserved = 0
    for key, group in grouped.items():
        if len(group) > 1:
            group = sorted(group, key=lambda x: x.created_at, reverse=True)
            to_keep = group[0]
            to_delete = group[1:]
            print(f'Keeping: {to_keep.id} ({to_keep.name}, user {to_keep.created_by.pk})')
            for ds in to_delete:
                ds.is_deleted = True
                ds.status = 'inactive'
                ds.deleted_at = ds.created_at
                ds.save()
                print(f'  Soft deleted: {ds.id} ({ds.name}, user {ds.created_by.pk})')
                total_deleted += 1
            total_preserved += 1
    print(f'Cleanup complete. Deleted {total_deleted} duplicate sources, preserved {total_preserved} unique sources.')

if __name__ == '__main__':
    cleanup_duplicates() 