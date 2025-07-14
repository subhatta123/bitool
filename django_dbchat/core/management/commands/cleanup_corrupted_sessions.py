from django.core.management.base import BaseCommand
from django.contrib.sessions.models import Session
from django.conf import settings
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Clean up corrupted Django sessions that cannot be decoded (e.g., due to binary data)'

    def handle(self, *args, **options):
        cleaned_count = 0
        total = 0
        for session in Session.objects.all():
            total += 1
            try:
                _ = session.get_decoded()
            except Exception as e:
                logger.warning(f"Deleting corrupted session {session.session_key}: {e}")
                session.delete()
                cleaned_count += 1
        self.stdout.write(self.style.SUCCESS(f"Checked {total} sessions. Cleaned up {cleaned_count} corrupted sessions.")) 