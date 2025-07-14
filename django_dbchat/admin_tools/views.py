import os
import re
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

from django.shortcuts import render
from django.contrib.admin.views.decorators import staff_member_required
from django.http import JsonResponse, HttpResponse
from django.conf import settings
from django.contrib import messages
from django.utils.decorators import method_decorator
from django.views.generic import View
from django.core.paginator import Paginator
from django.utils import timezone

logger = logging.getLogger(__name__)


@method_decorator(staff_member_required, name='dispatch')
class AdminLoggerView(View):
    """Admin interface for viewing application logs"""
    
    def get(self, request):
        """Display log viewer interface"""
        context = {
            'title': 'ConvaBI Application Logs',
            'log_levels': ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
            'log_files': self.get_available_log_files(),
        }
        return render(request, 'admin_tools/logs.html', context)
    
    def get_available_log_files(self) -> List[str]:
        """Get list of available log files"""
        log_files = []
        
        try:
            logs_dir = os.path.join(settings.BASE_DIR, 'logs')
            if os.path.exists(logs_dir):
                for file in os.listdir(logs_dir):
                    if file.endswith('.log'):
                        log_files.append(file)
        except Exception as e:
            logger.error(f"Error listing log files: {e}")
        
        return sorted(log_files)


@method_decorator(staff_member_required, name='dispatch') 
class LogDataView(View):
    """API view for fetching log data"""
    
    def get(self, request):
        """Get log entries with filtering and pagination"""
        try:
            # Get query parameters
            log_file = request.GET.get('file', 'django.log')
            log_level = request.GET.get('level', 'all')
            search_query = request.GET.get('search', '')
            page = int(request.GET.get('page', 1))
            per_page = int(request.GET.get('per_page', 100))
            date_from = request.GET.get('date_from')
            date_to = request.GET.get('date_to')
            
            # Read log entries
            log_entries = self.read_log_file(log_file, log_level, search_query, date_from, date_to)
            
            # Paginate results
            paginator = Paginator(log_entries, per_page)
            page_obj = paginator.get_page(page)
            
            # Format data for JSON response
            formatted_entries = []
            for entry in page_obj:
                formatted_entries.append({
                    'timestamp': entry.get('timestamp', ''),
                    'level': entry.get('level', ''),
                    'logger': entry.get('logger', ''),
                    'message': entry.get('message', ''),
                    'thread': entry.get('thread', ''),
                    'process': entry.get('process', ''),
                    'full_text': entry.get('full_text', '')
                })
            
            return JsonResponse({
                'success': True,
                'entries': formatted_entries,
                'pagination': {
                    'current_page': page_obj.number,
                    'total_pages': paginator.num_pages,
                    'total_entries': paginator.count,
                    'has_next': page_obj.has_next(),
                    'has_previous': page_obj.has_previous(),
                    'per_page': per_page
                },
                'filters': {
                    'file': log_file,
                    'level': log_level,
                    'search': search_query,
                    'date_from': date_from,
                    'date_to': date_to
                }
            })
            
        except Exception as e:
            logger.error(f"Error fetching log data: {e}")
            return JsonResponse({
                'success': False,
                'error': str(e)
            }, status=500)
    
    def read_log_file(self, log_file: str, level_filter: str, search_query: str, 
                     date_from: str = None, date_to: str = None) -> List[Dict[str, Any]]:
        """Read and parse log file entries"""
        log_entries = []
        
        try:
            log_path = os.path.join(settings.BASE_DIR, 'logs', log_file)
            
            if not os.path.exists(log_path):
                return []
            
            # Parse date filters
            date_from_obj = None
            date_to_obj = None
            
            if date_from:
                try:
                    date_from_obj = datetime.strptime(date_from, '%Y-%m-%d')
                except ValueError:
                    pass
                    
            if date_to:
                try:
                    date_to_obj = datetime.strptime(date_to, '%Y-%m-%d') + timedelta(days=1)
                except ValueError:
                    pass
            
            # Read file in reverse order (newest first)
            with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
            
            # Parse log entries
            current_entry = None
            
            # Django log format: LEVEL YYYY-MM-DD HH:MM:SS,mmm logger thread process message
            log_pattern = re.compile(
                r'^(DEBUG|INFO|WARNING|ERROR|CRITICAL)\s+'
                r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d{3})\s+'
                r'(\w+)\s+'
                r'(\d+)\s+'
                r'(\d+)\s+'
                r'(.*)$'
            )
            
            for line in reversed(lines):
                line = line.strip()
                if not line:
                    continue
                
                match = log_pattern.match(line)
                if match:
                    # Save previous entry if exists
                    if current_entry:
                        if self.matches_filters(current_entry, level_filter, search_query, date_from_obj, date_to_obj):
                            log_entries.append(current_entry)
                    
                    # Start new entry
                    level, timestamp_str, logger_name, thread, process, message = match.groups()
                    
                    # Parse timestamp
                    try:
                        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S,%f')
                    except ValueError:
                        timestamp = None
                    
                    current_entry = {
                        'level': level,
                        'timestamp': timestamp_str,
                        'timestamp_obj': timestamp,
                        'logger': logger_name,
                        'thread': thread,
                        'process': process,
                        'message': message,
                        'full_text': line
                    }
                else:
                    # Multi-line log entry - append to current entry
                    if current_entry:
                        current_entry['message'] += '\n' + line
                        current_entry['full_text'] += '\n' + line
            
            # Don't forget the last entry
            if current_entry:
                if self.matches_filters(current_entry, level_filter, search_query, date_from_obj, date_to_obj):
                    log_entries.append(current_entry)
            
            return log_entries
            
        except Exception as e:
            logger.error(f"Error reading log file {log_file}: {e}")
            return []
    
    def matches_filters(self, entry: Dict[str, Any], level_filter: str, search_query: str,
                       date_from: datetime = None, date_to: datetime = None) -> bool:
        """Check if log entry matches the applied filters"""
        
        # Level filter
        if level_filter != 'all' and entry['level'] != level_filter:
            return False
        
        # Search query filter
        if search_query:
            search_text = f"{entry['message']} {entry['logger']}".lower()
            if search_query.lower() not in search_text:
                return False
        
        # Date range filter
        if (date_from or date_to) and entry.get('timestamp_obj'):
            entry_date = entry['timestamp_obj']
            
            if date_from and entry_date < date_from:
                return False
                
            if date_to and entry_date >= date_to:
                return False
        
        return True
    
    def get_available_log_files(self) -> List[str]:
        """Get list of available log files"""
        log_files = []
        
        try:
            logs_dir = os.path.join(settings.BASE_DIR, 'logs')
            if os.path.exists(logs_dir):
                for file in os.listdir(logs_dir):
                    if file.endswith('.log'):
                        log_files.append(file)
        except Exception as e:
            logger.error(f"Error listing log files: {e}")
        
        return sorted(log_files)


@staff_member_required
def download_log_file(request, filename):
    """Download a specific log file"""
    try:
        log_path = os.path.join(settings.BASE_DIR, 'logs', filename)
        
        if not os.path.exists(log_path):
            messages.error(request, f"Log file '{filename}' not found.")
            return JsonResponse({'error': 'File not found'}, status=404)
        
        # Security check - ensure file is in logs directory
        if not os.path.commonpath([log_path, os.path.join(settings.BASE_DIR, 'logs')]):
            return JsonResponse({'error': 'Access denied'}, status=403)
        
        with open(log_path, 'rb') as f:
            response = HttpResponse(f.read(), content_type='text/plain')
            response['Content-Disposition'] = f'attachment; filename="{filename}"'
            return response
            
    except Exception as e:
        logger.error(f"Error downloading log file {filename}: {e}")
        return JsonResponse({'error': str(e)}, status=500)


@staff_member_required
def clear_log_file(request, filename):
    """Clear/truncate a specific log file"""
    if request.method != 'POST':
        return JsonResponse({'error': 'POST method required'}, status=405)
    
    try:
        log_path = os.path.join(settings.BASE_DIR, 'logs', filename)
        
        if not os.path.exists(log_path):
            return JsonResponse({'error': 'File not found'}, status=404)
        
        # Security check
        if not os.path.commonpath([log_path, os.path.join(settings.BASE_DIR, 'logs')]):
            return JsonResponse({'error': 'Access denied'}, status=403)
        
        # Clear the file
        open(log_path, 'w').close()
        
        logger.info(f"Log file {filename} cleared by admin user {request.user.username}")
        
        return JsonResponse({
            'success': True,
            'message': f'Log file {filename} has been cleared.'
        })
        
    except Exception as e:
        logger.error(f"Error clearing log file {filename}: {e}")
        return JsonResponse({'error': str(e)}, status=500)


@staff_member_required
def get_log_stats(request):
    """Get log file statistics"""
    try:
        stats = {}
        logs_dir = os.path.join(settings.BASE_DIR, 'logs')
        
        if os.path.exists(logs_dir):
            for file in os.listdir(logs_dir):
                if file.endswith('.log'):
                    file_path = os.path.join(logs_dir, file)
                    file_stats = os.stat(file_path)
                    
                    # Count log levels in the file
                    level_counts = {'DEBUG': 0, 'INFO': 0, 'WARNING': 0, 'ERROR': 0, 'CRITICAL': 0}
                    
                    try:
                        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                            for line in f:
                                for level in level_counts.keys():
                                    if line.startswith(level):
                                        level_counts[level] += 1
                                        break
                    except Exception:
                        pass
                    
                    stats[file] = {
                        'size': file_stats.st_size,
                        'size_mb': round(file_stats.st_size / (1024 * 1024), 2),
                        'modified': datetime.fromtimestamp(file_stats.st_mtime).isoformat(),
                        'level_counts': level_counts,
                        'total_entries': sum(level_counts.values())
                    }
        
        return JsonResponse({
            'success': True,
            'stats': stats
        })
        
    except Exception as e:
        logger.error(f"Error getting log stats: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)
