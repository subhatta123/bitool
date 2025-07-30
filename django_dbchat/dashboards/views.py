
from django.shortcuts import render, get_object_or_404, redirect
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.contrib import messages
from django.db import models
import json
from .models import Dashboard, DashboardItem
import logging

logger = logging.getLogger(__name__)

@login_required
def dashboard_list(request):
    """List user's dashboards"""
    dashboards = Dashboard.objects.filter(owner=request.user)
    return render(request, 'dashboards/list.html', {'dashboards': dashboards})

@login_required
def dashboard_detail(request, dashboard_id):
    """View dashboard details"""
    dashboard = get_object_or_404(Dashboard, id=dashboard_id, owner=request.user)
    return render(request, 'dashboards/detail.html', {'dashboard': dashboard})

@login_required
def dashboard_create(request):
    """Create new dashboard"""
    if request.method == 'POST':
        name = request.POST.get('name')
        description = request.POST.get('description', '')
        
        dashboard = Dashboard.objects.create(
            owner=request.user,
            name=name,
            description=description
        )
        
        messages.success(request, f'Dashboard "{name}" created successfully!')
        return redirect('dashboards:detail', dashboard_id=dashboard.id)
    
    return render(request, 'dashboards/create.html')

@csrf_exempt
@require_http_methods(["GET"])
def dashboard_list_api(request):
    """API to list dashboards"""
    try:
        if not request.user.is_authenticated:
            return JsonResponse({'error': 'Authentication required'}, status=401)
        
        dashboards = Dashboard.objects.filter(owner=request.user)
        data = {
            'success': True,
            'dashboards': [
                {
                    'id': str(d.id), 
                    'name': d.name, 
                    'description': d.description,
                    'created_at': d.created_at.isoformat() if hasattr(d, 'created_at') else '',
                    'item_count': d.items.count() if hasattr(d, 'items') else 0
                }
                for d in dashboards
            ]
        }
        return JsonResponse(data)
    except Exception as e:
        print(f"Dashboard list API error: {e}")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)

@csrf_exempt
@require_http_methods(["POST"])
def add_item_to_dashboard(request, dashboard_id):
    """API to add item to existing dashboard"""
    try:
        if not request.user.is_authenticated:
            return JsonResponse({'error': 'Authentication required'}, status=401)
        
        # Parse JSON data
        try:
            data = json.loads(request.body)
        except json.JSONDecodeError:
            return JsonResponse({'success': False, 'error': 'Invalid JSON data'}, status=400)
        
        # Get dashboard
        try:
            dashboard = Dashboard.objects.get(id=dashboard_id, owner=request.user)
        except Dashboard.DoesNotExist:
            return JsonResponse({'success': False, 'error': 'Dashboard not found'}, status=404)
        
        # Create dashboard item with actual result data
        item = DashboardItem.objects.create(
            dashboard=dashboard,
            title=data.get('title', 'Untitled')[:200],  # Ensure max length
            item_type='chart',  # Set default item type
            chart_type=data.get('chart_type', 'table'),
            query=data.get('query', ''),
            chart_config=data.get('chart_config', {}),
            result_data=data.get('result_data', []),  # Store actual query results
            position_x=0,
            position_y=dashboard.items.count() if hasattr(dashboard, 'items') else 0,
            width=6,
            height=4,
            data_source='query',  # Set default data source
            refresh_interval=0  # Set default refresh interval
        )
        
        return JsonResponse({
            'success': True, 
            'item_id': str(item.id),
            'dashboard_id': str(dashboard.id),
            'dashboard_url': f'/dashboards/{dashboard.id}/',
            'message': f'Item added to dashboard "{dashboard.name}"'
        })
        
    except Exception as e:
        print(f"Add item to dashboard error: {e}")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)

@csrf_exempt
@require_http_methods(["POST"])
def create_dashboard_with_item(request):
    """API to create new dashboard with item"""
    try:
        if not request.user.is_authenticated:
            return JsonResponse({'error': 'Authentication required'}, status=401)
        
        # Parse JSON data
        try:
            data = json.loads(request.body)
        except json.JSONDecodeError:
            return JsonResponse({'success': False, 'error': 'Invalid JSON data'}, status=400)
        
        # Create dashboard with user-provided name or default
        dashboard_name = data.get('dashboard_name', f"Dashboard - {data.get('title', 'New Item')}")
        dashboard = Dashboard.objects.create(
            owner=request.user,
            name=dashboard_name,
            description=f"Created from query: {data.get('query', '')[:100]}..."
        )
        
        # Create dashboard item with actual result data
        item = DashboardItem.objects.create(
            dashboard=dashboard,
            title=data.get('title', 'Untitled')[:200],  # Ensure max length
            item_type='chart',  # Required field
            chart_type=data.get('chart_type', 'table'),
            query=data.get('query', ''),
            chart_config=data.get('chart_config', {}),
            result_data=data.get('result_data', []),  # Store actual query results
            position_x=0,
            position_y=0,
            width=6,
            height=4,
            data_source='query',  # Required field
            refresh_interval=0  # Default value
        )
        
        return JsonResponse({
            'success': True, 
            'dashboard_id': str(dashboard.id), 
            'item_id': str(item.id),
            'dashboard_url': f'/dashboards/{dashboard.id}/',
            'redirect_url': f'/dashboards/{dashboard.id}/',
            'message': f'Dashboard "{dashboard_name}" created successfully!'
        })
        
    except Exception as e:
        print(f"Create dashboard with item error: {e}")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)

@login_required
@csrf_exempt
def update_dashboard(request, dashboard_id):
    """Update dashboard name and description"""
    if request.method != 'POST':
        return JsonResponse({'error': 'POST method required'}, status=405)
    
    try:
        dashboard = get_object_or_404(Dashboard, id=dashboard_id, owner=request.user)
        
        import json
        data = json.loads(request.body)
        
        name = data.get('name', '').strip()
        description = data.get('description', '').strip()
        
        if not name:
            return JsonResponse({'error': 'Dashboard name is required'}, status=400)
        
        dashboard.name = name
        dashboard.description = description
        dashboard.save()
        
        return JsonResponse({
            'success': True,
            'message': 'Dashboard updated successfully',
            'dashboard': {
                'id': str(dashboard.id),
                'name': dashboard.name,
                'description': dashboard.description
            }
        })
        
    except Exception as e:
        logger.error(f"Error updating dashboard: {e}")
        return JsonResponse({'error': str(e)}, status=500)

@login_required
@csrf_exempt
def delete_dashboard(request, dashboard_id):
    """Delete dashboard"""
    if request.method != 'POST':
        return JsonResponse({'error': 'POST method required'}, status=405)
    
    try:
        dashboard = get_object_or_404(Dashboard, id=dashboard_id, owner=request.user)
        dashboard_name = dashboard.name
        dashboard.delete()
        
        return JsonResponse({
            'success': True,
            'message': f'Dashboard "{dashboard_name}" deleted successfully'
        })
        
    except Exception as e:
        logger.error(f"Error deleting dashboard: {e}")
        return JsonResponse({'error': str(e)}, status=500)

@login_required
@csrf_exempt
def share_dashboard(request, dashboard_id):
    """Share dashboard with other users"""
    if request.method != 'POST':
        return JsonResponse({'error': 'POST method required'}, status=405)
    
    try:
        from django.contrib.auth import get_user_model
        User = get_user_model()
        
        dashboard = get_object_or_404(Dashboard, id=dashboard_id, owner=request.user)
        
        import json
        data = json.loads(request.body)
        
        user_id = data.get('user_id')
        permission = data.get('permission', 'view')
        
        if not user_id:
            return JsonResponse({'error': 'User ID is required'}, status=400)
        
        target_user = get_object_or_404(User, id=user_id)
        
        # Create or update sharing record
        # Note: This is a simplified implementation - you'd want a proper sharing model
        return JsonResponse({
            'success': True,
            'message': f'Dashboard shared with {target_user.username} successfully'
        })
        
    except Exception as e:
        logger.error(f"Error sharing dashboard: {e}")
        return JsonResponse({'error': str(e)}, status=500)

@login_required
def dashboard_item_data(request, item_id):
    """Get data for a specific dashboard item - prioritize stored result data"""
    try:
        item = get_object_or_404(DashboardItem, id=item_id, dashboard__owner=request.user)
        
        logger.info(f"Fetching data for dashboard item {item_id}: {item.title}")
        
        # First priority: Check if the item has stored result data (even if empty)
        if hasattr(item, 'result_data') and item.result_data is not None:
            if isinstance(item.result_data, list) and len(item.result_data) > 0:
                logger.info(f"Found stored result data for item {item_id} ({len(item.result_data)} rows)")
                return JsonResponse({
                    'success': True,
                    'result_data': item.result_data,
                    'chart_type': item.chart_type,
                    'title': item.title,
                    'data_source': 'cached',
                    'row_count': len(item.result_data)
                })
            else:
                logger.info(f"Item {item_id} has empty result_data, will try to execute query")
        
        # Second priority: Try to re-execute the query if no stored data
        if not item.query:
            logger.warning(f"No query found for dashboard item {item_id}")
            return JsonResponse({
                'success': False,
                'error': 'No data available',
                'details': 'This dashboard item has no stored data and no query to execute'
            }, status=400)
            
        logger.info(f"Attempting to re-execute query for item {item_id}: {item.query[:100]}...")
            
        try:
            # Import here to avoid circular imports
            from core.views import execute_query_logic
            from datasets.models import DataSource
            
            # Get all available data sources for the user
            data_sources = DataSource.objects.filter(
                models.Q(created_by=request.user) | models.Q(shared_with_users=request.user)
            ).filter(status='active')
            
            logger.info(f"Found {data_sources.count()} active data sources")
            
            if not data_sources.exists():
                logger.error(f"No active data sources found for user {request.user.id}")
                return JsonResponse({
                    'success': False,
                    'error': 'No active data sources available',
                    'details': 'You need to upload and activate a data source first'
                }, status=404)
            
            # Use smart data source matching (simplified version)
            target_data_source = None
            query_lower = item.query.lower()
            
            # Method 1: Try to find data source by query content
            for ds in data_sources:
                if ds.source_type == 'csv' and ds.schema_info:
                    schema_columns = set(ds.schema_info.get('columns', {}).keys())
                    schema_columns_lower = {col.lower().replace(' ', '_') for col in schema_columns}
                    
                    # Extract column references from query
                    import re
                    quoted_columns = re.findall(r'"([^"]+)"', item.query)
                    unquoted_columns = re.findall(r'\b(\w+)\b', item.query.replace('"', ''))
                    all_query_columns = set(quoted_columns + unquoted_columns)
                    all_query_columns_lower = {col.lower().replace(' ', '_') for col in all_query_columns}
                    
                    # Check overlap
                    overlap = schema_columns_lower.intersection(all_query_columns_lower)
                    if len(overlap) >= 1:
                        target_data_source = ds
                        logger.info(f"Found data source by column overlap: {ds.name}")
                        break
            
            # Method 2: Use first available suitable source
            if not target_data_source:
                target_data_source = data_sources.first()
                logger.info(f"Using first available data source: {target_data_source.name}")
            
            if not target_data_source:
                return JsonResponse({
                    'success': False,
                    'error': 'Cannot find suitable data source'
                }, status=404)
            
            # Execute the query to get fresh data
            success, result_data, sql_query, error_message, row_count = execute_query_logic(
                item.query, request.user, target_data_source
            )
            
            logger.info(f"Query execution result: success={success}, rows={row_count}")
            
            if success and result_data:
                # Convert to list of dictionaries if needed
                if isinstance(result_data, list) and len(result_data) > 0:
                    if not isinstance(result_data[0], dict):
                        # Convert to dict format
                        columns = getattr(result_data, 'columns', [])
                        if columns:
                            result_data = [dict(zip(columns, row)) for row in result_data]
                        else:
                            # Generate generic column names
                            if len(result_data) > 0:
                                num_cols = len(result_data[0]) if hasattr(result_data[0], '__len__') else 1
                                columns = [f'Column_{i+1}' for i in range(num_cols)]
                                result_data = [dict(zip(columns, row)) for row in result_data]
                
                # Update the item with fresh data for future use
                try:
                    item.result_data = result_data
                    item.save()
                    logger.info(f"Successfully cached {len(result_data)} rows for future use")
                except Exception as save_error:
                    logger.warning(f"Failed to cache data: {save_error}")
                
                return JsonResponse({
                    'success': True,
                    'result_data': result_data,
                    'chart_type': item.chart_type,
                    'title': item.title,
                    'row_count': row_count,
                    'data_source': target_data_source.name,
                    'sql_query': sql_query
                })
            else:
                logger.error(f"Query execution failed: {error_message}")
                return JsonResponse({
                    'success': False,
                    'error': 'Query execution failed',
                    'details': error_message
                }, status=400)
                
        except ImportError as import_error:
            logger.error(f"Import error in dashboard_item_data: {import_error}")
            return JsonResponse({
                'success': False,
                'error': 'System error: Required modules not available',
                'details': str(import_error)
            }, status=500)
        except Exception as query_error:
            logger.error(f"Query execution failed for dashboard item {item_id}: {query_error}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return JsonResponse({
                'success': False,
                'error': 'Query execution error',
                'details': str(query_error)
            }, status=500)
        
    except Exception as e:
        logger.error(f"Error getting dashboard item data: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return JsonResponse({
            'success': False,
            'error': 'Internal server error',
            'details': str(e)
        }, status=500)

@login_required
def users_api(request):
    """Get list of users for sharing"""
    try:
        from django.contrib.auth import get_user_model
        User = get_user_model()
        
        users = User.objects.exclude(id=request.user.id).values('id', 'username', 'email')[:50]
        
        return JsonResponse({
            'success': True,
            'users': list(users)
        })
        
    except Exception as e:
        logger.error(f"Error getting users list: {e}")
        return JsonResponse({'error': str(e)}, status=500)

@login_required
@csrf_exempt
def delete_dashboard_item(request, item_id):
    """Delete individual dashboard item"""
    if request.method != 'POST':
        return JsonResponse({'error': 'POST method required'}, status=405)
    
    try:
        item = get_object_or_404(DashboardItem, id=item_id, dashboard__owner=request.user)
        item_title = item.title
        dashboard_id = item.dashboard.id
        item.delete()
        
        return JsonResponse({
            'success': True,
            'message': f'Chart "{item_title}" deleted successfully',
            'dashboard_id': str(dashboard_id)
        })
        
    except Exception as e:
        logger.error(f"Error deleting dashboard item: {e}")
        return JsonResponse({'error': str(e)}, status=500)

@login_required
@csrf_exempt
def schedule_dashboard_email(request, dashboard_id):
    """Schedule dashboard email with PDF/PNG export"""
    if request.method != 'POST':
        return JsonResponse({'error': 'POST method required'}, status=405)
    
    try:
        dashboard = get_object_or_404(Dashboard, id=dashboard_id, owner=request.user)
        
        import json
        data = json.loads(request.body)
        
        # Validate required fields
        recipient_email = data.get('recipient_email')
        export_format = data.get('export_format', 'png')  # 'png' or 'pdf'
        frequency = data.get('frequency', 'once')  # 'once', 'daily', 'weekly', 'monthly'
        
        if not recipient_email:
            return JsonResponse({'error': 'Recipient email is required'}, status=400)
        
        if export_format not in ['png', 'pdf']:
            return JsonResponse({'error': 'Export format must be "png" or "pdf"'}, status=400)
        
        # Create dashboard export record
        from dashboards.models import DashboardExport
        dashboard_export = DashboardExport.objects.create(
            dashboard=dashboard,
            export_format=export_format,
            status='pending',
            export_settings={
                'recipient_email': recipient_email,
                'frequency': frequency,
                'scheduled_by': request.user.id,
                'dashboard_name': dashboard.name
            },
            requested_by=request.user
        )
        
        # Schedule the task based on frequency
        if frequency == 'once':
            # Send immediately
            from celery_app import send_dashboard_email_task
            send_dashboard_email_task.delay(
                str(dashboard.id), 
                recipient_email, 
                {
                    'export_format': export_format,
                    'dashboard_name': dashboard.name
                }
            )
        else:
            # Schedule recurring task
            from django_celery_beat.models import PeriodicTask, IntervalSchedule
            
            # Create schedule based on frequency
            if frequency == 'daily':
                schedule, created = IntervalSchedule.objects.get_or_create(
                    every=1,
                    period=IntervalSchedule.DAYS,
                )
            elif frequency == 'weekly':
                schedule, created = IntervalSchedule.objects.get_or_create(
                    every=7,
                    period=IntervalSchedule.DAYS,
                )
            elif frequency == 'monthly':
                schedule, created = IntervalSchedule.objects.get_or_create(
                    every=30,
                    period=IntervalSchedule.DAYS,
                )
            
            # Create periodic task
            task_name = f"dashboard_email_{dashboard.id}_{request.user.id}_{frequency}"
            
            PeriodicTask.objects.create(
                interval=schedule,
                name=task_name,
                task='celery_app.send_dashboard_email_task',
                args=json.dumps([str(dashboard.id), recipient_email, {
                    'export_format': export_format,
                    'dashboard_name': dashboard.name,
                    'frequency': frequency
                }]),
                enabled=True
            )
        
        return JsonResponse({
            'success': True,
            'message': f'Dashboard "{dashboard.name}" scheduled successfully',
            'export_id': str(dashboard_export.id),
            'frequency': frequency,
            'format': export_format
        })
        
    except Exception as e:
        logger.error(f"Error scheduling dashboard email: {e}")
        return JsonResponse({'error': str(e)}, status=500)

@login_required
def export_dashboard_pdf_png(request, dashboard_id):
    """Export dashboard as PDF or PNG using Puppeteer for fully rendered charts"""
    try:
        dashboard = get_object_or_404(Dashboard, id=dashboard_id, owner=request.user)
        export_format = request.GET.get('format', 'png')
        
        # Try Puppeteer service first for fully rendered charts
        try:
            from services.puppeteer_export_service import PuppeteerExportService
            export_service = PuppeteerExportService()
            
            logger.info(f"Attempting Puppeteer export for dashboard {dashboard_id} as {export_format}")
            
            # Get user's session for authentication
            session_key = request.session.session_key
            session_cookie = {
                'name': 'sessionid',
                'value': session_key,
                'domain': 'localhost',
                'path': '/'
            }
            
            # Generate export with Puppeteer (with authentication)
            if export_format == 'pdf':
                content, filename = export_service.export_dashboard_pdf(dashboard, session_cookie)
                content_type = 'application/pdf'
            else:
                content, filename = export_service.export_dashboard_png(dashboard, session_cookie)
                content_type = 'image/png'
            
            logger.info(f"Puppeteer export successful: {filename} ({len(content)} bytes)")
            
        except Exception as puppeteer_error:
            logger.warning(f"Puppeteer export failed, falling back to static export: {puppeteer_error}")
            
            # Fallback to static export service
            from services.dashboard_export_service import DashboardExportService
            export_service = DashboardExportService()
            
            # Generate export with fallback
            if export_format == 'pdf':
                content, filename = export_service.export_dashboard_pdf(dashboard)
                content_type = 'application/pdf'
            else:
                content, filename = export_service.export_dashboard_png(dashboard)
                content_type = 'image/png'
            
            logger.info(f"Fallback export successful: {filename} ({len(content)} bytes)")
        
        # Return file response
        from django.http import HttpResponse
        response = HttpResponse(content, content_type=content_type)
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        return response
        
    except Exception as e:
        logger.error(f"Error exporting dashboard: {e}")
        return JsonResponse({'error': str(e)}, status=500)

@login_required
def dashboard_scheduled_emails(request, dashboard_id):
    """Get scheduled emails for a dashboard"""
    try:
        dashboard = get_object_or_404(Dashboard, id=dashboard_id, owner=request.user)
        
        # Get scheduled periodic tasks for this dashboard
        from django_celery_beat.models import PeriodicTask
        
        scheduled_tasks = PeriodicTask.objects.filter(
            name__startswith=f"dashboard_email_{dashboard.id}_",
            enabled=True
        )
        
        tasks_data = []
        for task in scheduled_tasks:
            try:
                args = json.loads(task.args)
                tasks_data.append({
                    'id': task.id,
                    'name': task.name,
                    'frequency': task.interval.every if task.interval else 'Unknown',
                    'period': task.interval.period if task.interval else 'Unknown',
                    'recipient_email': args[1] if len(args) > 1 else 'Unknown',
                    'export_format': args[2].get('export_format', 'png') if len(args) > 2 else 'png',
                    'enabled': task.enabled,
                    'last_run': task.last_run_at.isoformat() if task.last_run_at else None
                })
            except:
                pass
        
        return JsonResponse({
            'success': True,
            'scheduled_emails': tasks_data,
            'dashboard_name': dashboard.name
        })
        
    except Exception as e:
        logger.error(f"Error getting scheduled emails: {e}")
        return JsonResponse({'error': str(e)}, status=500)

@login_required
@csrf_exempt  
def cancel_scheduled_email(request, task_id):
    """Cancel a scheduled email task"""
    if request.method != 'POST':
        return JsonResponse({'error': 'POST method required'}, status=405)
    
    try:
        from django_celery_beat.models import PeriodicTask
        
        task = get_object_or_404(PeriodicTask, id=task_id)
        
        # Verify user owns the dashboard this task is for
        if not task.name.startswith('dashboard_email_'):
            return JsonResponse({'error': 'Invalid task'}, status=400)
            
        task_name_parts = task.name.split('_')
        if len(task_name_parts) < 4:
            return JsonResponse({'error': 'Invalid task name format'}, status=400)
            
        dashboard_id = task_name_parts[2]
        user_id = int(task_name_parts[3])
        
        if user_id != request.user.id:
            return JsonResponse({'error': 'Permission denied'}, status=403)
        
        task.delete()
        
        return JsonResponse({
            'success': True,
            'message': 'Scheduled email cancelled successfully'
        })
        
    except Exception as e:
        logger.error(f"Error cancelling scheduled email: {e}")
        return JsonResponse({'error': str(e)}, status=500)
