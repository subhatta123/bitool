"""
Dashboard views for ConvaBI Application
Handles dashboard creation, management, sharing, and export functionality
"""

import json
import uuid
from typing import Dict, List, Any, Optional
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.utils.decorators import method_decorator
from django.views import View
from django.views.generic import ListView, DetailView, CreateView
from django.http import JsonResponse, HttpResponse
from django.contrib import messages
from django.core.paginator import Paginator
from django.db import transaction, models
from django.urls import reverse
import logging

from .models import Dashboard, DashboardItem, DashboardShare
from services.email_service import EmailService, send_dashboard_share_notification
from core.utils import generate_dashboard_html, create_plotly_figure, format_data_for_display
from accounts.models import CustomUser

logger = logging.getLogger(__name__)


def get_dashboard_permissions(dashboard, user):
    """
    Get user permissions for a dashboard.
    Returns: (has_access, can_edit, permission_level)
    """
    if dashboard.owner == user:
        return True, True, 'owner'
    
    # Check for shared permissions
    try:
        share = DashboardShare.objects.get(dashboard=dashboard, user=user)
        has_access = True
        can_edit = share.permission in ['edit', 'admin']
        return has_access, can_edit, share.permission
    except DashboardShare.DoesNotExist:
        return False, False, None


@method_decorator(login_required, name='dispatch')
class DashboardListView(ListView):
    """List user dashboards with search and filtering"""
    
    model = Dashboard
    template_name = 'dashboards/list.html'
    context_object_name = 'dashboards'
    paginate_by = 12
    
    def get_queryset(self):
        """Get dashboards accessible to the current user"""
        user = self.request.user
        
        # Get owned dashboards and shared dashboards
        queryset = Dashboard.objects.filter(
            models.Q(owner=user) | models.Q(shared_with_users=user)
        ).distinct().order_by('-updated_at')
        
        # Apply search filter
        search_query = self.request.GET.get('search')
        if search_query:
            queryset = queryset.filter(
                models.Q(name__icontains=search_query) |
                models.Q(description__icontains=search_query)
            )
        
        # Apply filters (category filter removed - field doesn't exist)
        
        return queryset
    
    def get_context_data(self, **kwargs):
        """Add additional context for the template"""
        context = super().get_context_data(**kwargs)
        
        # Add dashboard statistics
        user = self.request.user
        context['owned_count'] = Dashboard.objects.filter(owner=user).count()
        context['shared_count'] = Dashboard.objects.filter(shared_with_users=user).count()
        
        # Add search and filter values
        context['search_query'] = self.request.GET.get('search', '')
        context['selected_category'] = ''  # Category field removed
        
        # Add available categories (removed - field doesn't exist)
        context['categories'] = []
        
        return context


@method_decorator(login_required, name='dispatch')
class DashboardDetailView(DetailView):
    """Display and edit dashboard"""
    
    model = Dashboard
    template_name = 'dashboards/detail.html'
    context_object_name = 'dashboard'
    
    def get_object(self):
        """Get dashboard with access check"""
        dashboard = get_object_or_404(Dashboard, id=self.kwargs['pk'])
        
        # Check if user has access using the permission helper
        has_access, can_edit, permission_level = get_dashboard_permissions(dashboard, self.request.user)
        
        if not has_access:
            messages.error(self.request, "You don't have access to this dashboard")
            return redirect('dashboards:list')
        
        return dashboard
    
    def get_context_data(self, **kwargs):
        """Add dashboard items and configuration data"""
        context = super().get_context_data(**kwargs)
        dashboard = self.object
        
        # Get dashboard items
        dashboard_items = DashboardItem.objects.filter(
            dashboard=dashboard
        ).order_by('position_x', 'position_y')
        
        # Prepare items for frontend
        items_data = []
        for item in dashboard_items:
            item_data = {
                'id': str(item.id),
                'title': item.title,
                'type': item.item_type,
                'position_x': item.position_x,
                'position_y': item.position_y,
                'width': item.width,
                'height': item.height,
                'chart_config': item.chart_config,
                'query': item.query,
                'data_source': item.data_source,  # Add missing data_source field
            }
            items_data.append(item_data)
        
        # Check user permissions for editing
        has_access, can_edit, permission_level = get_dashboard_permissions(dashboard, self.request.user)
        
        context['dashboard_items'] = dashboard_items
        context['items_json'] = json.dumps(items_data)
        context['can_edit'] = can_edit
        context['permission_level'] = permission_level
        context['is_owner'] = dashboard.owner == self.request.user
        
        return context
    
    def post(self, request, *args, **kwargs):
        """Handle dashboard updates"""
        dashboard = self.get_object()
        
        # Check if user has edit permissions
        has_access, can_edit, permission_level = get_dashboard_permissions(dashboard, request.user)
        
        if not can_edit:
            return JsonResponse({'error': 'You do not have edit permissions for this dashboard'}, status=403)
        
        try:
            data = json.loads(request.body)
            action = data.get('action')
            
            if action == 'update_layout':
                # Update dashboard item positions
                items = data.get('items', [])
                with transaction.atomic():
                    for item_data in items:
                        try:
                            item = DashboardItem.objects.get(
                                id=item_data['id'], 
                                dashboard=dashboard
                            )
                            item.position_x = item_data.get('position_x', item.position_x)
                            item.position_y = item_data.get('position_y', item.position_y)
                            item.width = item_data.get('width', item.width)
                            item.height = item_data.get('height', item.height)
                            item.save()
                        except DashboardItem.DoesNotExist:
                            continue
                
                return JsonResponse({'success': True})
            
            elif action == 'update_dashboard':
                # Update dashboard metadata
                dashboard.name = data.get('name', dashboard.name)
                dashboard.description = data.get('description', dashboard.description)
                # dashboard.category removed - field doesn't exist
                dashboard.save()
                
                return JsonResponse({'success': True})
            
            else:
                return JsonResponse({'error': 'Unknown action'}, status=400)
                
        except Exception as e:
            logger.error(f"Error updating dashboard: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    def delete(self, request, *args, **kwargs):
        """Handle dashboard deletion"""
        dashboard = self.get_object()
        
        # Only owners can delete dashboards
        if dashboard.owner != request.user:
            return JsonResponse({'error': 'Permission denied. You can only delete dashboards you own.'}, status=403)
        
        try:
            dashboard_name = dashboard.name
            dashboard.delete()
            logger.info(f"Dashboard '{dashboard_name}' deleted successfully by user {request.user.username}.")
            return JsonResponse({'success': True, 'message': f"Dashboard '{dashboard_name}' and all its items have been deleted."})
        except Exception as e:
            logger.error(f"Error deleting dashboard {dashboard.id}: {e}")
            return JsonResponse({'error': 'An unexpected error occurred during deletion.'}, status=500)


@method_decorator(login_required, name='dispatch')
class DashboardCreateView(CreateView):
    """Create new dashboard"""
    
    model = Dashboard
    template_name = 'dashboards/create.html'
    fields = ['name', 'description']
    
    def form_valid(self, form):
        """Set the creator and handle successful creation"""
        form.instance.owner = self.request.user
        response = super().form_valid(form)
        
        messages.success(self.request, f"Dashboard '{form.instance.name}' created successfully!")
        return response
    
    def get_success_url(self):
        """Redirect to the new dashboard"""
        return reverse('dashboards:detail', kwargs={'pk': self.object.pk})


@method_decorator(login_required, name='dispatch')
class DashboardShareView(View):
    """Manage dashboard sharing"""
    
    def get(self, request, pk):
        """Display sharing interface"""
        dashboard = get_object_or_404(Dashboard, id=pk)
        
        if dashboard.owner != request.user:
            messages.error(request, "You can only share dashboards you created")
            return redirect('dashboards:detail', pk=pk)
        
        # Get dashboard shares with permission levels
        dashboard_shares = DashboardShare.objects.filter(dashboard=dashboard).select_related('user')
        shared_users = [share.user for share in dashboard_shares]
        
        # Get all users for sharing options
        all_users = CustomUser.objects.exclude(id=request.user.id).order_by('username')
        
        context = {
            'dashboard': dashboard,
            'dashboard_shares': dashboard_shares,
            'shared_users': shared_users,
            'all_users': all_users,
        }
        
        return render(request, 'dashboards/share.html', context)
    
    def post(self, request, pk):
        """Handle sharing actions"""
        dashboard = get_object_or_404(Dashboard, id=pk)
        
        if dashboard.owner != request.user:
            return JsonResponse({'error': 'Permission denied'}, status=403)
        
        try:
            data = json.loads(request.body)
            action = data.get('action')
            
            if action == 'add_user':
                user_id = data.get('user_id')
                permission = data.get('permission', 'edit')  # Get permission from frontend, default to edit
                
                try:
                    user = CustomUser.objects.get(id=user_id)
                    
                    # Create DashboardShare instance with specified permission
                    from .models import DashboardShare
                    share, created = DashboardShare.objects.get_or_create(
                        dashboard=dashboard,
                        user=user,
                        defaults={
                            'shared_by': request.user,
                            'permission': permission
                        }
                    )
                    
                    if created:
                        # Try to send notification email (fail gracefully if email not configured)
                        try:
                            send_dashboard_share_notification.delay(
                                str(dashboard.id),
                                user.email,
                                request.user.get_full_name() or request.user.username
                            )
                        except Exception as e:
                            logger.warning(f"Failed to send dashboard share notification: {e}")
                        
                        return JsonResponse({
                            'success': True,
                            'message': f'Dashboard shared with {user.username} ({permission} permission)'
                        })
                    else:
                        # If share already exists, update the permission
                        if share.permission != permission:
                            share.permission = permission
                            share.save()
                            return JsonResponse({
                                'success': True,
                                'message': f'Updated {user.username} permission to {permission}'
                            })
                        else:
                            return JsonResponse({
                                'success': True,
                                'message': f'Dashboard already shared with {user.username} ({permission} permission)'
                            })
                except CustomUser.DoesNotExist:
                    return JsonResponse({'error': 'User not found'}, status=404)
            
            elif action == 'remove_user':
                user_id = data.get('user_id')
                try:
                    user = CustomUser.objects.get(id=user_id)
                    
                    # Remove DashboardShare instance
                    from .models import DashboardShare
                    DashboardShare.objects.filter(
                        dashboard=dashboard,
                        user=user
                    ).delete()
                    
                    return JsonResponse({
                        'success': True,
                        'message': f'Removed {user.username} from dashboard'
                    })
                except CustomUser.DoesNotExist:
                    return JsonResponse({'error': 'User not found'}, status=404)
            
            elif action == 'update_permission':
                user_id = data.get('user_id')
                permission = data.get('permission')
                
                if permission not in ['view', 'edit', 'admin']:
                    return JsonResponse({'error': 'Invalid permission level'}, status=400)
                
                try:
                    user = CustomUser.objects.get(id=user_id)
                    
                    # Update DashboardShare permission
                    from .models import DashboardShare
                    share = DashboardShare.objects.get(
                        dashboard=dashboard,
                        user=user
                    )
                    share.permission = permission
                    share.save()
                    
                    return JsonResponse({
                        'success': True,
                        'message': f'Updated {user.username} permission to {permission}'
                    })
                except CustomUser.DoesNotExist:
                    return JsonResponse({'error': 'User not found'}, status=404)
                except DashboardShare.DoesNotExist:
                    return JsonResponse({'error': 'Share not found'}, status=404)
            
            else:
                return JsonResponse({'error': 'Unknown action'}, status=400)
                
        except Exception as e:
            logger.error(f"Error managing dashboard sharing: {e}")
            return JsonResponse({'error': str(e)}, status=500)


@method_decorator(login_required, name='dispatch')
class DashboardExportView(View):
    """Handle dashboard export functionality"""
    
    def get(self, request, pk):
        """Display export options"""
        dashboard = get_object_or_404(Dashboard, id=pk)
        
        # Check if user has access (view permissions are sufficient for export)
        has_access, can_edit, permission_level = get_dashboard_permissions(dashboard, request.user)
        
        if not has_access:
            messages.error(request, "You don't have access to this dashboard")
            return redirect('dashboards:list')
        
        context = {
            'dashboard': dashboard,
        }
        
        return render(request, 'dashboards/export.html', context)
    
    def post(self, request, pk):
        """Handle export requests"""
        dashboard = get_object_or_404(Dashboard, id=pk)
        
        # Check access
        if not (dashboard.owner == request.user or 
                request.user in dashboard.shared_with_users.all()):
            return JsonResponse({'error': 'Permission denied'}, status=403)
        
        try:
            data = json.loads(request.body)
            export_type = data.get('export_type')
            
            if export_type == 'email':
                # Email export
                recipient_email = data.get('recipient_email')
                include_html = data.get('include_html', True)
                include_pdf = data.get('include_pdf', False)
                include_image = data.get('include_image', True)
                
                if not recipient_email:
                    return JsonResponse({'error': 'Recipient email is required'}, status=400)
                
                # Generate dashboard content
                dashboard_items = list(DashboardItem.objects.filter(
                    dashboard=dashboard
                ).values())
                
                email_service = EmailService()
                dashboard_html = email_service.generate_dashboard_html(
                    dashboard_items, dashboard.name
                )
                
                # Prepare attachments
                attachments = []
                if include_html:
                    attachments.append({
                        'content': dashboard_html,
                        'filename': dashboard.name.replace(' ', '_'),
                        'type': 'html'
                    })
                
                if include_pdf:
                    try:
                        from services.dashboard_export_service import DashboardExportService
                        export_service = DashboardExportService()
                        pdf_content = export_service.export_to_pdf(dashboard, dashboard_items)
                        attachments.append({
                            'content': pdf_content,
                            'filename': dashboard.name.replace(' ', '_'),
                            'type': 'pdf'
                        })
                    except Exception as e:
                        logger.warning(f"Failed to generate PDF for email: {e}")
                
                if include_image:
                    try:
                        from services.dashboard_export_service import DashboardExportService
                        export_service = DashboardExportService()
                        png_content = export_service.export_to_png(dashboard, dashboard_items)
                        attachments.append({
                            'content': png_content,
                            'filename': dashboard.name.replace(' ', '_'),
                            'type': 'png'
                        })
                    except Exception as e:
                        logger.warning(f"Failed to generate PNG for email: {e}")
                        # Fallback to old method
                        image_bytes = email_service.generate_dashboard_image(
                            dashboard_html, dashboard.name
                        )
                        if image_bytes:
                            attachments.append({
                                'content': image_bytes,
                                'filename': dashboard.name.replace(' ', '_'),
                                'type': 'png'
                            })
                
                # Send email
                subject = f"Dashboard Export: {dashboard.name}"
                body = f"<p>Dashboard <strong>{dashboard.name}</strong> is attached.</p>"
                
                success = email_service.send_dashboard_email(
                    recipient_email=recipient_email,
                    subject=subject,
                    body=body,
                    attachments=attachments
                )
                
                if success:
                    return JsonResponse({'success': True, 'message': 'Dashboard emailed successfully'})
                else:
                    return JsonResponse({'error': 'Failed to send email'}, status=500)
            
            elif export_type == 'download':
                # Direct download
                format_type = data.get('format', 'html')
                
                dashboard_items = list(DashboardItem.objects.filter(
                    dashboard=dashboard
                ).values())
                
                if format_type == 'html':
                    email_service = EmailService()
                    content = email_service.generate_dashboard_html(
                        dashboard_items, dashboard.name
                    )
                    
                    response = HttpResponse(content, content_type='text/html')
                    response['Content-Disposition'] = f'attachment; filename="{dashboard.name}.html"'
                    return response
                
                elif format_type == 'pdf':
                    # PDF export using new export service
                    from services.dashboard_export_service import DashboardExportService
                    
                    export_service = DashboardExportService()
                    try:
                        pdf_content = export_service.export_to_pdf(dashboard, dashboard_items)
                        filename = export_service.get_export_filename(dashboard, 'pdf')
                        
                        response = HttpResponse(pdf_content, content_type='application/pdf')
                        response['Content-Disposition'] = f'attachment; filename="{filename}"'
                        return response
                    except Exception as e:
                        return JsonResponse({'error': f'PDF generation failed: {str(e)}'}, status=500)
                
                elif format_type == 'png':
                    # PNG export using new export service
                    from services.dashboard_export_service import DashboardExportService
                    
                    export_service = DashboardExportService()
                    try:
                        png_content = export_service.export_to_png(dashboard, dashboard_items)
                        filename = export_service.get_export_filename(dashboard, 'png')
                        
                        response = HttpResponse(png_content, content_type='image/png')
                        response['Content-Disposition'] = f'attachment; filename="{filename}"'
                        return response
                    except Exception as e:
                        return JsonResponse({'error': f'PNG generation failed: {str(e)}'}, status=500)
                
                else:
                    return JsonResponse({'error': 'Unsupported format'}, status=400)
            
            else:
                return JsonResponse({'error': 'Unknown export type'}, status=400)
                
        except Exception as e:
            logger.error(f"Error exporting dashboard: {e}")
            return JsonResponse({'error': str(e)}, status=500)


@method_decorator(login_required, name='dispatch')
class DashboardItemView(View):
    """Manage individual dashboard items"""
    
    def post(self, request, pk):
        """Create new dashboard item"""
        dashboard = get_object_or_404(Dashboard, id=pk)
        
        # Check if user has edit permissions
        has_access, can_edit, permission_level = get_dashboard_permissions(dashboard, request.user)
        
        if not can_edit:
            return JsonResponse({'error': 'You do not have edit permissions for this dashboard'}, status=403)
        
        try:
            data = json.loads(request.body)
            action = data.get('action')
            
            if action == 'create':
                # Create new dashboard item
                item = DashboardItem.objects.create(
                    dashboard=dashboard,
                    title=data.get('title', 'New Item'),
                    item_type=data.get('type', 'chart'),
                    position_x=data.get('position_x', 0),
                    position_y=data.get('position_y', 0),
                    width=data.get('width', 4),
                    height=data.get('height', 3),
                    chart_config=data.get('chart_config', {}),
                    query=data.get('query', ''),
                    data_source=data.get('data_source', '')
                )
                
                return JsonResponse({
                    'success': True,
                    'item_id': str(item.id),
                    'message': 'Dashboard item created'
                })
            
            elif action == 'update':
                # Update existing item
                item_id = data.get('item_id')
                try:
                    item = DashboardItem.objects.get(id=item_id, dashboard=dashboard)
                    
                    # Update fields that exist in the model
                    item.title = data.get('title', item.title)
                    item.width = data.get('width', item.width)
                    item.height = data.get('height', item.height)
                    item.chart_config = data.get('chart_config', item.chart_config)
                    item.query = data.get('query', item.query)
                    item.data_source = data.get('data_source', item.data_source)
                    item.save()
                    
                    return JsonResponse({'success': True, 'message': 'Item updated'})
                except DashboardItem.DoesNotExist:
                    return JsonResponse({'error': 'Item not found'}, status=404)
            
            elif action == 'delete':
                # Delete item
                item_id = data.get('item_id')
                try:
                    item = DashboardItem.objects.get(id=item_id, dashboard=dashboard)
                    item.delete()
                    return JsonResponse({'success': True, 'message': 'Item deleted'})
                except DashboardItem.DoesNotExist:
                    return JsonResponse({'error': 'Item not found'}, status=404)
            
            else:
                return JsonResponse({'error': 'Unknown action'}, status=400)
                
        except Exception as e:
            logger.error(f"Error managing dashboard item: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    def delete(self, request, pk, item_id):
        """Delete specific dashboard item"""
        dashboard = get_object_or_404(Dashboard, id=pk)
        
        # Check if user has edit permissions
        has_access, can_edit, permission_level = get_dashboard_permissions(dashboard, request.user)
        
        if not can_edit:
            return JsonResponse({'error': 'You do not have edit permissions for this dashboard'}, status=403)
        
        try:
            item = DashboardItem.objects.get(id=item_id, dashboard=dashboard)
            item.delete()
            return JsonResponse({'success': True, 'message': 'Item deleted'})
        except DashboardItem.DoesNotExist:
            return JsonResponse({'error': 'Item not found'}, status=404)
        except Exception as e:
            logger.error(f"Error deleting dashboard item: {e}")
            return JsonResponse({'error': str(e)}, status=500)


@login_required
def dashboard_data_refresh(request, pk):
    """Refresh dashboard data"""
    dashboard = get_object_or_404(Dashboard, id=pk)
    
    # Check if user has access (view permissions are sufficient for refresh)
    has_access, can_edit, permission_level = get_dashboard_permissions(dashboard, request.user)
    
    if not has_access:
        return JsonResponse({'error': 'You do not have access to this dashboard'}, status=403)
    
    try:
        # Get all dashboard items that need data refresh
        items = DashboardItem.objects.filter(dashboard=dashboard)
        refreshed_items = []
        
        for item in items:
            if item.query:  # Using correct field name 'query'
                # Here you would re-execute the SQL query and update the data
                # This is a placeholder for the actual data refresh logic
                refreshed_items.append({
                    'id': str(item.id),
                    'title': item.title,
                    'status': 'refreshed'
                })
        
        return JsonResponse({
            'success': True,
            'refreshed_items': refreshed_items,
            'message': f'Refreshed {len(refreshed_items)} items'
        })
        
    except Exception as e:
        logger.error(f"Error refreshing dashboard data: {e}")
        return JsonResponse({'error': str(e)}, status=500)


@login_required  
def dashboard_clone(request, pk):
    """Clone an existing dashboard"""
    dashboard = get_object_or_404(Dashboard, id=pk)
    
    # Check if user has access (view permissions are sufficient for cloning)
    has_access, can_edit, permission_level = get_dashboard_permissions(dashboard, request.user)
    
    if not has_access:
        messages.error(request, "You don't have access to this dashboard")
        return redirect('dashboards:list')
    
    try:
        with transaction.atomic():
            # Clone dashboard
            cloned_dashboard = Dashboard.objects.create(
                name=f"{dashboard.name} (Copy)",
                description=dashboard.description,
                # category removed - field doesn't exist
                owner=request.user
            )
            
            # Clone dashboard items
            items = DashboardItem.objects.filter(dashboard=dashboard)
            for item in items:
                DashboardItem.objects.create(
                    dashboard=cloned_dashboard,
                    title=item.title,
                    item_type=item.item_type,
                    position_x=item.position_x,
                    position_y=item.position_y,
                    width=item.width,
                    height=item.height,
                    chart_config=item.chart_config,
                    query=item.query,
                    data_source=item.data_source
                )
            
            messages.success(request, f"Dashboard cloned as '{cloned_dashboard.name}'")
            return redirect('dashboards:detail', pk=cloned_dashboard.pk)
            
    except Exception as e:
        logger.error(f"Error cloning dashboard: {e}")
        messages.error(request, "Failed to clone dashboard")
        return redirect('dashboards:detail', pk=pk)


@login_required
def dashboard_list(request):
    """List user dashboards"""
    return render(request, 'dashboards/list.html')

@login_required  
def dashboard_detail(request, pk):
    """Display dashboard details"""
    return render(request, 'dashboards/detail.html') 