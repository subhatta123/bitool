"""
Dataset views for ConvaBI Application
Handles data source management, integration, and semantic layer functionality
"""

import json
import pandas as pd
from typing import Dict, List, Any, Optional
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.utils.decorators import method_decorator
from django.views import View
from django.views.generic import ListView, CreateView
from django.http import JsonResponse, HttpResponse
from django.contrib import messages
from django.core.files.storage import default_storage
from django.core.files.base import ContentFile
from django.db import transaction
from django.utils import timezone
import logging
import os
from django.conf import settings
import time
import uuid
from django.db import models
from django.db import connection as db_connection
import re
from rest_framework.decorators import api_view
from django.views.decorators.csrf import csrf_exempt
import numpy as np

from .models import DataSource, ETLOperation, DataIntegrationJob, ScheduledETLJob, ETLJobRunLog
from services.data_service import DataService
from services.integration_service import DataIntegrationService
from services.semantic_service import SemanticService
from utils.workflow_manager import WorkflowManager, WorkflowStep
from utils.type_helpers import get_column_type_info
from licensing.decorators import creator_required, viewer_or_creator_required

logger = logging.getLogger(__name__)

def safe_serialize_for_template(data: Any) -> Any:
    """
    Safely serialize data for Django templates, ensuring no DataFrames are passed directly.
    This prevents the 'DataFrame truth value is ambiguous' error.
    """
    if isinstance(data, pd.DataFrame):
        # Convert DataFrame to template-safe format
        if data.empty:
            return {
                'columns': [],
                'rows': [],
                'total_rows': 0,
                'is_dataframe': True
            }
        else:
            return {
                'columns': list(data.columns),
                'rows': data.head(100).to_dict('records'),  # Limit to first 100 rows
                'total_rows': len(data),
                'is_dataframe': True
            }
    elif isinstance(data, dict):
        # Recursively process dictionary values
        return {key: safe_serialize_for_template(value) for key, value in data.items()}
    elif isinstance(data, (list, tuple)):
        # Recursively process list/tuple items
        return [safe_serialize_for_template(item) for item in data]
    else:
        # Return as-is for other types
        return data


@method_decorator(login_required, name='dispatch')
@method_decorator(creator_required, name='dispatch')
class DataSourceListView(ListView):
    """List and manage data sources"""
    
    model = DataSource
    template_name = 'datasets/list.html'
    context_object_name = 'data_sources'
    paginate_by = 20
    
    def get_queryset(self):
        """Get data sources for current user (owned + shared) with workflow status"""
        from django.db import models
        sources = DataSource.objects.filter(
            models.Q(created_by=self.request.user) | models.Q(shared_with_users=self.request.user),
            status='active'
        ).distinct().order_by('-created_at')
        
        # Add workflow status to each source
        for source in sources:
            if not source.workflow_status:
                source.workflow_status = WorkflowManager.get_default_status()
                source.save()
        
        return sources
    
    def get_context_data(self, **kwargs):
        """Add additional context"""
        context = super().get_context_data(**kwargs)
        
        # Add integration summary
        integration_service = DataIntegrationService()
        context['integration_summary'] = integration_service.get_data_sources_summary()
        
        return context


@method_decorator(login_required, name='dispatch')
@method_decorator(creator_required, name='dispatch')
class DataSourceCreateView(CreateView):
    """Create new data source"""
    
    model = DataSource
    template_name = 'datasets/create.html'
    fields = ['name', 'source_type', 'description']
    
    def get_context_data(self, **kwargs):
        """Add source type choices"""
        context = super().get_context_data(**kwargs)
        context['source_types'] = [
            ('postgresql', 'PostgreSQL Database'),
            ('sqlite', 'SQLite Database'),
            ('csv', 'CSV File'),
            ('api', 'REST API'),
        ]
        return context
    
    def form_valid(self, form):
        """Handle form submission with connection testing"""
        form.instance.created_by = self.request.user
        
        # Get connection parameters from POST data
        connection_params = self._extract_connection_params()
        
        # Test connection
        data_service = DataService()
        success, message = data_service.test_connection(connection_params)
        
        if not success:
            form.add_error(None, f"Connection test failed: {message}")
            return self.form_invalid(form)
        
        # Save with connection parameters  
        form.instance.connection_info = connection_params
        response = super().form_valid(form)
        
        # Add to integration system
        integration_service = DataIntegrationService()
        integration_service.add_data_source(
            name=form.instance.name,
            source_type=form.instance.source_type,
            connection_info=connection_params,
            user_id=self.request.user.pk or 0
        )
        
        messages.success(self.request, f"Data source '{form.instance.name}' created successfully!")
        return response
    
    def _extract_connection_params(self) -> Dict[str, Any]:
        """Extract connection parameters from form data"""
        source_type = self.request.POST.get('source_type')
        
        if source_type == 'postgresql':
            return {
                'type': 'postgresql',
                'host': self.request.POST.get('host'),
                'port': int(self.request.POST.get('port', 5432)),
                'database': self.request.POST.get('database'),
                'username': self.request.POST.get('username'),
                'password': self.request.POST.get('password')
            }
        elif source_type == 'sqlite':
            return {
                'type': 'sqlite',
                'path': self.request.POST.get('path', ':memory:')
            }
        elif source_type == 'csv':
            # Handle file upload
            csv_file = self.request.FILES.get('csv_file')
            if csv_file:
                # Save file content properly
                file_content = csv_file.read()
                file_path = default_storage.save(f'csv_files/{csv_file.name}', ContentFile(file_content))
                return {
                    'type': 'csv',
                    'file_path': file_path
                }
        
        return {}


@method_decorator(login_required, name='dispatch')
class DataSourceDetailView(View):
    """View and manage individual data source"""
    
    def get(self, request, pk):
        """Display data source details"""
        # Allow access to owned or shared data sources
        from django.db import models
        from .models import DataSourceShare
        
        data_source = get_object_or_404(DataSource, id=pk, status='active')
        
        # Check if user has access (owner or shared)
        has_access = False
        can_edit = False
        
        if data_source.created_by == request.user:
            has_access = True
            can_edit = True
        else:
            # Check if shared
            share = DataSourceShare.objects.filter(
                data_source=data_source,
                user=request.user
            ).first()
            if share:
                has_access = True
                can_edit = share.permission in ['edit']
        
        if not has_access:
            messages.error(request, "You don't have access to this data source")
            return redirect('datasets:list')
        
        # For CSV files, use stored schema and sample data if available
        if data_source.source_type == 'csv' and data_source.schema_info and data_source.sample_data:
            schema_info = data_source.schema_info
            preview_data_formatted = data_source.sample_data
            success = True
            preview_error = None
        else:
            # Get schema info from live connection
            data_service = DataService()
            schema_info = data_service.get_schema_info(data_source.connection_info)
            
            # Get data preview from live connection
            success, preview_data = data_service.get_data_preview(data_source.connection_info)
            
            # Convert DataFrame to template-friendly format
            if success and preview_data is not None:
                try:
                    if hasattr(preview_data, 'to_dict'):
                        # Convert DataFrame to list of dictionaries
                        preview_data_formatted = preview_data.to_dict('records')
                    else:
                        preview_data_formatted = preview_data
                except Exception as e:
                    preview_data_formatted = None
                    success = False
                    preview_data = f"Error formatting preview data: {str(e)}"
            else:
                preview_data_formatted = None
            
            preview_error = None if success else preview_data
        
        # Safely serialize only preview data to prevent DataFrame boolean evaluation errors
        context = {
            'data_source': data_source,
            'schema_info': schema_info,  # Keep schema info in original format for semantic service
            'preview_data': safe_serialize_for_template(preview_data_formatted) if success else None,
            'preview_error': preview_error
        }
        
        return render(request, 'datasets/detail.html', context)
    
    def post(self, request, pk):
        """Handle data source operations"""
        data_source = get_object_or_404(DataSource, id=pk, created_by=request.user)
        
        try:
            data = json.loads(request.body)
            action = data.get('action')
            
            if action == 'test_connection':
                data_service = DataService()
                success, message = data_service.test_connection(data_source.connection_info)
                return JsonResponse({
                    'success': success,
                    'message': message
                })
            
            elif action == 'refresh_schema':
                data_service = DataService()
                schema_info = data_service.get_schema_info(data_source.connection_info)
                
                if 'error' in schema_info:
                    return JsonResponse({
                        'success': False,
                        'error': schema_info['error']
                    })
                else:
                    # Update the data source with new schema info
                    data_source.schema_info = schema_info
                    data_source.save()
                    
                    return JsonResponse({
                        'success': True,
                        'schema_info': schema_info
                    })
            
            elif action == 'get_preview':
                table_name = data.get('table_name')
                data_service = DataService()
                success, preview_data = data_service.get_data_preview(
                    data_source.connection_info, 
                    table_name
                )
                
                if success:
                    # Convert DataFrame to template-friendly format
                    if hasattr(preview_data, 'to_dict'):
                        formatted_data = preview_data.to_dict('records')
                    else:
                        formatted_data = str(preview_data)
                    
                    return JsonResponse({
                        'success': True,
                        'preview_data': formatted_data
                    })
                else:
                    return JsonResponse({
                        'success': False,
                        'error': preview_data
                    })
            
            elif action == 'delete':
                # Delete the data source with PostgreSQL cleanup
                try:
                    # Delete from PostgreSQL unified_data_storage first
                    with db_connection.cursor() as cursor:
                        cursor.execute("""
                            DELETE FROM unified_data_storage 
                            WHERE data_source_name = %s OR table_name LIKE %s
                        """, [data_source.name, f'%{data_source.name.lower().replace(" ", "_").replace("-", "_")}%'])
                    
                    # Delete physical CSV file if it exists
                    if data_source.source_type == 'csv' and data_source.connection_info.get('file_path'):
                        try:
                            file_path = data_source.connection_info.get('file_path')
                            if default_storage.exists(file_path):
                                default_storage.delete(file_path)
                        except Exception:
                            pass  # File deletion is not critical
                    
                    # Delete the data source
                    data_source.delete()
                    
                    return JsonResponse({
                        'success': True,
                        'message': 'Data source and PostgreSQL data deleted successfully',
                        'redirect_url': '/datasets/'
                    })
                except Exception as e:
                    logger.error(f"Error in delete action: {e}")
                    return JsonResponse({'error': f'Failed to delete data source: {str(e)}'}, status=500)
            
            else:
                return JsonResponse({'error': 'Unknown action'}, status=400)
                
        except Exception as e:
            logger.error(f"Error handling data source operation: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    def delete(self, request, pk):
        """Handle HTTP DELETE requests to delete data source with comprehensive PostgreSQL cascade deletion"""
        try:
            data_source = get_object_or_404(DataSource, id=pk, created_by=request.user)
            
            # Log the deletion
            logger.info(f"Starting comprehensive deletion of data source: {data_source.name} (ID: {data_source.id}) by user {request.user.username}")
            
            deletion_summary = {
                'data_source_name': data_source.name,
                'semantic_tables': 0,
                'semantic_columns': 0,
                'semantic_metrics': 0,
                'etl_operations': 0,
                'query_logs': 0,
                'dashboard_items': 0,
                'integration_data': False,
                'postgresql_data': False
            }
            
            with transaction.atomic():
                # 1. Delete from PostgreSQL unified_data_storage first
                try:
                    with db_connection.cursor() as cursor:
                        # Find matching records in unified storage
                        cursor.execute("""
                            SELECT COUNT(*) FROM unified_data_storage 
                            WHERE data_source_name = %s OR table_name LIKE %s
                        """, [data_source.name, f'%{data_source.name.lower().replace(" ", "_").replace("-", "_")}%'])
                        
                        postgresql_count = cursor.fetchone()[0]
                        
                        if postgresql_count > 0:
                            # Delete from unified storage
                            cursor.execute("""
                                DELETE FROM unified_data_storage 
                                WHERE data_source_name = %s OR table_name LIKE %s
                            """, [data_source.name, f'%{data_source.name.lower().replace(" ", "_").replace("-", "_")}%'])
                            
                            deletion_summary['postgresql_data'] = True
                            logger.info(f"Deleted {postgresql_count} records from PostgreSQL unified_data_storage")
                        
                except Exception as pg_error:
                    logger.error(f"Error deleting from PostgreSQL: {pg_error}")
                    # Continue with other deletions even if PostgreSQL deletion fails
                
                # 2. Count semantic layer objects before they are cascade-deleted
                from .models import SemanticTable, SemanticColumn, SemanticMetric
                
                semantic_tables = SemanticTable.objects.filter(data_source=data_source)
                deletion_summary['semantic_tables'] = semantic_tables.count()
                
                for table in semantic_tables:
                    deletion_summary['semantic_columns'] += SemanticColumn.objects.filter(semantic_table=table).count()
                    deletion_summary['semantic_metrics'] += SemanticMetric.objects.filter(base_table=table).count()
                
                # 3. Delete ETL operations that reference this data source
                from .models import ETLOperation
                all_etl_operations = ETLOperation.objects.filter(created_by=request.user)
                etl_operations_to_delete = []
                for etl_op in all_etl_operations:
                    if (str(data_source.id) in str(etl_op.source_tables) or 
                        str(data_source.id) in str(etl_op.parameters) or
                        data_source.name in str(etl_op.source_tables) or
                        data_source.name in str(etl_op.parameters)):
                        etl_operations_to_delete.append(etl_op.pk)
                
                if etl_operations_to_delete:
                    deletion_count = ETLOperation.objects.filter(id__in=etl_operations_to_delete).count()
                    ETLOperation.objects.filter(id__in=etl_operations_to_delete).delete()
                    deletion_summary['etl_operations'] = deletion_count

                # 4. Delete query logs that reference this data source
                try:
                    from core.models import QueryLog
                    all_query_logs = QueryLog.objects.filter(user=request.user)
                    query_logs_to_delete = []
                    for query_log in all_query_logs:
                        # Check if this data source name or ID is mentioned in various query fields
                        text_to_check = ' '.join(filter(None, [
                            query_log.natural_query or '',
                            query_log.generated_sql or '',
                            query_log.final_sql or ''
                        ]))
                        json_to_check = str(query_log.query_results or {})

                        if (data_source.name in text_to_check or str(data_source.id) in text_to_check or
                            data_source.name in json_to_check or str(data_source.id) in json_to_check):
                            query_logs_to_delete.append(query_log.pk)

                    if query_logs_to_delete:
                        deletion_count = QueryLog.objects.filter(id__in=query_logs_to_delete).count()
                        QueryLog.objects.filter(id__in=query_logs_to_delete).delete()
                        deletion_summary['query_logs'] = deletion_count
                except ImportError:
                    logger.info("QueryLog model not available, skipping query log cleanup")
                
                # 5. Delete dashboard items that use queries referencing this data source
                try:
                    from dashboards.models import DashboardItem
                    
                    all_dashboard_items = DashboardItem.objects.filter(dashboard__owner=request.user)
                    items_to_delete = []
                    for item in all_dashboard_items:
                        if (data_source.name in item.query or str(data_source.id) in item.query or
                            data_source.name == item.data_source or str(data_source.id) == item.data_source):
                            items_to_delete.append(item.pk)

                    if items_to_delete:
                        deletion_count = DashboardItem.objects.filter(pk__in=items_to_delete).count()
                        DashboardItem.objects.filter(pk__in=items_to_delete).delete()
                        deletion_summary['dashboard_items'] = deletion_count
                    
                except ImportError:
                    logger.info("Dashboard models not available, skipping dashboard item cleanup")
                
                # 6. Remove from legacy integration system (if still present)
                try:
                    from services.integration_service import DataIntegrationService
                    integration_service = DataIntegrationService()
                    
                    from utils.table_name_helper import get_integrated_table_name
                    table_name = get_integrated_table_name(data_source)
                    
                    if integration_service.check_table_exists(table_name) and hasattr(integration_service.integrated_db, 'execute'):
                         integration_service.integrated_db.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                         deletion_summary['integration_data'] = True
                         logger.info(f"Dropped legacy integration table: {table_name}")
                    
                except Exception as integration_error:
                    logger.warning(f"Could not clean up legacy integration data: {integration_error}")
                
                # 7. Delete physical CSV file if it exists
                if data_source.source_type == 'csv' and data_source.connection_info.get('file_path'):
                    try:
                        file_path = data_source.connection_info.get('file_path')
                        if default_storage.exists(file_path):
                            default_storage.delete(file_path)
                            logger.info(f"Deleted physical CSV file: {file_path}")
                    except Exception as file_error:
                        logger.warning(f"Could not delete CSV file: {file_error}")
                
                # 8. Finally delete the data source (this will cascade delete semantic objects)
                data_source.delete()
                
            # Log deletion summary
            logger.info(f"Deletion completed for {deletion_summary['data_source_name']}: {deletion_summary}")
            
            return JsonResponse({
                'success': True,
                'message': f'Data source "{deletion_summary["data_source_name"]}" and all related data deleted successfully from PostgreSQL.',
                'deletion_summary': deletion_summary,
                'redirect_url': '/datasets/'
            })
            
        except Exception as e:
            import traceback
            logger.error(f"Error deleting data source: {e}\n{traceback.format_exc()}")
            return JsonResponse({'error': f'Failed to delete data source: {str(e)}'}, status=500)


@method_decorator(login_required, name='dispatch')
@method_decorator(creator_required, name='dispatch')
class DataSourceTestView(View):
    """Test data source connections"""
    
    def post(self, request, pk):
        """Test connection to data source"""
        data_source = get_object_or_404(DataSource, id=pk, created_by=request.user)
        
        try:
            data_service = DataService()
            success, message = data_service.test_connection(data_source.connection_info)
            
            # Update connection status
            data_source.status = 'active' if success else 'error'
            data_source.save()
            
            return JsonResponse({
                'success': success,
                'message': message,
                'status': data_source.status
            })
            
        except Exception as e:
            logger.error(f"Error testing data source connection: {e}")
            return JsonResponse({'error': str(e)}, status=500)


@method_decorator(login_required, name='dispatch')
@method_decorator(creator_required, name='dispatch')
class DataSourceShareView(View):
    """Manage data source sharing"""
    
    def get(self, request, pk):
        """Display sharing interface"""
        data_source = get_object_or_404(DataSource, id=pk, created_by=request.user)
        
        # Get currently shared users through the DataSourceShare model
        from .models import DataSourceShare
        shared_users = DataSourceShare.objects.filter(data_source=data_source).select_related('user')
        
        # Get all users for sharing options (excluding current user)
        from accounts.models import CustomUser
        all_users = CustomUser.objects.exclude(id=request.user.id)
        
        context = {
            'data_source': data_source,
            'shared_users': shared_users,
            'all_users': all_users,
        }
        
        return render(request, 'datasets/share.html', context)
    
    def post(self, request, pk):
        """Handle sharing actions"""
        data_source = get_object_or_404(DataSource, id=pk, created_by=request.user)
        
        try:
            data = json.loads(request.body)
            action = data.get('action')
            
            if action == 'add_user':
                user_id = data.get('user_id')
                permission = data.get('permission', 'query')
                
                try:
                    from accounts.models import CustomUser
                    from .models import DataSourceShare
                    
                    user = CustomUser.objects.get(id=user_id)
                    
                    # Create DataSourceShare instance
                    share, created = DataSourceShare.objects.get_or_create(
                        data_source=data_source,
                        user=user,
                        defaults={
                            'shared_by': request.user,
                            'permission': permission
                        }
                    )
                    
                    if created:
                        return JsonResponse({
                            'success': True,
                            'message': f'Data source shared with {user.username}'
                        })
                    else:
                        # Update permission if already exists
                        share.permission = permission
                        share.save()
                        return JsonResponse({
                            'success': True,
                            'message': f'Updated permissions for {user.username}'
                        })
                except CustomUser.DoesNotExist:
                    return JsonResponse({'error': 'User not found'}, status=404)
            
            elif action == 'remove_user':
                user_id = data.get('user_id')
                try:
                    from accounts.models import CustomUser
                    from .models import DataSourceShare
                    
                    user = CustomUser.objects.get(id=user_id)
                    
                    # Remove DataSourceShare instance
                    DataSourceShare.objects.filter(
                        data_source=data_source,
                        user=user
                    ).delete()
                    
                    return JsonResponse({
                        'success': True,
                        'message': f'Removed {user.username} from data source'
                    })
                except CustomUser.DoesNotExist:
                    return JsonResponse({'error': 'User not found'}, status=404)
            
            else:
                return JsonResponse({'error': 'Unknown action'}, status=400)
                
        except Exception as e:
            logger.error(f"Error managing data source sharing: {e}")
            return JsonResponse({'error': str(e)}, status=500)


@method_decorator(login_required, name='dispatch')
@method_decorator(creator_required, name='dispatch')
class DataIntegrationView(View):
    """Manage data integration and ETL operations"""
    
    def get(self, request):
        """Display data integration interface"""
        integration_service = DataIntegrationService()
        
        # Get integration summary
        summary = integration_service.get_data_sources_summary()
        
        # Get suggested joins
        suggested_joins = integration_service.get_suggested_joins()
        
        # Get recent ETL operations
        recent_operations = ETLOperation.objects.filter(
            created_by=request.user
        ).order_by('-created_at')[:10]
        
        # Get all user's data sources (not just ones in integration system)
        user_data_sources = DataSource.objects.filter(
            created_by=request.user,
            status='active'
        ).order_by('-created_at')
        
        # Convert data sources to format expected by template
        data_sources_list = []
        for ds in user_data_sources:
            data_sources_list.append({
                'id': str(ds.id),
                'name': ds.name,
                'source_type': ds.source_type,
                'status': ds.status,
                'created_at': ds.created_at,
                'table_count': len(ds.schema_info.get('columns', [])) if ds.schema_info else 0,
                'schema_info': ds.schema_info or {}
            })
        
        # Update summary with all user data sources
        summary['data_sources'] = data_sources_list
        summary['total_tables'] = sum(source.get('table_count', 0) for source in data_sources_list)
        summary['total_sources'] = len(data_sources_list)
        
        context = {
            'integration_summary': summary,  # Keep original format for internal processing
            'suggested_joins': suggested_joins,  # Keep original format for internal processing
            'recent_operations': recent_operations,
            'user_data_sources': user_data_sources,  # Add direct access to data sources
            'data_sources': user_data_sources  # Add this for template compatibility
        }
        
        return render(request, 'datasets/integration.html', context)
    
    def post(self, request):
        """Handle integration operations"""
        try:
            data = json.loads(request.body)
            action = data.get('action')
            
            integration_service = DataIntegrationService()
            
            if action == 'create_join':
                # Create join operation
                operation_name = data.get('name', 'Join Operation')
                source_tables = data.get('source_tables', [])
                join_params = data.get('parameters', {})
                
                operation_id = integration_service.create_etl_operation(
                    name=operation_name,
                    operation_type='join',
                    source_tables=source_tables,
                    parameters=join_params,
                    user_id=request.user.id
                )
                
                if operation_id:
                    return JsonResponse({
                        'success': True,
                        'operation_id': operation_id,
                        'message': 'Join operation created successfully'
                    })
                else:
                    return JsonResponse({'error': 'Failed to create join operation'}, status=500)
            
            elif action == 'create_union':
                # Create union operation
                operation_name = data.get('name', 'Union Operation')
                source_tables = data.get('source_tables', [])
                union_params = data.get('parameters', {})
                
                operation_id = integration_service.create_etl_operation(
                    name=operation_name,
                    operation_type='union',
                    source_tables=source_tables,
                    parameters=union_params,
                    user_id=request.user.id
                )
                
                if operation_id:
                    return JsonResponse({
                        'success': True,
                        'operation_id': operation_id,
                        'message': 'Union operation created successfully'
                    })
                else:
                    return JsonResponse({'error': 'Failed to create union operation'}, status=500)
            
            elif action == 'get_suggestions':
                # Get new relationship suggestions
                suggestions = integration_service.get_suggested_joins()
                return JsonResponse({
                    'success': True,
                    'suggestions': suggestions
                })
            
            else:
                return JsonResponse({'error': 'Unknown action'}, status=400)
                
        except Exception as e:
            logger.error(f"Error handling integration operation: {e}")
            return JsonResponse({'error': str(e)}, status=500)


@method_decorator(login_required, name='dispatch')
@method_decorator(creator_required, name='dispatch')
class SemanticLayerView(View):
    """Manage semantic layer metadata"""
    
    def get(self, request, pk=None):
        """Display semantic layer interface"""
        semantic_service = SemanticService()
        
        # Get semantic tables and columns
        from datasets.models import SemanticTable, SemanticColumn, SemanticMetric
        
        # Get all user's data sources for context
        user_data_sources = DataSource.objects.filter(
            created_by=request.user,
            status='active'
        ).order_by('-created_at')
        
        # If pk is provided, filter by specific data source
        if pk:
            try:
                data_source = DataSource.objects.get(id=pk, created_by=request.user)
                semantic_tables = SemanticTable.objects.filter(data_source=data_source).prefetch_related('columns')
                semantic_metrics = SemanticMetric.objects.filter(base_table__data_source=data_source)
                
                context = {
                    'data_source': data_source,
                    'semantic_tables': semantic_tables,
                    'semantic_metrics': semantic_metrics,
                    'is_data_source_specific': True,
                    'user_data_sources': user_data_sources,
                    'data_sources': user_data_sources  # For template compatibility
                }
            except DataSource.DoesNotExist:
                messages.error(request, "Data source not found")
                return redirect('datasets:list')
        else:
            # Show all semantic metadata and available data sources (with prefetched columns)
            semantic_tables = SemanticTable.objects.prefetch_related('columns').all()
            semantic_metrics = SemanticMetric.objects.all()
            
            context = {
                'semantic_tables': semantic_tables,
                'semantic_metrics': semantic_metrics,
                'is_data_source_specific': False,
                'user_data_sources': user_data_sources,
                'data_sources': user_data_sources  # For template compatibility
            }
        
        return render(request, 'datasets/semantic.html', context)
    
    def post(self, request):
        """Handle semantic layer operations"""
        try:
            data = json.loads(request.body)
            action = data.get('action')
            
            semantic_service = SemanticService()
            
            if action == 'auto_generate_all' or action == 'auto_generate' or action == 'auto_generate_selected':
                # Auto-generate semantic metadata with enhanced error handling
                try:
                    integration_service = DataIntegrationService()
                    
                    # Determine which data sources to process
                    from datasets.models import DataSource
                    
                    if action == 'auto_generate_selected':
                        # Process only selected data sources
                        selected_source_ids = data.get('selected_source_ids', [])
                        if not selected_source_ids:
                            return JsonResponse({
                                'error': 'No data sources selected',
                                'details': 'Please select at least one data source to generate semantic metadata.'
                            }, status=400)
                        
                        user_data_sources = DataSource.objects.filter(
                            id__in=selected_source_ids, 
                            created_by=request.user, 
                            is_deleted=False
                        )
                        
                        if not user_data_sources.exists():
                            return JsonResponse({
                                'error': 'Selected data sources not found',
                                'details': 'The selected data sources are no longer available.'
                            }, status=400)
                        
                        logger.info(f"Processing {len(selected_source_ids)} selected data sources for semantic generation")
                        
                    else:
                        # Process all user data sources (backward compatibility)
                        user_data_sources = DataSource.objects.filter(created_by=request.user, is_deleted=False)
                        if not user_data_sources.exists():
                            return JsonResponse({
                                'error': 'No data sources available',
                                'details': 'Please upload or connect data sources before generating semantic metadata.'
                            }, status=400)
                    
                    # Check if any data sources have completed ETL
                    etl_ready_sources = user_data_sources.filter(
                        workflow_status__has_key='etl_completed'
                    ).filter(workflow_status__etl_completed=True)
                    
                    if not etl_ready_sources.exists():
                        return JsonResponse({
                            'error': 'No ETL-ready data sources',
                            'details': 'Please complete ETL transformations for your data sources before generating semantic metadata.'
                        }, status=400)
                        
                    # Try to generate semantic layer for selected/all available sources
                    success_count = 0
                    error_count = 0
                    created_tables = []
                    created_metrics = []
                    
                    for data_source in user_data_sources:
                        try:
                            # Generate semantic layer for this specific source
                            result = self._generate_semantic_for_source(data_source, semantic_service, integration_service)
                            
                            # FIXED: Add robust error handling for result access
                            try:
                                # Check if result is a dictionary and has expected structure
                                if isinstance(result, dict):
                                    if result.get('success', False):
                                        success_count += 1
                                        created_tables.extend(result.get('tables', []))
                                        created_metrics.extend(result.get('metrics', []))
                                    else:
                                        error_count += 1
                                        error_msg = result.get('error', 'Unknown error')
                                        logger.warning(f"Failed to generate semantic layer for {data_source.name}: {error_msg}")
                                        
                                        # Add diagnostic information
                                        details = result.get('details', '')
                                        suggestion = result.get('suggestion', '')
                                        if details or suggestion:
                                            logger.info(f"Additional details for {data_source.name}: {details} | {suggestion}")
                                else:
                                    # Handle unexpected result types
                                    error_count += 1
                                    logger.error(f"Unexpected result type from _generate_semantic_for_source for {data_source.name}: {type(result)}")
                                    logger.error(f"Result content: {str(result)[:200]}...")
                                    
                            except Exception as result_error:
                                error_count += 1
                                logger.error(f"Error processing result for {data_source.name}: {result_error}")
                                logger.error(f"Result type: {type(result)}, Result: {str(result)[:200]}...")
                                
                        except Exception as source_error:
                            error_count += 1
                            logger.error(f"Error generating semantic layer for {data_source.name}: {source_error}")
                            
                            # Add detailed error logging for debugging data source issues
                            logger.error(f"Data source details - Name: {data_source.name}, Type: {data_source.source_type}, ID: {data_source.id}")
                            if hasattr(data_source, 'connection_info'):
                                file_path = data_source.connection_info.get('file_path', 'N/A')
                                logger.error(f"Connection info - File path: {file_path}")
                            import traceback
                            logger.error(f"Full traceback: {traceback.format_exc()}")
                    
                    if success_count > 0:
                        message = f"Generated semantic layer for {success_count} data source(s). "
                        message += f"Created {len(created_tables)} tables and {len(created_metrics)} metrics."
                        if error_count > 0:
                            message += f" {error_count} source(s) had issues."
                        
                        return JsonResponse({
                            'success': True,
                            'message': message,
                            'details': {
                                'sources_processed': success_count,
                                'sources_failed': error_count,
                                'tables_created': len(created_tables),
                                'metrics_created': len(created_metrics)
                            }
                        })
                    else:
                        return JsonResponse({
                            'error': 'Failed to generate semantic metadata',
                            'details': f'No semantic objects were created from {len(user_data_sources)} data source(s). This may indicate that data integration is not complete or data sources are not properly configured.',
                            'guidance': 'Please ensure your data sources have been uploaded and processed through ETL operations.'
                        }, status=400)
                
                except Exception as semantic_error:
                    # Handle specific database constraint errors
                    error_str = str(semantic_error).lower()
                    if 'not null constraint failed' in error_str and 'aggregation_default' in error_str:
                        logger.error(f"Database constraint error during semantic generation: {semantic_error}")
                        return JsonResponse({
                            'error': 'Database constraint error',
                            'details': 'A database constraint failed during semantic metadata generation. This issue should be resolved by running database migrations.',
                            'technical_details': str(semantic_error)
                        }, status=500)
                    elif 'unique constraint' in error_str:
                        logger.error(f"Unique constraint error during semantic generation: {semantic_error}")
                        return JsonResponse({
                            'error': 'Duplicate data detected',
                            'details': 'Duplicate data sources or semantic objects detected. Please check for existing entries.',
                            'technical_details': str(semantic_error)
                        }, status=400)
                    else:
                        logger.error(f"Unexpected error during semantic generation: {semantic_error}")
                        return JsonResponse({
                            'error': 'Semantic generation failed',
                            'details': str(semantic_error)
                        }, status=500)
            
            elif action == 'update_table':
                # Update semantic table metadata
                table_id = data.get('table_id')
                table_data = data.get('table_data')
                
                try:
                    from datasets.models import SemanticTable
                    table = SemanticTable.objects.get(id=table_id)
                    
                    table.display_name = table_data.get('display_name', table.display_name)
                    table.description = table_data.get('description', table.description)
                    # Note: business_purpose field is the actual field name in the model
                    if hasattr(table, 'business_purpose'):
                        table.business_purpose = table_data.get('business_context', table.business_purpose)
                    table.save()
                    
                    return JsonResponse({
                        'success': True,
                        'message': 'Table metadata updated'
                    })
                except:
                    return JsonResponse({'error': 'Table not found'}, status=404)
            
            elif action == 'create_metric':
                # Create new business metric
                metric_data = data.get('metric_data')
                
                try:
                    from datasets.models import SemanticMetric
                    metric = SemanticMetric.objects.create(
                        name=metric_data['name'],
                        display_name=metric_data['display_name'],
                        description=metric_data.get('description', ''),
                        calculation=metric_data['formula'],
                        metric_type=metric_data.get('category', 'simple')
                    )
                    
                    return JsonResponse({
                        'success': True,
                        'metric_id': str(metric.pk),
                        'message': 'Metric created successfully'
                    })
                except Exception as e:
                    return JsonResponse({'error': f'Failed to create metric: {str(e)}'}, status=500)
            
            else:
                return JsonResponse({'error': 'Unknown action'}, status=400)
                
        except Exception as e:
            logger.error(f"Error handling semantic layer operation: {e}")
            return JsonResponse({'error': str(e)}, status=500)

    def _generate_semantic_for_source(self, data_source, semantic_service, integration_service):
        """
        Generate semantic layer for data source using DuckDB transformed data
        FIXED: Now correctly loads ETL-transformed data from integrated DuckDB
        """
        try:
            from utils.dynamic_naming import dynamic_naming
            from datasets.models import SemanticTable, SemanticColumn, SemanticMetric
            from .data_access_layer import unified_data_access
            import pandas as pd
            import duckdb
            import os
            from django.conf import settings
            
            logger.info(f"[SEMANTIC] Starting semantic generation for: {data_source.name} (ID: {data_source.id})")
            
            # Step 1: Check for existing semantic layer (prevent duplicates)
            existing_semantic_tables = SemanticTable.objects.filter(data_source=data_source)
            if existing_semantic_tables.exists():
                logger.info(f"[SEMANTIC] Semantic layer already exists for {data_source.name}")
                
                # Count existing objects
                table_count = existing_semantic_tables.count()
                column_count = SemanticColumn.objects.filter(
                    semantic_table__data_source=data_source
                ).count()
                
                return {
                    'success': True,
                    'message': f'Semantic layer already exists! Found {table_count} tables and {column_count} columns.',
                    'tables': [{'name': table.name, 'display_name': table.display_name} for table in existing_semantic_tables],
                    'columns_created': column_count,
                    'metrics': [],
                    'already_existed': True
                }
            
            # Step 2: Load TRANSFORMED data from integrated DuckDB
            # CRITICAL FIX: Use the same table naming convention as ETL save operation
            logger.info(f"[DUCKDB] Loading transformed data from integrated DuckDB for {data_source.name}")
            
            data = None
            table_name = None
            
            try:
                # FIXED: Use the exact same table naming pattern as ETL transformation save
                # This matches the pattern in execute_etl_transformation: f"source_{data_source.id.hex.replace('-', '_')}"
                etl_table_name = f"source_{data_source.id.hex.replace('-', '_')}"
                logger.info(f"[TABLE_NAME] Looking for ETL-transformed table: {etl_table_name}")
                
                # Connect to integrated DuckDB database
                db_path = os.path.join(settings.BASE_DIR, 'data', 'integrated.duckdb')
                if not os.path.exists(db_path):
                    logger.error(f"[ERROR] Integrated DuckDB not found at: {db_path}")
                    raise Exception(f"Integrated database not found: {db_path}")
                
                conn = duckdb.connect(db_path)
                
                try:
                    # First, check if the ETL-transformed table exists
                    tables_query = "SHOW TABLES"
                    all_tables = conn.execute(tables_query).fetchall()
                    available_table_names = [table[0] for table in all_tables]
                    
                    logger.info(f"[AVAILABLE_TABLES] Found tables in DuckDB: {available_table_names}")
                    
                    if etl_table_name in available_table_names:
                        # GREAT! ETL-transformed table exists - load it
                        test_query = f"SELECT COUNT(*) FROM \"{etl_table_name}\""
                        count_result = conn.execute(test_query).fetchone()
                        
                        if count_result and count_result[0] > 0:
                            # Load the TRANSFORMED data
                            data_query = f"SELECT * FROM \"{etl_table_name}\""
                            data = conn.execute(data_query).fetchdf()
                            table_name = etl_table_name
                            
                            # LOG THE DATA TYPES TO VERIFY TRANSFORMATION
                            logger.info(f"[SUCCESS] Loaded {len(data)} rows from ETL-transformed table: {etl_table_name}")
                            logger.info(f"[DATA_TYPES] Transformed data types: {dict(data.dtypes)}")
                            
                            # Count non-object types to verify transformations were applied
                            non_object_types = sum(1 for dtype in data.dtypes if str(dtype) != 'object')
                            total_types = len(data.dtypes)
                            logger.info(f"[TYPE_CHECK] {non_object_types}/{total_types} columns have transformed types (non-object)")
                        else:
                            logger.warning(f"[WARNING] ETL-transformed table {etl_table_name} exists but is empty")
                    else:
                        # ETL table doesn't exist yet - try the original integration table
                        logger.warning(f"[MISSING] ETL-transformed table {etl_table_name} not found")
                        logger.info(f"[FALLBACK] Trying original integration table patterns...")
                        
                        # Try alternative table naming patterns
                        alternative_patterns = [
                            f"ds_{data_source.id.hex.replace('-', '_')}",
                            f"source_{str(data_source.id).replace('-', '_')}",
                            f"ds_{str(data_source.id).replace('-', '_')}"
                        ]
                        
                        for alt_pattern in alternative_patterns:
                            if alt_pattern in available_table_names:
                                logger.info(f"[FOUND_ALT] Found alternative table: {alt_pattern}")
                                data_query = f"SELECT * FROM \"{alt_pattern}\""
                                data = conn.execute(data_query).fetchdf()
                                table_name = alt_pattern
                                logger.warning(f"[WARNING] Using original data (no ETL transformations applied): {len(data)} rows")
                                break
                        
                        if data is None:
                            logger.error(f"[ERROR] No suitable table found for data source {data_source.id}")
                            logger.error(f"[ERROR] Available tables: {available_table_names}")
                            logger.error(f"[ERROR] Expected ETL table: {etl_table_name}")
                            logger.error(f"[ERROR] Tried alternatives: {alternative_patterns}")
                    
                finally:
                    conn.close()
                    
            except Exception as duckdb_error:
                logger.error(f"[ERROR] DuckDB data loading failed: {duckdb_error}")
                
                # Enhanced fallback: Try integration service
                logger.info(f"[FALLBACK] Trying integration service for transformed data...")
                try:
                    integration_data = integration_service.get_integrated_data_for_source(str(data_source.id))
                    if integration_data is not None and not integration_data.empty:
                        data = integration_data
                        logger.info(f"[FALLBACK_SUCCESS] Loaded {len(data)} rows from integration service")
                        logger.info(f"[FALLBACK_TYPES] Data types from integration service: {dict(data.dtypes)}")
                except Exception as integration_error:
                    logger.warning(f"[FALLBACK_FAILED] Integration service failed: {integration_error}")
            
            # Step 3: Final validation and type checking
            if data is None or data.empty:
                error_msg = (
                    f"No transformed data available for semantic generation. "
                    f"Expected ETL table: {etl_table_name if 'etl_table_name' in locals() else 'unknown'}. "
                    f"Please ensure ETL transformations are completed before generating semantic layer."
                )
                logger.error(f"[FAILED] {error_msg}")
                return {
                    'success': False, 
                    'error': error_msg,
                    'details': 'Run ETL transformations first, then generate semantic layer',
                    'etl_required': True
                }
            
            logger.info(f"[DATA] Processing {len(data)} rows, {len(data.columns)} columns")
            logger.info(f"[COLUMNS] {list(data.columns)[:10]}...")  # Show first 10 columns
            
            # CRITICAL: Log data types to verify transformations
            object_columns = [col for col, dtype in data.dtypes.items() if str(dtype) == 'object']
            non_object_columns = [col for col, dtype in data.dtypes.items() if str(dtype) != 'object']
            
            if len(object_columns) == len(data.columns):
                logger.warning(f"[WARNING] ALL columns are 'object' type - ETL transformations may not have been applied!")
                logger.warning(f"[WARNING] This will result in incorrect semantic type inference")
            else:
                logger.info(f"[SUCCESS] Found {len(non_object_columns)} transformed columns with proper data types")
                logger.info(f"[TRANSFORMED_TYPES] Non-object columns: {dict((col, str(data[col].dtype)) for col in non_object_columns[:5])}")
            
            # Step 4: Create semantic table with transaction safety
            from django.db import transaction
            
            with transaction.atomic():
                # Create unique semantic table name
                semantic_table_name = f"semantic_{data_source.id.hex.replace('-', '_')}"
                
                semantic_table, created = SemanticTable.objects.get_or_create(
                    data_source=data_source,
                    name=semantic_table_name,
                    defaults={
                        'display_name': data_source.name,
                        'description': f'Semantic layer for {data_source.name}',
                        'business_purpose': f'Business analytics data from {data_source.source_type} source',
                        'is_fact_table': True,
                        'is_dimension_table': False,
                        'row_count_estimate': len(data)
                    }
                )
                
                logger.info(f"[SEMANTIC_TABLE] {'Created' if created else 'Found'} semantic table: {semantic_table.name}")
                
                # Step 5: Create semantic columns based on ACTUAL TRANSFORMED data types
                columns_created = 0
                
                for col_name in data.columns:
                    try:
                        # Analyze column data for semantic type
                        col_data = data[col_name]
                        pandas_dtype = str(col_data.dtype)
                        
                        # ENHANCED: Infer semantic type based on ACTUAL data type from ETL transformations
                        semantic_type = self._infer_semantic_type_from_transformed_data(col_name, col_data, pandas_dtype)
                        
                        # Get sample values (non-null)
                        sample_values = []
                        try:
                            non_null_values = col_data.dropna().head(5)
                            sample_values = [str(val) for val in non_null_values.tolist()]
                        except:
                            sample_values = []
                        
                        # Create semantic column with duplicate prevention
                        semantic_column, col_created = SemanticColumn.objects.get_or_create(
                            semantic_table=semantic_table,
                            name=col_name,
                            defaults={
                                'display_name': col_name.replace('_', ' ').title(),
                                'description': f'Column {col_name} containing {semantic_type} data',
                                'data_type': pandas_dtype,  # Store the actual transformed data type
                                'semantic_type': semantic_type,
                                'sample_values': sample_values,
                                'is_nullable': col_data.isnull().any(),
                                'is_editable': True,
                                'etl_enriched': pandas_dtype != 'object'  # Mark as enriched if not object type
                            }
                        )
                        
                        if col_created:
                            columns_created += 1
                            logger.debug(f"[COLUMN] Created semantic column: {col_name} ({semantic_type}, dtype: {pandas_dtype})")
                        
                    except Exception as col_error:
                        logger.error(f"[ERROR] Failed to create semantic column {col_name}: {col_error}")
                        continue
                
                # Step 6: Update workflow status
                workflow_status = data_source.workflow_status or {}
                workflow_status['semantics_completed'] = True
                workflow_status['semantics_table_name'] = semantic_table.name
                workflow_status['etl_data_used'] = table_name == etl_table_name if 'etl_table_name' in locals() else False
                data_source.workflow_status = workflow_status
                data_source.save()
                
                logger.info(f"[SUCCESS] Semantic layer created: {columns_created} columns")
                
                # Step 6: Generate business metrics automatically
                try:
                    from services.business_metrics_service import BusinessMetricsService
                    business_service = BusinessMetricsService()
                    
                    logger.info(f"[BUSINESS_METRICS] Generating business metrics for table: {semantic_table.name}")
                    
                    # Generate automatic business metrics for this table
                    metrics_created = 0
                    
                    # Get user for metric creation
                    current_user_id = 1  # Default system user
                    if hasattr(data_source, 'created_by') and data_source.created_by:
                        current_user_id = data_source.created_by.id
                    
                    # Basic count metric
                    count_metric_name = f"{semantic_table.name}_record_count"
                    success, message, metric_id = business_service.create_custom_metric(
                        name=count_metric_name,
                        display_name=f"Total {semantic_table.display_name} Records",
                        description=f"Total number of records in {semantic_table.display_name}",
                        metric_type="simple",
                        calculation="COUNT(*)",
                        unit="count",
                        base_table_id=str(semantic_table.id),
                        user_id=current_user_id
                    )
                    
                    if success:
                        metrics_created += 1
                        logger.info(f"[METRIC] Created count metric: {count_metric_name}")
                    
                    # Generate metrics for numeric columns (measures)
                    numeric_columns = SemanticColumn.objects.filter(
                        semantic_table=semantic_table,
                        data_type__in=['integer', 'float'],
                        is_measure=True
                    )
                    
                    for col in numeric_columns:
                        # Sum metric
                        sum_metric_name = f"{semantic_table.name}_{col.name}_total"
                        success, message, metric_id = business_service.create_custom_metric(
                            name=sum_metric_name,
                            display_name=f"Total {col.display_name}",
                            description=f"Sum of all {col.display_name} values",
                            metric_type="simple",
                            calculation=f"SUM({col.name})",
                            unit="units",
                            base_table_id=str(semantic_table.id),
                            user_id=current_user_id
                        )
                        
                        if success:
                            metrics_created += 1
                            logger.info(f"[METRIC] Created sum metric: {sum_metric_name}")
                        
                        # Average metric
                        avg_metric_name = f"{semantic_table.name}_{col.name}_average"
                        success, message, metric_id = business_service.create_custom_metric(
                            name=avg_metric_name,
                            display_name=f"Average {col.display_name}",
                            description=f"Average value of {col.display_name}",
                            metric_type="simple",
                            calculation=f"AVG({col.name})",
                            unit="units",
                            base_table_id=str(semantic_table.id),
                            user_id=current_user_id
                        )
                        
                        if success:
                            metrics_created += 1
                            logger.info(f"[METRIC] Created average metric: {avg_metric_name}")
                    
                    # Generate distinct count for identifier columns
                    identifier_columns = SemanticColumn.objects.filter(
                        semantic_table=semantic_table,
                        semantic_type='identifier'
                    )
                    
                    for col in identifier_columns:
                        distinct_metric_name = f"{semantic_table.name}_{col.name}_unique_count"
                        success, message, metric_id = business_service.create_custom_metric(
                            name=distinct_metric_name,
                            display_name=f"Unique {col.display_name} Count",
                            description=f"Number of unique {col.display_name} values",
                            metric_type="simple",
                            calculation=f"COUNT(DISTINCT {col.name})",
                            unit="count",
                            base_table_id=str(semantic_table.id),
                            user_id=current_user_id
                        )
                        
                        if success:
                            metrics_created += 1
                            logger.info(f"[METRIC] Created distinct count metric: {distinct_metric_name}")
                    
                    logger.info(f"[SUCCESS] Created {metrics_created} business metrics for {semantic_table.display_name}")
                    
                except Exception as metrics_error:
                    logger.error(f"[WARNING] Failed to generate business metrics: {metrics_error}")
                    # Continue execution - metrics are not critical for semantic layer
                    metrics_created = 0
                
                return {
                    'success': True,
                    'message': f'Semantic layer generated successfully! Created {columns_created} columns using {"transformed" if table_name and "source_" in table_name else "original"} data.',
                    'tables': [{'name': semantic_table.name, 'display_name': semantic_table.display_name}],
                    'columns_created': columns_created,
                    'metrics': [],
                    'data_source_id': str(data_source.id),
                    'table_name': table_name,
                    'etl_data_used': table_name == etl_table_name if 'etl_table_name' in locals() else False,
                    'data_type_summary': {
                        'total_columns': len(data.columns),
                        'object_columns': len(object_columns),
                        'transformed_columns': len(non_object_columns)
                    }
                }
                
        except Exception as e:
            logger.error(f"[ERROR] Error generating semantic layer for {data_source.name}: {e}")
            import traceback
            logger.error(f"[TRACEBACK] {traceback.format_exc()}")
            
            return {
                'success': False, 
                'error': f'Error generating semantic layer: {str(e)}',
                'details': 'Check logs for detailed error information'
            }
    
    def _infer_semantic_type_from_transformed_data(self, col_name: str, col_data, pandas_dtype: str) -> str:
        """
        ENHANCED: Infer semantic type from column name, data, AND actual pandas data type after ETL
        """
        col_name_lower = col_name.lower()
        
        # Use actual data type from ETL transformations for better inference
        if 'int' in pandas_dtype.lower():
            # Integer columns are usually measures or identifiers
            if any(pattern in col_name_lower for pattern in ['id', 'key', 'number', 'code']):
                return 'identifier'
            else:
                return 'measure'  # Count, quantity, etc.
        
        elif 'float' in pandas_dtype.lower():
            # Float columns are typically measures
            return 'measure'
        
        elif 'datetime' in pandas_dtype.lower() or 'timestamp' in pandas_dtype.lower():
            # Datetime columns are dates
            return 'date'
        
        elif 'bool' in pandas_dtype.lower():
            # Boolean columns are dimensions
            return 'dimension'
        
        else:
            # String/object columns - use name-based inference
            # Identifier patterns
            if any(pattern in col_name_lower for pattern in ['id', 'key', 'number', 'code']):
                return 'identifier'
            
            # Date patterns (for string dates that weren't converted)
            if any(pattern in col_name_lower for pattern in ['date', 'time', 'created', 'updated']):
                return 'date'
            
            # Measure patterns (string representation of numbers)
            if any(pattern in col_name_lower for pattern in ['sales', 'revenue', 'profit', 'amount', 'price', 'cost', 'quantity', 'count']):
                return 'measure'
            
            # Default to dimension for categorical/string data
            return 'dimension'

    def _generate_auto_description(self, column_name: str, semantic_type, column_data) -> str:
        """Auto-generate meaningful column descriptions"""
        try:
            col_name_lower = column_name.lower()
            
            # Business-friendly descriptions based on column names
            if 'id' in col_name_lower:
                if 'customer' in col_name_lower:
                    return "Unique identifier for each customer in the system"
                elif 'order' in col_name_lower:
                    return "Unique identifier for each order transaction"
                elif 'product' in col_name_lower:
                    return "Unique identifier for each product in the catalog"
                else:
                    return f"Unique identifier for {column_name.replace('_', ' ').lower()}"
            
            elif 'name' in col_name_lower:
                if 'customer' in col_name_lower:
                    return "Full name of the customer who placed the order"
                elif 'product' in col_name_lower:
                    return "Full name and description of the product"
                else:
                    return f"Name of the {column_name.replace('_name', '').replace('_', ' ').lower()}"
            
            elif 'date' in col_name_lower:
                if 'order' in col_name_lower:
                    return "Date when the order was placed by the customer"
                elif 'ship' in col_name_lower:
                    return "Date when the order was shipped to the customer"
                else:
                    return f"Date of the {column_name.replace('_date', '').replace('_', ' ').lower()}"
            
            elif 'sales' in col_name_lower or 'revenue' in col_name_lower:
                return "Total sales amount in USD for this transaction"
            
            elif 'profit' in col_name_lower:
                return "Profit margin earned from this transaction"
            
            elif 'quantity' in col_name_lower:
                return "Number of units sold in this transaction"
            
            elif 'discount' in col_name_lower:
                return "Discount percentage applied to this transaction"
            
            elif 'region' in col_name_lower:
                return "Geographic region where the customer is located"
            
            elif 'state' in col_name_lower:
                return "State or province where the customer is located"
            
            elif 'city' in col_name_lower:
                return "City where the customer is located"
            
            elif 'category' in col_name_lower:
                return "Product category classification for business analysis"
            
            elif 'segment' in col_name_lower:
                return "Customer segment classification (Consumer, Corporate, Home Office)"
            
            elif 'mode' in col_name_lower and 'ship' in col_name_lower:
                return "Shipping method used for order delivery"
            
            else:
                # Generate description based on semantic type and data analysis
                if hasattr(semantic_type, 'value'):
                    semantic_type_str = semantic_type.value
                else:
                    semantic_type_str = str(semantic_type)
                
                if semantic_type_str == 'measure':
                    return f"Numeric measure representing {column_name.replace('_', ' ').lower()}"
                elif semantic_type_str == 'dimension':
                    return f"Categorical dimension for grouping and filtering data by {column_name.replace('_', ' ').lower()}"
                elif semantic_type_str == 'identifier':
                    return f"Unique identifier for {column_name.replace('_', ' ').lower()}"
                elif semantic_type_str == 'date':
                    return f"Date/time field for {column_name.replace('_', ' ').lower()}"
                else:
                    return f"Data field containing {column_name.replace('_', ' ').lower()} information"
        
        except Exception as e:
            logger.error(f"Error generating description for {column_name}: {e}")
            return f"Data field for {column_name.replace('_', ' ').lower()}"
    
    def _infer_business_term(self, column_name: str) -> str:
        """Infer business glossary term from column name"""
        col_name_lower = column_name.lower()
        
        business_terms = {
            'customer': 'Customer',
            'sales': 'Revenue',
            'profit': 'Profit Margin',
            'order': 'Transaction',
            'product': 'Product',
            'region': 'Geographic Region',
            'segment': 'Customer Segment',
            'category': 'Product Category',
            'discount': 'Discount Rate'
        }
        
        for term, business_term in business_terms.items():
            if term in col_name_lower:
                return business_term
        
        return column_name.replace('_', ' ').title()
    
    def _generate_common_filters(self, column_name: str, column_data) -> list:
        """Generate common filter suggestions for a column"""
        try:
            filters = []
            col_name_lower = column_name.lower()
            
            # Get unique values for categorical data
            if hasattr(column_data, 'nunique') and column_data.nunique() < 20:
                unique_values = column_data.dropna().unique()[:5]
                for value in unique_values:
                    filters.append(f"= '{value}'")
            
            # Add common filters based on column type
            if 'date' in col_name_lower:
                filters.extend([
                    "Last 30 days",
                    "Last quarter",
                    "This year",
                    "Last year"
                ])
            elif 'sales' in col_name_lower or 'profit' in col_name_lower:
                filters.extend([
                    "> 0 (Positive values)",
                    "> 1000 (High value)",
                    "Top 10%"
                ])
            elif 'region' in col_name_lower:
                filters.extend([
                    "= 'South'",
                    "= 'West'",
                    "= 'East'",
                    "= 'Central'"
                ])
            
            return filters[:5]  # Limit to 5 filters
        
        except Exception as e:
            logger.error(f"Error generating filters for {column_name}: {e}")
            return []
    
    def _generate_business_rules(self, column_name: str, column_data) -> list:
        """Generate business rules for a column"""
        try:
            rules = []
            col_name_lower = column_name.lower()
            
            # Add business rules based on column type
            if 'sales' in col_name_lower or 'revenue' in col_name_lower:
                rules.extend([
                    "Must be greater than 0",
                    "Used for revenue calculations",
                    "Key performance indicator"
                ])
            elif 'profit' in col_name_lower:
                rules.extend([
                    "Can be negative (loss)",
                    "Calculated as Sales - Cost",
                    "Key profitability metric"
                ])
            elif 'id' in col_name_lower:
                rules.extend([
                    "Must be unique",
                    "Cannot be null",
                    "Primary identifier"
                ])
            elif 'date' in col_name_lower:
                rules.extend([
                    "Must be valid date format",
                    "Used for time-based analysis",
                    "Required for trend analysis"
                ])
            elif 'quantity' in col_name_lower:
                rules.extend([
                    "Must be positive integer",
                    "Represents units sold",
                    "Used for volume analysis"
                ])
            
            return rules[:3]  # Limit to 3 rules
        
        except Exception as e:
            logger.error(f"Error generating business rules for {column_name}: {e}")
            return []

    def _map_etl_type_to_semantic(self, etl_type):
        """Map ETL data types to semantic types"""
        mapping = {
            'string': 'dimension',
            'integer': 'measure',
            'float': 'measure',
            'date': 'date',
            'datetime': 'date',
            'boolean': 'dimension'
        }
        return mapping.get(etl_type, 'dimension')
    
    def _pandas_to_semantic_type(self, pandas_type):
        """Convert pandas dtype to semantic type"""
        if 'int' in pandas_type.lower():
            return 'integer'
        elif 'float' in pandas_type.lower():
            return 'float'
        elif 'datetime' in pandas_type.lower():
            return 'datetime'
        elif 'bool' in pandas_type.lower():
            return 'boolean'
        else:
            return 'string'
    
    def _generate_display_name(self, column_name):
        """Generate a proper display name from column name"""
        col_str = str(column_name)
        
        # If it's a numeric column name (0, 1, 2, etc.), create a better name
        if col_str.isdigit():
            return f"Column {col_str}"
        
        # If it's already a Column_N format, keep it
        if col_str.startswith('Column_'):
            return col_str.replace('_', ' ')
        
        # Otherwise, clean up the column name
        return col_str.replace('_', ' ').replace('-', ' ').title()
    
    def _infer_unit_from_column_name(self, column_name):
        """Infer unit from column name"""
        col_str = str(column_name).lower()
        
        if any(word in col_str for word in ['price', 'cost', 'revenue', 'amount', 'value', 'salary']):
            return 'USD'
        elif any(word in col_str for word in ['percent', 'rate', 'ratio']):
            return '%'
        elif any(word in col_str for word in ['count', 'quantity', 'number']):
            return 'count'
        elif any(word in col_str for word in ['weight']):
            return 'lbs'
        elif any(word in col_str for word in ['distance', 'length']):
            return 'miles'
        else:
            return None
    def _generate_column_description(self, display_name, semantic_type, data_type):
        """Generate a business-friendly description for a column"""
        if semantic_type == "identifier":
            return f"Unique identifier for {display_name.replace(' Id', '').replace(' ID', '')}"
        elif semantic_type == "measure":
            return f"Numeric value representing {display_name}"
        elif semantic_type == "date":
            return f"Date and time information for {display_name.replace(' Date', '').replace(' Time', '')}"
        else:
            return f"Categorical information about {display_name}"
    
    def _generate_fix_recommendation(self, target_type: str, error_patterns: dict) -> str:
        """Generate specific fix recommendations based on error patterns"""
        if target_type == 'integer':
            if error_patterns.get('decimal_values', 0) > 0:
                return "Convert to 'float' type first, or round decimal values to integers"
            elif error_patterns.get('text_values', 0) > 0:
                return "Remove text characters or change target type to 'string'"
            elif error_patterns.get('missing_values', 0) > 0:
                return "Use string type to preserve missing value indicators, or clean data first"
            else:
                return "Check data format and remove non-numeric characters"
        
        elif target_type == 'float':
            if error_patterns.get('comma_separators', 0) > 0:
                return "Remove comma separators from numbers (1,234.56 → 1234.56)"
            elif error_patterns.get('currency_percent', 0) > 0:
                return "Remove currency symbols ($) and percentage signs (%)"
            elif error_patterns.get('text_values', 0) > 0:
                return "Remove text characters or change target type to 'string'"
            else:
                return "Clean numeric values and ensure proper decimal format"
        
        elif target_type == 'date':
            return "Verify date format matches your data (try DD-MM-YYYY for most data)"
        
        else:
            return "Review data format and consider alternative data types"
    
    def _generate_error_recovery_guidance(self, failed_transformations: list, detailed_row_errors: dict, df) -> dict:
        """Generate comprehensive error recovery guidance"""
        guidance = {
            'quick_fixes': [],
            'column_specific_advice': {},
            'data_quality_insights': {},
            'recommended_actions': [],
            'alternative_approaches': []
        }
        
        # Analyze overall data quality
        guidance['data_quality_insights'] = {
            'total_rows': len(df),
            'overall_null_percentage': round((df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100, 1),
            'problematic_columns': len(failed_transformations),
            'success_columns': len(df.columns) - len(failed_transformations)
        }
        
        # Generate column-specific advice
        for column_name, error_details in detailed_row_errors.items():
            error_patterns = error_details.get('error_patterns', {})
            failed_count = error_details.get('total_failures', 0)
            target_type = error_details.get('target_type', 'unknown')
            
            advice = {
                'problem_summary': f"{failed_count} rows failed {target_type} conversion",
                'main_issues': list(error_patterns.keys()),
                'sample_fixes': error_details.get('sample_fixes', []),
                'recommended_action': error_details.get('recommendation', ''),
                'failed_row_sample': error_details.get('failed_rows', [])[:5],  # First 5 failed rows
                'failed_value_sample': error_details.get('failed_values', [])[:5]
            }
            
            # Add specific quick fixes based on error patterns
            if target_type == 'date':
                if 'slash_format' in error_patterns:
                    advice['quick_fix'] = "Try selecting 'DD/MM/YYYY' or 'MM/DD/YYYY' format in date settings"
                elif 'dash_format' in error_patterns:
                    advice['quick_fix'] = "Select 'DD-MM-YYYY' format (recommended for your data)"
                else:
                    advice['quick_fix'] = "Check date format matches your data pattern"
            
            elif target_type in ['integer', 'float']:
                if error_patterns.get('decimal_values', 0) > 0:
                    advice['quick_fix'] = f"Change target type from '{target_type}' to 'float' to handle decimal values"
                elif error_patterns.get('text_values', 0) > 0:
                    advice['quick_fix'] = f"Change target type from '{target_type}' to 'string' to preserve text data"
                else:
                    advice['quick_fix'] = "Clean data to remove non-numeric characters"
            
            guidance['column_specific_advice'][column_name] = advice
        
        # Generate quick fixes
        most_common_issue = None
        issue_counts = {}
        
        for error_details in detailed_row_errors.values():
            for issue in error_details.get('error_patterns', {}).keys():
                issue_counts[issue] = issue_counts.get(issue, 0) + 1
        
        if issue_counts:
            most_common_issue = max(issue_counts.items(), key=lambda x: x[1])
            
            if most_common_issue[0] == 'decimal_values':
                guidance['quick_fixes'].append("💡 Quick Fix: Change integer columns to 'float' type to handle decimal values")
            elif most_common_issue[0] == 'dash_format':
                guidance['quick_fixes'].append("💡 Quick Fix: Select 'DD-MM-YYYY' format for date columns")
            elif most_common_issue[0] == 'text_values':
                guidance['quick_fixes'].append("💡 Quick Fix: Use 'string' type for columns containing text")
        
        # Recommended actions
        guidance['recommended_actions'] = [
            "1. Review the failed rows and values shown above",
            "2. Apply the suggested quick fixes for each column",
            "3. Consider alternative data types if conversions continue to fail",
            "4. Clean source data if necessary before re-importing"
        ]
        
        # Alternative approaches
        guidance['alternative_approaches'] = [
            "Keep problematic columns as 'string' type initially",
            "Clean data in source system before importing",
            "Use data transformation tools to standardize formats",
            "Split complex columns into multiple simpler columns"
        ]
        
        # Add specific format guidance for dates
        date_columns = [col for col in detailed_row_errors.keys() 
                       if detailed_row_errors[col].get('target_type') == 'date']
        
        if date_columns:
            guidance['date_format_help'] = {
                'detected_formats': {},
                'recommended_format': 'DD-MM-YYYY',
                'format_examples': {
                    'DD-MM-YYYY': '31-12-2023',
                    'MM-DD-YYYY': '12-31-2023', 
                    'YYYY-MM-DD': '2023-12-31',
                    'DD/MM/YYYY': '31/12/2023'
                }
            }
            
            # Detect most common date format in failed data
            for col in date_columns:
                failed_values = detailed_row_errors[col].get('failed_values', [])
                for val in failed_values[:5]:
                    if re.match(r'\d{1,2}-\d{1,2}-\d{4}', str(val)):
                        guidance['date_format_help']['detected_formats']['DD-MM-YYYY'] = guidance['date_format_help']['detected_formats'].get('DD-MM-YYYY', 0) + 1
                    elif re.match(r'\d{1,2}/\d{1,2}/\d{4}', str(val)):
                        guidance['date_format_help']['detected_formats']['DD/MM/YYYY'] = guidance['date_format_help']['detected_formats'].get('DD/MM/YYYY', 0) + 1
        
        return guidance

    def _safe_sample_values(self, series, max_samples=5):
        """Safely extract sample values that can be JSON serialized"""
        try:
            raw_samples = series.dropna().head(max_samples)
            safe_samples = []
            
            for val in raw_samples:
                if pd.api.types.is_datetime64_any_dtype(type(val)) or hasattr(val, 'strftime'):
                    # Convert pandas Timestamp to string for JSON serialization
                    safe_samples.append(str(val))
                elif isinstance(val, (int, float, str, bool)):
                    # These types are naturally JSON serializable
                    safe_samples.append(val)
                elif pd.isna(val):
                    # Skip NaN values
                    continue
                else:
                    # Convert any other types to string for safety
                    safe_samples.append(str(val))
            
            return safe_samples
            
        except Exception as e:
            logger.warning(f"Error processing sample values: {e}")
            return []

    def _generate_sample_data_from_schema(self, data_source):
        """Generate sample DataFrame from schema when CSV file is missing"""
        try:
            schema_info = data_source.schema_info
            if not schema_info:
                return None
            
            import pandas as pd
            import numpy as np
            
            # Get column information
            columns_data = {}
            if 'tables' in schema_info and 'main_table' in schema_info['tables']:
                columns_data = schema_info['tables']['main_table'].get('columns', {})
            elif 'columns' in schema_info:
                # Convert array format to dict format
                for col in schema_info['columns']:
                    columns_data[col['name']] = col
            
            if not columns_data:
                return None
            
            # Generate sample data based on schema
            sample_size = 10  # Generate 10 sample rows for validation
            data = {}
            
            for col_name, col_info in columns_data.items():
                col_type = col_info.get('type', 'string')
                sample_values = col_info.get('sample_values', [])
                
                if sample_values:
                    # Use existing sample values and repeat them
                    values = (sample_values * (sample_size // len(sample_values) + 1))[:sample_size]
                else:
                    # Generate dummy data based on type
                    if col_type == 'integer':
                        values = list(range(1, sample_size + 1))
                    elif col_type == 'float':
                        values = [round(np.random.random() * 100, 2) for _ in range(sample_size)]
                    elif col_type == 'boolean':
                        values = [True, False] * (sample_size // 2)
                        if sample_size % 2:
                            values.append(True)
                    elif col_type in ['date', 'datetime']:
                        values = ['2023-01-01'] * sample_size
                    else:  # string
                        values = [f'Sample_{col_name}_{i}' for i in range(1, sample_size + 1)]
                
                data[col_name] = values
            
            df = pd.DataFrame(data)
            logger.info(f"Generated sample DataFrame with {len(df)} rows and {len(df.columns)} columns from schema")
            return df
            
        except Exception as e:
            logger.error(f"Error generating sample data from schema: {e}")
            return None

    def _resolve_csv_path_with_fallback(self, data_source):
        """Resolve CSV path with fallback options"""
        from services.data_service import DataService
        
        data_service = DataService()
        file_path = data_source.connection_info.get('file_path')
        
        if not file_path:
            return None, None
        
        # Try to resolve the file path
        full_file_path = data_service.resolve_csv_path(file_path)
        
        if full_file_path:
            return full_file_path, 'csv_file'
        
        # File not found, try PostgreSQL unified storage
        try:
            from django.db import connection
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT data FROM unified_data_storage WHERE data_source_name = %s ORDER BY created_at DESC LIMIT 1",
                    [data_source.name]
                )
                result = cursor.fetchone()
                
                if result:
                    return result[0], 'postgresql_data'
        except Exception as e:
            logger.error(f"Error checking PostgreSQL storage: {e}")
        
        return None, None


@login_required
@creator_required
def etl_operations(request):
    """View ETL operations"""
    operations = ETLOperation.objects.filter(
        created_by=request.user
    ).order_by('-created_at')
    
    context = {
        'operations': operations
    }
    
    return render(request, 'datasets/etl_operations.html', context)


@login_required
@creator_required
def etl_schedules(request):
    """View and manage ETL schedules"""
    schedules = ScheduledETLJob.objects.filter(
        created_by=request.user
    ).order_by('-created_at')
    
    context = {
        'schedules': schedules
    }
    
    return render(request, 'datasets/etl_schedules.html', context)


def _safe_sample_values_standalone(series, max_samples=5):
    """Safely extract sample values that can be JSON serialized - standalone version"""
    try:
        raw_samples = series.dropna().head(max_samples)
        safe_samples = []
        
        for val in raw_samples:
            if pd.api.types.is_datetime64_any_dtype(type(val)) or hasattr(val, 'strftime'):
                # Convert pandas Timestamp to string for JSON serialization
                safe_samples.append(str(val))
            elif isinstance(val, (int, float, str, bool)):
                # These types are naturally JSON serializable
                safe_samples.append(val)
            elif pd.isna(val):
                # Skip NaN values
                continue
            else:
                # Convert any other types to string as fallback
                safe_samples.append(str(val))
        
        return safe_samples
        
    except Exception as e:
        logger.warning(f"Error extracting safe sample values: {e}")
        return []


@login_required
@creator_required
def data_source_create_database(request):
    """Handle database connection creation"""
    if request.method == 'POST':
        try:
            # Get form data
            source_type = request.POST.get('source_type')
            name = request.POST.get('name')
            description = request.POST.get('description', '')
            
            # Get connection parameters
            connection_params = {
                'type': source_type,
                'host': request.POST.get('host'),
                'port': int(request.POST.get('port', 5432)),
                'database': request.POST.get('database'),
                'username': request.POST.get('username'),
                'password': request.POST.get('password')
            }
            
            # Add selected tables to connection info
            selected_tables_json = request.POST.get('selected_tables', '[]')
            try:
                selected_tables = json.loads(selected_tables_json)
                connection_params['tables'] = selected_tables
            except (json.JSONDecodeError, TypeError):
                return JsonResponse({'error': 'Invalid table selection data'}, status=400)
            
            # Test connection
            data_service = DataService()
            success, message = data_service.test_connection(connection_params)
            
            if not success:
                return JsonResponse({'error': f'Connection test failed: {message}'}, status=400)
            
            # Initialize workflow status for database connections
            # Database connections provide immediate data access, so mark data_loaded as True
            workflow_status = WorkflowManager.get_default_status()
            workflow_status = WorkflowManager.update_workflow_step(
                workflow_status, 
                WorkflowStep.DATA_LOADED, 
                True
            )
            
            # Create data source
            data_source = DataSource.objects.create(
                name=name,
                source_type=source_type,
                connection_info=connection_params,
                created_by=request.user,
                status='active',
                workflow_status=workflow_status
            )
            
            # Generate schema info immediately after creation
            try:
                data_service = DataService()
                schema_info = data_service.get_schema_info(connection_params, data_source)
                if schema_info and not schema_info.get('error'):
                    data_source.schema_info = schema_info
                    data_source.save()
                    logger.info(f"Successfully generated schema for {data_source.name}")
                else:
                    logger.warning(f"Failed to generate schema for {data_source.name}: {schema_info.get('error', 'Unknown error')}")
            except Exception as e:
                logger.error(f"Error generating schema for {data_source.name}: {e}")
            
            # Add to integration system with table information
            integration_service = DataIntegrationService()
            
            # Enhance connection params with data source info for integration
            integration_params = connection_params.copy()
            integration_params['data_source_id'] = str(data_source.id)
            integration_params['data_source_name'] = name
            
            integration_service.add_data_source(
                name=name,
                source_type=source_type,
                connection_info=integration_params,
                user_id=request.user.pk or 0
            )
            
            return JsonResponse({
                'success': True,
                'data_source_id': str(data_source.id),
                'message': f'Database connection created successfully!',
                'redirect_url': f'/datasets/{data_source.id}/'
            })
            
        except Exception as e:
            logger.error(f"Error creating database connection: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
@creator_required
def data_source_create_api(request):
    """Handle API connection creation"""
    if request.method == 'POST':
        try:
            # Get form data
            name = request.POST.get('name')
            description = request.POST.get('description', '')
            api_url = request.POST.get('api_url')
            auth_type = request.POST.get('auth_type', 'none')
            
            # Validate required fields
            if not all([name, api_url]):
                return JsonResponse({'error': 'Name and API URL are required'}, status=400)
            
            # Build connection info
            connection_info = {
                'type': 'api',
                'base_url': api_url,
                'auth_type': auth_type
            }
            
            # Add authentication details
            if auth_type == 'apikey':
                api_key = request.POST.get('api_key')
                api_key_header = request.POST.get('api_key_header', 'X-API-Key')
                if not api_key:
                    return JsonResponse({'error': 'API key is required'}, status=400)
                connection_info['api_key'] = api_key
                connection_info['api_key_header'] = api_key_header
                
            elif auth_type == 'bearer':
                bearer_token = request.POST.get('bearer_token')
                if not bearer_token:
                    return JsonResponse({'error': 'Bearer token is required'}, status=400)
                connection_info['bearer_token'] = bearer_token
                
            elif auth_type == 'basic':
                basic_username = request.POST.get('basic_username')
                basic_password = request.POST.get('basic_password')
                if not all([basic_username, basic_password]):
                    return JsonResponse({'error': 'Username and password are required for basic auth'}, status=400)
                connection_info['basic_username'] = basic_username
                connection_info['basic_password'] = basic_password
            
            # Test API connection
            data_service = DataService()
            success, message = data_service.test_api_connection(connection_info)
            
            if not success:
                return JsonResponse({'error': f'API connection test failed: {message}'}, status=400)
            
            # Create data source with duplicate prevention
            with transaction.atomic():
                # Check for existing data source first
                existing = DataSource.objects.filter(
                    created_by=request.user,
                    name=name,
                    is_deleted=False
                ).first()
                
                if existing:
                    return JsonResponse({
                        'error': f'Data source "{name}" already exists. Please choose a different name.'
                    }, status=400)
                
                # Initialize workflow status for API connections
                # API connections provide immediate data access, so mark data_loaded as True
                workflow_status = WorkflowManager.get_default_status()
                workflow_status = WorkflowManager.update_workflow_step(
                    workflow_status, 
                    WorkflowStep.DATA_LOADED, 
                    True
                )
                
                data_source = DataSource.objects.create(
                    name=name,
                    source_type='api',
                    connection_info=connection_info,
                    created_by=request.user,
                    status='active',
                    workflow_status=workflow_status
                )
                
                # Process with integration system using existing DataSource
                integration_service = DataIntegrationService()
                success = integration_service.process_existing_data_source(
                    data_source=data_source
                )
                
                if not success:
                    # If integration fails, clean up the created data source
                    data_source.delete()
                    raise Exception("Failed to process data source with integration service")
            
            return JsonResponse({
                'success': True,
                'data_source_id': str(data_source.id),
                'message': f'API connection created successfully.',
                'redirect_url': f'/datasets/{data_source.id}/'
            })
            
        except Exception as e:
            logger.error(f"Error creating API connection: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def data_source_test_connection(request, pk):
    """Test connection to existing data source"""
    if request.method == 'POST':
        try:
            data_source = get_object_or_404(DataSource, id=pk, created_by=request.user)
            
            # Test connection based on source type
            data_service = DataService()
            
            if data_source.source_type == 'api':
                success, message = data_service.test_api_connection(data_source.connection_info)
            else:
                success, message = data_service.test_connection(data_source.connection_info)
            
            # Update status
            data_source.status = 'active' if success else 'error'
            data_source.save()
            
            return JsonResponse({
                'success': success,
                'message': message,
                'status': data_source.status
            })
            
        except Exception as e:
            logger.error(f"Error testing connection: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def data_source_test_database_connection(request):
    """Test database connection before saving"""
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            
            connection_info = {
                'type': data.get('source_type'),
                'host': data.get('host'),
                'port': int(data.get('port')),
                'database': data.get('database'),
                'username': data.get('username'),
                'password': data.get('password')
            }
            
            if data.get('schema'):
                connection_info['schema'] = data.get('schema')
            
            # Test connection
            data_service = DataService()
            success, message = data_service.test_connection(connection_info)
            
            return JsonResponse({
                'success': success,
                'message': message
            })
            
        except Exception as e:
            logger.error(f"Error testing database connection: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def data_source_test_api_connection(request):
    """Test API connection before saving"""
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            
            connection_info = {
                'type': 'api',
                'base_url': data.get('api_url'),
                'auth_type': data.get('auth_type', 'none')
            }
            
            # Add authentication details
            auth_type = data.get('auth_type', 'none')
            if auth_type == 'apikey':
                connection_info['api_key'] = data.get('api_key')
                connection_info['api_key_header'] = data.get('api_key_header', 'X-API-Key')
            elif auth_type == 'bearer':
                connection_info['bearer_token'] = data.get('bearer_token')
            elif auth_type == 'basic':
                connection_info['basic_username'] = data.get('basic_username')
                connection_info['basic_password'] = data.get('basic_password')
            
            # Test connection
            data_service = DataService()
            success, message = data_service.test_api_connection(connection_info)
            
            return JsonResponse({
                'success': success,
                'message': message
            })
            
        except Exception as e:
            logger.error(f"Error testing API connection: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
@creator_required
def generate_semantic_layer(request, pk):
    """Generate semantic layer for a data source with enhanced validation"""
    if request.method == 'POST':
        try:
            data_source = get_object_or_404(DataSource, id=pk, created_by=request.user)
            
            # CHECK: If semantic layer already exists for this data source
            from .models import SemanticTable, SemanticColumn, SemanticMetric
            existing_semantic_tables = SemanticTable.objects.filter(data_source=data_source)
            if existing_semantic_tables.exists():
                logger.info(f"Semantic layer already exists for data source {data_source.id}")
                
                # Update workflow status to reflect completion
                workflow_status = data_source.workflow_status or WorkflowManager.get_default_status()
                workflow_status = WorkflowManager.update_workflow_step(
                    workflow_status, 
                    WorkflowStep.SEMANTICS_COMPLETED,
                    True
                )
                data_source.workflow_status = workflow_status
                data_source.save()
                
                # Count existing semantic objects
                table_count = existing_semantic_tables.count()
                column_count = SemanticColumn.objects.filter(
                    semantic_table__data_source=data_source
                ).count()
                metric_count = SemanticMetric.objects.filter(
                    base_table__data_source=data_source
                ).count()
                
                return JsonResponse({
                    'success': True,
                    'message': f'Semantic layer already exists! Found {table_count} tables, {column_count} columns, and {metric_count} metrics.',
                    'details': {
                        'tables_existing': table_count,
                        'columns_existing': column_count,
                        'metrics_existing': metric_count,
                        'already_existed': True
                    }
                })
            
            # Skip the strict validation and use robust data loading approach
            logger.info(f"Starting semantic layer generation for data source {data_source.id}")
            
            # Use the same robust generation approach as auto_generate_all
            semantic_service = SemanticService()
            integration_service = DataIntegrationService()
            
            # Use the enhanced SemanticLayerView method for individual source generation
            from django.views import View
            semantic_view = SemanticLayerView()
            result = semantic_view._generate_semantic_for_source(data_source, semantic_service, integration_service)
            
            if result['success']:
                return JsonResponse({
                    'success': True,
                    'message': f'Semantic layer generated successfully! Created {result.get("columns_created", 0)} columns and {len(result.get("metrics", []))} metrics.',
                    'details': {
                        'tables_created': len(result.get('tables', [])),
                        'metrics_created': len(result.get('metrics', [])),
                        'columns_created': result.get('columns_created', 0)
                    }
                })
            else:
                return JsonResponse({
                    'error': 'Failed to generate semantic layer',
                    'details': result.get('error', 'Unknown error occurred'),
                    'guidance': 'Please ensure your CSV file is properly uploaded and accessible.'
                }, status=400)
            
        except Exception as e:
            logger.error(f"Error generating semantic layer for {pk}: {e}")
            return JsonResponse({
                'error': f'Failed to generate semantic layer: {str(e)}',
                'debug_info': {
                    'data_source_id': str(pk),
                    'error_type': type(e).__name__
                }
            }, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def check_data_readiness(request):
    """Check data readiness for all data sources"""
    if request.method == 'GET':
        try:
            logger.info(f"Checking data readiness for user: {request.user}")
            
            # Get all user's data sources
            data_sources = DataSource.objects.filter(
                created_by=request.user,
                status='active'
            )
            
            logger.info(f"Found {len(data_sources)} data sources for user")
            
            ready_sources = []
            pending_sources = []
            error_sources = []
            
            for data_source in data_sources:
                try:
                    logger.info(f"Validating data source: {data_source.name} ({data_source.id})")
                    validation_errors = _validate_data_source_readiness(data_source)
                    
                    source_info = {
                        'id': str(data_source.id),
                        'name': data_source.name,
                        'source_type': data_source.source_type,
                        'issues': validation_errors,
                        'ready': len(validation_errors) == 0
                    }
                    
                    if len(validation_errors) == 0:
                        ready_sources.append(source_info)
                    elif any('not completed' in error.lower() for error in validation_errors):
                        pending_sources.append(source_info)
                    else:
                        error_sources.append(source_info)
                        
                except Exception as source_error:
                    logger.error(f"Error validating data source {data_source.id}: {source_error}")
                    # Add to error sources with the exception info
                    error_sources.append({
                        'id': str(data_source.id),
                        'name': data_source.name,
                        'source_type': data_source.source_type,
                        'issues': [f"Validation error: {str(source_error)}"],
                        'ready': False
                    })
            
            # Generate recommendations
            recommendations = []
            if pending_sources:
                recommendations.append("Complete ETL processing for pending data sources before generating semantic metadata")
            if error_sources:
                recommendations.append("Resolve data loading issues for sources with errors")
            if not ready_sources and not pending_sources and not error_sources:
                recommendations.append("Upload data sources first before attempting semantic generation")
            
            result = {
                'ready_count': len(ready_sources),
                'pending_count': len(pending_sources),
                'error_count': len(error_sources),
                'total_count': len(data_sources),
                'data_sources': ready_sources + pending_sources + error_sources,
                'recommendations': recommendations
            }
            
            logger.info(f"Data readiness check complete: {result['ready_count']} ready, {result['pending_count']} pending, {result['error_count']} errors")
            
            return JsonResponse(result)
            
        except Exception as e:
            logger.error(f"Error checking data readiness: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return JsonResponse({
                'error': str(e),
                'ready_count': 0,
                'pending_count': 0,
                'error_count': 0,
                'total_count': 0,
                'data_sources': [],
                'recommendations': ['Error occurred while checking data readiness']
            }, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def check_individual_data_source_status(request, pk):
    """Check status of an individual data source"""
    if request.method == 'GET':
        try:
            data_source = get_object_or_404(DataSource, id=pk, created_by=request.user)
            
            validation_errors = _validate_data_source_readiness(data_source)
            
            result = {
                'success': True,
                'id': str(data_source.id),
                'name': data_source.name,
                'source_type': data_source.source_type,
                'ready': len(validation_errors) == 0,
                'issues': validation_errors,
                'workflow_status': data_source.workflow_status or {}
            }
            
            return JsonResponse(result)
            
        except Exception as e:
            logger.error(f"Error checking individual data source status: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


def _validate_data_source_readiness(data_source) -> List[str]:
    """
    Helper method to validate that a data source is ready for semantic generation
    """
    validation_errors = []
    
    try:
        # Check basic data source information
        if not data_source.connection_info:
            validation_errors.append("Data source connection information is missing")
        
        # Check workflow status - strengthen the workflow status check
        workflow_status = data_source.workflow_status or {}
        etl_completed = workflow_status.get('etl_completed', False)
        
        # Remove CSV bypass logic - ensure CSV data is actually loaded
        if not etl_completed:
            validation_errors.append(f"ETL process not completed for data source '{data_source.name}'")
        
        # Verify actual table existence in integrated database
        integration_service = DataIntegrationService()
        
        # Get the proper table name for this data source
        from utils.table_name_helper import get_integrated_table_name
        table_name = get_integrated_table_name(data_source)
        
        # Check if the table actually exists
        if not integration_service.check_table_exists(table_name):
            validation_errors.append(f"Data table '{table_name}' does not exist in integrated database")
        else:
            # Verify table contains data
            try:
                test_data = integration_service.get_integrated_data(table_name)
                if test_data.empty:
                    validation_errors.append(f"Data table '{table_name}' exists but contains no data")
                else:
                    logger.debug(f"Data readiness verified: table '{table_name}' contains {len(test_data)} rows")
            except Exception as data_check_error:
                validation_errors.append(f"Could not verify data in table '{table_name}': {str(data_check_error)}")
        
        # Additional validation for CSV files
        if data_source.source_type == 'csv':
            file_path = data_source.connection_info.get('file_path')
            if not file_path:
                validation_errors.append("CSV file path is missing")
            elif not data_source.table_name:
                validation_errors.append("CSV data has not been loaded into integrated database")
        
        return validation_errors
        
    except Exception as e:
        logger.error(f"Error during data source readiness validation: {e}")
        return [f"Validation error: {str(e)}"]


@login_required
def get_data_source_schema_api(request, pk):
    """API endpoint to get data source schema with semantic types"""
    if request.method == 'GET':
        try:
            data_source = get_object_or_404(DataSource, id=pk, created_by=request.user)
            
            # Get schema info with semantic types
            schema_info = data_source.schema_info or {}
            
            # Handle both old and new schema formats
            schema = {}
            
            # Check if schema_info has the new structure with column data
            if 'tables' in schema_info and 'main_table' in schema_info['tables']:
                # New structure: schema_info.tables.main_table.columns
                columns_data = schema_info['tables']['main_table'].get('columns', {})
                for col_name, col_info in columns_data.items():
                    schema[col_name] = {
                        'type': col_info.get('type', 'string'),
                        'pandas_type': col_info.get('pandas_type', 'object'),
                        'sample_values': col_info.get('sample_values', [])
                    }
            elif 'columns' in schema_info and isinstance(schema_info['columns'], list):
                # Old structure: schema_info.columns (array format)
                columns_info = schema_info.get('columns', [])
                for col_info in columns_info:
                    col_name = col_info.get('name', '')
                    if col_name:
                        schema[col_name] = {
                            'type': col_info.get('type', 'string'),
                            'pandas_type': col_info.get('pandas_type', col_info.get('type', 'object')),
                            'sample_values': col_info.get('sample_values', [])
                        }
                logger.info(f"Parsed schema from list format: {len(schema)} columns")
            elif 'columns' in schema_info and isinstance(schema_info['columns'], dict):
                # Direct columns dict format
                columns_data = schema_info['columns']
                for col_name, col_info in columns_data.items():
                    schema[col_name] = {
                        'type': col_info.get('type', 'string'),
                        'pandas_type': col_info.get('pandas_type', 'object'),
                        'sample_values': col_info.get('sample_values', [])
                    }
            else:
                # Fallback: Try to load from different source types to regenerate schema
                logger.warning(f"No valid schema found for {data_source.name}, attempting to load from {data_source.source_type}")
                if data_source.source_type == 'csv':
                    schema = _generate_schema_from_csv(data_source)
                elif data_source.source_type == 'etl_result':
                    schema = _generate_schema_from_etl_result(data_source)
                else:
                    schema = _generate_schema_from_universal_loader(data_source)
                
                if not schema:
                    logger.error(f"No schema available for data source {data_source.name}")
                    return JsonResponse({'error': 'No schema information available for this data source'}, status=400)
            
            logger.info(f"Schema API for {data_source.name}: Found {len(schema)} columns")
            
            return JsonResponse({
                'success': True,
                'schema': schema,
                'row_count': schema_info.get('row_count', 0),
                'column_count': len(schema),
                'data_source_name': data_source.name
            })
            
        except Exception as e:
            logger.error(f"Error getting data source schema: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)

def _generate_schema_from_csv(data_source):
    """Generate schema by reading CSV file directly"""
    try:
        file_path = data_source.connection_info.get('file_path')
        if not file_path:
            return {}
        
        import os
        import pandas as pd
        from django.conf import settings
        
        full_path = os.path.join(settings.MEDIA_ROOT, file_path)
        if not os.path.exists(full_path):
            logger.error(f"CSV file not found: {full_path}")
            return {}
        
        # Load CSV with proper parameters
        delimiter = data_source.connection_info.get('delimiter', ',')
        has_header = data_source.connection_info.get('has_header', True)
        
        df = pd.read_csv(full_path, delimiter=delimiter, header=0 if has_header else None, nrows=100)
        
        schema = {}
        for col in df.columns:
            col_data = df[col]
            
            # Infer semantic type
            if pd.api.types.is_numeric_dtype(col_data):
                if col_data.dtype in ['int64', 'int32', 'int16', 'int8']:
                    data_type = 'integer'
                else:
                    data_type = 'float'
            elif pd.api.types.is_datetime64_any_dtype(col_data):
                data_type = 'datetime'
            elif pd.api.types.is_bool_dtype(col_data):
                data_type = 'boolean'
            else:
                data_type = 'string'
            
            schema[str(col)] = {
                'type': data_type,
                'pandas_type': str(col_data.dtype),
                'sample_values': col_data.dropna().head(3).astype(str).tolist()
            }
        
        logger.info(f"Generated schema from CSV for {data_source.name}: {len(schema)} columns")
        return schema
        
    except Exception as e:
        logger.error(f"Error generating schema from CSV: {e}")
        return {}

def _generate_schema_from_etl_result(data_source):
    """Generate schema by loading data from ETL result table"""
    try:
        from services.universal_data_loader import universal_data_loader
        import pandas as pd
        
        # Load data from ETL result
        success, df, message = universal_data_loader.load_data_for_transformation(data_source)
        
        if not success or df is None or df.empty:
            logger.error(f"Failed to load ETL result data for schema generation: {message}")
            return {}
        
        schema = {}
        row_count = len(df)
        
        for col in df.columns:
            col_data = df[col]
            
            # Infer semantic type from actual data
            if pd.api.types.is_numeric_dtype(col_data):
                if col_data.dtype in ['int64', 'int32', 'int16', 'int8', 'Int64']:
                    data_type = 'integer'
                else:
                    data_type = 'float'
            elif pd.api.types.is_datetime64_any_dtype(col_data):
                data_type = 'datetime'
            elif pd.api.types.is_bool_dtype(col_data):
                data_type = 'boolean'
            else:
                data_type = 'string'
            
            schema[str(col)] = {
                'type': data_type,
                'pandas_type': str(col_data.dtype),
                'sample_values': col_data.dropna().head(3).astype(str).tolist()
            }
        
        # Update data source schema info with row count
        data_source.schema_info = {
            'columns': [
                {
                    'name': col_name,
                    'type': col_info['type'],
                    'pandas_type': col_info['pandas_type'],
                    'sample_values': col_info['sample_values']
                }
                for col_name, col_info in schema.items()
            ],
            'row_count': row_count
        }
        data_source.save()
        
        logger.info(f"Generated schema from ETL result for {data_source.name}: {len(schema)} columns, {row_count} rows")
        return schema
        
    except Exception as e:
        logger.error(f"Error generating schema from ETL result: {e}")
        return {}

def _generate_schema_from_universal_loader(data_source):
    """Generate schema using universal data loader for any source type"""
    try:
        from services.universal_data_loader import universal_data_loader
        import pandas as pd
        
        logger.info(f"Attempting to load data for schema generation using universal loader for {data_source.name}")
        logger.info(f"Data source type: {data_source.source_type}")
        logger.info(f"Connection info keys: {list(data_source.connection_info.keys()) if data_source.connection_info else 'None'}")
        
        # Load data using universal loader
        success, df, message = universal_data_loader.load_data_for_transformation(data_source)
        
        if not success or df is None or df.empty:
            logger.error(f"Failed to load data for schema generation: {message}")
            return {}
        
        schema = {}
        row_count = len(df)
        
        logger.info(f"Successfully loaded {row_count} rows for schema generation")
        
        for col in df.columns:
            col_data = df[col]
            
            # Infer semantic type from actual data
            if pd.api.types.is_numeric_dtype(col_data):
                if col_data.dtype in ['int64', 'int32', 'int16', 'int8', 'Int64']:
                    data_type = 'integer'
                else:
                    data_type = 'float'
            elif pd.api.types.is_datetime64_any_dtype(col_data):
                data_type = 'datetime'
            elif pd.api.types.is_bool_dtype(col_data):
                data_type = 'boolean'
            else:
                data_type = 'string'
            
            schema[str(col)] = {
                'type': data_type,
                'pandas_type': str(col_data.dtype),
                'sample_values': col_data.dropna().head(3).astype(str).tolist()
            }
        
        # Update data source schema info with row count
        data_source.schema_info = {
            'columns': [
                {
                    'name': col_name,
                    'type': col_info['type'],
                    'pandas_type': col_info['pandas_type'],
                    'sample_values': col_info['sample_values']
                }
                for col_name, col_info in schema.items()
            ],
            'row_count': row_count
        }
        data_source.save()
        
        logger.info(f"Generated schema from universal loader for {data_source.name}: {len(schema)} columns, {row_count} rows")
        return schema
        
    except Exception as e:
        logger.error(f"Error generating schema from universal loader for {data_source.name}: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return {}


@login_required
@creator_required
def execute_etl_transformation(request):
    """Execute ETL data type transformation with enhanced error handling"""
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data_source_id = data.get('data_source_id')
            operation_name = data.get('operation_name', 'Data Type Transformation')
            transformations = data.get('transformations', {})
            
            logger.info(f"ETL transformation request: data_source_id={data_source_id}, transformations={transformations}")
            
            if not data_source_id:
                return JsonResponse({'error': 'Data source ID is required'}, status=400)
            
            if not transformations:
                return JsonResponse({'error': 'No transformations specified'}, status=400)
            
            # Get the data source
            data_source = get_object_or_404(DataSource, id=data_source_id, created_by=request.user)
            
            # Load data using Universal Data Loader (supports all source types)
            from services.universal_data_loader import universal_data_loader
            import pandas as pd
            
            # Load data from any source type
            success, df, message = universal_data_loader.load_data_for_transformation(data_source)
            
            if not success or df is None or df.empty:
                logger.warning(f"Failed to load data for ETL transformations from {data_source.source_type} source '{data_source.name}': {message}")
                return JsonResponse({
                    'error': f'Data not accessible for ETL transformations: {message}',
                    'title': 'Data Not Available',
                    'details': f'Unable to load data from {data_source.source_type} source for ETL processing.',
                    'suggestion': f'Please check your {data_source.source_type} data source configuration and try again.',
                    'data_access_attempted': True,
                    'source_type': data_source.source_type
                }, status=400)
            
            logger.info(f"Successfully loaded data for ETL transformations: {len(df)} rows, {len(df.columns)} columns from {data_source.source_type} source")
            
            # Validate transformations against actual column names
            available_columns = list(df.columns)
            logger.info(f"Available columns: {available_columns}")
            
            # Clean up transformations - handle both column names and indices
            valid_transformations = {}
            # FIXED: Handle date format information separately
            date_formats = {}
            transformation_keys = {}
            
            # First pass: extract date formats and actual transformations
            for key, target_type in transformations.items():
                if key.endswith('_date_format'):
                    # This is a date format specification, not a column transformation
                    base_column = key.replace('_date_format', '')
                    date_formats[base_column] = target_type
                    logger.info(f"Found date format for '{base_column}': {target_type}")
                elif target_type and target_type != 'No change':
                    transformation_keys[key] = target_type
            
            # Second pass: process actual column transformations
            for key, target_type in transformation_keys.items():
                try:
                    if key.isdigit():  # If key is numeric index
                        col_index = int(key)
                        if 0 <= col_index < len(available_columns):
                            column_name = available_columns[col_index]
                            valid_transformations[column_name] = target_type
                            logger.info(f"Mapped index {col_index} to column '{column_name}' -> {target_type}")
                        else:
                            logger.warning(f"Column index {col_index} out of range")
                    else:  # If key is column name
                        if key in available_columns:
                            valid_transformations[key] = target_type
                            logger.info(f"Using column name '{key}' -> {target_type}")
                        else:
                            logger.warning(f"Column '{key}' not found in DataFrame")
                except (ValueError, TypeError) as e:
                    logger.warning(f"Error processing transformation key '{key}': {e}")
            
            if not valid_transformations:
                return JsonResponse({'error': 'No valid transformations found for existing columns'}, status=400)
            
            logger.info(f"Valid transformations: {valid_transformations}")
            
            # Apply transformations with enhanced validation
            transformation_results = []
            failed_transformations = []
            detailed_row_errors = {}  # NEW: Track row-level errors
            
            for column_name, target_type in valid_transformations.items():
                try:
                    original_dtype = str(df[column_name].dtype)
                    original_sample = df[column_name].dropna().head(3).tolist()
                    
                    # Apply transformation
                    if target_type == 'string':
                        df[column_name] = df[column_name].astype(str)
                        
                    elif target_type == 'integer':
                        # Convert to integer with error handling
                        df[column_name] = pd.to_numeric(df[column_name], errors='coerce').astype('Int64')
                        
                    elif target_type == 'float':
                        # Convert to float with error handling
                        df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
                        
                    elif target_type == 'date':
                        # Convert to date with error handling
                        df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
                        
                    elif target_type == 'datetime':
                        # Convert to datetime with error handling
                        df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
                        
                    elif target_type == 'boolean':
                        # Convert to boolean with mapping
                        bool_map = {
                            'true': True, 'True': True, 'TRUE': True, '1': True, 1: True,
                            'false': False, 'False': False, 'FALSE': False, '0': False, 0: False,
                            'yes': True, 'Yes': True, 'YES': True, 'y': True, 'Y': True,
                            'no': False, 'No': False, 'NO': False, 'n': False, 'N': False
                        }
                        df[column_name] = df[column_name].map(bool_map).fillna(df[column_name].astype(str).str.lower() == 'true')
                    
                    # Validation: Check transformation was successful
                    new_dtype = str(df[column_name].dtype)
                    new_sample = df[column_name].dropna().head(3).tolist()
                    
                    # Count NaN/NaT values after transformation
                    null_count = df[column_name].isna().sum()
                    null_percentage = (null_count / len(df)) * 100
                    
                    transformation_results.append({
                        'column': column_name,
                        'original_type': original_dtype,
                        'new_type': new_dtype,
                        'target_type': target_type,
                        'success': True,
                        'original_sample': [str(x) for x in original_sample],
                        'new_sample': [str(x) for x in new_sample],
                        'null_count': int(null_count),
                        'null_percentage': round(null_percentage, 2)
                    })
                    
                    logger.info(f"Successfully transformed column '{column_name}': {original_dtype} -> {new_dtype} (target: {target_type}), {null_count} nulls ({null_percentage:.1f}%)")
                    
                except Exception as e:
                    error_msg = f"Failed to transform column '{column_name}' to {target_type}: {str(e)}"
                    logger.error(error_msg)
                    
                    failed_transformations.append({
                        'column': column_name,
                        'target_type': target_type,
                        'error': str(e)
                    })
                    
                    transformation_results.append({
                        'column': column_name,
                        'target_type': target_type,
                        'success': False,
                        'error': str(e)
                    })
            
            # Check for failed transformations
            if failed_transformations:
                logger.info(f"Transformation failed for {len(failed_transformations)} columns")
                
                return JsonResponse({
                    'success': False,
                    'error': f"Transformation failed for {len(failed_transformations)} columns",
                    'failed_transformations': failed_transformations,
                    'partial_results': transformation_results,
                    'total_columns': len(valid_transformations),
                    'successful_columns': len(transformation_results) - len(failed_transformations),
                    'can_retry': True
                }, status=400)
            
            # Persist transformed data to integrated database
            try:
                from services.integration_service import DataIntegrationService
                integration_service = DataIntegrationService()
                
                # Save transformed data to integrated database
                table_name = f"source_{data_source.id.hex.replace('-', '_')}"
                success = integration_service.store_transformed_data(
                    table_name=table_name,
                    data=df,
                    transformations=valid_transformations,
                    source_id=str(data_source.id)
                )
                
                if not success:
                    raise Exception("Failed to persist transformed data to integrated database")
                
                logger.info(f"Successfully persisted transformed data to integrated database: {table_name}")
                
            except Exception as persist_error:
                logger.error(f"Failed to persist transformed data: {persist_error}")
                return JsonResponse({
                    'error': f'Transformations applied but failed to persist data: {str(persist_error)}',
                    'transformation_results': transformation_results
                }, status=500)
            
            # Update schema info with new types
            schema_info = data_source.schema_info.copy()
            columns_info = schema_info.get('columns', [])
            
            for col_info in columns_info:
                col_name = col_info.get('name')
                if col_name in valid_transformations:
                    target_type = valid_transformations[col_name]
                    col_info['type'] = target_type
                    col_info['pandas_type'] = str(df[col_name].dtype)
            
            # Save updated schema
            data_source.schema_info = schema_info
            data_source.save()
            
            # Create ETL operation record
            etl_operation = ETLOperation.objects.create(
                name=operation_name,
                operation_type='transform',
                source_tables=[str(data_source.id)],
                parameters={
                    'transformations': valid_transformations,
                    'data_source_id': str(data_source.id)
                },
                output_table_name=f"{data_source.name}_transformed",
                status='completed',
                created_by=request.user,
                row_count=len(df),
                result_summary={
                    'transformed_columns': list(valid_transformations.keys()),
                    'transformation_results': transformation_results,
                    'row_count': len(df),
                    'success': True
                },
                data_lineage={
                    'source_data_source': str(data_source.id),
                    'transformation_type': 'data_type_transformation',
                    'timestamp': timezone.now().isoformat()
                }
            )
            
            # Update workflow status to mark ETL as completed
            workflow_status = data_source.workflow_status or WorkflowManager.get_default_status()
            
            # Enhanced validation before marking ETL as completed
            if not WorkflowManager.validate_etl_transformation_results(transformation_results):
                logger.error(f"ETL validation failed for {data_source.name}")
                
                # ENHANCED: Generate detailed error recovery guidance even for validation failures
                recovery_guidance = {
                    'message': 'ETL validation failed but data transformations were successful',
                    'suggestion': 'You can proceed with the current transformations or adjust column types'
                }
                
                return JsonResponse({
                    'success': False,
                    'error': 'ETL transformations failed final validation. High null rates or critical column issues detected.',
                    'transformation_results': transformation_results,
                    'validation_failed': True,
                    'automatic_error_recovery': True,
                    'recovery_guidance': recovery_guidance,
                    'can_proceed_anyway': True,  # Allow user to proceed despite warnings
                    'detailed_row_errors': {},
                    'total_columns': len(valid_transformations),
                    'successful_columns': len([r for r in transformation_results if r.get('success', False)]),
                    'data_summary': {
                        'total_rows': len(df),
                        'columns_analyzed': list(valid_transformations.keys()),
                        'high_null_columns': [r['column'] for r in transformation_results if r.get('null_percentage', 0) > 80]
                    }
                }, status=200)  # Use 200 for recoverable validation issues
            
            # Validate stage transition
            from services.integration_service import DataIntegrationService
            integration_service = DataIntegrationService()
            
            can_transition, transition_message = WorkflowManager.validate_stage_transition(
                data_source=data_source,
                from_step=WorkflowStep.DATA_LOADED,
                to_step=WorkflowStep.ETL_COMPLETED,
                integration_service=integration_service,
                transformation_results=transformation_results
            )
            
            if not can_transition:
                logger.error(f"Stage transition validation failed: {transition_message}")
                return JsonResponse({
                    'error': f'Cannot complete ETL stage: {transition_message}',
                    'transformation_results': transformation_results,
                    'workflow_validation_failed': True
                }, status=400)
            
            # Mark ETL as completed only after successful validation
            workflow_status = WorkflowManager.update_workflow_step(
                workflow_status, 
                WorkflowStep.ETL_COMPLETED, 
                True
            )
            data_source.workflow_status = workflow_status
            data_source.save()
            
            logger.info(f"ETL stage completed and validated for {data_source.name}")
            
            # --- CRITICAL: REGENERATE SEMANTIC LAYER WITH ETL-TRANSFORMED DATA ---
            try:
                from services.semantic_service import SemanticService
                semantic_service = SemanticService()
                
                # Clear existing semantic layer for this table
                output_table_name = table_name
                semantic_service.clear_semantic_layer_for_table(output_table_name)
                logger.info(f"Cleared existing semantic layer for table: {output_table_name}")
                
                # Regenerate semantic layer with ETL-transformed data
                semantic_success = semantic_service.auto_generate_metadata_from_table(output_table_name)
                
                if semantic_success:
                    logger.info(f"SUCCESS: Semantic layer regenerated for ETL output: {output_table_name}")
                    semantic_message = "Semantic layer updated with ETL-transformed data types"
                else:
                    logger.warning(f"WARNING: Failed to regenerate semantic layer for: {output_table_name}")
                    semantic_message = "ETL completed but semantic layer regeneration failed"
                    
            except Exception as semantic_error:
                logger.error(f"Error regenerating semantic layer: {semantic_error}")
                semantic_message = f"ETL completed but semantic layer error: {str(semantic_error)}"
                semantic_success = False
            
            return JsonResponse({
                'success': True,
                'message': f'Transformation completed and validated successfully! {len(valid_transformations)} columns transformed.',
                'operation_id': str(etl_operation.id),
                'row_count': len(df),
                'transformation_results': transformation_results,
                'workflow_updated': True,
                'validation_passed': True,
                'transition_message': transition_message,
                'semantic_layer_updated': semantic_success,
                'semantic_message': semantic_message
            })
            
        except Exception as e:
            logger.error(f"Error executing ETL transformation: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


# Enhanced Semantic Layer API Views

@login_required
def get_semantic_table_api(request, table_id):
    """API endpoint to get semantic table details"""
    if request.method == 'GET':
        try:
            from datasets.models import SemanticTable
            table = get_object_or_404(SemanticTable, id=table_id)
            
            table_data = {
                'id': str(table.pk),
                'name': table.name,
                'display_name': table.display_name,
                'description': table.description,
                'business_purpose': table.business_purpose,
                'is_fact_table': table.is_fact_table,
                'is_dimension_table': table.is_dimension_table,
                'row_count_estimate': table.row_count_estimate
            }
            
            return JsonResponse({
                'success': True,
                'table': table_data
            })
            
        except Exception as e:
            logger.error(f"Error getting semantic table {table_id}: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required  
def get_semantic_table_columns_api(request, table_id):
    """API endpoint to get semantic table columns"""
    if request.method == 'GET':
        try:
            from datasets.models import SemanticTable, SemanticColumn
            
            # Add debugging
            logger.info(f"Loading columns for semantic table {table_id}")
            
            table = get_object_or_404(SemanticTable, id=table_id)
            logger.info(f"Found semantic table: {table.display_name}")
            
            columns = SemanticColumn.objects.filter(semantic_table=table)
            logger.info(f"Found {columns.count()} columns for table {table.display_name}")
            
            columns_data = []
            
            for column in columns:
                try:
                    # Helper function to safely parse JSON or return list/empty list
                    def safe_json_parse(value, default=None):
                        if default is None:
                            default = []
                        
                        if value is None:
                            return default
                        elif isinstance(value, list):
                            return value
                        elif isinstance(value, str):
                            try:
                                return json.loads(value)
                            except (json.JSONDecodeError, ValueError):
                                return default
                        else:
                            return default
                    
                    # Safely get field values with defaults
                    sample_values = safe_json_parse(getattr(column, 'sample_values', None), [])
                    common_filters = safe_json_parse(getattr(column, 'common_filters', None), [])
                    business_rules = safe_json_parse(getattr(column, 'business_rules', None), [])
                    
                    column_data = {
                        'id': str(column.pk),
                        'name': column.name,
                        'column_name': column.name,  # Add alias for template compatibility
                        'display_name': column.display_name or column.name,
                        'description': column.description or 'No description available',
                        'data_type': column.data_type or 'string',
                        'semantic_type': column.semantic_type or 'dimension',
                        'is_nullable': getattr(column, 'is_nullable', True),
                        'sample_values': sample_values,
                        'common_filters': common_filters,
                        'business_rules': business_rules,
                        'business_purpose': column.description or 'No description available',  # Add business purpose for template
                        'aggregation_default': getattr(column, 'aggregation_default', None),
                        'is_editable': getattr(column, 'is_editable', True),
                        'etl_enriched': getattr(column, 'etl_enriched', False),
                        'business_glossary_term': getattr(column, 'business_glossary_term', '')
                    }
                    
                    columns_data.append(column_data)
                    
                except Exception as col_error:
                    logger.error(f"Error processing column {column.name}: {col_error}")
                    # Add a basic column entry even if there's an error
                    columns_data.append({
                        'id': str(column.pk),
                        'name': column.name,
                        'column_name': column.name,
                        'display_name': column.name,
                        'description': 'Error loading column details',
                        'data_type': 'string',
                        'semantic_type': 'dimension',
                        'is_nullable': True,
                        'sample_values': [],
                        'common_filters': [],
                        'business_rules': [],
                        'business_purpose': 'Error loading column details',
                        'aggregation_default': None,
                        'is_editable': True,
                        'etl_enriched': False,
                        'business_glossary_term': ''
                    })
                    continue
            
            logger.info(f"Successfully processed {len(columns_data)} columns")
            
            return JsonResponse({
                'success': True,
                'table_name': table.display_name,
                'columns': columns_data
            })
            
        except Exception as e:
            logger.error(f"Error getting semantic table columns {table_id}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return JsonResponse({
                'success': False,
                'error': str(e)
            }, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def update_semantic_table_api(request, table_id):
    """API endpoint to update semantic table"""
    if request.method == 'POST':
        try:
            from datasets.models import SemanticTable
            table = get_object_or_404(SemanticTable, id=table_id)
            
            data = json.loads(request.body)
            
            # Update table fields
            if 'display_name' in data:
                table.display_name = data['display_name']
            if 'description' in data:
                table.description = data['description']
            if 'business_purpose' in data:
                table.business_purpose = data['business_purpose']
            if 'is_fact_table' in data:
                table.is_fact_table = data['is_fact_table']
            if 'is_dimension_table' in data:
                table.is_dimension_table = data['is_dimension_table']
            
            table.save()
            
            return JsonResponse({
                'success': True,
                'message': 'Table updated successfully'
            })
            
        except Exception as e:
            logger.error(f"Error updating semantic table {table_id}: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def delete_semantic_table_api(request, table_id):
    """API endpoint to delete semantic table"""
    if request.method == 'DELETE':
        try:
            from datasets.models import SemanticTable
            table = get_object_or_404(SemanticTable, id=table_id)
            
            table_name = table.display_name
            table.delete()
            
            return JsonResponse({
                'success': True,
                'message': f'Table "{table_name}" deleted successfully'
            })
            
        except Exception as e:
            logger.error(f"Error deleting semantic table {table_id}: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def get_semantic_metric_api(request, metric_id):
    """API endpoint to get semantic metric details"""
    if request.method == 'GET':
        try:
            from datasets.models import SemanticMetric
            metric = get_object_or_404(SemanticMetric, id=metric_id)
            
            metric_data = {
                'id': str(metric.pk),
                'name': metric.name,
                'display_name': metric.display_name,
                'description': metric.description,
                'metric_type': metric.metric_type,
                'calculation': metric.calculation,
                'unit': metric.unit,
                'is_active': metric.is_active
            }
            
            return JsonResponse({
                'success': True,
                'metric': metric_data
            })
            
        except Exception as e:
            logger.error(f"Error getting semantic metric {metric_id}: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def update_semantic_metric_api(request, metric_id):
    """API endpoint to update semantic metric"""
    if request.method == 'POST':
        try:
            from datasets.models import SemanticMetric
            metric = get_object_or_404(SemanticMetric, id=metric_id)
            
            data = json.loads(request.body)
            
            # Update metric fields
            if 'name' in data:
                metric.name = data['name']
            if 'display_name' in data:
                metric.display_name = data['display_name']
            if 'description' in data:
                metric.description = data['description']
            if 'metric_type' in data:
                metric.metric_type = data['metric_type']
            if 'calculation' in data:
                metric.calculation = data['calculation']
            if 'unit' in data:
                metric.unit = data['unit']
            
            metric.save()
            
            return JsonResponse({
                'success': True,
                'message': 'Metric updated successfully'
            })
            
        except Exception as e:
            logger.error(f"Error updating semantic metric {metric_id}: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def delete_semantic_metric_api(request, metric_id):
    """API endpoint to delete semantic metric"""
    if request.method == 'DELETE':
        try:
            from datasets.models import SemanticMetric
            metric = get_object_or_404(SemanticMetric, id=metric_id)
            
            metric_name = metric.display_name
            metric.delete()
            
            return JsonResponse({
                'success': True,
                'message': f'Metric "{metric_name}" deleted successfully'
            })
            
        except Exception as e:
            logger.error(f"Error deleting semantic metric {metric_id}: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def cleanup_duplicate_sources(request):
    """Simple endpoint to clean up duplicate data sources"""
    if request.method == 'GET':
        try:
            # Get all data sources grouped by name
            data_sources = DataSource.objects.all().order_by('name', '-created_at')
            names = {}
            
            for ds in data_sources:
                if ds.name in names:
                    names[ds.name].append(ds)
                else:
                    names[ds.name] = [ds]
            
            removed_count = 0
            kept_sources = []
            removed_sources = []
            
            for name, sources in names.items():
                if len(sources) > 1:
                    # Keep the most recent one
                    keeper = sources[0]
                    duplicates = sources[1:]
                    
                    kept_sources.append({
                        'name': keeper.name,
                        'id': str(keeper.id),
                        'created_at': keeper.created_at.strftime('%Y-%m-%d %H:%M')
                    })
                    
                    for dup in duplicates:
                        removed_sources.append({
                            'name': dup.name,
                            'id': str(dup.id),
                            'created_at': dup.created_at.strftime('%Y-%m-%d %H:%M')
                        })
                        dup.delete()
                        removed_count += 1
                else:
                    # No duplicates, just add to kept list
                    kept_sources.append({
                        'name': sources[0].name,
                        'id': str(sources[0].id),
                        'created_at': sources[0].created_at.strftime('%Y-%m-%d %H:%M')
                    })
            
            remaining = DataSource.objects.all()
            
            return JsonResponse({
                'success': True,
                'message': f'Cleanup complete! Removed {removed_count} duplicate sources.',
                'removed_count': removed_count,
                'remaining_count': len(remaining),
                'kept_sources': kept_sources,
                'removed_sources': removed_sources
            })
            
        except Exception as e:
            logger.error(f"Error cleaning up duplicates: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def get_semantic_column_api(request, column_id):
    """API endpoint to get semantic column details"""
    if request.method == 'GET':
        try:
            from datasets.models import SemanticColumn
            column = get_object_or_404(SemanticColumn, id=column_id)
            
            # Helper function to safely parse JSON or return list/empty list
            def safe_json_parse(value, default=None):
                if default is None:
                    default = []
                
                if value is None:
                    return default
                elif isinstance(value, list):
                    return value
                elif isinstance(value, str):
                    try:
                        return json.loads(value)
                    except (json.JSONDecodeError, ValueError):
                        return default
                else:
                    return default
            
            column_data = {
                'id': str(column.pk),
                'name': column.name,
                'display_name': column.display_name,
                'description': column.description,
                'data_type': column.data_type,
                'semantic_type': column.semantic_type,
                'is_nullable': column.is_nullable,
                'sample_values': safe_json_parse(column.sample_values, []),
                'common_filters': safe_json_parse(column.common_filters, []),
                'business_rules': safe_json_parse(column.business_rules, []),
                'aggregation_default': column.aggregation_default,
                'is_editable': getattr(column, 'is_editable', True),
                'etl_enriched': getattr(column, 'etl_enriched', False)
            }
            
            return JsonResponse({
                'success': True,
                'column': column_data
            })
            
        except Exception as e:
            logger.error(f"Error getting semantic column {column_id}: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def update_semantic_column_api(request, column_id):
    """API endpoint to update semantic column"""
    if request.method == 'POST':
        try:
            from datasets.models import SemanticColumn
            column = get_object_or_404(SemanticColumn, id=column_id)
            
            data = json.loads(request.body)
            
            # Update column fields
            if 'display_name' in data:
                column.display_name = data['display_name']
            if 'description' in data:
                column.description = data['description']
            if 'semantic_type' in data:
                column.semantic_type = data['semantic_type']
            if 'data_type' in data:
                column.data_type = data['data_type']
            if 'aggregation_default' in data:
                column.aggregation_default = data['aggregation_default']
            if 'business_rules' in data:
                # Ensure it's stored as JSON
                if isinstance(data['business_rules'], list):
                    column.business_rules = data['business_rules']
                else:
                    column.business_rules = [data['business_rules']] if data['business_rules'] else []
            if 'common_filters' in data:
                # Ensure it's stored as JSON
                if isinstance(data['common_filters'], list):
                    column.common_filters = data['common_filters']
                else:
                    column.common_filters = [data['common_filters']] if data['common_filters'] else []
            
            column.save()
            
            return JsonResponse({
                'success': True,
                'message': 'Column updated successfully'
            })
            
        except Exception as e:
            logger.error(f"Error updating semantic column {column_id}: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def delete_semantic_column_api(request, column_id):
    """API endpoint to delete semantic column"""
    if request.method == 'DELETE':
        try:
            from datasets.models import SemanticColumn
            column = get_object_or_404(SemanticColumn, id=column_id)
            
            column_name = column.display_name
            column.delete()
            
            return JsonResponse({
                'success': True,
                'message': f'Column "{column_name}" deleted successfully'
            })
            
        except Exception as e:
            logger.error(f"Error deleting semantic column {column_id}: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def delete_all_semantic_data_api(request):
    """API endpoint to delete all semantic data"""
    if request.method == 'DELETE':
        try:
            from datasets.models import SemanticTable, SemanticColumn, SemanticMetric
            
            # Count before deletion for reporting
            table_count = SemanticTable.objects.count()
            column_count = SemanticColumn.objects.count()
            metric_count = SemanticMetric.objects.count()
            
            # Delete all semantic data
            SemanticMetric.objects.all().delete()
            SemanticColumn.objects.all().delete()
            SemanticTable.objects.all().delete()
            
            logger.info(f"User {request.user.username} deleted all semantic data: {table_count} tables, {column_count} columns, {metric_count} metrics")
            
            return JsonResponse({
                'success': True,
                'message': f'Successfully deleted all semantic data: {table_count} tables, {column_count} columns, {metric_count} metrics'
            })
            
        except Exception as e:
            logger.error(f"Error deleting all semantic data: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def delete_all_business_metrics_api(request):
    """API endpoint to delete all business metrics"""
    if request.method == 'DELETE':
        try:
            from datasets.models import SemanticMetric
            
            # Delete all business metrics
            metric_count = SemanticMetric.objects.count()
            SemanticMetric.objects.all().delete()
            
            logger.info(f"User {request.user.username} deleted all business metrics: {metric_count} metrics")
            
            return JsonResponse({
                'success': True,
                'message': f'Successfully deleted {metric_count} business metrics'
            })
            
        except Exception as e:
            logger.error(f"Error deleting all business metrics: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def debug_semantic_layer_api(request):
    """Debug endpoint to check semantic layer status"""
    if request.method == 'GET':
        try:
            from datasets.models import SemanticTable, SemanticColumn, SemanticMetric
            
            debug_info = {
                'database_connected': True,
                'semantic_tables_count': SemanticTable.objects.count(),
                'semantic_columns_count': SemanticColumn.objects.count(),
                'semantic_metrics_count': SemanticMetric.objects.count(),
                'database_queries': len(db_connection.queries),
                'user_authenticated': request.user.is_authenticated,
                'user_id': request.user.id if request.user.is_authenticated else None
            }
            
            # Try to get a sample table and columns
            sample_table = SemanticTable.objects.first()
            if sample_table:
                debug_info['sample_table'] = {
                    'id': str(sample_table.id),
                    'name': sample_table.name,
                    'display_name': sample_table.display_name,
                    'columns_count': SemanticColumn.objects.filter(semantic_table=sample_table).count()
                }
                
                # Try to get sample columns
                sample_columns = SemanticColumn.objects.filter(semantic_table=sample_table)[:3]
                debug_info['sample_columns'] = []
                for col in sample_columns:
                    try:
                        col_data = {
                            'id': str(col.id),
                            'name': col.name,
                            'display_name': col.display_name,
                            'has_sample_values': bool(col.sample_values),
                            'sample_values_type': type(col.sample_values).__name__
                        }
                        debug_info['sample_columns'].append(col_data)
                    except Exception as col_error:
                        debug_info['sample_columns'].append({
                            'error': str(col_error),
                            'id': str(col.id) if hasattr(col, 'id') else 'unknown'
                        })
            
            return JsonResponse({
                'success': True,
                'debug_info': debug_info
            })
            
        except Exception as e:
            logger.error(f"Error in debug endpoint: {e}")
            import traceback
            return JsonResponse({
                'success': False,
                'error': str(e),
                'traceback': traceback.format_exc()
            })
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@api_view(['POST'])
@csrf_exempt
def validate_etl_transformations(request):
    """
    Validate ETL transformations without applying them
    This allows users to test transformations before committing
    """
    try:
        # FIXED: Only read request.body once and store it
        try:
            request_body = request.body
            data = json.loads(request_body)
        except json.JSONDecodeError:
            return JsonResponse({'error': 'Invalid JSON in request body'}, status=400)
        except Exception as e:
            return JsonResponse({'error': f'Error reading request body: {str(e)}'}, status=400)
        
        data_source_id = data.get('data_source_id')
        transformations = data.get('transformations', {})
        
        if not data_source_id:
            return JsonResponse({'error': 'Data source ID is required'}, status=400)
        
        if not transformations:
            return JsonResponse({'error': 'At least one transformation is required'}, status=400)
        
        # Get data source
        try:
            data_source = DataSource.objects.get(id=data_source_id)
        except DataSource.DoesNotExist:
            return JsonResponse({'error': 'Data source not found'}, status=404)
        
        # Load the data for validation (small sample)
        data_service = DataService()
        df = data_service.get_data_source_preview(data_source, limit=1000)  # Test on sample
        
        if df is None or df.empty:
            return JsonResponse({'error': 'No data available for validation'}, status=400)
        
        validation_results = []
        warnings = []
        
        # Validate each transformation
        for column_name, target_type in transformations.items():
            if column_name not in df.columns:
                validation_results.append({
                    'column': column_name,
                    'target_type': target_type,
                    'valid': False,
                    'error': f'Column "{column_name}" not found in data source'
                })
                continue
            
            # Test the transformation on a sample
            test_series = df[column_name].copy()
            result = {
                'column': column_name,
                'target_type': target_type,
                'original_type': str(test_series.dtype),
                'sample_size': min(len(test_series), 1000),
                'null_count': test_series.isnull().sum()
            }
            
            try:
                # Perform validation logic similar to actual transformation
                if target_type == 'date':
                    # Test date conversion - ENHANCED for DD-MM-YYYY format
                    converted_count = 0
                    failed_samples = []
                    
                    # Get non-null values for testing
                    non_null_series = test_series.dropna()
                    
                    for idx, val in enumerate(non_null_series.head(100)):
                        if pd.isna(val):
                            continue
                        
                        val_str = str(val).strip()
                        success = False
                        
                        # FIXED: Try DD-MM-YYYY format first (most likely for your data)
                        date_formats = ['%d-%m-%Y', '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d', '%m-%d-%Y']
                        for fmt in date_formats:
                            try:
                                pd.to_datetime(val_str, format=fmt, errors='raise')
                                success = True
                                break
                            except:
                                continue
                        
                        if not success:
                            try:
                                pd.to_datetime(val_str, errors='raise')
                                success = True
                            except:
                                pass
                        
                        if success:
                            converted_count += 1
                        else:
                            if len(failed_samples) < 5:
                                failed_samples.append(str(val))
                    
                    total_non_null = len(non_null_series)
                    success_rate = converted_count / max(total_non_null, 1) * 100
                    
                    # FIXED: Use more lenient threshold (50% instead of 80%)
                    result.update({
                        'valid': success_rate >= 50,
                        'success_rate': success_rate,
                        'converted_count': converted_count,
                        'failed_samples': failed_samples,
                        'non_null_tested': total_non_null
                    })
                    
                    if success_rate < 50:
                        result['error'] = f'Only {success_rate:.1f}% of non-null values can be converted to dates'
                    
                elif target_type in ['integer', 'float']:
                    # Test numeric conversion
                    converted_count = 0
                    failed_samples = []
                    
                    for val in test_series.dropna().head(100):
                        try:
                            if target_type == 'integer':
                                int(float(str(val)))
                            else:
                                float(str(val))
                            converted_count += 1
                        except:
                            if len(failed_samples) < 5:
                                failed_samples.append(str(val))
                    
                    total_non_null = len(test_series.dropna())
                    success_rate = converted_count / max(total_non_null, 1) * 100
                    
                    # FIXED: More lenient threshold for numeric (70% instead of 80%)
                    result.update({
                        'valid': success_rate >= 70,
                        'success_rate': success_rate,
                        'converted_count': converted_count,
                        'failed_samples': failed_samples
                    })
                    
                    if success_rate < 70:
                        result['error'] = f'Only {success_rate:.1f}% of values can be converted to {target_type}'
                
                else:
                    # String and other types are generally safe
                    result.update({
                        'valid': True,
                        'success_rate': 100,
                        'converted_count': len(test_series.dropna())
                    })
                
            except Exception as e:
                result.update({
                    'valid': False,
                    'error': str(e),
                    'success_rate': 0
                })
            
            validation_results.append(result)
            
            # Add warnings for low success rates
            if result.get('success_rate', 0) < 90 and result.get('valid', False):
                warnings.append(f"Column '{column_name}': {result['success_rate']:.1f}% success rate (some data may be lost)")
        
        # Summary
        valid_transformations = [r for r in validation_results if r.get('valid', False)]
        invalid_transformations = [r for r in validation_results if not r.get('valid', False)]
        
        return JsonResponse({
            'success': True,
            'validation_results': validation_results,
            'summary': {
                'total_transformations': len(transformations),
                'valid_transformations': len(valid_transformations),
                'invalid_transformations': len(invalid_transformations),
                'warnings': warnings
            },
            'can_proceed': len(invalid_transformations) == 0
        })
        
    except Exception as e:
        logger.error(f"Error validating transformations: {e}")
        return JsonResponse({'error': f'Validation failed: {str(e)}'}, status=500)


@login_required
def force_proceed_etl(request):
    """Allow user to proceed with ETL despite validation warnings"""
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data_source_id = data.get('data_source_id')
            force_proceed = data.get('force_proceed', False)
            transformation_results = data.get('transformation_results', [])
            
            if not data_source_id or not force_proceed:
                return JsonResponse({'error': 'Invalid request parameters'}, status=400)
            
            # Get the data source
            data_source = get_object_or_404(DataSource, id=data_source_id, created_by=request.user)
            
            logger.info(f"User {request.user.username} forcing ETL proceed for {data_source.name}")
            
            # Update workflow status to mark ETL as completed despite warnings
            workflow_status = data_source.workflow_status or WorkflowManager.get_default_status()
            workflow_status = WorkflowManager.update_workflow_step(
                workflow_status, 
                WorkflowStep.ETL_COMPLETED, 
                True
            )
            workflow_status['forced_proceed'] = True
            workflow_status['forced_proceed_timestamp'] = timezone.now().isoformat()
            workflow_status['forced_proceed_reason'] = 'High null rates accepted by user'
            
            data_source.workflow_status = workflow_status
            data_source.save()
            
            # Create ETL operation record for the forced proceed
            try:
                etl_operation = ETLOperation.objects.create(
                    name=f"Forced ETL Completion - {data_source.name}",
                    operation_type='transform',
                    source_tables=[str(data_source.id)],
                    parameters={
                        'forced_proceed': True,
                        'validation_warnings_accepted': True,
                        'data_source_id': str(data_source.id)
                    },
                    output_table_name=f"{data_source.name}_forced_complete",
                    status='completed_with_warnings',
                    created_by=request.user,
                    row_count=transformation_results[0].get('row_count', 0) if transformation_results else 0,
                    result_summary={
                        'transformation_results': transformation_results,
                        'forced_proceed': True,
                        'validation_warnings': 'High null rates in data',
                        'success': True
                    },
                    data_lineage={
                        'source_data_source': str(data_source.id),
                        'transformation_type': 'forced_completion',
                        'timestamp': timezone.now().isoformat(),
                        'user_decision': 'Proceeded despite validation warnings'
                    }
                )
                
                logger.info(f"Created forced proceed ETL operation: {etl_operation.id}")
                
            except Exception as etl_error:
                logger.warning(f"Failed to create ETL operation record: {etl_error}")
                # Don't fail the whole operation for this
            
            return JsonResponse({
                'success': True,
                'message': 'ETL marked as completed despite warnings. You can now proceed to semantic layer.',
                'workflow_updated': True,
                'forced_proceed': True,
                'next_step': 'semantic_layer'
            })
            
        except Exception as e:
            logger.error(f"Error in force proceed ETL: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def execute_etl_join(request):
    """Execute ETL join operation between two data sources with comprehensive validation"""
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)
    
    try:
        data = json.loads(request.body)
        
        left_source_id = data.get('left_source_id')
        right_source_id = data.get('right_source_id')
        left_column = data.get('left_column')
        right_column = data.get('right_column')
        join_type = data.get('join_type', 'INNER')
        operation_name = data.get('operation_name', f'Join_{timezone.now().strftime("%Y%m%d_%H%M%S")}')
        
        if not all([left_source_id, right_source_id, left_column, right_column]):
            return JsonResponse({
                'error': 'Missing required parameters: left_source_id, right_source_id, left_column, right_column'
            }, status=400)
        
        # Get data sources
        try:
            left_source = DataSource.objects.get(id=left_source_id, status='active')
            right_source = DataSource.objects.get(id=right_source_id, status='active')
        except DataSource.DoesNotExist:
            return JsonResponse({'error': 'One or more data sources not found'}, status=404)
        
        logger.info(f"Starting improved join operation: {left_source.name} + {right_source.name}")
        
        # Get DuckDB connection
        try:
            import duckdb
            from datasets.data_access_layer import unified_data_access
            from services.schema_aware_etl_join_service import schema_aware_etl_join_service
            
            # Ensure DuckDB connection
            conn = unified_data_access.duckdb_connection
            if not conn:
                unified_data_access._ensure_duckdb_connection()
                conn = unified_data_access.duckdb_connection
            
            if not conn:
                raise Exception("Could not connect to DuckDB - unified data access connection failed")
            
            # Set the connection for the schema-aware service
            schema_aware_etl_join_service.set_connection(conn)
            
            # Execute join with schema-aware validation
            join_result = schema_aware_etl_join_service.execute_join_with_schema_validation(
                left_source_id=str(left_source_id),
                right_source_id=str(right_source_id),
                left_column=left_column,
                right_column=right_column,
                join_type=join_type,
                operation_name=operation_name,
                user=request.user
            )
            
            if join_result.success:
                # Successful join
                logger.info(f"[SUCCESS] Schema-aware join operation completed successfully: {join_result.etl_operation_id}")
                
                return JsonResponse({
                    'success': True,
                    'operation_id': join_result.etl_operation_id,
                    'operation_name': operation_name,
                    'output_table': join_result.execution_details.get('output_table'),
                    'message': f'Join operation "{operation_name}" executed successfully with {join_result.execution_details.get("row_count", 0)} rows',
                    'details': {
                        'left_source': left_source.name,
                        'right_source': right_source.name,
                        'join_type': join_type,
                        'left_column': left_column,
                        'right_column': right_column,
                        'row_count': join_result.execution_details.get('row_count', 0),
                        'execution_time': join_result.execution_details.get('execution_time', 0),
                        'validated_tables': {
                            'left_table': join_result.left_table_result.qualified_table_name if join_result.left_table_result else None,
                            'right_table': join_result.right_table_result.qualified_table_name if join_result.right_table_result else None,
                            'left_schema': join_result.left_table_result.schema_name if join_result.left_table_result else None,
                            'right_schema': join_result.right_table_result.schema_name if join_result.right_table_result else None
                        }
                    },
                    'validation_info': {
                        'pre_validation_performed': True,
                        'tables_validated': True,
                        'columns_validated': True
                    }
                })
            else:
                # Join failed - return detailed error information
                logger.error(f"ERROR: Join operation failed: {join_result.error_message}")
                
                response_data = {
                    'success': False,
                    'error': join_result.error_message,
                    'error_type': 'join_validation_failure',
                    'operation_id': join_result.etl_operation_id,
                    'recommendations': join_result.recommendations,
                    'guidance': [
                        'Join operation failed during validation or execution',
                        'Please review the recommendations below',
                        'Check that both data sources are properly configured'
                    ]
                }
                
                # Add validation details if available
                if join_result.validation_details:
                    validation_info = {
                        'left_table_status': {
                            'valid': join_result.validation_details.left_table_result.is_valid,
                            'table_name': join_result.validation_details.left_table_result.table_name,
                            'error': join_result.validation_details.left_table_result.error_message,
                            'row_count': join_result.validation_details.left_table_result.row_count,
                            'alternatives': join_result.validation_details.left_table_result.alternative_names[:5]
                        },
                        'right_table_status': {
                            'valid': join_result.validation_details.right_table_result.is_valid,
                            'table_name': join_result.validation_details.right_table_result.table_name,
                            'error': join_result.validation_details.right_table_result.error_message,
                            'row_count': join_result.validation_details.right_table_result.row_count,
                            'alternatives': join_result.validation_details.right_table_result.alternative_names[:5]
                        },
                        'column_validation': join_result.validation_details.column_validation
                    }
                    response_data['validation_details'] = validation_info
                
                # Add root cause analysis if available
                if join_result.root_cause_analysis:
                    response_data['root_cause_analysis'] = join_result.root_cause_analysis
                
                return JsonResponse(response_data, status=400)
            
        except Exception as conn_error:
            logger.error(f"Error setting up connection for improved join: {conn_error}")
            return JsonResponse({
                'error': f'Failed to initialize join service: {str(conn_error)}',
                'error_type': 'connection_error'
            }, status=500)
        
    except Exception as e:
        logger.error(f"Error executing ETL join operation: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return JsonResponse({
            'error': f'Failed to execute join operation: {str(e)}',
            'error_type': 'general_error'
        }, status=500)


@login_required
def execute_etl_union(request):
    """Execute ETL union operation between multiple data sources"""
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)
    
    try:
        data = json.loads(request.body)
        
        source_ids = data.get('source_ids', [])
        union_type = data.get('union_type', 'UNION ALL')
        operation_name = data.get('operation_name', f'Union_{timezone.now().strftime("%Y-%m-%dT%H%M%S")}')
        
        logger.info(f"Executing union operation: {operation_name} with sources: {source_ids}")
        
        # Validate inputs
        if len(source_ids) < 2:
            return JsonResponse({
                'error': 'Union operation requires at least 2 data sources'
            }, status=400)
        
        # Execute union using the new synchronous service
        from services.etl_union_service import etl_union_service
        
        success, result = etl_union_service.execute_union_operation(
            source_ids=source_ids,
            operation_name=operation_name,
            union_type=union_type.upper(),
            user_id=request.user.id
        )
        
        if success:
            logger.info(f"Union operation completed successfully: {result['operation_id']}")
            return JsonResponse({
                'success': True,
                'operation_id': result['operation_id'],
                'operation_name': result['operation_name'],
                'output_table': result['output_table'],
                'message': f'Union operation "{operation_name}" executed successfully',
                'details': {
                    'sources': result['sources'],
                    'union_type': result['union_type'],
                    'source_count': len(result['sources']),
                    'row_count': result['row_count'],
                    'column_count': result['column_count'],
                    'schema_alignment': result.get('schema_alignment', {})
                }
            })
        else:
            error_msg = result.get('error', 'Unknown error occurred')
            logger.error(f"Union operation failed: {error_msg}")
            return JsonResponse({
                'error': f'Failed to execute union operation: {error_msg}'
            }, status=500)
        
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON data'}, status=400)
    except Exception as e:
        logger.error(f"Error executing ETL union operation: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return JsonResponse({
            'error': f'Failed to execute union operation: {str(e)}'
        }, status=500)


@login_required
def execute_etl_aggregate(request):
    """Execute ETL aggregation operation on a data source"""
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)
    
    try:
        data = json.loads(request.body)
        
        source_id = data.get('source_id')
        group_by_columns = data.get('group_by_columns', [])
        aggregate_functions = data.get('aggregate_functions', {})
        operation_name = data.get('operation_name', f'Aggregate_{timezone.now().strftime("%Y%m%d_%H%M%S")}')
        
        if not source_id:
            return JsonResponse({'error': 'Missing required parameter: source_id'}, status=400)
        
        if not group_by_columns and not aggregate_functions:
            return JsonResponse({
                'error': 'Either group_by_columns or aggregate_functions must be specified'
            }, status=400)
        
        # Get data source
        try:
            source = DataSource.objects.get(id=source_id, status='active')
        except DataSource.DoesNotExist:
            return JsonResponse({'error': 'Data source not found'}, status=404)
        
        # Create ETL operation using integration service
        from services.integration_service import DataIntegrationService
        integration_service = DataIntegrationService()
        
        source_table = f"source_{source.id.hex.replace('-', '_')}"
        
        etl_params = {
            'group_by_columns': group_by_columns,
            'aggregate_functions': aggregate_functions
        }
        
        # Create ETL operation
        etl_operation_id = integration_service.create_etl_operation(
            name=operation_name,
            operation_type='aggregate',
            source_tables=[source_table],
            parameters=etl_params,
            user_id=request.user.id
        )
        
        # Get the created operation details
        etl_operation = ETLOperation.objects.get(id=etl_operation_id)
        
        return JsonResponse({
            'success': True,
            'operation_id': str(etl_operation_id),
            'operation_name': operation_name,
            'output_table': etl_operation.output_table_name,
            'message': f'Aggregation operation "{operation_name}" executed successfully',
            'details': {
                'source': source.name,
                'group_by_columns': group_by_columns,
                'aggregate_functions': aggregate_functions
            }
        })
        
    except Exception as e:
        logger.error(f"Error executing ETL aggregation operation: {e}")
        return JsonResponse({
            'error': f'Failed to execute aggregation operation: {str(e)}'
        }, status=500)


@login_required
def get_etl_results(request, operation_id):
    """Get the results of an ETL operation as intermediate table data"""
    try:
        # Get the ETL operation
        try:
            etl_operation = ETLOperation.objects.get(id=operation_id, created_by=request.user)
        except ETLOperation.DoesNotExist:
            return JsonResponse({'error': 'ETL operation not found'}, status=404)
        
        logger.info(f"Loading ETL results for operation: {etl_operation.name} (table: {etl_operation.output_table_name})")
        
        # Load result data directly from DuckDB
        try:
            import duckdb
            from datasets.data_access_layer import unified_data_access
            
            # Get DuckDB connection
            conn = unified_data_access.duckdb_connection
            
            if not conn:
                # Try to ensure connection
                unified_data_access._ensure_duckdb_connection()
                conn = unified_data_access.duckdb_connection
            
            if not conn:
                raise Exception("Could not connect to DuckDB")
            
            # Check if table exists
            table_exists = conn.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = '{etl_operation.output_table_name}'
            """).fetchone()[0]
            
            if not table_exists:
                return JsonResponse({
                    'success': True,
                    'operation_id': operation_id,
                    'operation_name': etl_operation.name,
                    'row_count': 0,
                    'columns': [],
                    'data': [],
                    'message': f'Output table {etl_operation.output_table_name} not found'
                })
            
            # Get table info and data
            row_count_result = conn.execute(f"SELECT COUNT(*) FROM {etl_operation.output_table_name}").fetchone()
            total_rows = row_count_result[0] if row_count_result else 0
            
            if total_rows == 0:
                return JsonResponse({
                    'success': True,
                    'operation_id': operation_id,
                    'operation_name': etl_operation.name,
                    'row_count': 0,
                    'columns': [],
                    'data': [],
                    'message': 'No data in result table'
                })
            
            # Get column information
            columns_result = conn.execute(f"DESCRIBE {etl_operation.output_table_name}").fetchall()
            columns = [col[0] for col in columns_result]  # Column names
            column_types = [col[1] for col in columns_result]  # Column types
            
            # Get sample data (first 100 rows)
            sample_data_result = conn.execute(f"SELECT * FROM {etl_operation.output_table_name} LIMIT 100").fetchall()
            
            # Convert to JSON-serializable format
            data = []
            for row in sample_data_result:
                row_data = {}
                for i, col in enumerate(columns):
                    value = row[i]
                    # Handle different data types
                    if value is None:
                        row_data[col] = None
                    elif isinstance(value, (int, float, str, bool)):
                        row_data[col] = value
                    else:
                        row_data[col] = str(value)
                data.append(row_data)
            
            # Build column information
            column_info = []
            for i, col in enumerate(columns):
                # Get sample values for this column
                sample_values_result = conn.execute(f"""
                    SELECT DISTINCT "{col}" FROM {etl_operation.output_table_name} 
                    WHERE "{col}" IS NOT NULL 
                    LIMIT 3
                """).fetchall()
                sample_values = [str(row[0]) for row in sample_values_result]
                
                # Get non-null count
                non_null_result = conn.execute(f"""
                    SELECT COUNT("{col}") FROM {etl_operation.output_table_name} 
                    WHERE "{col}" IS NOT NULL
                """).fetchone()
                non_null_count = non_null_result[0] if non_null_result else 0
                
                col_info = {
                    'name': col,
                    'type': column_types[i],
                    'non_null_count': non_null_count,
                    'sample_values': sample_values
                }
                column_info.append(col_info)
            
            logger.info(f"Successfully loaded ETL results: {total_rows} rows, {len(columns)} columns")
            
            return JsonResponse({
                'success': True,
                'operation_id': operation_id,
                'operation_name': etl_operation.name,
                'operation_type': etl_operation.operation_type,
                'status': etl_operation.status,
                'output_table_name': etl_operation.output_table_name,
                'row_count': total_rows,
                'displayed_rows': len(data),
                'columns': column_info,
                'data': data,
                'created_at': etl_operation.created_at.isoformat(),
                'summary': etl_operation.result_summary or {}
            })
            
        except Exception as data_error:
            logger.error(f"Error loading ETL result data: {data_error}")
            return JsonResponse({
                'error': f'Failed to load result data: {str(data_error)}'
            }, status=500)
            
    except Exception as e:
        logger.error(f"Error getting ETL results: {e}")
        return JsonResponse({
            'error': f'Failed to get ETL results: {str(e)}'
        }, status=500)

@login_required
def create_data_source_from_etl_result(request):
    """Create a new data source from ETL operation result table"""
    if request.method != 'POST':
        return JsonResponse({'error': 'Invalid request method'}, status=405)
    
    try:
        data = json.loads(request.body)
        operation_id = data.get('operation_id')
        
        if not operation_id:
            return JsonResponse({'error': 'Operation ID required'}, status=400)
        
        # Get the ETL operation
        try:
            etl_operation = ETLOperation.objects.get(id=operation_id, created_by=request.user)
        except ETLOperation.DoesNotExist:
            return JsonResponse({'error': 'ETL operation not found'}, status=404)
        
        if etl_operation.status != 'completed':
            return JsonResponse({'error': 'ETL operation not completed successfully'}, status=400)
        
        if not etl_operation.output_table_name:
            return JsonResponse({'error': 'ETL operation has no output table'}, status=400)
        
        logger.info(f"Creating data source from ETL result: {etl_operation.output_table_name}")
        
        # Check if table exists in database
        from datasets.data_access_layer import unified_data_access
        unified_data_access._ensure_duckdb_connection()
        conn = unified_data_access.duckdb_connection
        
        if not conn:
            return JsonResponse({'error': 'Database connection not available'}, status=500)
        
        # Validate that the output table exists
        try:
            # Try to describe the table to check if it exists
            columns_result = conn.execute(f"DESCRIBE {etl_operation.output_table_name}").fetchall()
            if not columns_result:
                return JsonResponse({'error': f'Output table {etl_operation.output_table_name} is empty'}, status=400)
        except Exception as e:
            logger.error(f"Error checking output table: {e}")
            return JsonResponse({'error': f'Output table {etl_operation.output_table_name} does not exist'}, status=400)
        
        # Get table metadata
        try:
            row_count_result = conn.execute(f"SELECT COUNT(*) FROM {etl_operation.output_table_name}").fetchone()
            row_count = row_count_result[0] if row_count_result else 0
            
            # Get sample data
            sample_data_result = conn.execute(f"SELECT * FROM {etl_operation.output_table_name} LIMIT 5").fetchall()
            column_names = [desc[0] for desc in columns_result]
            
            sample_data = []
            for row in sample_data_result:
                sample_data.append(dict(zip(column_names, row)))
            
            # Build schema info
            schema_info = {
                'columns': []
            }
            
            for col_desc in columns_result:
                col_name = col_desc[0]
                col_type = col_desc[1]
                schema_info['columns'].append({
                    'name': col_name,
                    'type': col_type,
                    'nullable': True  # Default assumption
                })
            
        except Exception as e:
            logger.error(f"Error getting table metadata: {e}")
            return JsonResponse({'error': f'Error reading table metadata: {str(e)}'}, status=500)
        
        # Generate unique name for the new data source
        new_name = f"{etl_operation.name} - Result"
        counter = 1
        original_name = new_name
        
        # Ensure unique name
        while DataSource.objects.filter(name=new_name, created_by=request.user).exists():
            counter += 1
            new_name = f"{original_name} ({counter})"
        
        # Create new data source
        try:
            new_data_source = DataSource.objects.create(
                name=new_name,
                source_type='etl_result',  # Special type for ETL results
                connection_info={
                    'type': 'etl_result',
                    'source_etl_operation_id': str(etl_operation.id),
                    'source_table_name': etl_operation.output_table_name,
                    'schema_qualified': True,  # Indicates table is already in database
                    'created_from_etl': True
                },
                schema_info=schema_info,
                sample_data={
                    'data': sample_data,
                    'row_count': row_count,
                    'source': 'etl_result'
                },
                table_name=etl_operation.output_table_name,  # Use the existing table
                status='active',
                created_by=request.user,
                estimated_row_count=row_count,
                workflow_status={
                    'data_loaded': True,  # Already loaded from ETL
                    'etl_completed': False,  # Can still be further transformed
                    'semantics_completed': False,
                    'query_enabled': False,
                    'dashboard_enabled': False,
                    'current_step': 'etl_ready',
                    'last_updated': timezone.now().isoformat(),
                    'progress_percentage': 20,  # Data loaded
                    'created_from_etl_result': True,
                    'source_operation_id': str(etl_operation.id)
                },
                source_lineage={
                    'type': 'etl_result',
                    'source_operation': {
                        'id': str(etl_operation.id),
                        'name': etl_operation.name,
                        'type': etl_operation.operation_type,
                        'created_at': etl_operation.created_at.isoformat()
                    },
                    'parent_sources': etl_operation.source_tables,
                    'transformation_applied': etl_operation.operation_type,
                    'created_at': timezone.now().isoformat()
                }
            )
            
            logger.info(f"Created new data source from ETL result: {new_data_source.id} - {new_data_source.name}")
            
            return JsonResponse({
                'success': True,
                'data_source_id': str(new_data_source.id),
                'data_source_name': new_data_source.name,
                'message': f'Data source "{new_data_source.name}" created successfully from ETL results',
                'table_name': etl_operation.output_table_name,
                'row_count': row_count,
                'redirect_url': f'/datasets/integration/?source={new_data_source.id}&tab=transform',
                'source_lineage': new_data_source.source_lineage
            })
            
        except Exception as e:
            logger.error(f"Error creating data source from ETL result: {e}")
            return JsonResponse({'error': f'Error creating data source: {str(e)}'}, status=500)
        
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON data'}, status=400)
    except Exception as e:
        logger.error(f"Unexpected error in create_data_source_from_etl_result: {e}")
        return JsonResponse({'error': str(e)}, status=500)


@csrf_exempt
@login_required
def analyze_csv_structure(request):
    """Analyze CSV structure and provide parsing recommendations"""
    if request.method == 'POST':
        try:
            csv_file = request.FILES.get('csv_file')
            if not csv_file:
                return JsonResponse({'error': 'No CSV file provided'}, status=400)
            
            # Validate file size (limit to 100MB)
            max_size = 100 * 1024 * 1024  # 100MB
            if csv_file.size > max_size:
                return JsonResponse({
                    'error': f'File too large. Maximum size is {max_size//1024//1024}MB'
                }, status=400)
            
            # Save file temporarily for analysis
            file_path = default_storage.save(f'temp_csv/{csv_file.name}', ContentFile(csv_file.read()))
            full_file_path = os.path.join(settings.MEDIA_ROOT, file_path)
            
            # Initialize enhanced CSV processor
            from services.enhanced_csv_processor import EnhancedCSVProcessor
            processor = EnhancedCSVProcessor()
            
            # Analyze structure with enhanced error handling
            analysis = processor.detect_csv_structure(full_file_path)
            
            # Check if analysis contains an error
            if 'error' in analysis:
                logger.warning(f"CSV analysis completed with errors: {analysis['error']}")
                # Return partial success with error details
                return JsonResponse({
                    'success': True,
                    'analysis': analysis,
                    'warning': f"Analysis completed with some issues: {analysis['error']}"
                })
            
            # Clean up temporary file
            try:
                os.remove(full_file_path)
            except:
                pass
            
            return JsonResponse({
                'success': True,
                'analysis': analysis
            })
            
        except Exception as e:
            logger.error(f"Error analyzing CSV structure: {e}")
            
            # Provide more specific error messages
            error_message = str(e)
            if "tokenizing data" in error_message.lower():
                error_message = "The CSV file appears to have inconsistent formatting. This is common with real-world data files. Click 'Next Step' to continue - our system can handle these inconsistencies."
            elif "encoding" in error_message.lower():
                error_message = "The file encoding could not be detected automatically. Please try selecting a different encoding option."
            elif "permission" in error_message.lower():
                error_message = "Unable to access the uploaded file. Please try uploading again."
            
            return JsonResponse({
                'success': False,
                'error': error_message,
                'can_continue': "tokenizing data" in str(e).lower()  # Allow continuation for parsing errors
            }, status=400)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@csrf_exempt
@login_required
def preview_csv_parsing(request):
    """Preview how CSV will be parsed with given options"""
    if request.method == 'POST':
        try:
            csv_file = request.FILES.get('csv_file')
            if not csv_file:
                return JsonResponse({'error': 'No CSV file provided'}, status=400)
            
            # Get parsing options from request
            parsing_options = {
                'delimiter': request.POST.get('delimiter', ','),
                'has_header': request.POST.get('has_header', 'true').lower() == 'true',
                'encoding': request.POST.get('encoding', 'utf-8'),
                'split_columns': json.loads(request.POST.get('split_columns', '{}')),
                'parse_dates': json.loads(request.POST.get('parse_dates', '[]'))
            }
            
            # Save file temporarily for processing
            file_path = default_storage.save(f'temp_csv/{csv_file.name}', ContentFile(csv_file.read()))
            full_file_path = os.path.join(settings.MEDIA_ROOT, file_path)
            
            # Initialize enhanced CSV processor
            from services.enhanced_csv_processor import EnhancedCSVProcessor
            processor = EnhancedCSVProcessor()
            
            # Create preview
            preview = processor.create_parsing_preview(full_file_path, parsing_options)
            
            # Clean up temporary file
            try:
                os.remove(full_file_path)
            except:
                pass
            
            return JsonResponse(preview)
            
        except Exception as e:
            logger.error(f"Error creating CSV parsing preview: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@csrf_exempt
@login_required
def upload_csv_with_enhanced_options(request):
    """Upload CSV with enhanced parsing options"""
    if request.method == 'POST':
        try:
            # Get form data
            name = request.POST.get('name')
            description = request.POST.get('description', '')
            csv_file = request.FILES.get('csv_file')
            
            if not csv_file:
                return JsonResponse({'error': 'No file uploaded'}, status=400)
            
            if not name:
                return JsonResponse({'error': 'Data source name is required'}, status=400)
            
            # Get enhanced parsing options
            parsing_options = {
                'delimiter': request.POST.get('delimiter', ','),
                'has_header': request.POST.get('has_header', 'true').lower() == 'true',
                'encoding': request.POST.get('encoding', 'utf-8'),
                'split_columns': json.loads(request.POST.get('split_columns', '{}')),
                'parse_dates': json.loads(request.POST.get('parse_dates', '[]'))
            }
            
            # Check for existing data sources with the same name (prevent duplicates)
            from datasets.data_access_layer import unified_data_access
            
            duplicate_check = unified_data_access.check_for_duplicate_data_sources(name, request.user.id)
            if duplicate_check:
                return JsonResponse({
                    'success': False,
                    'error': 'Data source already exists',
                    'details': f'A data source with the name "{name}" already exists. Please choose a different name, or delete the existing data source first.',
                    'suggestion': 'Go to Data Sources list and delete the existing one, or choose a different name for this upload',
                    'action_required': 'rename_or_delete_existing'
                }, status=400)
            
            # Clear any potential DuckDB cache conflicts before upload
            logger.info(f"[CLEANUP] Clearing any potential DuckDB conflicts for new upload")
            unified_data_access.clear_duckdb_cache()
            
            # Save the file
            file_path = default_storage.save(f'csv_files/{csv_file.name}', ContentFile(csv_file.read()))
            full_file_path = os.path.join(settings.MEDIA_ROOT, file_path)
            
            # Process CSV with enhanced options
            from services.enhanced_csv_processor import EnhancedCSVProcessor
            processor = EnhancedCSVProcessor()
            
            success, df, message = processor.process_csv_with_options(full_file_path, parsing_options)
            
            if not success:
                return JsonResponse({'error': message}, status=400)
            
            logger.info(f"Successfully processed CSV with enhanced options: {len(df)} rows, {len(df.columns)} columns")
            
            # Validate that we have data
            if df.empty:
                return JsonResponse({'error': 'CSV file appears to be empty after processing'}, status=400)
            
            if len(df.columns) == 0:
                return JsonResponse({'error': 'No columns found in CSV file after processing'}, status=400)
            
            # Build schema info
            columns_data = {}
            columns_array = []
            
            for col in df.columns:
                col_data = df[col]
                sample_values = _safe_sample_values_standalone(col_data)
                
                col_info = {
                    'name': str(col),
                    'type': str(col_data.dtype),
                    'sample_values': sample_values,
                    'null_count': int(col_data.isnull().sum()),
                    'null_percentage': round((col_data.isnull().sum() / len(df)) * 100, 2),
                    'unique_count': int(col_data.nunique())
                }
                
                # Store in both formats
                columns_data[str(col)] = col_info
                columns_array.append(col_info)
            
            # Schema info with multiple format support
            schema_info = {
                'row_count': len(df),
                'column_count': len(df.columns),
                'columns': columns_array,
                'tables': {
                    'main_table': {
                        'columns': columns_data
                    }
                },
                'parsing_options': parsing_options  # Store the parsing options used
            }
            
            # Create data source with workflow status
            with transaction.atomic():
                # Initialize workflow status
                workflow_status = WorkflowManager.get_default_status()
                workflow_status = WorkflowManager.update_workflow_step(
                    workflow_status, 
                    WorkflowStep.DATA_LOADED, 
                    True
                )
                
                # Import safe serialization
                from .postgresql_data_service import PostgreSQLDataService
                pg_service = PostgreSQLDataService()
                
                logger.info(f"[CREATE] Creating new data source: {name}")
                logger.info(f"[INFO] Data shape: {len(df)} rows, {len(df.columns)} columns")
                logger.info(f"[INFO] Data columns: {list(df.columns)[:10]}...")
                
                data_source = DataSource.objects.create(
                    name=name,
                    created_by=request.user,
                    source_type='csv',
                    connection_info={
                        'type': 'csv',
                        'description': description,  # Store description in connection_info
                        'file_path': file_path,
                        'upload_timestamp': timezone.now().isoformat(),
                        'original_filename': csv_file.name,
                        'row_count': len(df),
                        'column_count': len(df.columns),
                        'parsing_options': parsing_options,  # Store parsing options
                        'enhanced_processing': True  # Flag to indicate enhanced processing was used
                    },
                    schema_info=schema_info,
                    sample_data=pg_service._safe_json_serialize(df.head(10)),
                    workflow_status=workflow_status,
                    status='active'
                )
                
                logger.info(f"[SUCCESS] Created data source with ID: {data_source.id}")
                
                # Store data in DuckDB with unique table name BEFORE integration
                logger.info(f"[STORE] Storing data in DuckDB for data source {data_source.id}")
                unified_data_access._store_in_duckdb(data_source, df)
                
                # Process with integration system using existing DataSource
                integration_service = DataIntegrationService()
                success = integration_service.process_existing_data_source(
                    data_source=data_source,
                    data=df
                )
                
                if not success:
                    # If integration fails, clean up the created data source
                    data_source.delete()
                    raise Exception("Failed to process data source with integration service")
            
            return JsonResponse({
                'success': True,
                'data_source_id': str(data_source.id),
                'message': f'CSV file uploaded successfully with enhanced processing. {len(df)} rows, {len(df.columns)} columns.',
                'redirect_url': f'/datasets/{data_source.id}/',
                'enhanced_processing': True,
                'parsing_options': parsing_options
            })
            
        except Exception as e:
            logger.error(f"Error uploading CSV file with enhanced options: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)

@login_required
def diagnose_business_metrics_api(request):
    """API endpoint to diagnose business metrics and table mapping issues"""
    if request.method == 'GET':
        try:
            from datasets.models import SemanticMetric, SemanticTable, DataSource
            from services.semantic_service import SemanticService
            from services.integration_service import DataIntegrationService
            from utils.table_name_helper import get_integrated_table_name
            import duckdb
            import os
            from django.conf import settings
            
            semantic_service = SemanticService()
            integration_service = DataIntegrationService()
            
            # Gather diagnostic information
            diagnostic_info = {
                'total_data_sources': DataSource.objects.count(),
                'total_semantic_tables': SemanticTable.objects.count(),
                'total_business_metrics': SemanticMetric.objects.count(),
                'metrics_with_base_table': SemanticMetric.objects.filter(base_table__isnull=False).count(),
                'metrics_without_base_table': SemanticMetric.objects.filter(base_table__isnull=True).count(),
                'data_sources': [],
                'duckdb_tables': [],
                'semantic_tables': [],
                'business_metrics': [],
                'table_mapping_issues': [],
                'recommendations': []
            }
            
            # Check DuckDB connection and tables
            try:
                db_path = os.path.join(settings.BASE_DIR, 'data', 'integrated.duckdb')
                if os.path.exists(db_path):
                    conn = duckdb.connect(db_path)
                    try:
                        result = conn.execute("SHOW TABLES").fetchall()
                        diagnostic_info['duckdb_tables'] = [table[0] for table in result]
                    except Exception as e:
                        diagnostic_info['duckdb_tables'] = [f"Error: {str(e)}"]
                    finally:
                        conn.close()
                else:
                    diagnostic_info['duckdb_tables'] = ["DuckDB file not found"]
            except Exception as e:
                diagnostic_info['duckdb_tables'] = [f"DuckDB connection error: {str(e)}"]
            
            # Analyze data sources and their table mappings
            for data_source in DataSource.objects.all():
                try:
                    expected_table_name = get_integrated_table_name(data_source)
                    table_exists_in_duckdb = expected_table_name in diagnostic_info['duckdb_tables']
                    
                    # Check if semantic table exists
                    semantic_table = SemanticTable.objects.filter(data_source=data_source).first()
                    has_semantic_table = semantic_table is not None
                    
                    # Check metrics for this table
                    metrics_count = 0
                    if semantic_table:
                        metrics_count = SemanticMetric.objects.filter(base_table=semantic_table).count()
                    
                    ds_info = {
                        'id': str(data_source.id),
                        'name': data_source.name,
                        'source_type': data_source.source_type,
                        'status': data_source.status,
                        'expected_table_name': expected_table_name,
                        'table_exists_in_duckdb': table_exists_in_duckdb,
                        'has_semantic_table': has_semantic_table,
                        'semantic_table_name': semantic_table.name if semantic_table else None,
                        'metrics_count': metrics_count,
                        'workflow_status': data_source.workflow_status,
                        'created_at': data_source.created_at.isoformat()
                    }
                    
                    diagnostic_info['data_sources'].append(ds_info)
                    
                    # Identify issues
                    if not table_exists_in_duckdb:
                        diagnostic_info['table_mapping_issues'].append(
                            f"Data source '{data_source.name}' expected table '{expected_table_name}' not found in DuckDB"
                        )
                    
                    if not has_semantic_table:
                        diagnostic_info['table_mapping_issues'].append(
                            f"Data source '{data_source.name}' has no semantic table"
                        )
                    
                    if has_semantic_table and metrics_count == 0:
                        diagnostic_info['table_mapping_issues'].append(
                            f"Semantic table '{semantic_table.name}' has no business metrics"
                        )
                
                except Exception as ds_error:
                    diagnostic_info['table_mapping_issues'].append(
                        f"Error analyzing data source '{data_source.name}': {str(ds_error)}"
                    )
            
            # Get semantic tables info
            for semantic_table in SemanticTable.objects.all():
                columns_count = SemanticColumn.objects.filter(semantic_table=semantic_table).count()
                metrics_count = SemanticMetric.objects.filter(base_table=semantic_table).count()
                
                st_info = {
                    'id': str(semantic_table.id),
                    'name': semantic_table.name,
                    'display_name': semantic_table.display_name,
                    'data_source_name': semantic_table.data_source.name,
                    'columns_count': columns_count,
                    'metrics_count': metrics_count
                }
                diagnostic_info['semantic_tables'].append(st_info)
            
            # Get business metrics info
            for metric in SemanticMetric.objects.all():
                metric_info = {
                    'id': str(metric.id),
                    'name': metric.name,
                    'display_name': metric.display_name,
                    'metric_type': metric.metric_type,
                    'calculation': metric.calculation,
                    'base_table_name': metric.base_table.name if metric.base_table else None,
                    'has_base_table': metric.base_table is not None,
                    'is_active': metric.is_active,
                    'created_at': metric.created_at.isoformat()
                }
                diagnostic_info['business_metrics'].append(metric_info)
            
            # Generate recommendations
            if diagnostic_info['total_business_metrics'] == 0:
                diagnostic_info['recommendations'].append("No business metrics found. Run regeneration to create metrics from semantic tables.")
            
            if diagnostic_info['metrics_without_base_table'] > 0:
                diagnostic_info['recommendations'].append(f"{diagnostic_info['metrics_without_base_table']} metrics are not linked to semantic tables. Run regeneration to fix linkages.")
            
            if len(diagnostic_info['table_mapping_issues']) > 0:
                diagnostic_info['recommendations'].append("Table mapping issues detected. Check ETL processes and DuckDB data integrity.")
            
            if diagnostic_info['total_semantic_tables'] == 0:
                diagnostic_info['recommendations'].append("No semantic tables found. Run semantic layer generation first.")
            
            return JsonResponse({
                'success': True,
                'diagnostic_info': diagnostic_info
            })
            
        except Exception as e:
            logger.error(f"Error diagnosing business metrics: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)

@login_required
def create_business_metric_api(request):
    """API endpoint to create new business metrics with validation"""
    if request.method == 'POST':
        try:
            from services.business_metrics_service import BusinessMetricsService
            
            data = json.loads(request.body)
            
            # Required fields
            name = data.get('name')
            display_name = data.get('display_name')
            description = data.get('description', '')
            metric_type = data.get('metric_type')
            calculation = data.get('calculation')
            unit = data.get('unit', '')
            base_table_id = data.get('base_table_id')
            
            if not all([name, display_name, metric_type, calculation]):
                return JsonResponse({
                    'error': 'Name, display name, metric type, and calculation are required'
                }, status=400)
            
            # Create metric using enhanced service
            metrics_service = BusinessMetricsService()
            success, message, metric_id = metrics_service.create_custom_metric(
                name=name,
                display_name=display_name,
                description=description,
                metric_type=metric_type,
                calculation=calculation,
                unit=unit,
                base_table_id=base_table_id,
                user_id=request.user.id
            )
            
            if success:
                return JsonResponse({
                    'success': True,
                    'message': message,
                    'metric_id': metric_id
                })
            else:
                return JsonResponse({'error': message}, status=400)
                
        except Exception as e:
            logger.error(f"Error creating business metric: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def validate_formula_api(request):
    """API endpoint to validate metric formulas with suggestions"""
    if request.method == 'POST':
        try:
            from services.business_metrics_service import BusinessMetricsService
            
            data = json.loads(request.body)
            formula = data.get('formula', '')
            table_name = data.get('table_name')
            
            metrics_service = BusinessMetricsService()
            is_valid, message, suggestions = metrics_service.validate_formula(formula, table_name)
            
            return JsonResponse({
                'success': True,
                'is_valid': is_valid,
                'message': message,
                'suggestions': suggestions
            })
            
        except Exception as e:
            logger.error(f"Error validating formula: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def get_formula_suggestions_api(request):
    """API endpoint to get autocomplete suggestions for formula building"""
    if request.method == 'GET':
        try:
            from services.business_metrics_service import BusinessMetricsService
            
            partial_formula = request.GET.get('partial', '')
            table_name = request.GET.get('table_name')
            
            metrics_service = BusinessMetricsService()
            suggestions = metrics_service.get_formula_suggestions(partial_formula, table_name)
            
            return JsonResponse({
                'success': True,
                'suggestions': suggestions
            })
            
        except Exception as e:
            logger.error(f"Error getting formula suggestions: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def test_metric_calculation_api(request):
    """API endpoint to test metric calculations against real data"""
    if request.method == 'POST':
        try:
            from services.business_metrics_service import BusinessMetricsService
            
            data = json.loads(request.body)
            calculation = data.get('calculation', '')
            table_name = data.get('table_name')
            
            if not calculation or not table_name:
                return JsonResponse({
                    'error': 'Calculation and table name are required'
                }, status=400)
            
            metrics_service = BusinessMetricsService()
            success, message, result = metrics_service.test_metric_calculation(calculation, table_name)
            
            return JsonResponse({
                'success': success,
                'message': message,
                'result': result if success else None
            })
            
        except Exception as e:
            logger.error(f"Error testing metric calculation: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def get_table_columns_api(request, table_id):
    """API endpoint to get columns for a specific semantic table"""
    if request.method == 'GET':
        try:
            from datasets.models import SemanticTable, SemanticColumn
            
            # Get the semantic table
            try:
                semantic_table = SemanticTable.objects.get(id=table_id)
            except SemanticTable.DoesNotExist:
                return JsonResponse({'error': 'Table not found'}, status=404)
            
            # Get columns for this table
            columns = SemanticColumn.objects.filter(semantic_table=semantic_table).order_by('name')
            
            column_data = []
            for col in columns:
                column_data.append({
                    'name': col.name,
                    'display_name': col.display_name,
                    'data_type': col.data_type,
                    'semantic_type': col.semantic_type,
                    'description': col.description or '',
                    'is_measure': col.is_measure,
                    'aggregation_default': col.aggregation_default or ''
                })
            
            return JsonResponse({
                'success': True,
                'table_name': semantic_table.name,
                'table_display_name': semantic_table.display_name,
                'columns': column_data
            })
            
        except Exception as e:
            logger.error(f"Error getting table columns: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def regenerate_business_metrics_api(request):
    """API endpoint to regenerate business metrics from current semantic tables"""
    if request.method == 'POST':
        try:
            from services.semantic_service import SemanticService
            from datasets.models import SemanticMetric, SemanticTable
            
            data = json.loads(request.body) if request.body else {}
            table_name = data.get('table_name')  # Optional: regenerate for specific table
            
            semantic_service = SemanticService()
            
            if table_name:
                # Regenerate for specific table
                success = semantic_service.regenerate_business_metrics_for_table(table_name)
                if success:
                    metrics_count = SemanticMetric.objects.filter(base_table__name=table_name).count()
                    return JsonResponse({
                        'success': True,
                        'message': f'Successfully regenerated business metrics for table {table_name}',
                        'metrics_count': metrics_count
                    })
                else:
                    return JsonResponse({
                        'error': f'Failed to regenerate metrics for table {table_name}'
                    }, status=400)
            else:
                # Regenerate all business metrics
                logger.info(f"User {request.user.username} initiated business metrics regeneration")
                
                # Delete all existing metrics
                old_metrics_count = SemanticMetric.objects.count()
                SemanticMetric.objects.all().delete()
                logger.info(f"Deleted {old_metrics_count} existing business metrics")
                
                # Regenerate metrics from semantic tables
                semantic_service._generate_dynamic_business_metrics()
                
                # Count new metrics
                new_metrics_count = SemanticMetric.objects.count()
                
                logger.info(f"Regenerated {new_metrics_count} business metrics")
                
                return JsonResponse({
                    'success': True,
                    'message': f'Successfully regenerated all business metrics',
                    'old_metrics_count': old_metrics_count,
                    'new_metrics_count': new_metrics_count,
                    'metrics_created': new_metrics_count
                })
            
        except Exception as e:
            logger.error(f"Error regenerating business metrics: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)

@csrf_exempt
@login_required
@creator_required
def get_next_workflow_step(request, pk):
    """Get the next workflow step for a data source"""
    try:
        data_source = get_object_or_404(DataSource, pk=pk, created_by=request.user)
        
        workflow_status = data_source.workflow_status or {}
        
        # Determine next step based on current workflow state
        if not workflow_status.get('etl_completed', False):
            next_step = {
                'step': 'etl',
                'title': 'ETL Processing',
                'description': 'Process and transform your data',
                'url': reverse('datasets:integration')
            }
        elif not workflow_status.get('semantics_completed', False):
            next_step = {
                'step': 'semantics',
                'title': 'Semantic Layer',
                'description': 'Generate business-friendly metadata',
                'url': reverse('datasets:semantic_detail', kwargs={'pk': pk})
            }
        elif not workflow_status.get('query_enabled', False):
            next_step = {
                'step': 'query',
                'title': 'Query Testing',
                'description': 'Test your data with natural language queries',
                'url': reverse('core:query') + f'?data_source_id={pk}'
            }
        else:
            next_step = {
                'step': 'dashboard',
                'title': 'Create Dashboard',
                'description': 'Build visualizations and dashboards',
                'url': reverse('dashboards:create')
            }
        
        return JsonResponse({
            'success': True,
            'next_step': next_step,
            'workflow_status': workflow_status
        })
        
    except Exception as e:
        logger.error(f"Error getting next workflow step for {pk}: {e}")
        return JsonResponse({'error': str(e)}, status=500)


# ETL Scheduling Views

@login_required
@creator_required
def scheduled_etl_jobs_list(request):
    """List all scheduled ETL jobs for the current user."""
    try:
        # Get user's scheduled ETL jobs
        jobs = ScheduledETLJob.objects.filter(
            created_by=request.user
        ).order_by('-created_at')
        
        jobs_data = []
        for job in jobs:
            # Get recent run logs
            recent_runs = job.run_logs.order_by('-started_at')[:5]
            
            # Calculate success rate
            if recent_runs:
                successful_runs = sum(1 for run in recent_runs if run.status == 'success')
                success_rate = (successful_runs / len(recent_runs)) * 100
            else:
                success_rate = 0
            
            job_data = {
                'id': str(job.id),
                'name': job.name,
                'description': job.description,
                'schedule_type': job.schedule_type,
                'schedule_display': job.get_schedule_type_display(),
                'timezone': job.timezone,
                'is_active': job.is_active,
                'status': job.status,
                'last_run': job.last_run.isoformat() if job.last_run else None,
                'next_run': job.next_run.isoformat() if job.next_run else None,
                'last_run_status': job.last_run_status,
                'consecutive_failures': job.consecutive_failures,
                'success_rate': round(success_rate, 1),
                'data_sources_count': job.data_sources.count(),
                'recent_runs': [
                    {
                        'id': str(run.id),
                        'status': run.status,
                        'started_at': run.started_at.isoformat(),
                        'duration': run.duration_formatted(),
                        'records_processed': run.total_records_processed
                    }
                    for run in recent_runs
                ]
            }
            jobs_data.append(job_data)
        
        return JsonResponse({
            'success': True,
            'jobs': jobs_data,
            'total_count': len(jobs_data)
        })
        
    except Exception as e:
        logger.error(f"Error listing scheduled ETL jobs: {e}")
        return JsonResponse({'error': str(e)}, status=500)


@csrf_exempt
@login_required
@creator_required
def create_scheduled_etl_job(request):
    """Create a new scheduled ETL job."""
    if request.method != 'POST':
        return JsonResponse({'error': 'POST method required'}, status=405)
    
    try:
        data = json.loads(request.body)
        
        # Validate required fields
        name = data.get('name', '').strip()
        if not name:
            return JsonResponse({'error': 'Job name is required'}, status=400)
        
        schedule_type = data.get('schedule_type', 'daily')
        if schedule_type not in dict(ScheduledETLJob.SCHEDULE_TYPE_CHOICES):
            return JsonResponse({'error': 'Invalid schedule type'}, status=400)
        
        data_source_ids = data.get('data_source_ids', [])
        if not data_source_ids:
            return JsonResponse({'error': 'At least one data source is required'}, status=400)
        
        # Validate data sources belong to user
        data_sources = DataSource.objects.filter(
            id__in=data_source_ids,
            created_by=request.user,
            status='active'
        )
        
        if data_sources.count() != len(data_source_ids):
            return JsonResponse({'error': 'One or more data sources not found or not accessible'}, status=400)
        
        # Create the scheduled job
        job = ScheduledETLJob.objects.create(
            name=name,
            description=data.get('description', ''),
            schedule_type=schedule_type,
            timezone=data.get('timezone', 'UTC'),
            hour=data.get('hour', 2),
            minute=data.get('minute', 0),
            day_of_week=data.get('day_of_week'),
            day_of_month=data.get('day_of_month'),
            max_retries=data.get('max_retries', 3),
            retry_delay_minutes=data.get('retry_delay_minutes', 5),
            failure_threshold=data.get('failure_threshold', 5),
            etl_config=data.get('etl_config', {}),
            notify_on_success=data.get('notify_on_success', False),
            notify_on_failure=data.get('notify_on_failure', True),
            notification_emails=data.get('notification_emails', []),
            created_by=request.user
        )
        
        # Add data sources
        job.data_sources.set(data_sources)
        
        # Calculate initial next run time
        job.update_next_run()
        
        logger.info(f"Created scheduled ETL job: {job.name} by {request.user.username}")
        
        return JsonResponse({
            'success': True,
            'message': 'Scheduled ETL job created successfully',
            'job': {
                'id': str(job.id),
                'name': job.name,
                'schedule_type': job.schedule_type,
                'next_run': job.next_run.isoformat() if job.next_run else None
            }
        })
        
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON data'}, status=400)
    except Exception as e:
        logger.error(f"Error creating scheduled ETL job: {e}")
        return JsonResponse({'error': str(e)}, status=500)


@login_required
@creator_required  
def scheduled_etl_job_detail(request, job_id):
    """Get details of a specific scheduled ETL job."""
    try:
        job = get_object_or_404(ScheduledETLJob, id=job_id, created_by=request.user)
        
        # Get schedule status from manager
        from services.etl_schedule_manager import etl_schedule_manager
        schedule_status = etl_schedule_manager.get_schedule_status(job)
        
        # Get recent run logs
        recent_runs = job.run_logs.order_by('-started_at')[:20]
        
        # Calculate statistics
        total_runs = job.run_logs.count()
        if total_runs > 0:
            successful_runs = job.run_logs.filter(status='success').count()
            failed_runs = job.run_logs.filter(status='failed').count()
            success_rate = (successful_runs / total_runs) * 100
        else:
            successful_runs = failed_runs = 0
            success_rate = 0
        
        job_data = {
            'id': str(job.id),
            'name': job.name,
            'description': job.description,
            'schedule_type': job.schedule_type,
            'schedule_display': job.get_schedule_type_display(),
            'timezone': job.timezone,
            'hour': job.hour,
            'minute': job.minute,
            'day_of_week': job.day_of_week,
            'day_of_month': job.day_of_month,
            'is_active': job.is_active,
            'status': job.status,
            'last_run': job.last_run.isoformat() if job.last_run else None,
            'next_run': job.next_run.isoformat() if job.next_run else None,
            'last_run_status': job.last_run_status,
            'consecutive_failures': job.consecutive_failures,
            'max_retries': job.max_retries,
            'retry_delay_minutes': job.retry_delay_minutes,
            'failure_threshold': job.failure_threshold,
            'etl_config': job.etl_config,
            'notify_on_success': job.notify_on_success,
            'notify_on_failure': job.notify_on_failure,
            'notification_emails': job.notification_emails,
            'created_at': job.created_at.isoformat(),
            'updated_at': job.updated_at.isoformat(),
            'data_sources': [
                {
                    'id': str(ds.id),
                    'name': ds.name,
                    'source_type': ds.source_type,
                    'status': ds.status,
                    'last_synced': ds.last_synced.isoformat() if ds.last_synced else None
                }
                for ds in job.data_sources.all()
            ],
            'statistics': {
                'total_runs': total_runs,
                'successful_runs': successful_runs,
                'failed_runs': failed_runs,
                'success_rate': round(success_rate, 1)
            },
            'schedule_status': schedule_status,
            'recent_runs': [
                {
                    'id': str(run.id),
                    'status': run.status,
                    'started_at': run.started_at.isoformat(),
                    'completed_at': run.completed_at.isoformat() if run.completed_at else None,
                    'duration': run.duration_formatted(),
                    'total_records_processed': run.total_records_processed,
                    'total_records_added': run.total_records_added,
                    'total_records_updated': run.total_records_updated,
                    'data_sources_processed': len(run.data_sources_processed),
                    'data_sources_failed': len(run.data_sources_failed),
                    'error_message': run.error_message,
                    'triggered_by': run.triggered_by
                }
                for run in recent_runs
            ]
        }
        
        return JsonResponse({
            'success': True,
            'job': job_data
        })
        
    except Exception as e:
        logger.error(f"Error getting scheduled ETL job detail for {job_id}: {e}")
        return JsonResponse({'error': str(e)}, status=500)


@csrf_exempt
@login_required
@creator_required
def run_scheduled_etl_job_now(request, job_id):
    """Trigger immediate execution of a scheduled ETL job."""
    if request.method != 'POST':
        return JsonResponse({'error': 'POST method required'}, status=405)
    
    try:
        job = get_object_or_404(ScheduledETLJob, id=job_id, created_by=request.user)
        
        if not job.is_active:
            return JsonResponse({'error': 'Job is not active'}, status=400)
        
        # FIXED: Check if Redis is available, otherwise execute directly
        from django.conf import settings
        
        # Check if we should use Redis or execute directly
        use_redis = getattr(settings, 'USE_REDIS', False)
        always_eager = getattr(settings, 'CELERY_TASK_ALWAYS_EAGER', False)
        
        if use_redis and not always_eager:
            # Use Celery with Redis (production mode)
            from services.scheduled_etl_service import execute_scheduled_etl_job
            task = execute_scheduled_etl_job.delay(str(job.id), 'manual_api')
            task_id = task.id
            logger.info(f"Queued ETL job {job.name} in Celery by {request.user.username}")
        else:
            # Execute directly without Redis (development mode)
            from services.scheduled_etl_service import ScheduledETLService
            etl_service = ScheduledETLService()
            
            # Execute immediately and synchronously
            success = etl_service.execute_scheduled_job(str(job.id), triggered_by='manual_api')
            task_id = 'immediate_execution'
            
            if success:
                logger.info(f"Successfully executed ETL job {job.name} immediately by {request.user.username}")
            else:
                logger.error(f"Failed to execute ETL job {job.name} by {request.user.username}")
                return JsonResponse({
                    'error': f'ETL job "{job.name}" execution failed',
                    'details': 'Check the job logs for more information'
                }, status=500)
        
        return JsonResponse({
            'success': True,
            'message': f'ETL job "{job.name}" has been triggered',
            'task_id': task_id,
            'execution_mode': 'immediate' if always_eager or not use_redis else 'queued'
        })
        
    except Exception as e:
        logger.error(f"Error triggering ETL job {job_id}: {e}")
        return JsonResponse({'error': str(e)}, status=500)


@csrf_exempt
@login_required
@creator_required
def enable_scheduled_etl_job(request, job_id):
    """Enable a scheduled ETL job."""
    if request.method != 'POST':
        return JsonResponse({'error': 'POST method required'}, status=405)
    
    try:
        job = get_object_or_404(ScheduledETLJob, id=job_id, created_by=request.user)
        
        job.is_active = True
        job.status = 'active'
        job.save()
        
        # Update next run time
        job.update_next_run()
        
        logger.info(f"Enabled ETL job {job.name} by {request.user.username}")
        
        return JsonResponse({
            'success': True,
            'message': f'ETL job "{job.name}" has been enabled',
            'next_run': job.next_run.isoformat() if job.next_run else None
        })
        
    except Exception as e:
        logger.error(f"Error enabling ETL job {job_id}: {e}")
        return JsonResponse({'error': str(e)}, status=500)


@csrf_exempt
@login_required
@creator_required
def disable_scheduled_etl_job(request, job_id):
    """Disable a scheduled ETL job."""
    if request.method != 'POST':
        return JsonResponse({'error': 'POST method required'}, status=405)
    
    try:
        job = get_object_or_404(ScheduledETLJob, id=job_id, created_by=request.user)
        
        job.is_active = False
        job.status = 'inactive'
        job.save()
        
        logger.info(f"Disabled ETL job {job.name} by {request.user.username}")
        
        return JsonResponse({
            'success': True,
            'message': f'ETL job "{job.name}" has been disabled'
        })
        
    except Exception as e:
        logger.error(f"Error disabling ETL job {job_id}: {e}")
        return JsonResponse({'error': str(e)}, status=500)


@csrf_exempt
@login_required
@creator_required
def delete_scheduled_etl_job(request, job_id):
    """Delete a scheduled ETL job."""
    if request.method != 'DELETE':
        return JsonResponse({'error': 'DELETE method required'}, status=405)
    
    try:
        job = get_object_or_404(ScheduledETLJob, id=job_id, created_by=request.user)
        job_name = job.name
        
        # Delete the job (signals will handle Celery schedule cleanup)
        job.delete()
        
        logger.info(f"Deleted ETL job {job_name} by {request.user.username}")
        
        return JsonResponse({
            'success': True,
            'message': f'ETL job "{job_name}" has been deleted'
        })
        
    except Exception as e:
        logger.error(f"Error deleting ETL job {job_id}: {e}")
        return JsonResponse({'error': str(e)}, status=500)


@login_required
@creator_required
def scheduled_etl_job_status(request, job_id):
    """Get current status of a scheduled ETL job including running status."""
    try:
        job = get_object_or_404(ScheduledETLJob, id=job_id, created_by=request.user)
        
        # Get the most recent run log to determine current status
        latest_run = job.run_logs.order_by('-started_at').first()
        
        # Determine current status
        current_status = 'idle'  # Default if no runs
        
        if latest_run:
            # Check if the job is currently running
            if latest_run.status in ['started', 'running'] and not latest_run.completed_at:
                current_status = 'running'
            elif latest_run.status == 'success':
                current_status = 'completed'
            elif latest_run.status == 'failed':
                current_status = 'failed'
            else:
                current_status = latest_run.status
        
        # Additional status information
        status_data = {
            'job_id': str(job.id),
            'job_name': job.name,
            'status': current_status,
            'job_is_active': job.is_active,
            'job_status': job.status,
            'last_run_status': job.last_run_status,
            'consecutive_failures': job.consecutive_failures,
            'next_run': job.next_run.isoformat() if job.next_run else None,
            'last_run': job.last_run.isoformat() if job.last_run else None,
        }
        
        # Include latest run details if available
        if latest_run:
            status_data['latest_run'] = {
                'id': str(latest_run.id),
                'status': latest_run.status,
                'started_at': latest_run.started_at.isoformat(),
                'completed_at': latest_run.completed_at.isoformat() if latest_run.completed_at else None,
                'duration_seconds': latest_run.execution_time_seconds,
                'records_processed': latest_run.total_records_processed,
                'error_message': latest_run.error_message
            }
        
        return JsonResponse({
            'success': True,
            'status': current_status,
            'details': status_data
        })
        
    except Exception as e:
        logger.error(f"Error getting ETL job status for {job_id}: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@login_required
@creator_required
def scheduled_etl_job_logs(request, job_id):
    """Get execution logs for a scheduled ETL job."""
    try:
        job = get_object_or_404(ScheduledETLJob, id=job_id, created_by=request.user)
        
        # Get logs with pagination
        page = int(request.GET.get('page', 1))
        page_size = int(request.GET.get('page_size', 20))
        status_filter = request.GET.get('status')
        
        logs_query = job.run_logs.all()
        
        if status_filter:
            logs_query = logs_query.filter(status=status_filter)
        
        logs_query = logs_query.order_by('-started_at')
        
        # Calculate pagination
        total_logs = logs_query.count()
        start_index = (page - 1) * page_size
        end_index = start_index + page_size
        
        logs = logs_query[start_index:end_index]
        
        logs_data = []
        for log in logs:
            log_data = {
                'id': str(log.id),
                'status': log.status,
                'started_at': log.started_at.isoformat(),
                'completed_at': log.completed_at.isoformat() if log.completed_at else None,
                'duration': log.duration_formatted(),
                'total_records_processed': log.total_records_processed,
                'total_records_added': log.total_records_added,
                'total_records_updated': log.total_records_updated,
                'total_records_deleted': log.total_records_deleted,
                'data_sources_processed': log.data_sources_processed,
                'data_sources_failed': log.data_sources_failed,
                'data_sources_skipped': log.data_sources_skipped,
                'error_message': log.error_message,
                'triggered_by': log.triggered_by,
                'retry_count': log.retry_count,
                'celery_task_id': log.celery_task_id,
                'worker_hostname': log.worker_hostname
            }
            logs_data.append(log_data)
        
        return JsonResponse({
            'success': True,
            'logs': logs_data,
            'pagination': {
                'current_page': page,
                'page_size': page_size,
                'total_logs': total_logs,
                'total_pages': (total_logs + page_size - 1) // page_size,
                'has_next': end_index < total_logs,
                'has_previous': page > 1
            }
        })
        
    except Exception as e:
        logger.error(f"Error getting ETL job logs for {job_id}: {e}")
        return JsonResponse({'error': str(e)}, status=500)

@login_required
def get_database_tables(request):
    """Get list of tables from database after successful connection test"""
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            
            connection_info = {
                'type': data.get('source_type'),
                'host': data.get('host'),
                'port': int(data.get('port')),
                'database': data.get('database'),
                'username': data.get('username'),
                'password': data.get('password')
            }
            
            if data.get('schema'):
                connection_info['schema'] = data.get('schema')
            
            # Get tables from database
            data_service = DataService()
            success, tables, message = data_service.get_database_tables(connection_info)
            
            return JsonResponse({
                'success': success,
                'tables': tables,
                'message': message
            })
            
        except Exception as e:
            logger.error(f"Error getting database tables: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)

@csrf_exempt
@login_required
def check_and_run_overdue_jobs(request):
    """
    Check for overdue ETL jobs and execute them automatically.
    This provides a fallback when Celery Beat isn't running.
    """
    if request.method != 'POST':
        return JsonResponse({'error': 'POST method required'}, status=405)
    
    try:
        from django.utils import timezone
        from django.conf import settings
        
        # Get all overdue jobs for this user
        now = timezone.now()
        overdue_jobs = ScheduledETLJob.objects.filter(
            created_by=request.user,
            is_active=True,
            status='active',
            next_run__lt=now
        ).order_by('next_run')
        
        executed_jobs = []
        failed_jobs = []
        
        for job in overdue_jobs:
            try:
                # Check if job can run (safety check)
                if job.can_run_now():
                    # Check if we should use Redis or execute directly
                    use_redis = getattr(settings, 'USE_REDIS', False)
                    always_eager = getattr(settings, 'CELERY_TASK_ALWAYS_EAGER', False)
                    
                    if use_redis and not always_eager:
                        # Use Celery with Redis (production mode)
                        from services.scheduled_etl_service import execute_scheduled_etl_job
                        task = execute_scheduled_etl_job.delay(str(job.id), 'auto_catchup')
                        executed_jobs.append({
                            'id': str(job.id),
                            'name': job.name,
                            'method': 'celery',
                            'task_id': task.id
                        })
                    else:
                        # Execute directly without Redis (development mode)
                        from services.scheduled_etl_service import ScheduledETLService
                        
                        with ScheduledETLService() as etl_service:
                            success, message, results = etl_service.execute_scheduled_job(
                                str(job.id), 
                                'auto_catchup'
                            )
                        
                        if success:
                            executed_jobs.append({
                                'id': str(job.id),
                                'name': job.name,
                                'method': 'direct',
                                'message': message
                            })
                        else:
                            failed_jobs.append({
                                'id': str(job.id),
                                'name': job.name,
                                'error': message
                            })
                            
                    logger.info(f"Auto-executed overdue ETL job: {job.name}")
                    
            except Exception as job_error:
                logger.error(f"Error auto-executing job {job.name}: {job_error}")
                failed_jobs.append({
                    'id': str(job.id),
                    'name': job.name,
                    'error': str(job_error)
                })
        
        return JsonResponse({
            'success': True,
            'message': f'Checked {overdue_jobs.count()} overdue jobs',
            'executed_count': len(executed_jobs),
            'failed_count': len(failed_jobs),
            'executed_jobs': executed_jobs,
            'failed_jobs': failed_jobs
        })
        
    except Exception as e:
        logger.error(f"Error checking overdue jobs: {e}")
        return JsonResponse({'error': str(e)}, status=500)


@login_required
def etl_scheduling_diagnostics(request):
    """
    Diagnostic view to understand the ETL scheduling system status.
    Helps identify why jobs might not be running automatically.
    """
    try:
        from django.utils import timezone
        from django.conf import settings
        import django
        
        now = timezone.now()
        
        # Get basic system info
        system_info = {
            'current_time': now.isoformat(),
            'django_version': django.get_version(),
            'use_redis': getattr(settings, 'USE_REDIS', False),
            'celery_always_eager': getattr(settings, 'CELERY_TASK_ALWAYS_EAGER', False),
            'time_zone': settings.TIME_ZONE,
        }
        
        # Check Celery Beat availability
        try:
            from django_celery_beat.models import PeriodicTask
            celery_beat_available = True
            periodic_tasks_count = PeriodicTask.objects.count()
            etl_periodic_tasks = PeriodicTask.objects.filter(
                task='services.scheduled_etl_service.execute_scheduled_etl_job'
            ).count()
        except ImportError:
            celery_beat_available = False
            periodic_tasks_count = 0
            etl_periodic_tasks = 0
        
        # Get user's ETL jobs status
        user_jobs = ScheduledETLJob.objects.filter(created_by=request.user)
        
        jobs_summary = {
            'total_jobs': user_jobs.count(),
            'active_jobs': user_jobs.filter(is_active=True, status='active').count(),
            'inactive_jobs': user_jobs.filter(is_active=False).count(),
            'error_jobs': user_jobs.filter(status='error').count(),
            'overdue_jobs': user_jobs.filter(
                is_active=True, 
                status='active', 
                next_run__lt=now
            ).count()
        }
        
        # Get overdue jobs details
        overdue_jobs = user_jobs.filter(
            is_active=True, 
            status='active', 
            next_run__lt=now
        ).order_by('next_run')
        
        overdue_details = []
        for job in overdue_jobs:
            overdue_since = now - job.next_run if job.next_run else None
            overdue_details.append({
                'id': str(job.id),
                'name': job.name,
                'next_run': job.next_run.isoformat() if job.next_run else None,
                'overdue_since': overdue_since.total_seconds() if overdue_since else None,
                'overdue_minutes': round(overdue_since.total_seconds() / 60) if overdue_since else None,
                'can_run_now': job.can_run_now(),
                'consecutive_failures': job.consecutive_failures,
                'celery_task_name': job.celery_task_name,
                'celery_schedule_id': job.celery_schedule_id
            })
        
        # Check for recent run logs
        recent_logs = ETLJobRunLog.objects.filter(
            scheduled_job__created_by=request.user,
            started_at__gte=now - timezone.timedelta(hours=24)
        ).order_by('-started_at')[:10]
        
        recent_logs_details = []
        for log in recent_logs:
            recent_logs_details.append({
                'id': str(log.id),
                'job_name': log.scheduled_job.name,
                'status': log.status,
                'started_at': log.started_at.isoformat(),
                'triggered_by': log.triggered_by,
                'execution_time': log.execution_time_seconds
            })
        
        diagnostics = {
            'system_info': system_info,
            'celery_beat': {
                'available': celery_beat_available,
                'total_periodic_tasks': periodic_tasks_count,
                'etl_periodic_tasks': etl_periodic_tasks
            },
            'jobs_summary': jobs_summary,
            'overdue_jobs': overdue_details,
            'recent_logs': recent_logs_details,
            'recommendations': []
        }
        
        # Add recommendations based on findings
        if not celery_beat_available:
            diagnostics['recommendations'].append({
                'type': 'warning',
                'message': 'django-celery-beat is not installed. ETL scheduling may not work automatically.',
                'action': 'Install django-celery-beat or use the manual "Check Overdue" button.'
            })
        
        if jobs_summary['overdue_jobs'] > 0:
            diagnostics['recommendations'].append({
                'type': 'error',
                'message': f"{jobs_summary['overdue_jobs']} job(s) are overdue and not executing automatically.",
                'action': 'Use the "Check Overdue" button to run them manually, or check if Celery Beat is running.'
            })
        
        if system_info['celery_always_eager']:
            diagnostics['recommendations'].append({
                'type': 'info',
                'message': 'Celery is in development mode (always eager). Jobs will not run automatically.',
                'action': 'This is normal for development. Use "Check Overdue" button or set up Redis for production.'
            })
        
        return JsonResponse({
            'success': True,
            'diagnostics': diagnostics
        })
        
    except Exception as e:
        logger.error(f"Error in ETL diagnostics: {e}")
        return JsonResponse({'error': str(e)}, status=500)

@login_required
def validate_business_metric_formula(request):
    """API endpoint to validate business metric formulas"""
    if request.method == 'POST':
        try:
            from services.business_metrics_service import BusinessMetricsService
            
            data = json.loads(request.body)
            formula = data.get('formula', '')
            table_name = data.get('table_name')
            
            metrics_service = BusinessMetricsService()
            is_valid, message, suggestions = metrics_service.validate_formula(formula, table_name)
            
            return JsonResponse({
                'success': True,
                'is_valid': is_valid,
                'message': message,
                'suggestions': suggestions
            })
            
        except Exception as e:
            logger.error(f"Error validating formula: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def list_business_metrics(request):
    """API endpoint to list all business metrics"""
    if request.method == 'GET':
        try:
            from datasets.models import SemanticMetric
            
            metrics = SemanticMetric.objects.filter(is_active=True).order_by('name')
            
            metrics_data = []
            for metric in metrics:
                metrics_data.append({
                    'id': metric.id,
                    'name': metric.name,
                    'display_name': metric.display_name,
                    'description': metric.description or '',
                    'metric_type': metric.metric_type,
                    'calculation': metric.calculation,
                    'unit': metric.unit or '',
                    'base_table': metric.base_table.name if metric.base_table else '',
                    'created_at': metric.created_at.isoformat(),
                    'is_active': metric.is_active
                })
            
            return JsonResponse({
                'success': True,
                'metrics': metrics_data,
                'count': len(metrics_data)
            })
            
        except Exception as e:
            logger.error(f"Error listing business metrics: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def get_business_metric_detail(request, metric_id):
    """API endpoint to get details of a specific business metric"""
    if request.method == 'GET':
        try:
            from datasets.models import SemanticMetric
            
            try:
                metric = SemanticMetric.objects.get(id=metric_id)
            except SemanticMetric.DoesNotExist:
                return JsonResponse({'error': 'Metric not found'}, status=404)
            
            metric_data = {
                'id': metric.id,
                'name': metric.name,
                'display_name': metric.display_name,
                'description': metric.description or '',
                'metric_type': metric.metric_type,
                'calculation': metric.calculation,
                'unit': metric.unit or '',
                'base_table': {
                    'id': metric.base_table.id,
                    'name': metric.base_table.name,
                    'display_name': metric.base_table.display_name
                } if metric.base_table else None,
                'created_at': metric.created_at.isoformat(),
                'updated_at': metric.updated_at.isoformat(),
                'is_active': metric.is_active,
                'business_owner': metric.business_owner or '',
                'validation_rules': metric.validation_rules or []
            }
            
            return JsonResponse({
                'success': True,
                'metric': metric_data
            })
            
        except Exception as e:
            logger.error(f"Error getting business metric detail: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def update_business_metric(request, metric_id):
    """API endpoint to update a business metric"""
    if request.method == 'POST':
        try:
            from services.business_metrics_service import BusinessMetricsService
            
            data = json.loads(request.body)
            
            metrics_service = BusinessMetricsService()
            success, message = metrics_service.update_metric(
                metric_id=str(metric_id),
                **data
            )
            
            if success:
                return JsonResponse({
                    'success': True,
                    'message': message
                })
            else:
                return JsonResponse({'error': message}, status=400)
                
        except Exception as e:
            logger.error(f"Error updating business metric: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def delete_business_metric(request, metric_id):
    """API endpoint to delete a business metric"""
    if request.method == 'DELETE':
        try:
            from datasets.models import SemanticMetric
            
            try:
                metric = SemanticMetric.objects.get(id=metric_id)
                metric_name = metric.name
                metric.delete()
                
                return JsonResponse({
                    'success': True,
                    'message': f'Metric "{metric_name}" deleted successfully'
                })
                
            except SemanticMetric.DoesNotExist:
                return JsonResponse({'error': 'Metric not found'}, status=404)
                
        except Exception as e:
            logger.error(f"Error deleting business metric: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)