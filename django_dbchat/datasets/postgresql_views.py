"""
PostgreSQL-specific views for unified data storage
"""

import json
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.contrib import messages
from django.shortcuts import redirect
import logging

from .postgresql_data_service import PostgreSQLDataService

logger = logging.getLogger(__name__)


@login_required
@csrf_exempt
@require_http_methods(["POST"])
def upload_csv_postgresql(request):
    """Upload CSV file directly to PostgreSQL unified storage"""
    try:
        if 'csv_file' not in request.FILES:
            return JsonResponse({'error': 'No CSV file provided'}, status=400)
        
        csv_file = request.FILES['csv_file']
        
        # Validate file type
        if not csv_file.name.endswith('.csv'):
            return JsonResponse({'error': 'File must be a CSV file'}, status=400)
        
        # Read file data
        file_data = csv_file.read()
        
        # Upload to PostgreSQL
        pg_service = PostgreSQLDataService()
        success, message, data_info = pg_service.upload_csv_data(
            file_data=file_data,
            filename=csv_file.name,
            user_id=request.user.id
        )
        
        if success:
            logger.info(f"User {request.user.username} uploaded CSV: {csv_file.name}")
            return JsonResponse({
                'success': True,
                'message': message,
                'data_info': data_info
            })
        else:
            return JsonResponse({'error': message}, status=400)
            
    except Exception as e:
        logger.error(f"Error uploading CSV: {e}")
        return JsonResponse({'error': f'Upload failed: {str(e)}'}, status=500)


@login_required
def postgresql_datasets_list(request):
    """List all datasets in PostgreSQL unified storage"""
    try:
        pg_service = PostgreSQLDataService()
        datasets = pg_service.get_all_datasets(user_id=request.user.id)
        stats = pg_service.get_dataset_stats()
        
        context = {
            'datasets': datasets,
            'stats': stats,
            'title': 'PostgreSQL Datasets'
        }
        
        return render(request, 'datasets/postgresql_list.html', context)
        
    except Exception as e:
        logger.error(f"Error getting PostgreSQL datasets: {e}")
        messages.error(request, f"Error loading datasets: {str(e)}")
        return render(request, 'datasets/postgresql_list.html', {'datasets': [], 'stats': {}})


@login_required
def postgresql_dataset_preview(request, table_name):
    """Get preview of a PostgreSQL dataset"""
    try:
        pg_service = PostgreSQLDataService()
        success, data = pg_service.get_dataset_preview(table_name, limit=100)
        
        if success:
            return JsonResponse({
                'success': True,
                'data': data
            })
        else:
            return JsonResponse({'error': data}, status=404)
            
    except Exception as e:
        logger.error(f"Error getting dataset preview: {e}")
        return JsonResponse({'error': f'Preview failed: {str(e)}'}, status=500)


@login_required
@csrf_exempt
@require_http_methods(["DELETE"])
def postgresql_dataset_delete(request, table_name):
    """Delete a dataset from PostgreSQL unified storage"""
    try:
        pg_service = PostgreSQLDataService()
        success, message = pg_service.delete_dataset(table_name)
        
        if success:
            logger.info(f"User {request.user.username} deleted dataset: {table_name}")
            return JsonResponse({
                'success': True,
                'message': message
            })
        else:
            return JsonResponse({'error': message}, status=404)
            
    except Exception as e:
        logger.error(f"Error deleting dataset: {e}")
        return JsonResponse({'error': f'Delete failed: {str(e)}'}, status=500)


@login_required
def postgresql_dataset_stats(request):
    """Get statistics about PostgreSQL datasets"""
    try:
        pg_service = PostgreSQLDataService()
        stats = pg_service.get_dataset_stats()
        
        return JsonResponse({
            'success': True,
            'stats': stats
        })
        
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return JsonResponse({'error': f'Stats failed: {str(e)}'}, status=500)


@login_required
def postgresql_upload_page(request):
    """Render the PostgreSQL CSV upload page"""
    if request.method == 'GET':
        # Get current stats
        pg_service = PostgreSQLDataService()
        stats = pg_service.get_dataset_stats()
        
        context = {
            'stats': stats,
            'title': 'Upload CSV to PostgreSQL'
        }
        
        return render(request, 'datasets/postgresql_upload.html', context)
    
    elif request.method == 'POST':
        # Handle form upload
        if 'csv_file' not in request.FILES:
            messages.error(request, 'No CSV file provided')
            return redirect('postgresql_upload_page')
        
        csv_file = request.FILES['csv_file']
        
        if not csv_file.name.endswith('.csv'):
            messages.error(request, 'File must be a CSV file')
            return redirect('postgresql_upload_page')
        
        try:
            # Read file data
            file_data = csv_file.read()
            
            # Upload to PostgreSQL
            pg_service = PostgreSQLDataService()
            success, message, data_info = pg_service.upload_csv_data(
                file_data=file_data,
                filename=csv_file.name,
                user_id=request.user.id
            )
            
            if success:
                messages.success(request, message)
                logger.info(f"User {request.user.username} uploaded CSV via form: {csv_file.name}")
            else:
                messages.error(request, message)
                
        except Exception as e:
            logger.error(f"Error in form upload: {e}")
            messages.error(request, f"Upload failed: {str(e)}")
        
        return redirect('postgresql_datasets_list')


@login_required
def postgresql_query_dataset(request, table_name):
    """Execute a query against a PostgreSQL dataset"""
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            sql_query = data.get('query', '')
            
            if not sql_query:
                return JsonResponse({'error': 'No query provided'}, status=400)
            
            pg_service = PostgreSQLDataService()
            success, result = pg_service.query_dataset(table_name, sql_query)
            
            if success:
                return JsonResponse({
                    'success': True,
                    'result': result
                })
            else:
                return JsonResponse({'error': result}, status=400)
                
        except json.JSONDecodeError:
            return JsonResponse({'error': 'Invalid JSON'}, status=400)
        except Exception as e:
            logger.error(f"Error querying dataset: {e}")
            return JsonResponse({'error': f'Query failed: {str(e)}'}, status=500)
    
    else:
        return JsonResponse({'error': 'Method not allowed'}, status=405) 