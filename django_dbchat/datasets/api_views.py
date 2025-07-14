#!/usr/bin/env python3
"""
API views for JOIN readiness checking and data source synchronization
"""

import logging
from django.http import JsonResponse
from django.contrib.auth.decorators import login_required
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
import json

logger = logging.getLogger(__name__)

@login_required
@require_http_methods(["POST"])
@csrf_exempt
def check_join_readiness(request):
    """Check if two data sources are ready for JOIN operation"""
    try:
        data = json.loads(request.body)
        
        left_source_id = data.get('left_source_id')
        right_source_id = data.get('right_source_id')
        
        if not left_source_id or not right_source_id:
            return JsonResponse({
                'error': 'Missing required parameters: left_source_id, right_source_id'
            }, status=400)
        
        # Get DuckDB connection
        try:
            from datasets.data_access_layer import unified_data_access
            
            conn = unified_data_access.duckdb_connection
            
            if not conn:
                unified_data_access._ensure_duckdb_connection()
                conn = unified_data_access.duckdb_connection
            
            if not conn:
                return JsonResponse({
                    'error': 'Could not connect to database'
                }, status=500)
        
        except Exception as conn_error:
            return JsonResponse({
                'error': f'Database connection failed: {str(conn_error)}'
            }, status=500)
        
        # Check join readiness
        from utils.data_source_sync import DataSourceSyncManager
        
        readiness_report = DataSourceSyncManager.get_join_readiness_report(
            left_source_id, right_source_id, request.user, conn
        )
        
        return JsonResponse({
            'success': True,
            'readiness_report': readiness_report,
            'ready_for_join': readiness_report.get('ready_for_join', False)
        })
        
    except Exception as e:
        logger.error(f"Error checking join readiness: {e}")
        return JsonResponse({
            'error': f'Failed to check join readiness: {str(e)}'
        }, status=500)

@login_required
@require_http_methods(["GET"])
def get_available_sources_for_joins(request):
    """Get list of data sources available for JOIN operations"""
    try:
        # Get DuckDB connection
        try:
            from datasets.data_access_layer import unified_data_access
            
            conn = unified_data_access.duckdb_connection
            
            if not conn:
                unified_data_access._ensure_duckdb_connection()
                conn = unified_data_access.duckdb_connection
            
            if not conn:
                return JsonResponse({
                    'error': 'Could not connect to database'
                }, status=500)
        
        except Exception as conn_error:
            return JsonResponse({
                'error': f'Database connection failed: {str(conn_error)}'
            }, status=500)
        
        # Get available sources
        from utils.data_source_sync import DataSourceSyncManager
        
        available_sources = DataSourceSyncManager.get_available_data_sources_for_joins(
            request.user, conn
        )
        
        return JsonResponse({
            'success': True,
            'available_sources': available_sources,
            'total_available': len(available_sources),
            'message': f'Found {len(available_sources)} data sources ready for joining'
        })
        
    except Exception as e:
        logger.error(f"Error getting available sources: {e}")
        return JsonResponse({
            'error': f'Failed to get available sources: {str(e)}'
        }, status=500)

@login_required
@require_http_methods(["POST"])
@csrf_exempt
def suggest_join_alternatives(request):
    """Suggest alternative data sources for JOIN operations"""
    try:
        data = json.loads(request.body)
        
        left_source_id = data.get('left_source_id')
        right_source_id = data.get('right_source_id')
        
        if not left_source_id or not right_source_id:
            return JsonResponse({
                'error': 'Missing required parameters: left_source_id, right_source_id'
            }, status=400)
        
        # Get DuckDB connection
        try:
            from datasets.data_access_layer import unified_data_access
            
            conn = unified_data_access.duckdb_connection
            
            if not conn:
                unified_data_access._ensure_duckdb_connection()
                conn = unified_data_access.duckdb_connection
            
            if not conn:
                return JsonResponse({
                    'error': 'Could not connect to database'
                }, status=500)
        
        except Exception as conn_error:
            return JsonResponse({
                'error': f'Database connection failed: {str(conn_error)}'
            }, status=500)
        
        # Get suggestions
        from utils.data_source_sync import DataSourceSyncManager
        
        suggestions = DataSourceSyncManager.suggest_join_alternatives(
            left_source_id, right_source_id, request.user, conn
        )
        
        return JsonResponse({
            'success': True,
            'suggestions': suggestions
        })
        
    except Exception as e:
        logger.error(f"Error getting join alternatives: {e}")
        return JsonResponse({
            'error': f'Failed to get join alternatives: {str(e)}'
        }, status=500) 