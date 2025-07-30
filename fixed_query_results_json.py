from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.http import JsonResponse
from django.core.paginator import Paginator
from django.db.models import Q
import json
import logging
from core.models import QueryLog

logger = logging.getLogger(__name__)

@login_required
def query_results(request):
    """
    Display query results for a specific query.
    Handles JSONField data properly without trying to decode as bytes.
    """
    try:
        # Get the query parameter
        query_text = request.GET.get('q', '').strip()
        
        if not query_text:
            messages.error(request, 'No query specified.')
            return redirect('query')
        
        # Find the most recent query log entry for this user and query
        query_log = QueryLog.objects.filter(
            user=request.user,
            natural_query__icontains=query_text
        ).order_by('-created_at').first()
        
        if not query_log:
            messages.error(request, f'No query found matching: {query_text}')
            return redirect('query')
        
        # Handle the query_results field properly
        # Since it's a JSONField, it should already be deserialized
        query_results_data = query_log.query_results
        
        # If query_results is None or empty, handle gracefully
        if query_results_data is None:
            context = {
                'query_log': query_log,
                'results': None,
                'error': 'No results available for this query.',
                'query_text': query_text
            }
            return render(request, 'core/query_result.html', context)
        
        # If it's already a dict/list (JSON data), use it directly
        if isinstance(query_results_data, (dict, list)):
            results = query_results_data
        else:
            # If it's a string, try to parse it as JSON
            try:
                if isinstance(query_results_data, str):
                    results = json.loads(query_results_data)
                else:
                    # If it's some other type, convert to string representation
                    results = str(query_results_data)
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse query_results as JSON: {e}")
                results = str(query_results_data)
        
        # Prepare context for template
        context = {
            'query_log': query_log,
            'results': results,
            'query_text': query_text,
            'execution_time': query_log.execution_time,
            'status': query_log.status,
            'created_at': query_log.created_at,
            'final_sql': query_log.final_sql
        }
        
        return render(request, 'core/query_result.html', context)
        
    except Exception as e:
        logger.error(f"Error in query_results view: {e}")
        messages.error(request, f'An error occurred while retrieving query results: {str(e)}')
        return redirect('query') 