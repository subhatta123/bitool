"""
Core Views
Main application views for ConvaBI
"""

import json
import logging
from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.views.decorators.csrf import csrf_exempt
from django.db import models
from .models import LLMConfig, EmailConfig
from .decorators import admin_required, viewer_or_creator_required

logger = logging.getLogger(__name__)

def health_check(request):
    """Health check endpoint for Docker and monitoring"""
    return JsonResponse({'status': 'healthy', 'service': 'ConvaBI'})

@login_required
@viewer_or_creator_required
def home(request):
    """Home page view"""
    try:
        # Get user's data sources for dashboard
        data_sources = []
        try:
            from datasets.models import DataSource
            data_sources = DataSource.objects.filter(
                models.Q(created_by=request.user) | models.Q(shared_with_users=request.user)
            ).filter(status='active').distinct().order_by('name')
        except Exception as e:
            logger.warning(f"DataSource table not ready: {e}")
            data_sources = []
        
        # Get recent activity
        recent_queries = []
        try:
            from .models import QueryLog
            recent_queries = QueryLog.objects.filter(user=request.user).order_by('-created_at')[:5]
        except Exception as e:
            logger.warning(f"QueryLog table not ready: {e}")
        
        context = {
            'data_sources': data_sources,
            'recent_queries': recent_queries,
        }
        
        return render(request, 'core/home.html', context)
        
    except Exception as e:
        logger.error(f"Error in home view: {e}")
        messages.error(request, 'Error loading home page')
        return render(request, 'core/home.html', {'data_sources': [], 'recent_queries': []})

@csrf_exempt
@login_required
@viewer_or_creator_required
def query(request):
    """Main query interface for natural language to SQL"""
    if request.method == 'GET':
        # Render the query interface page
        try:
            # Get user's data sources for query interface
            data_sources = []
            try:
                from datasets.models import DataSource
                data_sources = DataSource.objects.filter(
                    models.Q(created_by=request.user) | models.Q(shared_with_users=request.user)
                ).filter(status='active').distinct().order_by('name')
            except Exception as e:
                logger.warning(f"DataSource table not ready: {e}")
                data_sources = []
            
            context = {
                'data_sources': data_sources,
            }
            
            return render(request, 'core/query.html', context)
            
        except Exception as e:
            logger.error(f"Error in query view: {e}")
            messages.error(request, 'Error loading query interface')
            return render(request, 'core/query.html', {'data_sources': []})
    
    elif request.method == 'POST':
        # Process natural language query
        try:
            data = json.loads(request.body)
            natural_query = data.get('query', '').strip()
            data_source_id = data.get('data_source_id', '').strip()
            
            if not natural_query:
                return JsonResponse({'error': 'Query is required'}, status=400)
            
            if not data_source_id:
                return JsonResponse({'error': 'Data source is required'}, status=400)
            
            logger.info(f"Processing natural language query: {natural_query[:100]}...")
            
            # Get data source
            from datasets.models import DataSource
            try:
                data_source = DataSource.objects.get(
                    id=data_source_id,
                    created_by=request.user
                )
            except DataSource.DoesNotExist:
                return JsonResponse({'error': 'Data source not found'}, status=404)
            
            # Process query using LLM service
            from services.dynamic_llm_service import DynamicLLMService
            from services.data_service import DataService
            
            llm_service = DynamicLLMService()
            data_service = DataService()
            
            # Get schema information for the data source
            schema_info = data_service.get_schema_info(data_source.connection_info, data_source)
            
            # Generate SQL from natural language
            sql_success, sql_query = llm_service.generate_sql(
                natural_query, 
                data_source=data_source
            )
            
            if not sql_success:
                # Check if it's a clarification request
                if sql_query and sql_query.startswith('CLARIFICATION_NEEDED:'):
                    clarification_question = sql_query.replace('CLARIFICATION_NEEDED:', '').strip()
                    
                    # Create session for clarification
                    import uuid
                    session_id = str(uuid.uuid4())
                
                    return JsonResponse({
                        'success': False,
                        'needs_clarification': True,
                        'clarification_question': clarification_question,
                        'session_id': session_id,
                        'query_context': {
                            'original_query': natural_query,
                            'data_source_id': data_source_id
                        }
                    })
                else:
                    return JsonResponse({
                        'error': f'Failed to generate SQL: {sql_query}',
                        'suggestion': 'Please try rephrasing your question or be more specific.'
                    }, status=400)
            
            # Execute the generated SQL
            execute_success, result = data_service.execute_query(
                sql_query, 
                data_source.connection_info, 
                user_id=request.user.id
            )
            
            if not execute_success:
                return JsonResponse({
                    'error': f'Query execution failed: {result}',
                    'generated_sql': sql_query,
                    'suggestion': 'The SQL was generated but failed to execute. Please check your data source.'
                }, status=400)
            
            # Convert result to JSON-serializable format
            if hasattr(result, 'to_dict'):
                result_data = result.to_dict('records')
                row_count = len(result_data)
            elif isinstance(result, list):
                result_data = result
                row_count = len(result_data)
            else:
                result_data = [{'result': str(result)}]
                row_count = 1
            
            # Log successful query
            try:
                from .models import QueryLog
                QueryLog.objects.create(
                    user=request.user,
                    natural_query=natural_query,
                    generated_sql=sql_query,
                    status='completed',
                    llm_provider=llm_service.preferred_provider,
                    execution_time=0.0
                )
            except Exception as e:
                logger.warning(f"Failed to log query: {e}")
            
            return JsonResponse({
                'success': True,
                'result_data': result_data,
                'row_count': row_count,
                'generated_sql': sql_query,
                'data_source_name': data_source.name,
                'redirect_url': f'/query/results/?q={natural_query[:100]}'
            })
            
        except json.JSONDecodeError:
            return JsonResponse({'error': 'Invalid JSON in request'}, status=400)
        except Exception as e:
            logger.error(f"Error processing natural language query: {e}")
            return JsonResponse({
                'error': f'Query processing failed: {str(e)}',
                'suggestion': 'Please try again or contact support if the issue persists.'
            }, status=500)
    
    else:
        return JsonResponse({'error': 'Method not allowed'}, status=405)

@login_required
@viewer_or_creator_required
def query_history(request):
    """Query history view"""
    try:
        # Get user's query history if QueryLog model exists
        query_logs = []
        try:
            from .models import QueryLog
            query_logs = QueryLog.objects.filter(user=request.user).order_by('-created_at')[:50]
        except (ImportError, AttributeError):
            # QueryLog model doesn't exist, create empty list
            pass
        
        context = {
            'query_logs': query_logs,
        }
        
        return render(request, 'core/query_history.html', context)
            
    except Exception as e:
        logger.error(f"Error in query_history view: {e}")
        messages.error(request, 'Error loading query history')
        return render(request, 'core/query_history.html', {'query_logs': []})


@login_required
@login_required
@login_required
def query_results(request):
    """Display query results page"""
    try:
        query_text = request.GET.get('q', '')
        
        if not query_text:
            return redirect('core:query')
        
        # Get the most recent query result for this query
        from .models import QueryLog
        latest_query = QueryLog.objects.filter(
            user=request.user,
            natural_query__icontains=query_text
        ).order_by('-created_at').first()
        
        if latest_query:
            # Handle query_results as JSON data safely
            try:
                query_results_data = latest_query.query_results
                if isinstance(query_results_data, dict):
                    import json
                    result_display = json.dumps(query_results_data, indent=2, default=str)
                elif query_results_data is None:
                    result_display = "No results available"
                else:
                    result_display = str(query_results_data)
            except Exception as e:
                import logging
                logger = logging.getLogger(__name__)
                logger.error(f"Error processing query_results data: {e}")
                result_display = "Error processing results data"
            
            return render(request, 'core/query_result.html', {
                'query': latest_query.natural_query or "Unknown query",
                'result': result_display,
                'sql': latest_query.final_sql or "No SQL available",
                'execution_time': latest_query.execution_time or 0,
                'created_at': latest_query.created_at
            })
        else:
            # If no history found, redirect to query page
            return redirect('core:query')
            
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Error in query_results view: {e}")
        # Always return a response, even on error
                # Create new config with all required fields
                llm_config = LLMConfig.objects.create(
                    provider=provider,
                    api_key=api_key or '',  # Ensure API key is not None
                    base_url=base_url or 'https://api.openai.com/v1',  # Default for OpenAI
                    model_name=model_name or 'gpt-4o',  # Default model
                    temperature=0.1,  # Default temperature
                    max_tokens=1000,  # Default max tokens
                    system_prompt='You are a helpful assistant that converts natural language to SQL queries.',  # Default prompt
                    additional_settings={},  # Empty dict for additional settings
                    is_active=True,
                    updated_by=request.user
                )
            
            messages.success(request, 'LLM configuration saved successfully')
            return redirect('core:llm_config')
                
        except Exception as e:
            logger.error(f"Error saving LLM config: {e}")
            messages.error(request, f'Error saving configuration: {str(e)}')
    
    # Get existing active config
    llm_config = LLMConfig.get_active_config()
    
    context = {
        'llm_config': llm_config,
    }
    
    return render(request, 'core/llm_config.html', context)

@login_required
@admin_required
def email_config(request):
    """Email configuration view"""
    if request.method == 'POST':
        try:
            smtp_host = request.POST.get('smtp_host')
            smtp_port_str = request.POST.get('smtp_port', '587').strip()
            smtp_port = int(smtp_port_str) if smtp_port_str else 587
            smtp_user = request.POST.get('smtp_user')
            smtp_password = request.POST.get('smtp_password')
            use_tls = request.POST.get('use_tls') == 'on'
            
            # Get or create global email config
            email_config = EmailConfig.objects.first()
            if email_config:
                # Update existing config
                email_config.smtp_host = smtp_host
                email_config.smtp_port = smtp_port
                email_config.smtp_username = smtp_user
                email_config.smtp_password = smtp_password
                email_config.use_tls = use_tls
                email_config.updated_by = request.user
                email_config.save()
            else:
                # Create new config
                email_config = EmailConfig.objects.create(
                    smtp_host=smtp_host,
                    smtp_port=smtp_port,
                    smtp_username=smtp_user,
                    smtp_password=smtp_password,
                    use_tls=use_tls,
                    sender_email=smtp_user,
                    updated_by=request.user
                )
            
            messages.success(request, 'Email configuration saved successfully')
            return redirect('core:email_config')
        
        except Exception as e:
            logger.error(f"Error saving email config: {e}")
            messages.error(request, f'Error saving configuration: {str(e)}')
    
    # Get existing global config
    email_config = EmailConfig.objects.first()
    
    context = {
        'email_config': email_config,
    }
    
    return render(request, 'core/email_config.html', context)

@csrf_exempt
@login_required
@admin_required
def test_llm_connection(request):
    """AJAX endpoint to test LLM connection"""
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)
    
    try:
        provider = request.POST.get('provider', 'openai')
        api_key = request.POST.get('api_key', '')
        base_url = request.POST.get('base_url', '')
        model_name = request.POST.get('model_name', 'gpt-3.5-turbo')
        
        # Basic validation
        if provider == 'openai' and not api_key:
            return JsonResponse({
                'success': False,
                'message': 'API key is required for OpenAI'
            })
        
        # Test the connection based on provider
        if provider == 'openai':
            try:
                from services.openai_compatibility_fix import create_openai_client_with_fallback
                success, client, error = create_openai_client_with_fallback(api_key)
                
                if success:
                    return JsonResponse({
                        'success': True,
                        'message': 'OpenAI connection successful!'
                    })
                else:
                    return JsonResponse({
                        'success': False,
                        'message': f'OpenAI connection failed: {error}'
                    })
                
            except Exception as e:
                return JsonResponse({
                    'success': False,
                    'message': f'OpenAI connection failed: {str(e)}'
                })
        
        elif provider == 'ollama':
            try:
                import requests
                url = f"{base_url}/api/tags"
                response = requests.get(url, timeout=10)
                
                if response.status_code == 200:
                    return JsonResponse({
                        'success': True,
                        'message': 'Ollama connection successful!'
                    })
                else:
                    return JsonResponse({
                        'success': False,
                        'message': f'Ollama connection failed: HTTP {response.status_code}'
                    })
                    
            except Exception as e:
                return JsonResponse({
                    'success': False,
                    'message': f'Ollama connection failed: {str(e)}'
                })
        
        else:
            return JsonResponse({
                'success': False,
                'message': f'Unsupported provider: {provider}'
            })
    
    except Exception as e:
        logger.error(f"Error testing LLM connection: {e}")
        return JsonResponse({
            'success': False,
            'message': f'Connection test failed: {str(e)}'
        }, status=500)

@csrf_exempt
@login_required
@admin_required
def test_ollama_connection(request):
    """AJAX endpoint to test Ollama connection"""
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)
    
    try:
        base_url = request.POST.get('base_url', 'http://ollama:11434')
        
        # Test Ollama connection
        try:
            import requests
            url = f"{base_url}/api/tags"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                return JsonResponse({
                    'success': True,
                    'message': 'Ollama connection successful!'
                })
            else:
                return JsonResponse({
                    'success': False,
                    'message': f'Ollama connection failed: HTTP {response.status_code}'
                })
                
        except Exception as e:
            return JsonResponse({
                'success': False,
                'message': f'Ollama connection failed: {str(e)}'
            })
    
    except Exception as e:
        logger.error(f"Error testing Ollama connection: {e}")
        return JsonResponse({
            'success': False,
            'message': f'Connection test failed: {str(e)}'
        }, status=500)

@csrf_exempt
@login_required
@admin_required
def save_llm_config_ajax(request):
    """AJAX endpoint to save LLM configuration"""
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)
    
    try:
        # Handle both JSON and form data
        if request.content_type == 'application/json':
            import json
            try:
                data = json.loads(request.body)
                # Handle different field names from frontend
                provider = data.get('provider', 'openai')  # Default to 'openai' for OpenAI config
                api_key = data.get('api_key')
                base_url = data.get('base_url', 'https://api.openai.com/v1')  # Default for OpenAI
                model_name = data.get('model_name') or data.get('model')  # Handle both field names
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error: {e}")
                return JsonResponse({
                    'success': False,
                    'message': 'Invalid JSON data'
                }, status=400)
        else:
            # Handle form data
            provider = request.POST.get('provider', 'openai')  # Default to 'openai' for OpenAI config
            api_key = request.POST.get('api_key')
            base_url = request.POST.get('base_url', 'https://api.openai.com/v1')  # Default for OpenAI
            model_name = request.POST.get('model_name') or request.POST.get('model')  # Handle both field names
        
        # Get the active LLM config or create a new one
        llm_config = LLMConfig.get_active_config()
        if llm_config:
            # Update existing config
            llm_config.provider = provider
            llm_config.api_key = api_key or ''  # Ensure API key is not None
            llm_config.base_url = base_url
            llm_config.model_name = model_name
            llm_config.updated_by = request.user
            llm_config.save()
        else:
            # Create new config with all required fields
            llm_config = LLMConfig.objects.create(
                provider=provider,
                api_key=api_key or '',  # Ensure API key is not None
                base_url=base_url or 'https://api.openai.com/v1',  # Default for OpenAI
                model_name=model_name or 'gpt-4o',  # Default model
                temperature=0.1,  # Default temperature
                max_tokens=1000,  # Default max tokens
                system_prompt='You are a helpful assistant that converts natural language to SQL queries.',  # Default prompt
                additional_settings={},  # Empty dict for additional settings
                is_active=True,
                updated_by=request.user
            )
        
        return JsonResponse({
            'success': True,
            'message': 'LLM configuration saved successfully'
        })
        
    except Exception as e:
        logger.error(f"Error saving LLM config: {e}")
        return JsonResponse({
            'success': False,
            'message': f'Error saving configuration: {str(e)}'
        }, status=500)

@login_required
@admin_required
def save_ollama_config_ajax(request):
    """AJAX endpoint to save Ollama configuration"""
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)
    
    try:
        base_url = request.POST.get('base_url', 'http://ollama:11434')
        model_name = request.POST.get('model_name', 'llama3.2:1b')
        
        # Get the active LLM config or create a new one
        llm_config = LLMConfig.get_active_config()
        if llm_config:
            # Update existing config
            llm_config.provider = 'ollama'
            llm_config.base_url = base_url
            llm_config.model_name = model_name
            llm_config.updated_by = request.user
            llm_config.save()
        else:
            # Create new config
            llm_config = LLMConfig.objects.create(
                provider='ollama',
                api_key='',  # Empty string for Ollama (no API key needed)
                base_url=base_url,
                model_name=model_name,
                updated_by=request.user,
                is_active=True
            )
        
        return JsonResponse({
            'success': True,
            'message': 'Ollama configuration saved successfully'
        })
        
    except Exception as e:
        logger.error(f"Error saving Ollama config: {e}")
        return JsonResponse({
            'success': False,
            'message': f'Error saving configuration: {str(e)}'
        }, status=500)

@csrf_exempt
@login_required
@admin_required
def test_email_config(request):
    """AJAX endpoint to test email configuration"""
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)
    
    try:
        smtp_host = request.POST.get('smtp_host')
        smtp_port_str = request.POST.get('smtp_port', '587').strip()
        smtp_port = int(smtp_port_str) if smtp_port_str else 587
        smtp_user = request.POST.get('smtp_user')
        smtp_password = request.POST.get('smtp_password')
        use_tls = request.POST.get('use_tls') == 'on'
        
        # Basic validation
        if not all([smtp_host, smtp_user, smtp_password]):
            return JsonResponse({
                'success': False,
                'message': 'SMTP host, username, and password are required'
            })
        
        # Test email connection
        try:
            import smtplib
            from email.mime.text import MIMEText
            
            # Create test message
            msg = MIMEText('This is a test email from ConvaBI')
            msg['Subject'] = 'ConvaBI Email Test'
            msg['From'] = smtp_user
            msg['To'] = smtp_user  # Send to self for testing
            
            # Connect and send
            if use_tls:
                server = smtplib.SMTP(smtp_host, smtp_port)
                server.starttls()
            else:
                server = smtplib.SMTP_SSL(smtp_host, smtp_port)
            
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
            server.quit()
            
            return JsonResponse({
                'success': True,
                'message': 'Email test successful! Check your inbox.'
            })
            
        except Exception as e:
            return JsonResponse({
                'success': False,
                'message': f'Email test failed: {str(e)}'
            })
    
    except Exception as e:
        logger.error(f"Error testing email configuration: {e}")
        return JsonResponse({
            'success': False,
            'message': f'Email test failed: {str(e)}'
        }, status=500) 