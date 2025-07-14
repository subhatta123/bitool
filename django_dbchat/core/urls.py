"""
Core application URL configuration
"""
from django.urls import path
from django.views.generic import TemplateView
from . import views

app_name = 'core'

urlpatterns = [
    # Home page
    path('', views.home, name='home'),
    
    # Query interface
    path('query/', views.QueryView.as_view(), name='query'),
    
    # Query history page
    path('query/history/', views.QueryHistoryView.as_view(), name='query_history_page'),
    
    # Query result display
    path('query/result/<str:result_id>/', views.QueryResultView.as_view(), name='query_result'),
    
    # Test chart generation (for debugging)
    path('test-chart/', views.test_chart_generation, name='test_chart'),
    
    # Clarification flow
    path('query/clarification/<str:session_id>/', views.ClarificationView.as_view(), name='clarification'),
    
    # Data source management
    path('api/data-sources/', views.DataSourceView.as_view(), name='data_sources_api'),
    path('api/data-sources/<str:source_id>/info/', views.DataSourceInfoView.as_view(), name='data_source_info'),
    
    # Query history API
    path('api/query-history/', views.query_history, name='query_history'),
    
    # Health check endpoint
    path('health/', views.health_check, name='health_check'),
    
    # LLM Configuration (admin only)
    path('llm-config/', views.llm_config, name='llm_config'),
    path('llm-config/test-openai/', views.test_openai_connection, name='test_openai'),
    path('llm-config/test-ollama/', views.test_ollama_connection, name='test_ollama'),
    path('llm-config/ollama-models/', views.get_ollama_models, name='ollama_models'),
    path('llm-config/save-ollama/', views.save_ollama_config, name='save_ollama_config'),
    path('llm-config/save-openai/', views.save_openai_config, name='save_openai_config'),
    
    # Email Configuration (admin only)
    path('email-config/', views.email_config, name='email_config'),
    path('email-config/save/', views.save_email_config, name='save_email_config'),
    path('email-config/test/', views.test_email_config, name='test_email_config'),
    path('email-config/status/', views.get_email_status, name='email_status'),
] 