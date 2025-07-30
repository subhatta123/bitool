"""
Core URL Configuration
Handles routing for core application functionality
"""

from django.urls import path
from . import views

app_name = 'core'

urlpatterns = [
    # Health check endpoint (for Docker and monitoring)
    path('health/', views.health_check, name='health_check'),
    
    # Main application pages
    path('', views.home, name='home'),
    path('query/', views.query, name='query'),
    path('query/history/', views.query_history, name='query_history'),
    path('query/results/', views.query_results, name='query_results'),
    
    # Configuration pages
    path('llm-config/', views.llm_config, name='llm_config'),
    path('llm-config/test-openai/', views.test_llm_connection, name='test_llm_connection'),
    path('llm-config/test-ollama/', views.test_ollama_connection, name='test_ollama_connection'),
    path('llm-config/save-openai/', views.save_llm_config_ajax, name='save_llm_config_ajax'),
    path('llm-config/save-ollama/', views.save_ollama_config_ajax, name='save_ollama_config_ajax'),
    path('email-config/', views.email_config, name='email_config'),
    path('email-config/test/', views.test_email_config, name='test_email_config'),
] 