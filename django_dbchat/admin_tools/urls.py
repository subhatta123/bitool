from django.urls import path
from . import views

app_name = 'admin_tools'

urlpatterns = [
    # Log viewer interface
    path('logs/', views.AdminLoggerView.as_view(), name='logs'),
    
    # Log data API
    path('api/logs/data/', views.LogDataView.as_view(), name='log_data'),
    path('api/logs/stats/', views.get_log_stats, name='log_stats'),
    
    # Log file management
    path('api/logs/download/<str:filename>/', views.download_log_file, name='download_log'),
    path('api/logs/clear/<str:filename>/', views.clear_log_file, name='clear_log'),
] 