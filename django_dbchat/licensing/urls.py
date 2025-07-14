"""
URL configuration for licensing app
"""

from django.urls import path
from . import views

app_name = 'licensing'

urlpatterns = [
    # Admin authentication
    path('admin/login/', views.admin_login, name='admin_login'),
    
    # License management dashboard
    path('admin/', views.dashboard, name='dashboard'),
    path('admin/dashboard/', views.dashboard, name='admin_dashboard'),
    
    # License management
    path('admin/licenses/', views.license_list, name='license_list'),
    path('admin/licenses/create/', views.create_license, name='create_license'),
    path('admin/licenses/<str:license_code>/', views.license_detail, name='license_detail'),
    
    # User license management
    path('admin/assign/', views.assign_license, name='assign_license'),
    path('admin/users/<str:username>/licenses/', views.user_licenses, name='user_licenses'),
    path('admin/users/<str:username>/revoke/', views.revoke_license, name='revoke_license'),
    
    # License validation logs
    path('admin/logs/', views.validation_logs, name='validation_logs'),
    
    # API endpoints
    path('api/validate/', views.validate_license_api, name='validate_license_api'),
    path('api/generate/', views.generate_license_api, name='generate_license_api'),
    path('api/status/', views.license_status_api, name='license_status_api'),
    
    # User-facing pages
    path('required/', views.license_required_page, name='license_required'),
] 