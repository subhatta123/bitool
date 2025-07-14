"""
Dashboards application URL configuration
"""
from django.urls import path
from . import views

app_name = 'dashboards'

urlpatterns = [
    # Dashboard list page
    path('', views.DashboardListView.as_view(), name='list'),
    
    # Create new dashboard
    path('create/', views.DashboardCreateView.as_view(), name='create'),
    
    # View specific dashboard
    path('<uuid:pk>/', views.DashboardDetailView.as_view(), name='detail'),
    
    # Edit dashboard
    path('<uuid:pk>/edit/', views.DashboardDetailView.as_view(), name='edit'),
    
    # Share dashboard
    path('<uuid:pk>/share/', views.DashboardShareView.as_view(), name='share'),
    
    # Export dashboard
    path('<uuid:pk>/export/', views.DashboardExportView.as_view(), name='export'),
    
    # Delete dashboard
    path('<uuid:pk>/delete/', views.DashboardDetailView.as_view(), name='delete'),
    
    # Clone dashboard
    path('<uuid:pk>/clone/', views.dashboard_clone, name='clone'),
    
    # Refresh dashboard data
    path('<uuid:pk>/refresh/', views.dashboard_data_refresh, name='refresh'),
    
    # Dashboard item management
    path('<uuid:pk>/items/', views.DashboardItemView.as_view(), name='items'),
    path('<uuid:pk>/items/<uuid:item_id>/', views.DashboardItemView.as_view(), name='item_detail'),
] 