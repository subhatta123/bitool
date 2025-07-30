"""
API application URL configuration
"""
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views

# Create a router for DRF ViewSets
router = DefaultRouter()

urlpatterns = [
    # DRF browsable API
    path('', include(router.urls)),
    
    # Dashboard API endpoints (frontend expects these URLs)
    path('dashboards/item/<uuid:item_id>/data/', views.dashboard_item_data_proxy, name='dashboard_item_data_proxy'),
    
    # ETL Operations API
    path('etl-operations/<uuid:operation_id>/', views.ETLOperationAPIView.as_view(), name='etl_operation'),
    path('etl-operations/<uuid:operation_id>/download/', views.etl_operation_download, name='etl_operation_download'),
    path('etl-operations/<uuid:operation_id>/rerun/', views.ETLOperationAPIView.as_view(), name='etl_operation_rerun'),
    
    # ETL Transforms API
    path('etl-transforms/', views.ETLTransformAPIView.as_view(), name='etl_transforms'),
    
    # Data Sources API
    path('data-sources/<uuid:source_id>/', views.DataSourceAPIView.as_view(), name='data_source'),
    path('data-sources/<uuid:source_id>/info/', views.DataSourceAPIView.as_view(), name='data_source_info'),
    path('data-sources/<uuid:source_id>/schema/', views.DataSourceSchemaAPIView.as_view(), name='data_source_schema'),
    path('data-preview/<uuid:source_id>/', views.data_preview, name='data_preview'),
    
    # Dashboard API
    path('execute-dashboard-query/', views.execute_dashboard_query, name='execute_dashboard_query'),
    
    # Users API for dashboard sharing
    path('users/', views.users_list_api, name='users_list'),
    
    # API documentation (will be added with drf-spectacular)
    # path('schema/', SpectacularAPIView.as_view(), name='schema'),
    # path('docs/', SpectacularSwaggerView.as_view(url_name='api:schema'), name='swagger-ui'),
] 