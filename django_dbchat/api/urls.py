"""
API application URL configuration
"""
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views

app_name = 'api'

# DRF router for viewsets
router = DefaultRouter()
# TODO: Register viewsets here when they are created
# router.register(r'users', UserViewSet)
# router.register(r'dashboards', DashboardViewSet)

urlpatterns = [
    # DRF browsable API
    path('', include(router.urls)),
    
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
    
    # API documentation (will be added with drf-spectacular)
    # path('schema/', SpectacularAPIView.as_view(), name='schema'),
    # path('docs/', SpectacularSwaggerView.as_view(url_name='api:schema'), name='swagger-ui'),
] 