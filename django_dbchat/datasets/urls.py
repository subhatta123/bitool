"""
Dataset app URLs
"""
from django.urls import path
from . import views

app_name = 'datasets'

urlpatterns = [
    # Data source list and detail
    path('', views.DataSourceListView.as_view(), name='list'),
    path('<uuid:pk>/', views.DataSourceDetailView.as_view(), name='detail'),
    
    # Upload CSV
    path('upload-csv/', views.DataSourceCreateView.as_view(), name='create'),
    path('create-database/', views.data_source_create_database, name='create_database'),
    
    # APIs - Test connections
    path('api/create/', views.data_source_create_api, name='data_source_create_api'),
    
    # Connection testing endpoints  
    path('<uuid:pk>/test-connection/', views.data_source_test_connection, name='test_connection'),
    path('test/database/', views.data_source_test_database_connection, name='test_database_connection'),
    path('test/api/', views.data_source_test_api_connection, name='test_api_connection'),
    path('get-tables/', views.get_database_tables, name='get_database_tables'),
    
    # ETL operations  
    path('etl-operations/', views.etl_operations, name='etl_operations'),
    path('etl-schedules/', views.etl_schedules, name='etl_schedules'),
    path('api/check-overdue-jobs/', views.check_and_run_overdue_jobs, name='check_overdue_jobs'),
    path('api/etl-diagnostics/', views.etl_scheduling_diagnostics, name='etl_diagnostics'),
    
    # Semantic layer management
    path('semantic/', views.SemanticLayerView.as_view(), name='semantic'),
    
    # Data integration interface
    path('integration/', views.DataIntegrationView.as_view(), name='integration'),
    
    # Data source editing and sharing
    path('<uuid:pk>/edit/', views.DataSourceDetailView.as_view(), name='edit'),
    path('<uuid:pk>/share/', views.DataSourceShareView.as_view(), name='share'),
    
    # Data source status checking
    path('api/check-data-readiness/', views.check_data_readiness, name='check_data_readiness'),
    path('api/status/<uuid:pk>/', views.check_individual_data_source_status, name='check_data_source_status'),
    
    # Schema API endpoints
    path('api/schema/<uuid:pk>/', views.get_data_source_schema_api, name='get_data_source_schema'),
    
    # ADDED: Frontend-expected URL pattern for data source schema
    path('api/data-sources/<uuid:pk>/schema/', views.get_data_source_schema_api, name='get_data_source_schema_alt'),
    
    # Execute ETL transformations
    path('api/execute-etl-transformation/', views.execute_etl_transformation, name='execute_etl_transformation'),
    
    # ADDED: Frontend-expected URL pattern for ETL transform
    path('api/etl/transform/', views.execute_etl_transformation, name='etl_transform'),
    
    # ADDED: Additional frontend-expected ETL endpoints
    path('api/etl/join/', views.execute_etl_join, name='etl_join'),
    path('api/etl/union/', views.execute_etl_union, name='etl_union'),
    path('api/etl/aggregate/', views.execute_etl_aggregate, name='etl_aggregate'),
    path('api/etl/results/<uuid:operation_id>/', views.get_etl_results, name='etl_results'),
    path('api/etl/create-source-from-result/', views.create_data_source_from_etl_result, name='etl_create_source_from_result'),
    
    # CSV upload and analysis endpoints
    path('api/csv/analyze/', views.analyze_csv_structure, name='analyze_csv_structure'),
    path('api/csv/preview/', views.preview_csv_parsing, name='preview_csv_parsing'),
    path('api/csv/upload-enhanced/', views.upload_csv_with_enhanced_options, name='upload_csv_enhanced'),
    
    # Semantic layer API endpoints
    path('api/semantic/tables/<int:table_id>/', views.get_semantic_table_api, name='get_semantic_table'),
    path('api/semantic/tables/<int:table_id>/columns/', views.get_semantic_table_columns_api, name='get_semantic_table_columns'),
    path('api/semantic/tables/<int:table_id>/update/', views.update_semantic_table_api, name='update_semantic_table'),
    path('api/semantic/tables/<int:table_id>/delete/', views.delete_semantic_table_api, name='delete_semantic_table'),
    path('api/semantic/metrics/<int:metric_id>/', views.get_semantic_metric_api, name='get_semantic_metric'),
    path('api/semantic/metrics/<int:metric_id>/update/', views.update_semantic_metric_api, name='update_semantic_metric'),
    path('api/semantic/metrics/<int:metric_id>/delete/', views.delete_semantic_metric_api, name='delete_semantic_metric'),
    path('api/semantic/columns/<int:column_id>/', views.get_semantic_column_api, name='get_semantic_column'),
    path('api/semantic/columns/<int:column_id>/update/', views.update_semantic_column_api, name='update_semantic_column'),
    path('api/semantic/columns/<int:column_id>/delete/', views.delete_semantic_column_api, name='delete_semantic_column'),
    
    # Business Metrics API endpoints
    path('api/business-metrics/create/', views.create_business_metric_api, name='create_business_metric'),
    path('api/business-metrics/validate-formula/', views.validate_business_metric_formula, name='validate_business_metric_formula'),
    path('api/business-metrics/list/', views.list_business_metrics, name='list_business_metrics'),
    path('api/business-metrics/<int:metric_id>/', views.get_business_metric_detail, name='get_business_metric_detail'),
    path('api/business-metrics/<int:metric_id>/update/', views.update_business_metric, name='update_business_metric'),
    path('api/business-metrics/<int:metric_id>/delete/', views.delete_business_metric, name='delete_business_metric'),
    
    # Legacy table columns API (for backward compatibility)
    path('api/table/<int:table_id>/columns/', views.get_table_columns_api, name='get_table_columns'),
    
    # ETL Scheduling URLs - These are the ones we just implemented
    path('api/scheduled-etl-jobs/', views.scheduled_etl_jobs_list, name='scheduled_etl_jobs_list'),
    path('api/scheduled-etl-jobs/create/', views.create_scheduled_etl_job, name='create_scheduled_etl_job'),
    path('api/scheduled-etl-jobs/<uuid:job_id>/', views.scheduled_etl_job_detail, name='scheduled_etl_job_detail'),
    path('api/scheduled-etl-jobs/<uuid:job_id>/run/', views.run_scheduled_etl_job_now, name='run_scheduled_etl_job_now'),
    path('api/scheduled-etl-jobs/<uuid:job_id>/status/', views.scheduled_etl_job_status, name='scheduled_etl_job_status'),
    path('api/scheduled-etl-jobs/<uuid:job_id>/enable/', views.enable_scheduled_etl_job, name='enable_scheduled_etl_job'),
    path('api/scheduled-etl-jobs/<uuid:job_id>/disable/', views.disable_scheduled_etl_job, name='disable_scheduled_etl_job'),
    path('api/scheduled-etl-jobs/<uuid:job_id>/delete/', views.delete_scheduled_etl_job, name='delete_scheduled_etl_job'),
    path('api/scheduled-etl-jobs/<uuid:job_id>/logs/', views.scheduled_etl_job_logs, name='scheduled_etl_job_logs'),
] 