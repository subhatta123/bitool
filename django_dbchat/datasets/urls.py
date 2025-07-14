"""
Datasets application URL configuration
"""
from django.urls import path
from . import views
from . import postgresql_views

app_name = 'datasets'

urlpatterns = [
    # Data source list page
    path('', views.DataSourceListView.as_view(), name='list'),
    
    # Create new data source
    path('create/', views.DataSourceCreateView.as_view(), name='create'),
    
    # View specific data source
    path('<uuid:pk>/', views.DataSourceDetailView.as_view(), name='detail'),
    
    # Edit data source (redirect to detail for now)
    path('<uuid:pk>/edit/', views.DataSourceDetailView.as_view(), name='edit'),
    
    # Test data source connection
    path('<uuid:pk>/test/', views.DataSourceTestView.as_view(), name='test'),
    
    # Share data source
    path('<uuid:pk>/share/', views.DataSourceShareView.as_view(), name='share'),
    
    # Data integration interface
    path('integration/', views.DataIntegrationView.as_view(), name='integration'),
    
    # Semantic layer management
    path('semantic/', views.SemanticLayerView.as_view(), name='semantic'),
    path('<uuid:pk>/semantic/', views.SemanticLayerView.as_view(), name='semantic_detail'),
    
    # ETL operations
    path('etl/', views.etl_operations, name='etl'),
    path('etl-operations/', views.etl_operations, name='etl_operations'),
    
    # Data source creation endpoints
    
    # Enhanced CSV processing endpoints
    path('api/csv/analyze/', views.analyze_csv_structure, name='analyze_csv_structure'),
    path('api/csv/preview/', views.preview_csv_parsing, name='preview_csv_parsing'),
    path('api/csv/upload-enhanced/', views.upload_csv_with_enhanced_options, name='upload_csv_enhanced'),
    
    path('create/database/', views.data_source_create_database, name='create_database'),
    path('create/api/', views.data_source_create_api, name='create_api'),
    
    # Connection testing endpoints  
    path('<uuid:pk>/test-connection/', views.data_source_test_connection, name='test_connection'),
    path('test/database/', views.data_source_test_database_connection, name='test_database_connection'),
    path('test/api/', views.data_source_test_api_connection, name='test_api_connection'),
    
    # Workflow endpoints
    path('<uuid:pk>/generate-semantic/', views.generate_semantic_layer, name='generate_semantic'),
    path('semantic/check-readiness/', views.check_data_readiness, name='check_data_readiness'),
    path('api/data-source/<uuid:pk>/check-status/', views.check_individual_data_source_status, name='check_individual_status'),
    
    # API endpoints
    path('api/data-sources/<uuid:pk>/schema/', views.get_data_source_schema_api, name='api_schema'),
    path('api/etl/transform/', views.execute_etl_transformation, name='api_etl_transform'),
    path('api/etl/join/', views.execute_etl_join, name='api_etl_join'),
    path('api/etl/union/', views.execute_etl_union, name='api_etl_union'),
    path('api/etl/aggregate/', views.execute_etl_aggregate, name='api_etl_aggregate'),
    path('api/etl/results/<str:operation_id>/', views.get_etl_results, name='api_etl_results'),
    path('api/etl/validate/', views.validate_etl_transformations, name='api_etl_validate'),
    
    # Enhanced semantic layer API endpoints
    path('api/semantic/table/<str:table_id>/', views.get_semantic_table_api, name='api_semantic_table'),
    path('api/semantic/table/<str:table_id>/columns/', views.get_semantic_table_columns_api, name='api_semantic_table_columns'),
    path('api/semantic/table/<str:table_id>/update/', views.update_semantic_table_api, name='api_semantic_table_update'),
    path('api/semantic/table/<str:table_id>/delete/', views.delete_semantic_table_api, name='api_semantic_table_delete'),
    path('api/semantic/metric/<str:metric_id>/', views.get_semantic_metric_api, name='api_semantic_metric'),
    path('api/semantic/metric/<str:metric_id>/update/', views.update_semantic_metric_api, name='api_semantic_metric_update'),
    path('api/semantic/metric/<str:metric_id>/delete/', views.delete_semantic_metric_api, name='api_semantic_metric_delete'),
    
    # Semantic column API endpoints
    path('api/semantic/column/<str:column_id>/', views.get_semantic_column_api, name='api_semantic_column'),
    path('api/semantic/column/<str:column_id>/update/', views.update_semantic_column_api, name='api_semantic_column_update'),
    path('api/semantic/column/<str:column_id>/delete/', views.delete_semantic_column_api, name='api_semantic_column_delete'),
    
    # Cleanup utilities
    path('api/cleanup-duplicates/', views.cleanup_duplicate_sources, name='cleanup_duplicates'),
    
    # Delete all endpoints
    path('semantic/delete_all_data/', views.delete_all_semantic_data_api, name='delete_all_semantic_data'),
    path('semantic/delete_all_metrics/', views.delete_all_business_metrics_api, name='delete_all_business_metrics'),
    
    # Business metrics diagnostic and regeneration endpoints
    path('api/business-metrics/diagnose/', views.diagnose_business_metrics_api, name='api_business_metrics_diagnose'),
    path('api/business-metrics/regenerate/', views.regenerate_business_metrics_api, name='api_business_metrics_regenerate'),
    
    # Enhanced business metrics endpoints
    path('api/business-metrics/create/', views.create_business_metric_api, name='api_create_business_metric'),
    path('api/business-metrics/validate-formula/', views.validate_formula_api, name='api_validate_formula'),
    path('api/business-metrics/formula-suggestions/', views.get_formula_suggestions_api, name='api_formula_suggestions'),
    path('api/business-metrics/test-calculation/', views.test_metric_calculation_api, name='api_test_calculation'),
    path('api/table/<str:table_id>/columns/', views.get_table_columns_api, name='api_get_table_columns'),
    
    # Debug endpoint
    path('api/semantic/debug/', views.debug_semantic_layer_api, name='debug_semantic_layer'),
    
    # New force proceed ETL endpoint
    path('api/force-proceed-etl/', views.force_proceed_etl, name='api_force_proceed_etl'),
    
    # Create data source from ETL result
    path('api/etl/create-source-from-result/', views.create_data_source_from_etl_result, name='api_create_source_from_etl_result'),
    
    # PostgreSQL unified storage endpoints
    path('postgresql/', postgresql_views.postgresql_datasets_list, name='postgresql_datasets_list'),
    path('postgresql/upload/', postgresql_views.postgresql_upload_page, name='postgresql_upload_page'),
    path('postgresql/api/upload/', postgresql_views.upload_csv_postgresql, name='upload_csv_postgresql'),
    path('postgresql/api/stats/', postgresql_views.postgresql_dataset_stats, name='postgresql_dataset_stats'),
    path('postgresql/api/<str:table_name>/preview/', postgresql_views.postgresql_dataset_preview, name='postgresql_dataset_preview'),
    path('postgresql/api/<str:table_name>/delete/', postgresql_views.postgresql_dataset_delete, name='postgresql_dataset_delete'),
    path('postgresql/api/<str:table_name>/query/', postgresql_views.postgresql_query_dataset, name='postgresql_query_dataset'),
] 