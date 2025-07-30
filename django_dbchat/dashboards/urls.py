
from django.urls import path
from . import views

app_name = 'dashboards'

urlpatterns = [
    path('', views.dashboard_list, name='list'),
    path('create/', views.dashboard_create, name='create'),
    path('<uuid:dashboard_id>/', views.dashboard_detail, name='detail'),
    path('api/list/', views.dashboard_list_api, name='api_list'),
    path('<uuid:dashboard_id>/add-item/', views.add_item_to_dashboard, name='add_item'),
    path('create-with-item/', views.create_dashboard_with_item, name='create_with_item'),
    
    # Dashboard management endpoints
    path('<uuid:dashboard_id>/update/', views.update_dashboard, name='update'),
    path('<uuid:dashboard_id>/delete/', views.delete_dashboard, name='delete'),
    path('<uuid:dashboard_id>/share/', views.share_dashboard, name='share'),
    
    # New dashboard item and scheduling endpoints
    path('item/<uuid:item_id>/delete/', views.delete_dashboard_item, name='delete_item'),
    path('<uuid:dashboard_id>/schedule-email/', views.schedule_dashboard_email, name='schedule_email'),
    path('<uuid:dashboard_id>/export/', views.export_dashboard_pdf_png, name='export'),
    path('<uuid:dashboard_id>/scheduled-emails/', views.dashboard_scheduled_emails, name='scheduled_emails'),
    path('cancel-email/<int:task_id>/', views.cancel_scheduled_email, name='cancel_email'),
    
    # API endpoints
    path('api/dashboard-item/<uuid:item_id>/data/', views.dashboard_item_data, name='item_data'),
    path('api/users/', views.users_api, name='users_api'),
]
