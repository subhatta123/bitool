"""
URL configuration for dbchat_project.
"""
from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from django.views.generic import RedirectView

urlpatterns = [
    # Admin interface
    path('admin/', admin.site.urls),
    
    # Admin tools (logs, etc.) - requires staff permissions
    path('admin/', include('admin_tools.urls')),
    
    # License management URLs
    path('licensing/', include('licensing.urls')),
    
    # Authentication URLs
    path('accounts/', include('accounts.urls')),
    
    # Core application URLs (includes home page)
    path('', include('core.urls')),
    
    # Dataset management URLs
    path('datasets/', include('datasets.urls')),
    
    # Dashboard URLs
    path('dashboards/', include('dashboards.urls')),
    
    # API URLs
    path('api/', include('api.urls')),
]

# Serve media files in development
if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT) 