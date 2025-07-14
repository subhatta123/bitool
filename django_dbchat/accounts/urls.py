"""
URL configuration for accounts app.
"""
from django.urls import path
from . import views

app_name = 'accounts'

urlpatterns = [
    # Authentication URLs
    path('login/', views.LoginView.as_view(), name='login'),
    path('logout/', views.logout_view, name='logout'),
    path('register/', views.LoginView.as_view(), name='register'),  # Temporary redirect to login
    
    # User profile URLs
    path('profile/', views.ProfileView.as_view(), name='profile'),
    path('profile/edit/', views.ProfileUpdateView.as_view(), name='profile_edit'),
    path('preferences/', views.user_preferences, name='preferences'),
    
    # Admin user management URLs
    path('users/', views.UserManagementView.as_view(), name='user_management'),
    path('users/create/', views.UserCreateView.as_view(), name='user_create'),
    path('users/<int:pk>/edit/', views.UserUpdateView.as_view(), name='user_edit'),
    path('users/<int:pk>/delete/', views.UserDeleteView.as_view(), name='user_delete'),
    
    # License assignment URLs
    path('assign-license/', views.assign_license_to_user, name='assign_license'),
    path('users/<int:user_id>/assign-license/', views.assign_license_to_user, name='assign_license_to_user'),
    
    # AJAX endpoints
    path('users/<int:user_id>/roles/', views.update_user_roles, name='update_user_roles'),
    path('users/<int:user_id>/toggle-active/', views.toggle_user_active, name='toggle_user_active'),
    path('users/<int:user_id>/reset-password/', views.reset_user_password, name='reset_user_password'),
] 