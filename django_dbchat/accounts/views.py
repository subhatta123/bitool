"""
Authentication and user management views.
"""
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth import login, logout, authenticate
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.contrib import messages
from django.views.generic import ListView, CreateView, UpdateView, DeleteView, TemplateView
from django.urls import reverse_lazy
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.db.models import Q
from django.core.paginator import Paginator
import json
import logging

from .models import CustomUser
from .forms import CustomUserCreationForm, CustomUserChangeForm, LoginForm, UserLicenseAssignmentForm
from core.models import QueryLog

logger = logging.getLogger(__name__)


class LoginView(TemplateView):
    """User login view."""
    template_name = 'accounts/login.html'
    
    def get(self, request, *args, **kwargs):
        if request.user.is_authenticated:
            return redirect('core:query')
        return super().get(request, *args, **kwargs)
    
    def post(self, request, *args, **kwargs):
        form = LoginForm(request.POST)
        if form.is_valid():
            username = form.cleaned_data['username']
            password = form.cleaned_data['password']
            user = authenticate(request, username=username, password=password)
            
            if user is not None:
                if user.is_active:
                    login(request, user)
                    # Store login IP and track activity
                    user.last_login_ip = self.get_client_ip(request)
                    user.login_count = getattr(user, 'login_count', 0) + 1
                    user.save()
                    
                    # Log successful login
                    logger.info(f"User {username} logged in from IP {user.last_login_ip}")
                    
                    next_url = request.GET.get('next', 'core:query')
                    return redirect(next_url)
                else:
                    messages.error(request, 'Your account has been deactivated.')
            else:
                messages.error(request, 'Invalid username or password.')
                logger.warning(f"Failed login attempt for username: {username}")
        
        return render(request, self.template_name, {'form': form})
    
    def get_client_ip(self, request):
        """Get client IP address."""
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for:
            ip = x_forwarded_for.split(',')[0]
        else:
            ip = request.META.get('REMOTE_ADDR')
        return ip


@login_required
def logout_view(request):
    """User logout view."""
    username = request.user.username
    logout(request)
    logger.info(f"User {username} logged out")
    messages.success(request, 'You have been logged out successfully.')
    return redirect('accounts:login')


def is_admin(user):
    """Check if user is admin."""
    return user.is_authenticated and user.is_superuser


class UserManagementView(LoginRequiredMixin, UserPassesTestMixin, ListView):
    """Admin view for managing users."""
    model = CustomUser
    template_name = 'accounts/user_management.html'
    context_object_name = 'users'
    paginate_by = 20
    
    def test_func(self):
        return self.request.user.is_superuser
    
    def get_queryset(self):
        queryset = CustomUser.objects.all().order_by('username')
        
        # Apply search filter
        search_query = self.request.GET.get('search')
        if search_query:
            queryset = queryset.filter(
                Q(username__icontains=search_query) |
                Q(email__icontains=search_query) |
                Q(first_name__icontains=search_query) |
                Q(last_name__icontains=search_query)
            )
        
        # Apply role filter (SQLite compatible)
        role_filter = self.request.GET.get('role')
        if role_filter:
            # Use icontains for SQLite compatibility
            queryset = queryset.filter(roles__icontains=role_filter)
        
        return queryset
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        # Add user statistics
        context['total_users'] = CustomUser.objects.count()
        context['active_users'] = CustomUser.objects.filter(is_active=True).count()
        context['admin_users'] = CustomUser.objects.filter(roles__icontains='admin').count()
        
        # Add filter options
        context['search_query'] = self.request.GET.get('search', '')
        context['selected_role'] = self.request.GET.get('role', '')
        context['role_choices'] = ['admin', 'user', 'viewer']
        
        return context


class UserCreateView(LoginRequiredMixin, UserPassesTestMixin, CreateView):
    """Admin view for creating new users."""
    model = CustomUser
    form_class = CustomUserCreationForm
    template_name = 'accounts/user_form.html'
    success_url = reverse_lazy('accounts:user_management')
    
    def test_func(self):
        return self.request.user.is_admin()
    
    def form_valid(self, form):
        # Set default preferences for new users
        form.instance.preferences = {
            'theme': 'light',
            'notifications': True,
            'dashboard_refresh_interval': 300,
            'default_chart_type': 'bar',
            'timezone': 'UTC'
        }
        
        messages.success(self.request, f'User {form.instance.username} created successfully.')
        logger.info(f"Admin {self.request.user.username} created user {form.instance.username}")
        return super().form_valid(form)


class UserUpdateView(LoginRequiredMixin, UserPassesTestMixin, UpdateView):
    """Admin view for updating users."""
    model = CustomUser
    form_class = CustomUserChangeForm
    template_name = 'accounts/user_form.html'
    success_url = reverse_lazy('accounts:user_management')
    
    def test_func(self):
        return self.request.user.is_admin()
    
    def form_valid(self, form):
        messages.success(self.request, f'User {form.instance.username} updated successfully.')
        logger.info(f"Admin {self.request.user.username} updated user {form.instance.username}")
        return super().form_valid(form)


class UserDeleteView(LoginRequiredMixin, UserPassesTestMixin, DeleteView):
    """Admin view for deleting users."""
    model = CustomUser
    template_name = 'accounts/user_confirm_delete.html'
    success_url = reverse_lazy('accounts:user_management')
    
    def test_func(self):
        return self.request.user.is_admin()
    
    def delete(self, request, *args, **kwargs):
        user = self.get_object()
        if user == request.user:
            messages.error(request, 'You cannot delete your own account.')
            return redirect('accounts:user_management')
        
        username = user.username
        messages.success(request, f'User {username} deleted successfully.')
        logger.info(f"Admin {request.user.username} deleted user {username}")
        return super().delete(request, *args, **kwargs)


class ProfileView(LoginRequiredMixin, TemplateView):
    """User profile view with activity tracking."""
    template_name = 'accounts/profile.html'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        user = self.request.user
        
        context['user'] = user
        
        # Add user activity statistics - only count queries if user has data sources
        try:
            from datasets.models import DataSource
            data_source_count = DataSource.objects.filter(created_by=user).count()
            if data_source_count > 0:
                context['query_count'] = QueryLog.objects.filter(user=user).count()
                context['recent_queries'] = QueryLog.objects.filter(user=user).order_by('-created_at')[:5]
            else:
                context['query_count'] = 0
                context['recent_queries'] = []
        except (ImportError, Exception):
            context['query_count'] = 0
            context['recent_queries'] = []
        
        # Add dashboard statistics
        try:
            from dashboards.models import Dashboard
            context['dashboard_count'] = Dashboard.objects.filter(owner=user).count()
            context['shared_dashboard_count'] = Dashboard.objects.filter(shared_with_users=user).count()
        except (ImportError, Exception):
            context['dashboard_count'] = 0
            context['shared_dashboard_count'] = 0
        
        # Add data source statistics
        try:
            from datasets.models import DataSource
            context['data_source_count'] = DataSource.objects.filter(created_by=user).count()
        except (ImportError, Exception):
            context['data_source_count'] = 0
        
        return context


class ProfileUpdateView(LoginRequiredMixin, UpdateView):
    """User profile update view."""
    model = CustomUser
    fields = ['first_name', 'last_name', 'email']
    template_name = 'accounts/profile_form.html'
    success_url = reverse_lazy('accounts:profile')
    
    def get_object(self):
        return self.request.user
    
    def form_valid(self, form):
        messages.success(self.request, 'Profile updated successfully.')
        logger.info(f"User {self.request.user.username} updated their profile")
        return super().form_valid(form)


class UserPreferencesView(LoginRequiredMixin, TemplateView):
    """User preferences management view."""
    template_name = 'accounts/preferences.html'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['preferences'] = self.request.user.preferences or {}
        
        # Add preference categories
        context['preference_categories'] = {
            'appearance': {
                'theme': {'type': 'select', 'options': ['light', 'dark'], 'default': 'light'},
                'sidebar_collapsed': {'type': 'boolean', 'default': False},
            },
            'notifications': {
                'email_notifications': {'type': 'boolean', 'default': True},
                'dashboard_alerts': {'type': 'boolean', 'default': True},
                'query_completion': {'type': 'boolean', 'default': False},
            },
            'dashboard': {
                'default_chart_type': {'type': 'select', 'options': ['bar', 'line', 'pie', 'scatter'], 'default': 'bar'},
                'auto_refresh_interval': {'type': 'select', 'options': [60, 300, 600, 1800], 'default': 300},
                'items_per_page': {'type': 'select', 'options': [10, 20, 50], 'default': 20},
            },
            'query': {
                'save_query_history': {'type': 'boolean', 'default': True},
                'auto_format_sql': {'type': 'boolean', 'default': True},
                'query_timeout': {'type': 'select', 'options': [30, 60, 120, 300], 'default': 60},
            }
        }
        
        return context
    
    def post(self, request, *args, **kwargs):
        """Update user preferences via AJAX."""
        try:
            data = json.loads(request.body)
            user = request.user
            
            # Get current preferences or initialize empty dict
            preferences = user.preferences or {}
            
            # Update preferences
            for key, value in data.items():
                preferences[key] = value
            
            # Save updated preferences
            user.preferences = preferences
            user.save()
            
            logger.info(f"User {user.username} updated preferences: {list(data.keys())}")
            
            return JsonResponse({
                'success': True,
                'message': 'Preferences updated successfully',
                'preferences': preferences
            })
            
        except Exception as e:
            logger.error(f"Error updating preferences for user {request.user.username}: {e}")
            return JsonResponse({'error': str(e)}, status=400)


@login_required
@require_http_methods(["POST"])
def update_user_roles(request, user_id):
    """AJAX endpoint for updating user roles."""
    if not request.user.is_admin():
        return JsonResponse({'error': 'Permission denied'}, status=403)
    
    try:
        user = get_object_or_404(CustomUser, id=user_id)
        data = json.loads(request.body)
        roles = data.get('roles', [])
        
        # Validate roles
        valid_roles = ['admin', 'user', 'viewer']
        roles = [role for role in roles if role in valid_roles]
        
        user.roles = roles
        user.save()
        
        logger.info(f"Admin {request.user.username} updated roles for user {user.username}: {roles}")
        
        return JsonResponse({'success': True, 'roles': user.roles})
    
    except Exception as e:
        logger.error(f"Error updating roles for user {user_id}: {e}")
        return JsonResponse({'error': str(e)}, status=400)


@login_required
@require_http_methods(["POST"])
def toggle_user_active(request, user_id):
    """AJAX endpoint for toggling user active status."""
    if not request.user.is_admin():
        return JsonResponse({'error': 'Permission denied'}, status=403)
    
    try:
        user = get_object_or_404(CustomUser, id=user_id)
        
        if user == request.user:
            return JsonResponse({'error': 'Cannot deactivate your own account'}, status=400)
        
        user.is_active = not user.is_active
        user.save()
        
        action = 'activated' if user.is_active else 'deactivated'
        logger.info(f"Admin {request.user.username} {action} user {user.username}")
        
        return JsonResponse({'success': True, 'is_active': user.is_active})
    
    except Exception as e:
        logger.error(f"Error toggling active status for user {user_id}: {e}")
        return JsonResponse({'error': str(e)}, status=400)


@login_required
def user_preferences(request):
    """View and update user preferences."""
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            user = request.user
            preferences = user.preferences or {}
            
            for key, value in data.items():
                preferences[key] = value
            
            user.preferences = preferences
            user.save()
            
            return JsonResponse({'success': True, 'preferences': preferences})
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=400)
    
    return JsonResponse({'preferences': request.user.preferences or {}})


@login_required
def user_activity(request):
    """Get user activity data for profile dashboard."""
    try:
        user = request.user
        
        # Query activity
        query_logs = QueryLog.objects.filter(user=user).order_by('-created_at')[:10]
        
        # Dashboard activity
        try:
            from dashboards.models import Dashboard
            recent_dashboards = Dashboard.objects.filter(owner=user).order_by('-updated_at')[:5]
        except (ImportError, Exception):
            recent_dashboards = []
        
        activity_data = {
            'recent_queries': [
                {
                    'id': log.id,
                    'query': log.query[:100] + '...' if len(log.query) > 100 else log.query,
                    'status': log.status,
                    'created_at': log.created_at.isoformat(),
                    'rows_returned': log.rows_returned
                }
                for log in query_logs
            ],
            'recent_dashboards': [
                {
                    'id': str(dashboard.id),
                    'name': dashboard.name,
                    'updated_at': dashboard.updated_at.isoformat()
                }
                for dashboard in recent_dashboards
            ]
        }
        
        return JsonResponse(activity_data)
        
    except Exception as e:
        logger.error(f"Error fetching user activity for {request.user.username}: {e}")
        return JsonResponse({'error': str(e)}, status=500)


@login_required
@require_http_methods(["POST"])
def reset_user_password(request, user_id):
    """Admin endpoint for resetting user passwords."""
    if not request.user.is_admin():
        return JsonResponse({'error': 'Permission denied'}, status=403)
    
    try:
        user = get_object_or_404(CustomUser, id=user_id)
        data = json.loads(request.body)
        new_password = data.get('new_password')
        
        if not new_password or len(new_password) < 8:
            return JsonResponse({'error': 'Password must be at least 8 characters long'}, status=400)
        
        user.set_password(new_password)
        user.save()
        
        logger.info(f"Admin {request.user.username} reset password for user {user.username}")
        
        return JsonResponse({'success': True, 'message': 'Password reset successfully'})
        
    except Exception as e:
        logger.error(f"Error resetting password for user {user_id}: {e}")
        return JsonResponse({'error': str(e)}, status=500)


@login_required
def assign_license_to_user(request, user_id=None):
    """View for assigning license to a user"""
    if user_id:
        # Admin assigning license to another user
        if not request.user.is_superuser:
            messages.error(request, "You don't have permission to assign licenses to other users.")
            return redirect('accounts:user_management')
        
        target_user = get_object_or_404(CustomUser, id=user_id)
    else:
        # User assigning license to themselves
        target_user = request.user
    
    if request.method == 'POST':
        form = UserLicenseAssignmentForm(user=target_user, data=request.POST)
        if form.is_valid():
            success, message = form.assign_license()
            if success:
                messages.success(request, f"License assigned successfully to {target_user.username}")
                if user_id:
                    return redirect('accounts:user_management')
                else:
                    return redirect('accounts:profile')
            else:
                messages.error(request, f"Failed to assign license: {message}")
    else:
        form = UserLicenseAssignmentForm(user=target_user)
    
    # Check if user already has a license
    from licensing.models import get_user_license_info
    license_info = get_user_license_info(target_user)
    
    context = {
        'form': form,
        'target_user': target_user,
        'license_info': license_info,
        'is_admin_view': user_id is not None,
    }
    
    return render(request, 'accounts/assign_license.html', context) 