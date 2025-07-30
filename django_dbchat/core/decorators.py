from functools import wraps
from django.shortcuts import redirect
from django.contrib import messages
from django.http import HttpResponseForbidden

def admin_required(view_func):
    """Decorator to require admin privileges"""
    @wraps(view_func)
    def _wrapped_view(request, *args, **kwargs):
        if not request.user.is_authenticated:
            return redirect('login')
        
        # Check if user is superuser or has admin role
        if request.user.is_superuser:
            return view_func(request, *args, **kwargs)
        
        # Check for admin role in user profile if it exists
        try:
            if hasattr(request.user, 'profile') and request.user.profile.role == 'admin':
                return view_func(request, *args, **kwargs)
        except:
            pass
        
        messages.error(request, 'Admin privileges required')
        return HttpResponseForbidden('Admin privileges required')
    
    return _wrapped_view

def viewer_or_creator_required(view_func):
    """Decorator to require viewer or creator privileges"""
    @wraps(view_func)
    def _wrapped_view(request, *args, **kwargs):
        if not request.user.is_authenticated:
            return redirect('login')
        
        # Superusers have all privileges
        if request.user.is_superuser:
            return view_func(request, *args, **kwargs)
        
        # Check for viewer or creator role in user profile if it exists
        try:
            if hasattr(request.user, 'profile'):
                if request.user.profile.role in ['admin', 'creator', 'viewer']:
                    return view_func(request, *args, **kwargs)
        except:
            pass
        
        # If no profile or role system, allow all authenticated users
        return view_func(request, *args, **kwargs)
    
    return _wrapped_view

def creator_required(view_func):
    """Decorator to require creator privileges"""
    @wraps(view_func)
    def _wrapped_view(request, *args, **kwargs):
        if not request.user.is_authenticated:
            return redirect('login')
        
        # Superusers have all privileges
        if request.user.is_superuser:
            return view_func(request, *args, **kwargs)
        
        # Check for creator or admin role in user profile if it exists
        try:
            if hasattr(request.user, 'profile'):
                if request.user.profile.role in ['admin', 'creator']:
                    return view_func(request, *args, **kwargs)
        except:
            pass
        
        messages.error(request, 'Creator privileges required')
        return HttpResponseForbidden('Creator privileges required')
    
    return _wrapped_view 