"""
License management admin views
"""

import json
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.contrib.auth import authenticate, login
from django.contrib import messages
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.views import View
from django.db.models import Count, Q
from django.utils import timezone
from django.core.paginator import Paginator
import logging

from .models import License, UserLicense, LicenseValidationLog
from .services import LicenseValidationService, LicenseGenerationService
from .decorators import admin_required

logger = logging.getLogger(__name__)


def admin_login(request):
    """Admin login view with hardcoded credentials"""
    if request.method == 'POST':
        username = request.POST.get('username')
        password = request.POST.get('password')
        
        # Check hardcoded admin credentials
        if username == 'admin' and password == 'admin123':
            try:
                # Get or create admin user
                admin_user, created = User.objects.get_or_create(
                    username='admin',
                    defaults={
                        'is_staff': True,
                        'is_superuser': True,
                        'email': 'admin@convabi.com',
                        'first_name': 'Admin',
                        'last_name': 'User',
                    }
                )
                
                if created:
                    admin_user.set_password('admin123')
                    admin_user.save()
                    logger.info("Created admin user with hardcoded credentials")
                
                # Authenticate and login
                user = authenticate(request, username=username, password=password)
                if user:
                    login(request, user)
                    logger.info(f"Admin user {username} logged in successfully")
                    return redirect('licensing:dashboard')
                else:
                    # If authentication fails, try to update password
                    admin_user.set_password('admin123')
                    admin_user.save()
                    user = authenticate(request, username=username, password=password)
                    if user:
                        login(request, user)
                        return redirect('licensing:dashboard')
                    else:
                        messages.error(request, "Authentication failed")
                        
            except Exception as e:
                logger.error(f"Error during admin login: {e}")
                messages.error(request, "Login error occurred")
        else:
            messages.error(request, "Invalid admin credentials")
    
    return render(request, 'licensing/admin_login.html')


@admin_required
def dashboard(request):
    """License management dashboard"""
    try:
        # Get license statistics
        total_licenses = License.objects.count()
        active_licenses = License.objects.filter(status='active').count()
        expired_licenses = License.objects.filter(status='expired').count()
        
        # Get user license statistics
        total_user_licenses = UserLicense.objects.count()
        active_user_licenses = UserLicense.objects.filter(is_active=True).count()
        
        # Get license type distribution
        license_types = License.objects.values('license_type').annotate(
            count=Count('license_type')
        ).order_by('license_type')
        
        # Get recent license activities
        recent_validations = LicenseValidationLog.objects.select_related('user').order_by('-timestamp')[:10]
        recent_assignments = UserLicense.objects.select_related('user', 'license').order_by('-assigned_at')[:10]
        
        # Get licenses expiring soon (within 30 days)
        expiring_soon = License.objects.filter(
            valid_until__lte=timezone.now() + timezone.timedelta(days=30),
            valid_until__gt=timezone.now(),
            status='active'
        ).count()
        
        context = {
            'total_licenses': total_licenses,
            'active_licenses': active_licenses,
            'expired_licenses': expired_licenses,
            'total_user_licenses': total_user_licenses,
            'active_user_licenses': active_user_licenses,
            'license_types': license_types,
            'recent_validations': recent_validations,
            'recent_assignments': recent_assignments,
            'expiring_soon': expiring_soon,
        }
        
        return render(request, 'licensing/dashboard.html', context)
        
    except Exception as e:
        logger.error(f"Error loading license dashboard: {e}")
        messages.error(request, "Error loading dashboard")
        return render(request, 'licensing/dashboard.html', {})


@admin_required
def license_list(request):
    """List all licenses with filtering and search"""
    try:
        licenses = License.objects.all().order_by('-issued_at')
        
        # Search functionality
        search_query = request.GET.get('search', '')
        if search_query:
            licenses = licenses.filter(
                Q(license_code__icontains=search_query) |
                Q(description__icontains=search_query) |
                Q(license_type__icontains=search_query)
            )
        
        # Filter by status
        status_filter = request.GET.get('status', '')
        if status_filter:
            licenses = licenses.filter(status=status_filter)
        
        # Filter by license type
        type_filter = request.GET.get('type', '')
        if type_filter:
            licenses = licenses.filter(license_type=type_filter)
        
        # Pagination
        paginator = Paginator(licenses, 25)
        page_number = request.GET.get('page')
        page_obj = paginator.get_page(page_number)
        
        context = {
            'page_obj': page_obj,
            'search_query': search_query,
            'status_filter': status_filter,
            'type_filter': type_filter,
            'total_count': licenses.count(),
        }
        
        return render(request, 'licensing/license_list.html', context)
        
    except Exception as e:
        logger.error(f"Error loading license list: {e}")
        messages.error(request, "Error loading license list")
        return render(request, 'licensing/license_list.html', {})


@admin_required
def license_detail(request, license_code):
    """View license details and usage statistics"""
    try:
        license_obj = get_object_or_404(License, license_code=license_code)
        
        # Get license usage statistics
        validation_service = LicenseValidationService()
        usage_stats = validation_service.get_license_usage_stats(license_code)
        
        # Get user assignments
        user_assignments = UserLicense.objects.filter(
            license=license_obj
        ).select_related('user').order_by('-assigned_at')
        
        # Get validation logs
        validation_logs = LicenseValidationLog.objects.filter(
            license_code=license_code
        ).order_by('-timestamp')[:20]
        
        context = {
            'license': license_obj,
            'usage_stats': usage_stats,
            'user_assignments': user_assignments,
            'validation_logs': validation_logs,
        }
        
        return render(request, 'licensing/license_detail.html', context)
        
    except Exception as e:
        logger.error(f"Error loading license detail for {license_code}: {e}")
        messages.error(request, "Error loading license details")
        return redirect('licensing:license_list')


@admin_required
def create_license(request):
    """Create new license"""
    if request.method == 'POST':
        try:
            license_code = request.POST.get('license_code', '').strip().upper()
            license_type = request.POST.get('license_type')
            description = request.POST.get('description', '')
            max_users = int(request.POST.get('max_users', 1))
            valid_days = request.POST.get('valid_days', '')
            
            # Validate license code
            if not license_code:
                messages.error(request, "License code is required")
                return render(request, 'licensing/create_license.html')
            
            if License.objects.filter(license_code=license_code).exists():
                messages.error(request, "License code already exists")
                return render(request, 'licensing/create_license.html')
            
            # Calculate valid_until
            valid_until = None
            if valid_days:
                try:
                    days = int(valid_days)
                    valid_until = timezone.now() + timezone.timedelta(days=days)
                except ValueError:
                    messages.error(request, "Invalid validity period")
                    return render(request, 'licensing/create_license.html')
            
            # Create license
            license_obj = License.objects.create(
                license_code=license_code,
                license_type=license_type,
                description=description,
                max_users=max_users,
                valid_until=valid_until,
                created_by=request.user.username
            )
            
            logger.info(f"Created license {license_code} by {request.user.username}")
            messages.success(request, f"License {license_code} created successfully")
            
            return redirect('licensing:license_detail', license_code=license_code)
            
        except Exception as e:
            logger.error(f"Error creating license: {e}")
            messages.error(request, f"Error creating license: {str(e)}")
    
    return render(request, 'licensing/create_license.html')


@admin_required
def assign_license(request):
    """Assign license to user"""
    if request.method == 'POST':
        try:
            license_code = request.POST.get('license_code', '').strip().upper()
            username = request.POST.get('username', '').strip()
            
            if not license_code or not username:
                messages.error(request, "License code and username are required")
                return render(request, 'licensing/assign_license.html')
            
            # Get user
            try:
                user = User.objects.get(username=username)
            except User.DoesNotExist:
                messages.error(request, f"User '{username}' not found")
                return render(request, 'licensing/assign_license.html')
            
            # Assign license
            validation_service = LicenseValidationService()
            success, message = validation_service.assign_license_to_user(
                license_code, user, request.user.username
            )
            
            if success:
                messages.success(request, message)
                return redirect('licensing:user_licenses', username=username)
            else:
                messages.error(request, message)
                
        except Exception as e:
            logger.error(f"Error assigning license: {e}")
            messages.error(request, f"Error assigning license: {str(e)}")
    
    # Get available licenses for assignment
    available_licenses = License.objects.filter(
        status='active'
    ).order_by('license_type', 'license_code')
    
    # Get users for autocomplete
    users = User.objects.filter(is_active=True).order_by('username')
    
    context = {
        'available_licenses': available_licenses,
        'users': users,
    }
    
    return render(request, 'licensing/assign_license.html', context)


@admin_required
def user_licenses(request, username):
    """View user's license assignments"""
    try:
        user = get_object_or_404(User, username=username)
        
        # Get user's license assignments
        user_licenses = UserLicense.objects.filter(
            user=user
        ).select_related('license').order_by('-assigned_at')
        
        # Get user's current license info
        from .models import get_user_license_info
        license_info = get_user_license_info(user)
        
        context = {
            'target_user': user,
            'user_licenses': user_licenses,
            'license_info': license_info,
        }
        
        return render(request, 'licensing/user_licenses.html', context)
        
    except Exception as e:
        logger.error(f"Error loading user licenses for {username}: {e}")
        messages.error(request, "Error loading user licenses")
        return redirect('licensing:dashboard')


@admin_required
def revoke_license(request, username):
    """Revoke user's license"""
    if request.method == 'POST':
        try:
            user = get_object_or_404(User, username=username)
            reason = request.POST.get('reason', '')
            
            validation_service = LicenseValidationService()
            success, message = validation_service.revoke_user_license(
                user, reason, request.user.username
            )
            
            if success:
                messages.success(request, message)
            else:
                messages.error(request, message)
                
        except Exception as e:
            logger.error(f"Error revoking license for {username}: {e}")
            messages.error(request, f"Error revoking license: {str(e)}")
    
    return redirect('licensing:user_licenses', username=username)


@admin_required
def validation_logs(request):
    """View license validation logs"""
    try:
        logs = LicenseValidationLog.objects.select_related('user').order_by('-timestamp')
        
        # Filter by result
        result_filter = request.GET.get('result', '')
        if result_filter:
            logs = logs.filter(validation_result=result_filter == 'success')
        
        # Filter by license code
        license_filter = request.GET.get('license', '')
        if license_filter:
            logs = logs.filter(license_code__icontains=license_filter)
        
        # Pagination
        paginator = Paginator(logs, 50)
        page_number = request.GET.get('page')
        page_obj = paginator.get_page(page_number)
        
        context = {
            'page_obj': page_obj,
            'result_filter': result_filter,
            'license_filter': license_filter,
            'total_count': logs.count(),
        }
        
        return render(request, 'licensing/validation_logs.html', context)
        
    except Exception as e:
        logger.error(f"Error loading validation logs: {e}")
        messages.error(request, "Error loading validation logs")
        return render(request, 'licensing/validation_logs.html', {})


@csrf_exempt
@admin_required
def validate_license_api(request):
    """API endpoint to validate license code"""
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)
    
    try:
        data = json.loads(request.body)
        license_code = data.get('license_code', '').strip().upper()
        
        if not license_code:
            return JsonResponse({'error': 'License code is required'}, status=400)
        
        # Get client info
        ip_address = request.META.get('REMOTE_ADDR')
        user_agent = request.META.get('HTTP_USER_AGENT', '')
        
        # Validate license
        validation_service = LicenseValidationService()
        is_valid, message, license_info = validation_service.validate_license_code(
            license_code, request.user, ip_address, user_agent
        )
        
        return JsonResponse({
            'is_valid': is_valid,
            'message': message,
            'license_info': license_info
        })
        
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON'}, status=400)
    except Exception as e:
        logger.error(f"Error in validate_license_api: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)


@csrf_exempt
@admin_required
def generate_license_api(request):
    """API endpoint to generate license codes (for reference only)"""
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)
    
    try:
        data = json.loads(request.body)
        license_type = data.get('license_type', 'creator')
        count = min(int(data.get('count', 1)), 100)  # Max 100 at once
        max_users = int(data.get('max_users', 1))
        valid_days = data.get('valid_days')
        
        # Generate license codes using the service
        generation_service = LicenseGenerationService()
        licenses = generation_service.bulk_generate_licenses(
            license_type, count, max_users, valid_days
        )
        
        return JsonResponse({
            'licenses': licenses,
            'note': 'These are reference codes only. Use external tool for production.'
        })
        
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON'}, status=400)
    except Exception as e:
        logger.error(f"Error in generate_license_api: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)


def license_required_page(request):
    """Page shown when user needs a license"""
    return render(request, 'licensing/license_required.html')


def license_status_api(request):
    """API endpoint to check current user's license status"""
    if not request.user.is_authenticated:
        return JsonResponse({'error': 'Not authenticated'}, status=401)
    
    try:
        from .models import get_user_license_info
        license_info = get_user_license_info(request.user)
        
        return JsonResponse({
            'license_info': license_info
        })
        
    except Exception as e:
        logger.error(f"Error getting license status for user {request.user.username}: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)
