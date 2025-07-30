"""
Forms for user authentication and management.
"""
from django import forms
from django.contrib.auth.forms import UserCreationForm, UserChangeForm
from django.contrib.auth import authenticate
from .models import CustomUser


class LoginForm(forms.Form):
    """Form for user login."""
    username = forms.CharField(
        max_length=150,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Username',
            'required': True,
        })
    )
    password = forms.CharField(
        widget=forms.PasswordInput(attrs={
            'class': 'form-control',
            'placeholder': 'Password',
            'required': True,
        })
    )

    def clean(self):
        cleaned_data = super().clean()
        username = cleaned_data.get('username')
        password = cleaned_data.get('password')

        if username and password:
            # Additional validation can be added here
            pass

        return cleaned_data


class CustomUserCreationForm(UserCreationForm):
    """Form for creating new users."""
    email = forms.EmailField(
        required=True,
        widget=forms.EmailInput(attrs={
            'class': 'form-control',
            'placeholder': 'Email Address',
        })
    )
    first_name = forms.CharField(
        max_length=30,
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'First Name',
        })
    )
    last_name = forms.CharField(
        max_length=30,
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Last Name',
        })
    )
    license_code = forms.CharField(
        max_length=16,
        required=True,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Enter 16-digit license code',
            'style': 'text-transform: uppercase;',
            'maxlength': '16',
        }),
        help_text='Enter the 16-character license code provided by your administrator'
    )
    roles = forms.MultipleChoiceField(
        choices=[
            ('user', 'User'),
            ('creator', 'Creator'),
            ('viewer', 'Viewer'),
            ('admin', 'Administrator'),
        ],
        widget=forms.CheckboxSelectMultiple(),
        required=False,
        help_text='Select user roles (should match license type for proper permissions)',
        initial=['user']  # Default to user role
    )

    class Meta:
        model = CustomUser
        fields = ('username', 'email', 'first_name', 'last_name', 'password1', 'password2', 'license_code', 'roles')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Add Bootstrap classes to password fields
        self.fields['username'].widget.attrs.update({
            'class': 'form-control',
            'placeholder': 'Username',
        })
        self.fields['password1'].widget.attrs.update({
            'class': 'form-control',
            'placeholder': 'Password',
        })
        self.fields['password2'].widget.attrs.update({
            'class': 'form-control',
            'placeholder': 'Confirm Password',
        })

    def clean_license_code(self):
        license_code = self.cleaned_data.get('license_code', '').upper().strip()
        
        if not license_code:
            raise forms.ValidationError('License code is required.')
        
        if len(license_code) != 16:
            raise forms.ValidationError('License code must be exactly 16 characters.')
        
        if not license_code.isalnum():
            raise forms.ValidationError('License code must contain only letters and numbers.')
        
        # Validate license code with the licensing service
        from licensing.services import LicenseValidationService
        validation_service = LicenseValidationService()
        
        is_valid, message, license_info = validation_service.validate_license_code(license_code)
        
        if not is_valid:
            raise forms.ValidationError(f'Invalid license code: {message}')
        
        # Store license info for use in save method
        self.license_info = license_info
        
        return license_code

    def save(self, commit=True):
        user = super().save(commit=False)
        user.email = self.cleaned_data['email']
        user.first_name = self.cleaned_data['first_name']
        user.last_name = self.cleaned_data['last_name']
        
        if commit:
            user.save()
            
            # Set roles after saving
            roles = self.cleaned_data.get('roles', [])
            user.roles = list(roles)
            user.save()
            
            # Assign license to user
            license_code = self.cleaned_data.get('license_code')
            if license_code:
                from licensing.services import LicenseValidationService
                validation_service = LicenseValidationService()
                success, message = validation_service.assign_license_to_user(
                    license_code, user, "user_registration"
                )
                if not success:
                    # If license assignment fails, we should probably delete the user
                    # and raise an error, but for now we'll just log it
                    import logging
                    logger = logging.getLogger(__name__)
                    logger.error(f"Failed to assign license {license_code} to user {user.username}: {message}")
        
        return user


class CustomUserChangeForm(UserChangeForm):
    """Form for updating existing users."""
    email = forms.EmailField(
        required=True,
        widget=forms.EmailInput(attrs={
            'class': 'form-control',
        })
    )
    first_name = forms.CharField(
        max_length=30,
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
        })
    )
    last_name = forms.CharField(
        max_length=30,
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
        })
    )
    roles = forms.MultipleChoiceField(
        choices=[
            ('user', 'User'),
            ('creator', 'Creator'),
            ('viewer', 'Viewer'),
            ('admin', 'Administrator'),
        ],
        widget=forms.CheckboxSelectMultiple(),
        required=False,
        help_text='Select user roles (should match license type for proper permissions)'
    )
    is_active = forms.BooleanField(
        required=False,
        widget=forms.CheckboxInput(attrs={
            'class': 'form-check-input',
        }),
        help_text='Designates whether this user should be treated as active.'
    )

    class Meta:
        model = CustomUser
        fields = ('username', 'email', 'first_name', 'last_name', 'roles', 'is_active')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Remove password field from change form
        if 'password' in self.fields:
            del self.fields['password']
        
        # Add Bootstrap classes
        self.fields['username'].widget.attrs.update({
            'class': 'form-control',
        })
        
        # Set initial roles if instance exists
        if self.instance and self.instance.pk:
            if isinstance(self.instance.roles, list):
                self.fields['roles'].initial = self.instance.roles

    def save(self, commit=True):
        user = super().save(commit=False)
        
        if commit:
            user.save()
            # Set roles after saving
            roles = self.cleaned_data.get('roles', [])
            user.roles = list(roles)
            user.save()
        
        return user


class ProfileForm(forms.ModelForm):
    """Form for user profile updates."""
    email = forms.EmailField(
        required=True,
        widget=forms.EmailInput(attrs={
            'class': 'form-control',
        })
    )
    first_name = forms.CharField(
        max_length=30,
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
        })
    )
    last_name = forms.CharField(
        max_length=30,
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
        })
    )

    class Meta:
        model = CustomUser
        fields = ('first_name', 'last_name', 'email')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Make email field read-only for profile updates
        # Users should contact admin to change email
        self.fields['email'].widget.attrs['readonly'] = True
        self.fields['email'].help_text = 'Contact administrator to change email address.'


class PasswordChangeForm(forms.Form):
    """Form for changing user password."""
    current_password = forms.CharField(
        widget=forms.PasswordInput(attrs={
            'class': 'form-control',
            'placeholder': 'Current Password',
        }),
        help_text='Enter your current password.'
    )
    new_password1 = forms.CharField(
        widget=forms.PasswordInput(attrs={
            'class': 'form-control',
            'placeholder': 'New Password',
        }),
        help_text='Enter your new password.'
    )
    new_password2 = forms.CharField(
        widget=forms.PasswordInput(attrs={
            'class': 'form-control',
            'placeholder': 'Confirm New Password',
        }),
        help_text='Enter the same password as before, for verification.'
    )

    def __init__(self, user, *args, **kwargs):
        self.user = user
        super().__init__(*args, **kwargs)

    def clean_current_password(self):
        current_password = self.cleaned_data.get('current_password')
        if not self.user.check_password(current_password):
            raise forms.ValidationError('Current password is incorrect.')
        return current_password

    def clean(self):
        cleaned_data = super().clean()
        new_password1 = cleaned_data.get('new_password1')
        new_password2 = cleaned_data.get('new_password2')

        if new_password1 and new_password2:
            if new_password1 != new_password2:
                raise forms.ValidationError('New passwords do not match.')

        return cleaned_data

    def save(self):
        new_password = self.cleaned_data['new_password1']
        self.user.set_password(new_password)
        self.user.save()
        return self.user


class UserLicenseAssignmentForm(forms.Form):
    """Form for assigning license codes to existing users."""
    license_code = forms.CharField(
        max_length=16,
        required=True,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Enter 16-digit license code',
            'style': 'text-transform: uppercase;',
            'maxlength': '16',
        }),
        help_text='Enter the 16-character license code provided by your administrator'
    )

    def __init__(self, user=None, *args, **kwargs):
        self.user = user
        super().__init__(*args, **kwargs)

    def clean_license_code(self):
        license_code = self.cleaned_data.get('license_code', '').upper().strip()
        
        if not license_code:
            raise forms.ValidationError('License code is required.')
        
        if len(license_code) != 16:
            raise forms.ValidationError('License code must be exactly 16 characters.')
        
        if not license_code.isalnum():
            raise forms.ValidationError('License code must contain only letters and numbers.')
        
        # Validate license code with the licensing service
        from licensing.services import LicenseValidationService
        validation_service = LicenseValidationService()
        
        is_valid, message, license_info = validation_service.validate_license_code(
            license_code, self.user
        )
        
        if not is_valid:
            raise forms.ValidationError(f'Invalid license code: {message}')
        
        # Store license info for use in save method
        self.license_info = license_info
        
        return license_code

    def assign_license(self):
        """Assign the license to the user"""
        license_code = self.cleaned_data.get('license_code')
        if license_code and self.user:
            from licensing.services import LicenseValidationService
            validation_service = LicenseValidationService()
            success, message = validation_service.assign_license_to_user(
                license_code, self.user, "manual_assignment"
            )
            return success, message
        return False, "Invalid license code or user" 