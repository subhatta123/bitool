"""
Migration script to transfer user data from Streamlit app to Django.
This script migrates users from the existing app_users table to the new Django CustomUser model.
"""
import os
import sys
import django
import json
from pathlib import Path

# Add the Django project root to the Python path
django_root = Path(__file__).parent.parent
sys.path.insert(0, str(django_root))

# Configure Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from django.contrib.auth import get_user_model
from django.db import connection
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

User = get_user_model()


def check_old_users_table():
    """Check if the old app_users table exists and get user data."""
    try:
        with connection.cursor() as cursor:
            # Check if app_users table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'app_users'
                );
            """)
            
            result = cursor.fetchone()
            table_exists = result[0] if result else False
            
            if not table_exists:
                logger.warning("app_users table does not exist. No migration needed.")
                return []
            
            # Get all users from old table
            cursor.execute("""
                SELECT username, password_hash, roles, created_at, last_login, is_active
                FROM app_users
                ORDER BY created_at;
            """)
            
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            if rows is None:
                rows = []
            users = [dict(zip(columns, row)) for row in rows]
            
            logger.info(f"Found {len(users)} users in app_users table")
            return users
            
    except Exception as e:
        logger.error(f"Error checking old users table: {e}")
        return []


def migrate_user(old_user_data):
    """Migrate a single user from old format to Django format."""
    try:
        username = old_user_data['username']
        
        # Check if user already exists
        if User.objects.filter(username=username).exists():
            logger.info(f"User {username} already exists, skipping...")
            return False
        
        # Parse roles (could be JSON string or list)
        roles = old_user_data.get('roles', [])
        if isinstance(roles, str):
            try:
                roles = json.loads(roles)
            except json.JSONDecodeError:
                roles = [roles] if roles else []
        
        # Create new user
        user = User(
            username=username,
            email=f"{username}@example.com",  # Default email, should be updated
            is_active=old_user_data.get('is_active', True),
            roles=roles if isinstance(roles, list) else [],
            created_at=old_user_data.get('created_at'),
            last_login=old_user_data.get('last_login')
        )
        
        # Set password (assuming it's already hashed)
        password_hash = old_user_data.get('password_hash', '')
        if password_hash:
            # If the password is already hashed, we can set it directly
            user.password = password_hash
        else:
            # Set a default password that needs to be changed
            user.set_password('change_me_123')
        
        # Set admin status based on roles
        if 'admin' in roles:
            user.is_staff = True
            user.is_superuser = True
        
        user.save()
        logger.info(f"Successfully migrated user: {username}")
        return True
        
    except Exception as e:
        logger.error(f"Error migrating user {old_user_data.get('username', 'unknown')}: {e}")
        return False


def create_default_admin():
    """Create a default admin user if none exists."""
    try:
        if not User.objects.filter(is_superuser=True).exists():
            admin_user = User.objects.create_superuser(
                username='admin',
                email='admin@example.com',
                password='admin123',
                roles=['admin']
            )
            logger.info("Created default admin user (admin/admin123)")
            return True
        else:
            logger.info("Admin user already exists")
            return False
    except Exception as e:
        logger.error(f"Error creating default admin user: {e}")
        return False


def backup_old_table():
    """Create a backup of the old app_users table."""
    try:
        backup_table_name = "app_users_backup_" + str(int(time.time()))
        
        with connection.cursor() as cursor:
            cursor.execute(f"""
                CREATE TABLE {backup_table_name} AS 
                SELECT * FROM app_users;
            """)
            
        logger.info(f"Created backup table: {backup_table_name}")
        return backup_table_name
        
    except Exception as e:
        logger.error(f"Error creating backup: {e}")
        return None


def main():
    """Main migration function."""
    logger.info("Starting user migration from Streamlit to Django...")
    
    # Check if Django users already exist
    if User.objects.exists():
        logger.warning("Django users already exist. Use --force to override.")
        response = input("Continue with migration? (y/N): ")
        if response.lower() != 'y':
            logger.info("Migration cancelled by user.")
            return
    
    # Get old users
    old_users = check_old_users_table()
    
    if not old_users:
        logger.info("No users found to migrate.")
        create_default_admin()
        return
    
    # Create backup
    backup_table = backup_old_table()
    if backup_table:
        logger.info(f"Backup created: {backup_table}")
    
    # Migrate users
    migrated_count = 0
    failed_count = 0
    
    for user_data in old_users:
        if migrate_user(user_data):
            migrated_count += 1
        else:
            failed_count += 1
    
    # Create default admin if needed
    create_default_admin()
    
    # Summary
    logger.info(f"""
Migration completed:
- Total users found: {len(old_users)}
- Successfully migrated: {migrated_count}
- Failed: {failed_count}
- Django users now: {User.objects.count()}
""")
    
    if failed_count > 0:
        logger.warning("Some users failed to migrate. Check logs for details.")
    
    logger.info("User migration completed!")


if __name__ == "__main__":
    import time
    main() 