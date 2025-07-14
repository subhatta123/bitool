#!/usr/bin/env python3
"""
User-friendly error service for better join operation error messages
"""

from typing import Dict, List, Any, Optional
from datasets.models import DataSource

class UserFriendlyErrorService:
    """Service to provide user-friendly error messages and guidance"""
    
    @staticmethod
    def analyze_join_failure(left_source_id: str, right_source_id: str, 
                           validation_details: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Analyze join failure and provide user-friendly guidance
        
        Args:
            left_source_id: ID of left data source
            right_source_id: ID of right data source
            validation_details: Validation details from robust validator
            
        Returns:
            Dictionary with user-friendly error information
        """
        
        analysis = {
            'error_type': 'unknown',
            'user_message': 'Join operation failed',
            'technical_details': '',
            'recommendations': [],
            'quick_fixes': [],
            'data_sources_status': {}
        }
        
        # Check data source statuses
        left_status = UserFriendlyErrorService._check_data_source_status(left_source_id)
        right_status = UserFriendlyErrorService._check_data_source_status(right_source_id)
        
        analysis['data_sources_status'] = {
            'left': left_status,
            'right': right_status
        }
        
        # Determine primary error type
        if not left_status['exists']:
            analysis['error_type'] = 'left_source_missing'
            analysis['user_message'] = f"Left data source not found"
            
        elif not right_status['exists']:
            analysis['error_type'] = 'right_source_missing'
            analysis['user_message'] = f"Right data source not found"
            
        elif not left_status['processed']:
            analysis['error_type'] = 'left_source_not_processed'
            analysis['user_message'] = f"Left data source '{left_status['name']}' hasn't been processed yet"
            analysis['technical_details'] = "Data source exists but no table has been created in the database"
            
        elif not right_status['processed']:
            analysis['error_type'] = 'right_source_not_processed'
            analysis['user_message'] = f"Right data source '{right_status['name']}' hasn't been processed yet"
            analysis['technical_details'] = "Data source exists but no table has been created in the database"
            
        else:
            # Both sources exist and are processed - check for column issues
            analysis['error_type'] = 'column_validation_failed'
            analysis['user_message'] = "Column validation failed for join operation"
        
        # Generate specific recommendations based on error type
        analysis['recommendations'] = UserFriendlyErrorService._generate_recommendations(
            analysis['error_type'], left_status, right_status
        )
        
        # Generate quick fixes
        analysis['quick_fixes'] = UserFriendlyErrorService._generate_quick_fixes(
            analysis['error_type'], left_status, right_status
        )
        
        return analysis
    
    @staticmethod
    def _check_data_source_status(source_id: str) -> Dict[str, Any]:
        """Check the status of a data source"""
        status = {
            'exists': False,
            'name': 'Unknown',
            'processed': False,
            'status': 'unknown',
            'source_type': 'unknown',
            'upload_date': None,
            'issues': []
        }
        
        try:
            data_source = DataSource.objects.get(id=source_id)
            status.update({
                'exists': True,
                'name': data_source.name,
                'status': data_source.status,
                'source_type': data_source.source_type,
                'upload_date': data_source.created_at.strftime('%Y-%m-%d %H:%M') if data_source.created_at else None
            })
            
            # Check if data source has been processed (simplified check)
            # In a real implementation, you'd check if tables exist in DuckDB
            connection_info = data_source.connection_info or {}
            
            if data_source.source_type == 'csv':
                if 'file_path' in connection_info:
                    # For now, assume processed if has connection info
                    # In reality, you'd check if DuckDB table exists
                    status['processed'] = True  # Simplified for this example
                else:
                    status['issues'].append("CSV file path not found")
            
            if data_source.status != 'active':
                status['issues'].append(f"Data source status is '{data_source.status}' (should be 'active')")
                
        except DataSource.DoesNotExist:
            status['issues'].append("Data source not found in database")
            
        except Exception as e:
            status['issues'].append(f"Error checking data source: {str(e)}")
        
        return status
    
    @staticmethod
    def _generate_recommendations(error_type: str, left_status: Dict, right_status: Dict) -> List[str]:
        """Generate specific recommendations based on error type"""
        recommendations = []
        
        if error_type == 'left_source_not_processed':
            recommendations.extend([
                f"Process the left data source '{left_status['name']}' before attempting joins",
                "Go to Data Sources → Select the data source → Click 'Process' or 'Upload'",
                "Wait for the data processing to complete (this may take a few minutes)",
                "Check the data source status shows as 'Processed' or 'Ready'"
            ])
            
        elif error_type == 'right_source_not_processed':
            recommendations.extend([
                f"Process the right data source '{right_status['name']}' before attempting joins",
                "Go to Data Sources → Select the data source → Click 'Process' or 'Upload'", 
                "Wait for the data processing to complete (this may take a few minutes)",
                "Check the data source status shows as 'Processed' or 'Ready'"
            ])
            
        elif error_type == 'left_source_missing':
            recommendations.extend([
                "The left data source has been deleted or is not accessible",
                "Select a different data source for the left table",
                "Check with your administrator if you should have access to this data"
            ])
            
        elif error_type == 'right_source_missing':
            recommendations.extend([
                "The right data source has been deleted or is not accessible", 
                "Select a different data source for the right table",
                "Check with your administrator if you should have access to this data"
            ])
            
        else:
            recommendations.extend([
                "Check that both data sources have been properly uploaded and processed",
                "Verify that the join columns exist in both data sources",
                "Try refreshing the page and attempting the join again"
            ])
        
        recommendations.append("Contact support if the issue persists")
        return recommendations
    
    @staticmethod
    def _generate_quick_fixes(error_type: str, left_status: Dict, right_status: Dict) -> List[Dict[str, str]]:
        """Generate quick fix actions"""
        quick_fixes = []
        
        if error_type in ['left_source_not_processed', 'right_source_not_processed']:
            source_name = left_status['name'] if error_type == 'left_source_not_processed' else right_status['name']
            quick_fixes.extend([
                {
                    'action': 'process_data_source',
                    'label': f"Process '{source_name}'",
                    'description': 'Navigate to data source and start processing'
                },
                {
                    'action': 'select_different_source',
                    'label': 'Choose Different Data Source',
                    'description': 'Select a data source that has already been processed'
                }
            ])
            
        elif error_type in ['left_source_missing', 'right_source_missing']:
            quick_fixes.append({
                'action': 'select_different_source',
                'label': 'Choose Different Data Source',
                'description': 'Select an available data source'
            })
            
        else:
            quick_fixes.extend([
                {
                    'action': 'refresh_page',
                    'label': 'Refresh Page',
                    'description': 'Reload the page and try again'
                },
                {
                    'action': 'check_columns',
                    'label': 'Check Column Names',
                    'description': 'Verify that the join columns exist in both tables'
                }
            ])
        
        return quick_fixes

# Global instance
user_friendly_error_service = UserFriendlyErrorService() 