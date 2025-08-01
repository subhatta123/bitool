"""
Data Source Error Recovery and User Guidance Utility

Provides comprehensive error analysis, recovery suggestions, and automatic repair
capabilities for data source loading failures.
"""

import logging
import os
from typing import Dict, List, Any, Tuple, Optional
from django.conf import settings
from django.utils import timezone

logger = logging.getLogger(__name__)


class DataSourceErrorRecovery:
    """
    Comprehensive error recovery system for data source loading failures
    """
    
    def __init__(self):
        self.error_patterns = {}
        self.recovery_attempts = []
    
    def diagnose_data_source_issues(self, data_source) -> Dict[str, Any]:
        """
        Analyze a data source and identify specific problems
        
        Args:
            data_source: DataSource model instance
            
        Returns:
            Comprehensive diagnostic report
        """
        logger.info(f"🔍 Starting diagnostic analysis for data source: {data_source.name}")
        
        diagnosis = {
            'data_source_id': str(data_source.id),
            'data_source_name': data_source.name,
            'timestamp': timezone.now().isoformat(),
            'issues_identified': [],
            'severity_level': 'low',
            'recovery_priority': [],
            'technical_details': {},
            'user_actionable_items': []
        }
        
        # Check connection info
        connection_issues = self._analyze_connection_info(data_source)
        if connection_issues:
            diagnosis['issues_identified'].extend(connection_issues)
        
        # Check schema info
        schema_issues = self._analyze_schema_info(data_source)
        if schema_issues:
            diagnosis['issues_identified'].extend(schema_issues)
        
        # Check file accessibility (for CSV sources)
        if data_source.source_type == 'csv':
            file_issues = self._analyze_file_accessibility(data_source)
            if file_issues:
                diagnosis['issues_identified'].extend(file_issues)
        
        # Determine overall severity
        diagnosis['severity_level'] = self._calculate_severity(diagnosis['issues_identified'])
        
        # Generate recovery priority order
        diagnosis['recovery_priority'] = self._prioritize_recovery_actions(diagnosis['issues_identified'])
        
        logger.info(f"📊 Diagnosis complete. Found {len(diagnosis['issues_identified'])} issues with severity: {diagnosis['severity_level']}")
        
        return diagnosis
    
    def generate_recovery_suggestions(self, data_source, error_details: str = None) -> Dict[str, Any]:
        """
        Generate actionable recovery suggestions based on error type
        
        Args:
            data_source: DataSource model instance
            error_details: Specific error message or details
            
        Returns:
            Structured recovery guidance
        """
        logger.info(f"🔧 Generating recovery suggestions for {data_source.name}")
        
        # First run diagnostics
        diagnosis = self.diagnose_data_source_issues(data_source)
        
        recovery_plan = {
            'data_source_name': data_source.name,
            'timestamp': timezone.now().isoformat(),
            'immediate_actions': [],
            'medium_term_actions': [],
            'long_term_actions': [],
            'technical_steps': [],
            'user_friendly_guidance': '',
            'estimated_fix_time': '5-15 minutes',
            'success_probability': 'medium',
            'alternative_approaches': []
        }
        
        # Generate specific suggestions based on identified issues
        for issue in diagnosis['issues_identified']:
            suggestions = self._generate_issue_specific_suggestions(issue, data_source)
            
            recovery_plan['immediate_actions'].extend(suggestions.get('immediate', []))
            recovery_plan['medium_term_actions'].extend(suggestions.get('medium_term', []))
            recovery_plan['technical_steps'].extend(suggestions.get('technical', []))
        
        # Generate user-friendly guidance
        recovery_plan['user_friendly_guidance'] = self._create_user_friendly_guidance(
            data_source, 
            diagnosis['issues_identified']
        )
        
        return recovery_plan
    
    def _analyze_connection_info(self, data_source) -> List[Dict[str, Any]]:
        """Analyze connection info for issues"""
        issues = []
        
        if not data_source.connection_info:
            issues.append({
                'type': 'connection_info_missing',
                'severity': 'high',
                'description': 'Connection information is completely missing',
                'impact': 'Cannot access any data from this source'
            })
        else:
            # Check for required fields based on source type
            if data_source.source_type == 'csv':
                if 'file_path' not in data_source.connection_info:
                    issues.append({
                        'type': 'csv_file_path_missing',
                        'severity': 'high',
                        'description': 'CSV file path is missing from connection info',
                        'impact': 'Cannot locate the original CSV file'
                    })
        
        return issues
    
    def _analyze_schema_info(self, data_source) -> List[Dict[str, Any]]:
        """Analyze schema info for issues"""
        issues = []
        
        if not data_source.schema_info:
            issues.append({
                'type': 'schema_info_missing',
                'severity': 'medium',
                'description': 'Schema information is completely missing',
                'impact': 'Cannot generate sample data or understand data structure'
            })
        else:
            columns = data_source.schema_info.get('columns', [])
            if not columns:
                issues.append({
                    'type': 'schema_columns_empty',
                    'severity': 'medium',
                    'description': 'Schema exists but contains no column information',
                    'impact': 'Cannot understand data structure or generate samples'
                })
        
        return issues
    
    def _analyze_file_accessibility(self, data_source) -> List[Dict[str, Any]]:
        """Analyze file accessibility for CSV sources"""
        issues = []
        
        if data_source.source_type == 'csv' and data_source.connection_info:
            file_path = data_source.connection_info.get('file_path')
            if file_path:
                # Check if file exists at expected locations
                potential_paths = [
                    os.path.join(settings.MEDIA_ROOT, file_path),
                    os.path.join(settings.BASE_DIR, file_path),
                    file_path
                ]
                
                file_found = any(os.path.exists(path) for path in potential_paths)
                
                if not file_found:
                    issues.append({
                        'type': 'csv_file_not_found',
                        'severity': 'high',
                        'description': f'CSV file not found at any expected location: {file_path}',
                        'impact': 'Cannot read original data from file'
                    })
        
        return issues
    
    def _calculate_severity(self, issues: List[Dict[str, Any]]) -> str:
        """Calculate overall severity from list of issues"""
        if not issues:
            return 'low'
        
        severity_scores = {'high': 3, 'medium': 2, 'low': 1}
        max_severity = max(severity_scores[issue['severity']] for issue in issues)
        
        if max_severity == 3:
            return 'high'
        elif max_severity == 2:
            return 'medium'
        else:
            return 'low'
    
    def _prioritize_recovery_actions(self, issues: List[Dict[str, Any]]) -> List[str]:
        """Prioritize recovery actions based on issues"""
        actions = []
        
        # High priority: File access issues
        if any(issue['type'] in ['csv_file_not_found', 'connection_info_missing'] for issue in issues):
            actions.append('fix_file_access')
        
        # Medium priority: Schema issues
        if any(issue['type'] in ['schema_info_missing', 'schema_columns_empty'] for issue in issues):
            actions.append('regenerate_schema')
        
        return actions
    
    def _generate_issue_specific_suggestions(self, issue: Dict[str, Any], data_source) -> Dict[str, List[str]]:
        """Generate specific suggestions for an issue"""
        suggestions = {'immediate': [], 'medium_term': [], 'technical': []}
        
        issue_type = issue['type']
        
        if issue_type == 'csv_file_not_found':
            suggestions['immediate'] = [
                "Re-upload the CSV file using the data source upload interface",
                "Check if the file was moved to a different location"
            ]
            suggestions['technical'] = [
                f"Verify file exists at: {data_source.connection_info.get('file_path', 'unknown')}",
                "Check MEDIA_ROOT and BASE_DIR settings"
            ]
        
        elif issue_type == 'schema_info_missing':
            suggestions['immediate'] = [
                "Regenerate schema by re-analyzing the data source",
                "Upload data again to trigger schema generation"
            ]
            suggestions['technical'] = [
                "Run schema analysis script",
                "Check data_source.schema_info field in database"
            ]
        
        return suggestions
    
    def _create_user_friendly_guidance(self, data_source, issues: List[Dict[str, Any]]) -> str:
        """Create user-friendly guidance text"""
        if not issues:
            return "Your data source appears to be healthy. If you're still experiencing issues, try refreshing the page or running ETL operations."
        
        guidance = f"We found {len(issues)} issue(s) with your data source '{data_source.name}'. Here's how to fix them:\n\n"
        
        high_priority_issues = [issue for issue in issues if issue['severity'] == 'high']
        
        if high_priority_issues:
            guidance += "🚨 **Critical Issues (Fix First):**\n"
            for issue in high_priority_issues:
                guidance += f"• {issue['description']}\n"
            guidance += "\n"
        
        if data_source.source_type == 'csv':
            guidance += "💡 **Quick Fix for CSV Files:**\n"
            guidance += "1. Go to Data Sources → Upload CSV\n"
            guidance += "2. Re-upload your original file\n"
            guidance += "3. Wait for processing to complete\n"
            guidance += "4. Try generating the semantic layer again\n\n"
        
        guidance += "📞 **Need Help?** Contact support with this error ID: " + str(data_source.id)
        
        return guidance


# Global instance for easy access
error_recovery = DataSourceErrorRecovery()
