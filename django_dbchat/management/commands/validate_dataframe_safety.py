"""
Django management command to validate DataFrame safety across the codebase
"""

import os
import re
import ast
from pathlib import Path
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Validate DataFrame safety across the codebase to prevent boolean context issues'
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--path',
            type=str,
            default='.',
            help='Path to scan for DataFrame safety issues (default: current directory)'
        )
        parser.add_argument(
            '--fix',
            action='store_true',
            help='Attempt to automatically fix simple DataFrame boolean context issues'
        )
        parser.add_argument(
            '--report',
            type=str,
            default='dataframe_safety_report.txt',
            help='Output file for the safety report'
        )
        parser.add_argument(
            '--exclude',
            type=str,
            nargs='*',
            default=['migrations', '__pycache__', '.git', 'node_modules'],
            help='Directories to exclude from scanning'
        )
    
    def handle(self, *args, **options):
        """Main command handler"""
        scan_path = options['path']
        should_fix = options['fix']
        report_file = options['report']
        exclude_dirs = options['exclude']
        
        self.stdout.write(f"Scanning {scan_path} for DataFrame safety issues...")
        
        if should_fix:
            self.stdout.write(self.style.WARNING("Fix mode enabled - files will be modified!"))
        
        # Initialize results
        results = {
            'files_scanned': 0,
            'issues_found': 0,
            'fixes_applied': 0,
            'files_with_issues': [],
            'detailed_issues': []
        }
        
        # Scan Python files
        for py_file in self._find_python_files(scan_path, exclude_dirs):
            self._scan_file(py_file, results, should_fix)
        
        # Generate report
        self._generate_report(results, report_file)
        
        # Display summary
        self._display_summary(results)
    
    def _find_python_files(self, scan_path, exclude_dirs):
        """Find all Python files in the scan path"""
        python_files = []
        
        for root, dirs, files in os.walk(scan_path):
            # Remove excluded directories
            dirs[:] = [d for d in dirs if d not in exclude_dirs]
            
            for file in files:
                if file.endswith('.py'):
                    python_files.append(os.path.join(root, file))
        
        return python_files
    
    def _scan_file(self, file_path, results, should_fix):
        """Scan a single Python file for DataFrame safety issues"""
        results['files_scanned'] += 1
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Look for potential DataFrame boolean context patterns
            issues = self._find_dataframe_issues(content, file_path)
            
            if issues:
                results['files_with_issues'].append(file_path)
                results['issues_found'] += len(issues)
                results['detailed_issues'].extend(issues)
                
                if should_fix:
                    fixed_content, fixes_count = self._apply_fixes(content, issues)
                    if fixes_count > 0:
                        with open(file_path, 'w', encoding='utf-8') as f:
                            f.write(fixed_content)
                        results['fixes_applied'] += fixes_count
                        self.stdout.write(f"Fixed {fixes_count} issues in {file_path}")
        
        except Exception as e:
            logger.error(f"Error scanning {file_path}: {e}")
    
    def _find_dataframe_issues(self, content, file_path):
        """Find DataFrame boolean context issues in file content"""
        issues = []
        lines = content.split('\n')
        
        # Patterns that might indicate DataFrame boolean context usage
        dangerous_patterns = [
            # Direct boolean context
            (r'\bif\s+(\w+)\s*:', 'if_dataframe_direct'),
            (r'\bwhile\s+(\w+)\s*:', 'while_dataframe_direct'),
            (r'\band\s+(\w+)\b', 'and_dataframe'),
            (r'\bor\s+(\w+)\b', 'or_dataframe'),
            (r'\bnot\s+(\w+)\b', 'not_dataframe'),
            
            # Conditional expressions
            (r'(\w+)\s+if\s+(\w+)\s+else', 'ternary_dataframe'),
            
            # Function calls that might use boolean context
            (r'\bassert\s+(\w+)', 'assert_dataframe'),
            (r'\bany\s*\(\s*(\w+)', 'any_dataframe'),
            (r'\ball\s*\(\s*(\w+)', 'all_dataframe'),
        ]
        
        for line_num, line in enumerate(lines, 1):
            # Skip comments and strings
            if line.strip().startswith('#') or '"""' in line or "'''" in line:
                continue
            
            for pattern, issue_type in dangerous_patterns:
                matches = re.finditer(pattern, line)
                for match in matches:
                    variable_name = match.group(1) if match.groups() else match.group(0)
                    
                    # Check if this might be a DataFrame variable
                    if self._might_be_dataframe_variable(variable_name, content):
                        issues.append({
                            'file': file_path,
                            'line_number': line_num,
                            'line_content': line.strip(),
                            'issue_type': issue_type,
                            'variable_name': variable_name,
                            'pattern_matched': pattern,
                            'suggestion': self._get_fix_suggestion(issue_type, variable_name)
                        })
        
        return issues
    
    def _might_be_dataframe_variable(self, variable_name, content):
        """Check if a variable might be a DataFrame based on context"""
        # Look for DataFrame-related patterns in the content
        dataframe_indicators = [
            f'{variable_name} = pd.DataFrame',
            f'{variable_name} = pandas.DataFrame',
            f'{variable_name}.to_dict',
            f'{variable_name}.empty',
            f'{variable_name}.shape',
            f'{variable_name}.columns',
            f'isinstance({variable_name}, pd.DataFrame)',
            f'isinstance({variable_name}, pandas.DataFrame)',
        ]
        
        return any(indicator in content for indicator in dataframe_indicators)
    
    def _get_fix_suggestion(self, issue_type, variable_name):
        """Get a fix suggestion for the DataFrame boolean context issue"""
        suggestions = {
            'if_dataframe_direct': f'if not {variable_name}.empty:',
            'while_dataframe_direct': f'while not {variable_name}.empty:',
            'and_dataframe': f'and not {variable_name}.empty',
            'or_dataframe': f'or not {variable_name}.empty',
            'not_dataframe': f'{variable_name}.empty',
            'ternary_dataframe': f'result if not {variable_name}.empty else default',
            'assert_dataframe': f'assert not {variable_name}.empty',
            'any_dataframe': f'not {variable_name}.empty',
            'all_dataframe': f'not {variable_name}.empty',
        }
        
        return suggestions.get(issue_type, f'Check {variable_name} safely using .empty or .shape[0] > 0')
    
    def _apply_fixes(self, content, issues):
        """Apply automatic fixes to the content"""
        lines = content.split('\n')
        fixes_applied = 0
        
        # Sort issues by line number in reverse order to avoid line number shifts
        sorted_issues = sorted(issues, key=lambda x: x['line_number'], reverse=True)
        
        for issue in sorted_issues:
            line_num = issue['line_number'] - 1  # Convert to 0-based index
            if line_num < len(lines):
                original_line = lines[line_num]
                
                # Apply simple fixes based on issue type
                fixed_line = self._apply_single_fix(original_line, issue)
                
                if fixed_line != original_line:
                    lines[line_num] = fixed_line
                    fixes_applied += 1
        
        return '\n'.join(lines), fixes_applied
    
    def _apply_single_fix(self, line, issue):
        """Apply a single fix to a line"""
        issue_type = issue['issue_type']
        variable_name = issue['variable_name']
        
        # Simple fixes for common patterns
        if issue_type == 'if_dataframe_direct':
            return re.sub(
                rf'\bif\s+{re.escape(variable_name)}\s*:',
                f'if not {variable_name}.empty:',
                line
            )
        elif issue_type == 'while_dataframe_direct':
            return re.sub(
                rf'\bwhile\s+{re.escape(variable_name)}\s*:',
                f'while not {variable_name}.empty:',
                line
            )
        elif issue_type == 'not_dataframe':
            return re.sub(
                rf'\bnot\s+{re.escape(variable_name)}\b',
                f'{variable_name}.empty',
                line
            )
        
        # For other patterns, return the original line (manual fix needed)
        return line
    
    def _generate_report(self, results, report_file):
        """Generate a detailed safety report"""
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write("DataFrame Safety Validation Report\n")
                f.write("=" * 50 + "\n\n")
                
                # Summary
                f.write(f"Files scanned: {results['files_scanned']}\n")
                f.write(f"Issues found: {results['issues_found']}\n")
                f.write(f"Fixes applied: {results['fixes_applied']}\n")
                f.write(f"Files with issues: {len(results['files_with_issues'])}\n\n")
                
                # Detailed issues
                if results['detailed_issues']:
                    f.write("Detailed Issues:\n")
                    f.write("-" * 20 + "\n\n")
                    
                    for issue in results['detailed_issues']:
                        f.write(f"File: {issue['file']}\n")
                        f.write(f"Line {issue['line_number']}: {issue['line_content']}\n")
                        f.write(f"Issue: {issue['issue_type']}\n")
                        f.write(f"Variable: {issue['variable_name']}\n")
                        f.write(f"Suggestion: {issue['suggestion']}\n")
                        f.write("-" * 40 + "\n\n")
                
                # Files with issues
                if results['files_with_issues']:
                    f.write("Files with DataFrame safety issues:\n")
                    for file_path in results['files_with_issues']:
                        f.write(f"  - {file_path}\n")
            
            self.stdout.write(f"Report saved to: {report_file}")
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error generating report: {e}"))
    
    def _display_summary(self, results):
        """Display a summary of the validation results"""
        self.stdout.write("\n" + "=" * 50)
        self.stdout.write("DataFrame Safety Validation Summary")
        self.stdout.write("=" * 50)
        
        self.stdout.write(f"Files scanned: {results['files_scanned']}")
        
        if results['issues_found'] > 0:
            self.stdout.write(self.style.WARNING(f"Issues found: {results['issues_found']}"))
            self.stdout.write(f"Files with issues: {len(results['files_with_issues'])}")
            
            if results['fixes_applied'] > 0:
                self.stdout.write(self.style.SUCCESS(f"Fixes applied: {results['fixes_applied']}"))
            
            self.stdout.write("\nRecommendations:")
            self.stdout.write("- Use df.empty instead of if df:")
            self.stdout.write("- Use df.shape[0] > 0 for safer checks")
            self.stdout.write("- Always handle ValueError for DataFrame ambiguity")
            self.stdout.write("- Convert DataFrames before JSON serialization")
            
        else:
            self.stdout.write(self.style.SUCCESS("No DataFrame safety issues found!"))
        
        self.stdout.write("=" * 50 + "\n") 