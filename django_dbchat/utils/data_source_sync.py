#!/usr/bin/env python3
"""
Data source synchronization utility to ensure tables exist before JOIN operations
"""

import logging
from typing import Dict, List, Optional, Tuple, Any
from django.contrib.auth.models import User
from datasets.models import DataSource

logger = logging.getLogger(__name__)

class DataSourceSyncManager:
    """Manages data source synchronization and table availability"""
    
    @staticmethod
    def check_data_source_table_status(data_source: DataSource, connection) -> Dict[str, Any]:
        """
        Check if a data source's table exists in DuckDB
        
        Args:
            data_source: DataSource model instance
            connection: DuckDB connection
            
        Returns:
            Dict with status information
        """
        try:
            # Generate expected table name
            table_name = f"ds_{data_source.id.hex.replace('-', '_')}"
            
            # Check if table exists
            table_exists = connection.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = '{table_name}'
            """).fetchone()[0] > 0
            
            status = {
                'data_source_id': str(data_source.id),
                'data_source_name': data_source.name,
                'expected_table_name': table_name,
                'table_exists': table_exists,
                'status': data_source.status,
                'workflow_status': getattr(data_source, 'workflow_status', {})
            }
            
            if table_exists:
                # Get table info
                try:
                    row_count = connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    columns = connection.execute(f"DESCRIBE {table_name}").fetchall()
                    
                    status.update({
                        'row_count': row_count,
                        'column_count': len(columns),
                        'columns': [col[0] for col in columns],
                        'ready_for_joins': True
                    })
                except Exception as table_error:
                    status.update({
                        'table_error': str(table_error),
                        'ready_for_joins': False
                    })
            else:
                status['ready_for_joins'] = False
                
                # Check possible reasons why table doesn't exist
                workflow_status = getattr(data_source, 'workflow_status', {})
                etl_completed = workflow_status.get('etl_completed', False)
                
                if not etl_completed:
                    status['reason'] = 'ETL processing not completed'
                else:
                    status['reason'] = 'Table missing despite ETL completion'
            
            return status
            
        except Exception as e:
            return {
                'data_source_id': str(data_source.id),
                'data_source_name': data_source.name,
                'error': str(e),
                'ready_for_joins': False
            }
    
    @staticmethod
    def get_join_readiness_report(left_source_id: str, right_source_id: str, 
                                 user: User, connection) -> Dict[str, Any]:
        """
        Generate a report on whether two data sources are ready for joining
        
        Args:
            left_source_id: ID of left data source
            right_source_id: ID of right data source
            user: User requesting the join
            connection: DuckDB connection
            
        Returns:
            Dict with readiness report
        """
        try:
            # Get data sources
            try:
                left_source = DataSource.objects.get(id=left_source_id, created_by=user)
                right_source = DataSource.objects.get(id=right_source_id, created_by=user)
            except DataSource.DoesNotExist as e:
                return {
                    'ready_for_join': False,
                    'error': 'Data source not found or not owned by user',
                    'details': str(e)
                }
            
            # Check both sources
            left_status = DataSourceSyncManager.check_data_source_table_status(left_source, connection)
            right_status = DataSourceSyncManager.check_data_source_table_status(right_source, connection)
            
            # Determine overall readiness
            ready_for_join = left_status.get('ready_for_joins', False) and right_status.get('ready_for_joins', False)
            
            report = {
                'ready_for_join': ready_for_join,
                'left_source': left_status,
                'right_source': right_status,
                'recommendations': []
            }
            
            # Generate recommendations
            if not left_status.get('ready_for_joins', False):
                if not left_status.get('table_exists', False):
                    report['recommendations'].append(f"Complete ETL processing for '{left_source.name}' to create its table")
                else:
                    report['recommendations'].append(f"Check data integrity for '{left_source.name}'")
            
            if not right_status.get('ready_for_joins', False):
                if not right_status.get('table_exists', False):
                    report['recommendations'].append(f"Complete ETL processing for '{right_source.name}' to create its table")
                else:
                    report['recommendations'].append(f"Check data integrity for '{right_source.name}'")
            
            if ready_for_join:
                report['recommendations'].append("Both data sources are ready for joining!")
            
            return report
            
        except Exception as e:
            return {
                'ready_for_join': False,
                'error': str(e),
                'recommendations': ['Please check data source configuration and try again']
            }
    
    @staticmethod
    def get_available_data_sources_for_joins(user: User, connection) -> List[Dict[str, Any]]:
        """
        Get list of data sources that are available for JOIN operations
        
        Args:
            user: User to get data sources for
            connection: DuckDB connection
            
        Returns:
            List of data sources with their join readiness
        """
        try:
            # Get all active data sources for user
            data_sources = DataSource.objects.filter(created_by=user, status='active')
            
            available_sources = []
            
            for source in data_sources:
                status = DataSourceSyncManager.check_data_source_table_status(source, connection)
                
                if status.get('ready_for_joins', False):
                    available_sources.append({
                        'id': str(source.id),
                        'name': source.name,
                        'table_name': status['expected_table_name'],
                        'row_count': status.get('row_count', 0),
                        'columns': status.get('columns', []),
                        'created_at': source.created_at.isoformat() if hasattr(source, 'created_at') else None
                    })
            
            return available_sources
            
        except Exception as e:
            logger.error(f"Error getting available data sources: {e}")
            return []
    
    @staticmethod
    def suggest_join_alternatives(left_source_id: str, right_source_id: str, 
                                user: User, connection) -> Dict[str, Any]:
        """
        Suggest alternative data sources for joining when original selection fails
        
        Args:
            left_source_id: ID of problematic left source
            right_source_id: ID of problematic right source
            user: User requesting suggestions
            connection: DuckDB connection
            
        Returns:
            Dict with alternative suggestions
        """
        try:
            available_sources = DataSourceSyncManager.get_available_data_sources_for_joins(user, connection)
            
            suggestions = {
                'available_sources': available_sources,
                'total_available': len(available_sources),
                'recommendations': []
            }
            
            if len(available_sources) == 0:
                suggestions['recommendations'].append("No data sources are currently ready for joining. Please complete ETL processing for your data sources.")
            elif len(available_sources) == 1:
                suggestions['recommendations'].append(f"Only one data source ('{available_sources[0]['name']}') is ready. You need at least two sources for a join operation.")
            else:
                suggestions['recommendations'].append(f"You have {len(available_sources)} data sources ready for joining. Consider using these instead:")
                for source in available_sources[:3]:  # Show top 3
                    suggestions['recommendations'].append(f"  • {source['name']} ({source['row_count']} rows, {len(source['columns'])} columns)")
            
            return suggestions
            
        except Exception as e:
            return {
                'available_sources': [],
                'total_available': 0,
                'error': str(e),
                'recommendations': ['Unable to generate suggestions. Please check your data sources manually.']
            } 