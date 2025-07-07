"""
Enhanced Dashboard Module
Comprehensive dashboard with AI filters, cross-filtering, and modern UI
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import uuid
import json
from typing import Dict, List, Any, Optional
import copy

class DashboardFilter:
    """Represents a dashboard filter"""
    def __init__(self, filter_id: str, name: str, column: str, filter_type: str, 
                 values: List[Any], active_values: Optional[List[Any]] = None):
        self.filter_id = filter_id
        self.name = name
        self.column = column
        self.filter_type = filter_type  # 'multiselect', 'slider', 'date_range'
        self.values = values
        self.active_values = active_values if active_values is not None else values

class EnhancedDashboard:
    """Enhanced dashboard with AI filters and cross-filtering"""
    
    def __init__(self):
        self.dashboard_filters = {}
        self.cross_filter_selection = {}
        self.chart_interactions = {}
        self.drill_down_stack = []
        self.active_selections = {}
        
    def generate_ai_filters(self, dashboard_items: List[Dict]) -> List[DashboardFilter]:
        """Generate AI-suggested filters based on dashboard data"""
        filters = []
        all_columns = set()
        sample_data = None
        
        # Collect all unique columns from dashboard items
        for item in dashboard_items:
            if 'data_snapshot' in item and not item['data_snapshot'].empty:
                data = item['data_snapshot']
                all_columns.update(data.columns)
                if sample_data is None:
                    sample_data = data
        
        if sample_data is None:
            return filters
        
        # Generate filters for categorical columns with reasonable cardinality
        for col in all_columns:
            if col in sample_data.columns:
                col_data = sample_data[col]
                unique_count = col_data.nunique()
                
                # Categorical filters (2-20 unique values)
                if 2 <= unique_count <= 20 and col_data.dtype == 'object':
                    unique_values = sorted(col_data.dropna().unique().tolist())
                    filter_obj = DashboardFilter(
                        filter_id=f"filter_{col}",
                        name=col.replace('_', ' ').title(),
                        column=col,
                        filter_type='multiselect',
                        values=unique_values,
                        active_values=unique_values
                    )
                    filters.append(filter_obj)
                
                # Numeric range filters
                elif pd.api.types.is_numeric_dtype(col_data) and unique_count > 10:
                    min_val = float(col_data.min())
                    max_val = float(col_data.max())
                    filter_obj = DashboardFilter(
                        filter_id=f"filter_{col}",
                        name=col.replace('_', ' ').title(),
                        column=col,
                        filter_type='slider',
                        values=[min_val, max_val],
                        active_values=[min_val, max_val]
                    )
                    filters.append(filter_obj)
                
                # Date filters
                elif pd.api.types.is_datetime64_any_dtype(col_data):
                    min_date = col_data.min()
                    max_date = col_data.max()
                    filter_obj = DashboardFilter(
                        filter_id=f"filter_{col}",
                        name=col.replace('_', ' ').title(),
                        column=col,
                        filter_type='date_range',
                        values=[min_date, max_date],
                        active_values=[min_date, max_date]
                    )
                    filters.append(filter_obj)
        
        # Limit to most useful filters
        return filters[:5]
    
    def apply_filters_to_data(self, data: pd.DataFrame, filters: List[DashboardFilter]) -> pd.DataFrame:
        """Apply active filters to data"""
        filtered_data = data.copy()
        
        for filter_obj in filters:
            if filter_obj.column in filtered_data.columns:
                col_data = filtered_data[filter_obj.column]
                
                if filter_obj.filter_type == 'multiselect':
                    filtered_data = filtered_data[col_data.isin(filter_obj.active_values)]
                
                elif filter_obj.filter_type == 'slider':
                    min_val, max_val = filter_obj.active_values
                    filtered_data = filtered_data[
                        (col_data >= min_val) & (col_data <= max_val)
                    ]
                
                elif filter_obj.filter_type == 'date_range':
                    start_date, end_date = filter_obj.active_values
                    filtered_data = filtered_data[
                        (col_data >= start_date) & (col_data <= end_date)
                    ]
        
        return filtered_data
    
    def create_enhanced_chart(self, item: Dict, filtered_data: pd.DataFrame, 
                            chart_id: str, enable_cross_filter: bool = True) -> Optional[go.Figure]:
        """Create enhanced chart with cross-filtering capabilities"""
        chart_type = item['chart_type']
        params = item['params']
        title = item.get('title', chart_type)
        
        fig = None
        
        try:
            if chart_type == "Bar Chart":
                x_col, y_col = params.get('x'), params.get('y')
                if x_col in filtered_data.columns and y_col in filtered_data.columns:
                    fig = px.bar(filtered_data, x=x_col, y=y_col, 
                                color=params.get('color'), title=title)
            
            elif chart_type == "Line Chart":
                x_col, y_col = params.get('x'), params.get('y')
                if x_col in filtered_data.columns and y_col in filtered_data.columns:
                    fig = px.line(filtered_data, x=x_col, y=y_col, 
                                 color=params.get('color'), title=title)
            
            elif chart_type == "Scatter Plot":
                x_col, y_col = params.get('x'), params.get('y')
                if x_col in filtered_data.columns and y_col in filtered_data.columns:
                    fig = px.scatter(filtered_data, x=x_col, y=y_col, 
                                   color=params.get('color'), 
                                   size=params.get('size'), title=title)
            
            elif chart_type == "Pie Chart":
                names_col, values_col = params.get('names'), params.get('values')
                if names_col in filtered_data.columns and values_col in filtered_data.columns:
                    fig = px.pie(filtered_data, names=names_col, values=values_col, title=title)
            
            elif chart_type == "Histogram":
                x_col = params.get('x')
                if x_col in filtered_data.columns:
                    fig = px.histogram(filtered_data, x=x_col, title=title)
            
            if fig and enable_cross_filter:
                # Enable cross-filtering with click events
                fig.update_traces(
                    marker=dict(line=dict(width=2, color='white')),
                    selector=dict(type='bar')
                )
                
                # Add custom click data for cross-filtering
                fig.update_layout(
                    clickmode='event+select',
                    dragmode='select'
                )
            
            return fig
            
        except Exception as e:
            st.error(f"Error creating enhanced chart: {e}")
            return None
    
    def render_filter_sidebar(self, filters: List[DashboardFilter]) -> Dict[str, Any]:
        """Render dashboard filters in sidebar"""
        st.sidebar.markdown("### 🎛️ Dashboard Filters")
        
        filter_changes = {}
        
        for filter_obj in filters:
            with st.sidebar.expander(f"🔍 {filter_obj.name}", expanded=True):
                
                if filter_obj.filter_type == 'multiselect':
                    selected = st.multiselect(
                        "Select values:",
                        options=filter_obj.values,
                        default=filter_obj.active_values,
                        key=f"filter_{filter_obj.filter_id}"
                    )
                    if selected != filter_obj.active_values:
                        filter_changes[filter_obj.filter_id] = selected
                
                elif filter_obj.filter_type == 'slider':
                    min_val, max_val = filter_obj.values
                    selected_range = st.slider(
                        "Select range:",
                        min_value=min_val,
                        max_value=max_val,
                        value=filter_obj.active_values,
                        key=f"filter_{filter_obj.filter_id}"
                    )
                    if list(selected_range) != filter_obj.active_values:
                        filter_changes[filter_obj.filter_id] = list(selected_range)
                
                elif filter_obj.filter_type == 'date_range':
                    start_date, end_date = filter_obj.values
                    col1, col2 = st.columns(2)
                    with col1:
                        start_selected = st.date_input(
                            "From:",
                            value=filter_obj.active_values[0],
                            key=f"filter_{filter_obj.filter_id}_start"
                        )
                    with col2:
                        end_selected = st.date_input(
                            "To:",
                            value=filter_obj.active_values[1],
                            key=f"filter_{filter_obj.filter_id}_end"
                        )
                    
                    if [start_selected, end_selected] != filter_obj.active_values:
                        filter_changes[filter_obj.filter_id] = [start_selected, end_selected]
        
        # Clear all filters button
        if st.sidebar.button("🔄 Clear All Filters"):
            filter_changes = {f.filter_id: f.values for f in filters}
        
        return filter_changes
    
    def handle_chart_click(self, chart_id: str, clicked_data: Dict, 
                          dashboard_items: List[Dict]) -> Dict[str, Any]:
        """Handle chart click events for cross-filtering"""
        
        if not clicked_data or 'points' not in clicked_data:
            return {}
        
        # Extract selection data from clicked point
        point = clicked_data['points'][0] if clicked_data['points'] else {}
        
        selection_filters = {}
        
        # Extract filter criteria from the clicked point
        if 'x' in point:
            x_col = self._get_chart_x_column(chart_id, dashboard_items)
            if x_col:
                selection_filters[x_col] = point['x']
        
        if 'customdata' in point and point['customdata']:
            # Handle custom data for complex filtering
            custom_data = point['customdata']
            if isinstance(custom_data, dict):
                selection_filters.update(custom_data)
        
        # Store the active selection
        self.active_selections[chart_id] = {
            'filters': selection_filters,
            'chart_data': point
        }
        
        return selection_filters
    
    def apply_cross_filter_selection(self, source_chart_id: str, 
                                   selection_filters: Dict[str, Any],
                                   dashboard_items: List[Dict]) -> List[Dict]:
        """Apply cross-filter selection to all other charts"""
        
        filtered_items = []
        
        for item in dashboard_items:
            item_id = item.get('id', '')
            
            # Skip the source chart
            if item_id == source_chart_id:
                filtered_items.append(item)
                continue
            
            # Apply cross-filter to this item's data
            filtered_item = copy.deepcopy(item)
            data_snapshot = filtered_item['data_snapshot']
            
            # Apply each filter criterion
            for col, value in selection_filters.items():
                if col in data_snapshot.columns:
                    if isinstance(value, (list, tuple)):
                        # Multi-value selection
                        data_snapshot = data_snapshot[data_snapshot[col].isin(value)]
                    else:
                        # Single value selection
                        data_snapshot = data_snapshot[data_snapshot[col] == value]
            
            filtered_item['data_snapshot'] = data_snapshot
            filtered_item['cross_filtered'] = True
            filtered_item['cross_filter_source'] = source_chart_id
            
            filtered_items.append(filtered_item)
        
        return filtered_items
    
    def get_drill_down_suggestions(self, chart_data: pd.DataFrame, 
                                  selected_point: Dict) -> List[str]:
        """Generate intelligent drill-down suggestions based on selected data point"""
        suggestions = []
        
        # Get categorical columns for potential drill-downs
        categorical_cols = chart_data.select_dtypes(include=['object', 'category']).columns
        numeric_cols = chart_data.select_dtypes(include=['number']).columns
        
        # Suggest categorical drill-downs
        for col in categorical_cols:
            if col not in selected_point:
                unique_values = chart_data[col].nunique()
                if 2 <= unique_values <= 50:  # Reasonable for drill-down
                    suggestions.append({
                        'type': 'categorical',
                        'column': col,
                        'display': f"Drill down by {col.replace('_', ' ').title()}",
                        'values': chart_data[col].unique()[:10].tolist()
                    })
        
        # Suggest time-based drill-downs if date columns exist
        date_cols = chart_data.select_dtypes(include=['datetime64']).columns
        for col in date_cols:
            if col not in selected_point:
                suggestions.append({
                    'type': 'temporal',
                    'column': col,
                    'display': f"Drill down by time period ({col.replace('_', ' ').title()})",
                    'granularities': ['Year', 'Quarter', 'Month', 'Week', 'Day']
                })
        
        # Suggest metric-based drill-downs
        if len(numeric_cols) > 1:
            for col in numeric_cols:
                if col not in selected_point:
                    suggestions.append({
                        'type': 'metric',
                        'column': col,
                        'display': f"Compare by {col.replace('_', ' ').title()}",
                        'analysis_types': ['Top 10', 'Bottom 10', 'Above Average', 'Below Average']
                    })
        
        return suggestions[:5]  # Limit to top 5 suggestions
    
    def _get_chart_x_column(self, chart_id: str, dashboard_items: List[Dict]) -> Optional[str]:
        """Get the X-axis column for a specific chart"""
        for item in dashboard_items:
            if item.get('id') == chart_id:
                params = item.get('params', {})
                return params.get('x')
        return None
    
    def apply_drill_down(self, base_data: pd.DataFrame, drill_down_config: Dict) -> pd.DataFrame:
        """Apply drill-down transformation to data"""
        
        drill_type = drill_down_config.get('type')
        column = drill_down_config.get('column')
        
        if drill_type == 'categorical' and column in base_data.columns:
            # Group by the drill-down column
            selected_values = drill_down_config.get('selected_values', [])
            if selected_values:
                return base_data[base_data[column].isin(selected_values)]
        
        elif drill_type == 'temporal' and column in base_data.columns:
            # Time-based drill-down
            granularity = drill_down_config.get('granularity', 'Month')
            if granularity == 'Month':
                base_data['drill_period'] = base_data[column].dt.to_period('M')
            elif granularity == 'Quarter':
                base_data['drill_period'] = base_data[column].dt.to_period('Q')
            elif granularity == 'Year':
                base_data['drill_period'] = base_data[column].dt.to_period('Y')
            
        elif drill_type == 'metric' and column and column in base_data.columns:
            # Metric-based drill-down
            analysis_type = drill_down_config.get('analysis_type', 'Top 10')
            if analysis_type == 'Top 10':
                return base_data.nlargest(10, column)
            elif analysis_type == 'Bottom 10':
                return base_data.nsmallest(10, column)
            elif analysis_type == 'Above Average':
                avg_val = base_data[column].mean()
                return base_data[base_data[column] > avg_val]
            elif analysis_type == 'Below Average':
                avg_val = base_data[column].mean()
                return base_data[base_data[column] <= avg_val]
        
        return base_data
    
    def clear_cross_filters(self):
        """Clear all active cross-filter selections"""
        self.active_selections = {}
        self.cross_filter_selection = {}
    
    def get_filter_breadcrumbs(self) -> List[str]:
        """Get breadcrumb trail of applied filters"""
        breadcrumbs = []
        
        # Add AI filter breadcrumbs
        for filter_obj in self.dashboard_filters.values():
            if filter_obj.active_values != filter_obj.values:
                breadcrumbs.append(f"{filter_obj.name}: {filter_obj.active_values}")
        
        # Add cross-filter breadcrumbs
        for chart_id, selection in self.active_selections.items():
            filters = selection.get('filters', {})
            for col, val in filters.items():
                breadcrumbs.append(f"{col}: {val}")
        
        return breadcrumbs

# Enhanced dashboard rendering functions
def render_enhanced_dashboard():
    """Main enhanced dashboard rendering function"""
    
    # Initialize enhanced dashboard
    if 'enhanced_dashboard' not in st.session_state:
        st.session_state.enhanced_dashboard = EnhancedDashboard()
    
    enhanced_dash = st.session_state.enhanced_dashboard
    
    # Get dashboard items
    dashboard_items = st.session_state.get('dashboard_items', [])
    if not dashboard_items:
        st.info("Add charts to see enhanced dashboard features!")
        return
    
    # Generate AI filters if not already done
    if 'dashboard_ai_filters' not in st.session_state:
        st.session_state.dashboard_ai_filters = enhanced_dash.generate_ai_filters(dashboard_items)
    
    ai_filters = st.session_state.dashboard_ai_filters
    
    # Render filters in sidebar (only if filters exist)
    filter_changes = {}
    if ai_filters:
        filter_changes = enhanced_dash.render_filter_sidebar(ai_filters)
        
        # Update filter active values
        for filter_id, new_values in filter_changes.items():
            for filter_obj in ai_filters:
                if filter_obj.filter_id == filter_id:
                    filter_obj.active_values = new_values
                    break
    
    # Enhanced dashboard header
    col1, col2, col3 = st.columns([3, 1, 1])
    with col1:
        st.markdown("### 📊 Enhanced Dashboard")
    with col2:
        if st.button("🤖 Regenerate AI Filters", help="Regenerate AI-suggested filters"):
            st.session_state.dashboard_ai_filters = enhanced_dash.generate_ai_filters(dashboard_items)
            st.rerun()
    with col3:
        show_cross_filter = st.checkbox("🔗 Cross-filtering", value=True, 
                                       help="Enable cross-filtering between charts")
    
    # Display filter status
    if ai_filters:
        active_filters = [f for f in ai_filters if f.active_values != f.values]
        if active_filters:
            filter_info = ", ".join([f.name for f in active_filters])
            st.info(f"🎛️ Active filters: {filter_info}")
    
    return enhanced_dash, ai_filters, show_cross_filter

def render_compact_management_controls(item_index: int, total_items: int, 
                                     manage_mode: bool) -> None:
    """Render compact management controls for dashboard items"""
    if not manage_mode:
        return
    
    # Compact button row
    cols = st.columns([1, 1, 1, 1, 4])  # Small buttons, larger space
    
    with cols[0]:
        if st.button("⬆️", key=f"up_{item_index}", 
                    disabled=(item_index == 0),
                    help="Move Up"):
            # Move up logic here
            pass
    
    with cols[1]:
        if st.button("⬇️", key=f"down_{item_index}", 
                    disabled=(item_index == total_items - 1),
                    help="Move Down"):
            # Move down logic here
            pass
    
    with cols[2]:
        if st.button("📋", key=f"copy_{item_index}", help="Duplicate"):
            # Duplicate logic here
            pass
    
    with cols[3]:
        if st.button("🗑️", key=f"delete_{item_index}", help="Delete"):
            # Delete logic here
            pass

def create_cross_filtering_dashboard():
    """Create dashboard with cross-filtering capabilities"""
    
    # This would integrate with your existing dashboard
    st.markdown("""
    <style>
    .compact-btn {
        padding: 0.2rem 0.4rem !important;
        margin: 0.1rem !important;
        font-size: 0.8rem !important;
        min-height: 1.5rem !important;
    }
    
    .dashboard-item {
        border: 1px solid rgba(255,255,255,0.1);
        border-radius: 8px;
        padding: 1rem;
        margin: 0.5rem 0;
        background: rgba(255,255,255,0.05);
    }
    
    .filter-chip {
        display: inline-block;
        background: rgba(98, 0, 234, 0.2);
        color: white;
        padding: 0.2rem 0.5rem;
        border-radius: 12px;
        margin: 0.1rem;
        font-size: 0.8rem;
    }
    </style>
    """, unsafe_allow_html=True)

# Enhanced cross-filtering UI components
def render_cross_filter_controls(enhanced_dash: EnhancedDashboard) -> None:
    """Render cross-filtering control panel"""
    
    if enhanced_dash.active_selections:
        st.sidebar.markdown("### 🔗 Active Cross-Filters")
        
        for chart_id, selection in enhanced_dash.active_selections.items():
            filters = selection.get('filters', {})
            if filters:
                with st.sidebar.expander(f"Chart {chart_id[:8]}...", expanded=True):
                    for col, val in filters.items():
                        st.write(f"**{col}:** {val}")
                    
                    if st.button(f"Clear Filter", key=f"clear_{chart_id}"):
                        del enhanced_dash.active_selections[chart_id]
                        st.rerun()
        
        # Clear all filters button
        if st.sidebar.button("🔄 Clear All Cross-Filters"):
            enhanced_dash.clear_cross_filters()
            st.rerun()

def render_drill_down_panel(enhanced_dash: EnhancedDashboard, 
                           chart_data: pd.DataFrame, 
                           selected_point: Dict) -> None:
    """Render drill-down suggestions panel"""
    
    suggestions = enhanced_dash.get_drill_down_suggestions(chart_data, selected_point)
    
    if suggestions:
        st.sidebar.markdown("### 🔍 Drill-Down Options")
        
        for i, suggestion in enumerate(suggestions):
            with st.sidebar.expander(suggestion['display'], expanded=False):
                
                if suggestion['type'] == 'categorical':
                    values = suggestion.get('values', [])
                    selected_values = st.multiselect(
                        "Select values:",
                        options=values,
                        default=values[:3] if len(values) > 3 else values,
                        key=f"drill_cat_{i}"
                    )
                    
                    if st.button("Apply Drill-Down", key=f"apply_cat_{i}"):
                        drill_config = {
                            'type': 'categorical',
                            'column': suggestion['column'],
                            'selected_values': selected_values
                        }
                        # Store drill-down config in session state
                        st.session_state.pending_drill_down = drill_config
                        st.rerun()
                
                elif suggestion['type'] == 'temporal':
                    granularities = suggestion.get('granularities', [])
                    selected_granularity = st.selectbox(
                        "Time granularity:",
                        options=granularities,
                        key=f"drill_temp_{i}"
                    )
                    
                    if st.button("Apply Time Drill-Down", key=f"apply_temp_{i}"):
                        drill_config = {
                            'type': 'temporal',
                            'column': suggestion['column'],
                            'granularity': selected_granularity
                        }
                        st.session_state.pending_drill_down = drill_config
                        st.rerun()
                
                elif suggestion['type'] == 'metric':
                    analysis_types = suggestion.get('analysis_types', [])
                    selected_analysis = st.selectbox(
                        "Analysis type:",
                        options=analysis_types,
                        key=f"drill_metric_{i}"
                    )
                    
                    if st.button("Apply Metric Drill-Down", key=f"apply_metric_{i}"):
                        drill_config = {
                            'type': 'metric',
                            'column': suggestion['column'],
                            'analysis_type': selected_analysis
                        }
                        st.session_state.pending_drill_down = drill_config
                        st.rerun()

def render_filter_breadcrumbs(enhanced_dash: EnhancedDashboard) -> None:
    """Render breadcrumb trail of active filters"""
    
    breadcrumbs = enhanced_dash.get_filter_breadcrumbs()
    
    if breadcrumbs:
        st.markdown("### 📍 Active Filters")
        
        # Display breadcrumbs as chips
        breadcrumb_html = '<div style="margin: 10px 0;">'
        for breadcrumb in breadcrumbs:
            breadcrumb_html += f'''
            <span style="
                display: inline-block;
                background: rgba(98, 0, 234, 0.3);
                color: white;
                padding: 0.3rem 0.8rem;
                border-radius: 15px;
                margin: 0.2rem;
                font-size: 0.8rem;
            ">{breadcrumb}</span>
            '''
        breadcrumb_html += '</div>'
        
        st.markdown(breadcrumb_html, unsafe_allow_html=True)

def create_interactive_chart_with_click_handler(enhanced_dash: EnhancedDashboard,
                                              item: Dict, 
                                              filtered_data: pd.DataFrame,
                                              chart_id: str) -> Optional[go.Figure]:
    """Create interactive chart with cross-filtering click handlers"""
    
    fig = enhanced_dash.create_enhanced_chart(item, filtered_data, chart_id)
    
    if fig:
        # Enhanced click event configuration
        fig.update_layout(
            clickmode='event+select',
            dragmode='select',
            # Add custom click data
            updatemenus=[{
                'type': 'buttons',
                'direction': 'left',
                'showactive': True,
                'x': 0.01,
                'xanchor': 'left',
                'y': 1.02,
                'yanchor': 'top',
                'buttons': [
                    {
                        'label': 'Reset Zoom',
                        'method': 'relayout',
                        'args': ['xaxis.autorange', True, 'yaxis.autorange', True]
                    },
                    {
                        'label': 'Clear Selection',
                        'method': 'restyle',
                        'args': ['selectedpoints', None]
                    }
                ]
            }]
        )
        
        # Add cross-filter indication if this chart is filtered
        if item.get('cross_filtered'):
            fig.update_layout(
                title={
                    'text': f"{item.get('title', 'Chart')} (Filtered)",
                    'font': {'color': '#90CDF4'}
                },
                plot_bgcolor='rgba(100, 200, 255, 0.1)'
            )
    
    return fig

# Legacy function for backward compatibility
def handle_chart_click(chart_data: pd.DataFrame, clicked_point: Dict, 
                      dashboard_items: List[Dict]) -> None:
    """Legacy chart click handler - use enhanced_dash.handle_chart_click instead"""
    st.warning("⚠️ Using legacy chart click handler. Consider upgrading to enhanced mode.") 