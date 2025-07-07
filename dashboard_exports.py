"""
Dashboard Export Module for ConvaBI
Handles HTML, PDF, and email exports of dashboards
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import base64
import json
import os
import tempfile
from datetime import datetime
from typing import Dict, List, Any, Optional
import copy
from io import BytesIO
import zipfile

# Email imports
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

class DashboardExporter:
    """Main class for handling dashboard exports"""
    
    def __init__(self):
        self.temp_files = []
    
    def cleanup_temp_files(self):
        """Clean up temporary files"""
        for file_path in self.temp_files:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
            except Exception as e:
                print(f"[EXPORT] Could not delete temp file {file_path}: {e}")
        self.temp_files = []
    
    def export_to_html(self, dashboard_items: List[Dict], dashboard_name: str, 
                      username: str, include_data: bool = True) -> str:
        """Export dashboard to HTML format"""
        
        html_content = self._generate_html_content(
            dashboard_items, dashboard_name, username, include_data
        )
        
        return html_content
    
    def export_to_pdf(self, dashboard_items: List[Dict], dashboard_name: str, 
                     username: str) -> Optional[bytes]:
        """Export dashboard to PDF format with static images"""
        try:
            # Generate HTML with static images for PDF compatibility
            html_content = self._generate_html_content(
                dashboard_items, dashboard_name, username, include_data=False, for_pdf=True
            )
            
            # Convert to PDF using available libraries
            return self._html_to_pdf(html_content)
            
        except Exception as e:
            print(f"[EXPORT] PDF generation error: {e}")
            return None
    
    def export_to_email(self, dashboard_items: List[Dict], dashboard_name: str,
                       username: str, recipient_email: str, 
                       export_format: str = "html") -> bool:
        """Export dashboard via email"""
        try:
            # Get email settings from database
            email_settings = self._get_email_settings()
            if not email_settings:
                st.error("❌ Email settings not configured. Please configure SMTP settings in Admin panel.")
                return False
            
            # Generate export content
            if export_format.lower() == "pdf":
                content = self.export_to_pdf(dashboard_items, dashboard_name, username)
                attachment_name = f"{dashboard_name}_dashboard.pdf"
                content_type = "application/pdf"
            else:
                content = self.export_to_html(dashboard_items, dashboard_name, username)
                attachment_name = f"{dashboard_name}_dashboard.html"
                content_type = "text/html"
            
            if not content:
                st.error("❌ Failed to generate export content")
                return False
            
            # Send email
            return self._send_export_email(
                email_settings, recipient_email, dashboard_name, 
                content, attachment_name, content_type
            )
            
        except Exception as e:
            print(f"[EXPORT] Email export error: {e}")
            st.error(f"❌ Email export failed: {e}")
            return False
    
    def export_data_to_csv(self, dashboard_items: List[Dict], 
                          dashboard_name: str) -> Optional[bytes]:
        """Export dashboard data to CSV files in a ZIP archive"""
        try:
            zip_buffer = BytesIO()
            
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                for i, item in enumerate(dashboard_items):
                    if 'data_snapshot' in item and isinstance(item['data_snapshot'], pd.DataFrame):
                        df = item['data_snapshot']
                        if not df.empty:
                            # Create CSV content
                            csv_buffer = BytesIO()
                            df.to_csv(csv_buffer, index=False)
                            csv_content = csv_buffer.getvalue()
                            
                            # Add to ZIP
                            filename = f"{item.get('title', f'Chart_{i+1}').replace(' ', '_')}.csv"
                            zip_file.writestr(filename, csv_content)
                
                # Add summary file
                summary_content = self._generate_data_summary(dashboard_items, dashboard_name)
                zip_file.writestr("dashboard_summary.txt", summary_content)
            
            zip_buffer.seek(0)
            return zip_buffer.getvalue()
            
        except Exception as e:
            print(f"[EXPORT] CSV export error: {e}")
            return None
    
    def _generate_html_content(self, dashboard_items: List[Dict], dashboard_name: str,
                              username: str, include_data: bool = True, for_pdf: bool = False) -> str:
        """Generate comprehensive HTML content for dashboard"""
        
        # Separate KPIs and charts
        kpi_items = [item for item in dashboard_items if item.get('chart_type') == 'KPI']
        other_items = [item for item in dashboard_items if item.get('chart_type') != 'KPI']
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        html_content = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>{dashboard_name} - ConvaBI Dashboard</title>
            <script src='https://cdn.plot.ly/plotly-latest.min.js'></script>
            <style>
                {self._get_export_css()}
            </style>
        </head>
        <body>
            <div class="dashboard-container">
                <header class="dashboard-header">
                    <h1>📊 {dashboard_name}</h1>
                    <div class="dashboard-meta">
                        <p><strong>Created by:</strong> {username}</p>
                        <p><strong>Exported:</strong> {timestamp}</p>
                        <p><strong>Charts:</strong> {len(dashboard_items)} items</p>
                    </div>
                </header>
        """
        
        # Add KPI section
        if kpi_items:
            html_content += '<section class="kpi-section"><h2>📈 Key Performance Indicators</h2><div class="kpi-grid">'
            
            for item in kpi_items:
                params = item['params']
                data_snapshot = item['data_snapshot']
                label = params.get('label', 'KPI')
                value_col = params.get('value_col')
                delta_col = params.get('delta_col')
                
                value = "N/A"
                delta = ""
                
                if not data_snapshot.empty and value_col in data_snapshot.columns:
                    try:
                        raw_value = data_snapshot[value_col].iloc[0]
                        if pd.isna(raw_value):
                            value = "N/A"
                        else:
                            numeric_value = pd.to_numeric(raw_value)
                            if isinstance(numeric_value, float):
                                value = f"{numeric_value:,.2f}"
                            else:
                                value = f"{numeric_value:,}"
                    except:
                        value = str(raw_value)
                    
                    if delta_col and delta_col in data_snapshot.columns:
                        try:
                            delta_val = pd.to_numeric(data_snapshot[delta_col].iloc[0])
                            delta = f"Δ {delta_val:+.2f}"
                        except:
                            delta = str(data_snapshot[delta_col].iloc[0])
                
                html_content += f"""
                <div class="kpi-card">
                    <div class="kpi-label">{label}</div>
                    <div class="kpi-value">{value}</div>
                    {f'<div class="kpi-delta">{delta}</div>' if delta else ''}
                </div>
                """
            
            html_content += '</div></section>'
        
        # Add charts section
        if other_items:
            html_content += '<section class="charts-section"><h2>📊 Charts & Visualizations</h2><div class="charts-grid">'
            
            chart_js_code = ""
            
            for i, item in enumerate(other_items):
                title = item.get('title', item['chart_type'])
                chart_type = item['chart_type']
                data_snapshot = item['data_snapshot']
                params = item['params']
                
                chart_div_id = f'chart_{i}'
                
                html_content += f'''
                <div class="chart-card">
                    <h3 class="chart-title">{title}</h3>
                    <div id="{chart_div_id}" class="chart-container"></div>
                    <div class="chart-info">
                        <span class="chart-type">{chart_type}</span>
                        <span class="data-points">{len(data_snapshot)} data points</span>
                    </div>
                </div>
                '''
                
                # Generate chart JavaScript
                if chart_type == 'Table':
                    # Handle table display
                    selected_columns = params.get('columns', data_snapshot.columns.tolist())
                    display_columns = [col for col in selected_columns if col in data_snapshot.columns]
                    
                    if display_columns and not data_snapshot.empty:
                        table_html = data_snapshot[display_columns].to_html(
                            classes='export-table', 
                            escape=False,
                            table_id=f'table_{i}'
                        )
                        html_content += f'<div class="table-container">{table_html}</div>'
                
                else:
                    # Generate Plotly chart
                    fig = self._create_chart_figure(item, data_snapshot)
                    if fig:
                        try:
                            if for_pdf:
                                # For PDF export: Convert chart to static image
                                img_bytes = self._convert_chart_to_image(fig, f"chart_{i}")
                                if img_bytes:
                                    # Embed as base64 image directly in HTML
                                    b64_image = base64.b64encode(img_bytes).decode()
                                    html_content = html_content.replace(
                                        f'<div id="{chart_div_id}" class="chart-container"></div>',
                                        f'<div id="{chart_div_id}" class="chart-container"><img src="data:image/png;base64,{b64_image}" style="width:100%; height:auto; max-height:400px; border-radius:4px;"></div>'
                                    )
                                    print(f"[EXPORT] Chart {i} converted to static image for PDF")
                                else:
                                    # If image conversion fails, show chart info instead
                                    chart_title = item.get('title', chart_type)
                                    data_points = len(data_snapshot)
                                    chart_placeholder = f"""
                                    <div class="chart-placeholder">
                                        <h4>📊 {chart_title}</h4>
                                        <p><strong>Chart Type:</strong> {chart_type}</p>
                                        <p><strong>Data Points:</strong> {data_points}</p>
                                        <p><em>Chart preview not available in PDF export.</em></p>
                                        <p><em>Use HTML export for interactive charts.</em></p>
                                    </div>
                                    """
                                    html_content = html_content.replace(
                                        f'<div id="{chart_div_id}" class="chart-container"></div>',
                                        f'<div id="{chart_div_id}" class="chart-container">{chart_placeholder}</div>'
                                    )
                                    print(f"[EXPORT] Chart {i} using placeholder for PDF")
                            else:
                                # For HTML export: Use interactive JavaScript charts
                                fig_json = fig.to_json()
                                chart_js_code += f"""
                                try {{
                                    var figData_{i} = {fig_json};
                                    Plotly.newPlot('{chart_div_id}', figData_{i}.data, figData_{i}.layout, {{
                                        displayModeBar: false,
                                        responsive: true
                                    }});
                                }} catch(e) {{
                                    console.error('Error rendering chart {i}:', e);
                                    document.getElementById('{chart_div_id}').innerHTML = 
                                        '<p class="chart-error">Chart could not be rendered</p>';
                                }}
                                """
                        except Exception as e:
                            print(f"[EXPORT] Chart {i} processing error: {e}")
                            if not for_pdf:
                                # Fallback to JavaScript version for HTML export
                                try:
                                    fig_json = fig.to_json()
                                    chart_js_code += f"""
                                    try {{
                                        var figData_{i} = {fig_json};
                                        Plotly.newPlot('{chart_div_id}', figData_{i}.data, figData_{i}.layout, {{
                                            displayModeBar: false,
                                            responsive: true
                                        }});
                                    }} catch(e) {{
                                        console.error('Error rendering chart {i}:', e);
                                        document.getElementById('{chart_div_id}').innerHTML = 
                                            '<p class="chart-error">Chart could not be rendered</p>';
                                    }}
                                    """
                                except:
                                    pass
            
            html_content += '</div></section>'
            
            # Add JavaScript for charts
            if chart_js_code:
                html_content += f"""
                <script>
                document.addEventListener('DOMContentLoaded', function() {{
                    {chart_js_code}
                }});
                </script>
                """
        
        # Add data section if requested
        if include_data and other_items:
            html_content += self._generate_data_tables_html(other_items)
        
        # Footer
        html_content += f"""
                <footer class="dashboard-footer">
                    <p>Generated by ConvaBI Dashboard System</p>
                    <p>Export created on {timestamp}</p>
                </footer>
            </div>
        </body>
        </html>
        """
        
        return html_content
    
    def _convert_chart_to_image(self, fig: go.Figure, chart_id: str) -> Optional[bytes]:
        """Convert Plotly figure to static PNG image for PDF compatibility - simplified version"""
        try:
            print(f"[EXPORT] Converting chart {chart_id} to image...")
            
            # Try using kaleido with a timeout approach
            try:
                import plotly.io as pio
                
                # Configure for better PDF output
                img_bytes = pio.to_image(
                    fig, 
                    format="png",
                    width=600,  # Reduced size for faster processing
                    height=400,
                    scale=1,    # Reduced scale for faster processing
                    engine="kaleido"
                )
                print(f"[EXPORT] Successfully converted chart {chart_id} using kaleido")
                return img_bytes
                
            except ImportError:
                print("[EXPORT] Kaleido not available")
            except Exception as e:
                print(f"[EXPORT] Kaleido conversion failed: {e}")
            
            # If kaleido fails, return None and use placeholder
            print(f"[EXPORT] Chart {chart_id} will use placeholder in PDF")
            return None
            
        except Exception as e:
            print(f"[EXPORT] Chart to image conversion error: {e}")
            return None

    def _create_chart_figure(self, item: Dict, data_snapshot: pd.DataFrame) -> Optional[go.Figure]:
        """Create Plotly figure for export"""
        chart_type = item['chart_type']
        params = item['params']
        title = item.get('title', chart_type)
        
        if data_snapshot.empty:
            return None
        
        try:
            fig = None
            
            if chart_type == "Bar Chart":
                x_col, y_col = params.get('x'), params.get('y')
                if x_col in data_snapshot.columns and y_col in data_snapshot.columns:
                    fig = px.bar(data_snapshot, x=x_col, y=y_col, 
                               color=params.get('color'), title=title)
            
            elif chart_type == "Line Chart":
                x_col, y_col = params.get('x'), params.get('y')
                if x_col in data_snapshot.columns and y_col in data_snapshot.columns:
                    fig = px.line(data_snapshot, x=x_col, y=y_col, 
                                color=params.get('color'), title=title)
            
            elif chart_type == "Scatter Plot":
                x_col, y_col = params.get('x'), params.get('y')
                if x_col in data_snapshot.columns and y_col in data_snapshot.columns:
                    fig = px.scatter(data_snapshot, x=x_col, y=y_col, 
                                   color=params.get('color'), size=params.get('size'), title=title)
            
            elif chart_type == "Pie Chart":
                names_col, values_col = params.get('names'), params.get('values')
                if names_col in data_snapshot.columns and values_col in data_snapshot.columns:
                    fig = px.pie(data_snapshot, names=names_col, values=values_col, title=title)
            
            elif chart_type == "Histogram":
                x_col = params.get('x')
                if x_col in data_snapshot.columns:
                    fig = px.histogram(data_snapshot, x=x_col, title=title)
            
            if fig:
                # Apply export-friendly styling
                fig.update_layout(
                    template="plotly_white",
                    font=dict(size=14, family="Arial, sans-serif"),
                    title_font_size=18,
                    title_font_color="#333333",
                    width=800,
                    height=500,
                    margin=dict(l=60, r=60, t=80, b=60),
                    paper_bgcolor='white',
                    plot_bgcolor='white',
                    showlegend=True,
                    legend=dict(
                        font=dict(size=12),
                        bgcolor="rgba(255,255,255,0.8)",
                        bordercolor="#E0E0E0",
                        borderwidth=1
                    )
                )
                
                # Optimize axes for PDF readability
                fig.update_xaxes(
                    title_font=dict(size=14, color="#333333"),
                    tickfont=dict(size=12, color="#333333"),
                    linecolor="#CCCCCC",
                    gridcolor="#F0F0F0"
                )
                fig.update_yaxes(
                    title_font=dict(size=14, color="#333333"),
                    tickfont=dict(size=12, color="#333333"),
                    linecolor="#CCCCCC",
                    gridcolor="#F0F0F0"
                )
            
            return fig
            
        except Exception as e:
            print(f"[EXPORT] Chart creation error: {e}")
            return None
    
    def _generate_data_tables_html(self, dashboard_items: List[Dict]) -> str:
        """Generate HTML for data tables section"""
        html_content = '<section class="data-section"><h2>📋 Data Tables</h2>'
        
        for i, item in enumerate(dashboard_items):
            if 'data_snapshot' in item and isinstance(item['data_snapshot'], pd.DataFrame):
                df = item['data_snapshot']
                if not df.empty:
                    title = item.get('title', f'Chart {i+1}')
                    html_content += f'''
                    <div class="data-table-card">
                        <h3>{title} - Data</h3>
                        <div class="table-container">
                            {df.to_html(classes='export-table', escape=False)}
                        </div>
                    </div>
                    '''
        
        html_content += '</section>'
        return html_content
    
    def _generate_data_summary(self, dashboard_items: List[Dict], dashboard_name: str) -> str:
        """Generate text summary of dashboard data"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        summary = f"""
Dashboard Data Export Summary
============================

Dashboard Name: {dashboard_name}
Export Date: {timestamp}
Total Items: {len(dashboard_items)}

Items Breakdown:
"""
        
        for i, item in enumerate(dashboard_items, 1):
            chart_type = item.get('chart_type', 'Unknown')
            title = item.get('title', f'Item {i}')
            data_snapshot = item.get('data_snapshot')
            
            if isinstance(data_snapshot, pd.DataFrame):
                rows = len(data_snapshot)
                cols = len(data_snapshot.columns)
                summary += f"  {i}. {title} ({chart_type}): {rows} rows, {cols} columns\n"
            else:
                summary += f"  {i}. {title} ({chart_type}): No data\n"
        
        summary += f"\nGenerated by ConvaBI Dashboard System\n"
        return summary
    
    def _get_export_css(self) -> str:
        """Get CSS styling for exported dashboards"""
        return """
        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            color: #333;
            background-color: #f8f9fa;
        }
        
        .dashboard-container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }
        
        .dashboard-header {
            background: linear-gradient(135deg, #6200ea 0%, #200079 100%);
            color: white;
            padding: 2rem;
            border-radius: 12px;
            margin-bottom: 2rem;
            text-align: center;
        }
        
        .dashboard-header h1 {
            font-size: 2.5rem;
            margin-bottom: 1rem;
        }
        
        .dashboard-meta {
            display: flex;
            justify-content: center;
            gap: 2rem;
            flex-wrap: wrap;
        }
        
        .dashboard-meta p {
            margin: 0;
            opacity: 0.9;
        }
        
        .kpi-section, .charts-section, .data-section {
            margin-bottom: 3rem;
        }
        
        .kpi-section h2, .charts-section h2, .data-section h2 {
            color: #6200ea;
            margin-bottom: 1.5rem;
            border-bottom: 2px solid #6200ea;
            padding-bottom: 0.5rem;
        }
        
        .kpi-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2rem;
        }
        
        .kpi-card {
            background: white;
            padding: 1.5rem;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            text-align: center;
            border-left: 4px solid #6200ea;
        }
        
        .kpi-label {
            font-size: 0.9rem;
            color: #666;
            margin-bottom: 0.5rem;
            font-weight: 500;
        }
        
        .kpi-value {
            font-size: 2rem;
            font-weight: bold;
            color: #333;
            margin-bottom: 0.5rem;
        }
        
        .kpi-delta {
            font-size: 1rem;
            color: #6200ea;
            font-weight: 500;
        }
        
        .charts-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
            gap: 2rem;
        }
        
        .chart-card {
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            overflow: hidden;
        }
        
        .chart-title {
            background: #f8f9fa;
            padding: 1rem;
            margin: 0;
            font-size: 1.2rem;
            color: #333;
            border-bottom: 1px solid #e9ecef;
        }
        
        .chart-container {
            padding: 1rem;
            min-height: 400px;
        }
        
        .chart-info {
            background: #f8f9fa;
            padding: 0.5rem 1rem;
            border-top: 1px solid #e9ecef;
            display: flex;
            justify-content: space-between;
            font-size: 0.9rem;
            color: #666;
        }
        
        .chart-error {
            color: #dc3545;
            text-align: center;
            padding: 2rem;
            font-style: italic;
        }
        
        .chart-placeholder {
            background: #f8f9fa;
            border: 2px dashed #dee2e6;
            border-radius: 8px;
            padding: 2rem;
            text-align: center;
            color: #495057;
            margin: 1rem 0;
        }
        
        .chart-placeholder h4 {
            color: #6200ea;
            margin-bottom: 1rem;
            font-size: 1.2rem;
        }
        
        .chart-placeholder p {
            margin: 0.5rem 0;
            font-size: 0.9rem;
        }
        
        .chart-placeholder em {
            color: #6c757d;
            font-style: italic;
        }
        
        .table-container {
            overflow-x: auto;
            margin: 1rem 0;
        }
        
        .export-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
        }
        
        .export-table th, .export-table td {
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
        }
        
        .export-table th {
            background-color: #6200ea;
            color: white;
            font-weight: 600;
        }
        
        .export-table tr:nth-child(even) {
            background-color: #f9f9f9;
        }
        
        .data-table-card {
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            margin-bottom: 2rem;
            overflow: hidden;
        }
        
        .data-table-card h3 {
            background: #f8f9fa;
            padding: 1rem;
            margin: 0;
            border-bottom: 1px solid #e9ecef;
        }
        
        .dashboard-footer {
            text-align: center;
            padding: 2rem;
            color: #666;
            border-top: 1px solid #e9ecef;
            margin-top: 3rem;
        }
        
        @media print {
            body { background-color: white; }
            .dashboard-container { max-width: none; margin: 0; padding: 10px; }
            .chart-card, .kpi-card, .data-table-card { 
                break-inside: avoid; 
                margin-bottom: 1rem;
            }
            .charts-grid { grid-template-columns: 1fr; }
            .kpi-grid { grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); }
        }
        """
    
    def _html_to_pdf(self, html_content: str) -> Optional[bytes]:
        """Convert HTML to PDF using available libraries (Windows optimized)"""
        try:
            print("[EXPORT] Starting PDF conversion...")
            
            # Try using xhtml2pdf first (most reliable on Windows)
            try:
                print("[EXPORT] Attempting PDF generation with xhtml2pdf...")
                from xhtml2pdf import pisa
                from io import BytesIO
                
                # Create a BytesIO object to store the PDF
                pdf_buffer = BytesIO()
                
                # Generate PDF
                pisa_status = pisa.CreatePDF(html_content, dest=pdf_buffer)
                
                # Check if PDF was created successfully
                has_errors = hasattr(pisa_status, 'err') and bool(getattr(pisa_status, 'err', False))
                if not has_errors:
                    pdf_buffer.seek(0)
                    pdf_content = pdf_buffer.getvalue()
                    pdf_buffer.close()
                    print(f"[EXPORT] PDF generated successfully using xhtml2pdf ({len(pdf_content)} bytes)")
                    return pdf_content
                else:
                    error_msg = getattr(pisa_status, 'err', 'Unknown error')
                    print(f"[EXPORT] xhtml2pdf errors: {error_msg}")
                
            except ImportError:
                print("[EXPORT] xhtml2pdf not available")
            except Exception as e:
                print(f"[EXPORT] xhtml2pdf failed: {e}")
            
            # Try using reportlab as a simple fallback (text-only)
            try:
                print("[EXPORT] Attempting basic PDF generation with ReportLab...")
                from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
                from reportlab.lib.styles import getSampleStyleSheet
                from reportlab.lib.pagesizes import letter
                from io import BytesIO
                import re
                
                # Create a BytesIO object to store the PDF
                pdf_buffer = BytesIO()
                
                # Create the PDF document
                doc = SimpleDocTemplate(pdf_buffer, pagesize=letter)
                styles = getSampleStyleSheet()
                story = []
                
                # Extract text content from HTML (simple approach)
                text_content = re.sub('<[^<]+?>', '', html_content)
                text_content = re.sub(r'\s+', ' ', text_content).strip()
                
                # Add content to PDF
                story.append(Paragraph("Dashboard Export - ConvaBI", styles['Title']))
                story.append(Spacer(1, 12))
                story.append(Paragraph("This is a simplified text-only PDF export. For full interactive charts, please use HTML export.", styles['Normal']))
                story.append(Spacer(1, 12))
                
                # Split content into paragraphs
                paragraphs = text_content.split('\n\n')
                for para in paragraphs[:10]:  # Limit to first 10 paragraphs to avoid issues
                    if para.strip():
                        story.append(Paragraph(para.strip()[:200] + "..." if len(para) > 200 else para.strip(), styles['Normal']))
                        story.append(Spacer(1, 6))
                
                # Build PDF
                doc.build(story)
                pdf_buffer.seek(0)
                pdf_content = pdf_buffer.getvalue()
                pdf_buffer.close()
                
                print(f"[EXPORT] Basic PDF generated using ReportLab ({len(pdf_content)} bytes)")
                return pdf_content
                
            except ImportError:
                print("[EXPORT] ReportLab not available")
            except Exception as e:
                print(f"[EXPORT] ReportLab failed: {e}")
            
            # If all methods fail, return None
            print("[EXPORT] No PDF library worked - PDF generation failed")
            return None
            
        except Exception as e:
            print(f"[EXPORT] PDF conversion error: {e}")
            return None
    
    def _get_email_settings(self) -> Optional[Dict]:
        """Get email settings from database"""
        try:
            import database
            
            conn = database.get_db_connection()
            if not conn:
                return None
            
            cursor = conn.cursor()
            cursor.execute("""
                SELECT smtp_server, smtp_port, smtp_username, smtp_password, use_tls, from_email 
                FROM email_settings 
                WHERE is_active = TRUE 
                ORDER BY id DESC LIMIT 1
            """)
            result = cursor.fetchone()
            conn.close()
            
            if result is not None:
                return {
                    'smtp_server': result[0],
                    'smtp_port': result[1],
                    'smtp_username': result[2],
                    'smtp_password': result[3],
                    'use_tls': result[4],
                    'from_email': result[5]
                }
            
            return None
            
        except Exception as e:
            print(f"[EXPORT] Email settings error: {e}")
            return None
    
    def _send_export_email(self, email_settings: Dict, recipient_email: str,
                          dashboard_name: str, content: Any, attachment_name: str,
                          content_type: str) -> bool:
        """Send dashboard export via email"""
        try:
            # Create email message
            msg = MIMEMultipart()
            msg['From'] = email_settings['from_email']
            msg['To'] = recipient_email
            msg['Subject'] = f"Dashboard Export: {dashboard_name}"
            
            # Email body
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            body = f"""
Dear User,

Please find attached your requested dashboard export for "{dashboard_name}".

Export Details:
- Dashboard: {dashboard_name}
- Export Time: {timestamp}
- Format: {content_type}

This export was generated by the ConvaBI Dashboard System.

Best regards,
ConvaBI Team
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            # Add attachment
            if isinstance(content, str):
                content = content.encode('utf-8')
            
            attachment = MIMEApplication(content, _subtype='octet-stream')
            attachment.add_header('Content-Disposition', 'attachment', filename=attachment_name)
            msg.attach(attachment)
            
            # Send email
            server = smtplib.SMTP(email_settings['smtp_server'], email_settings['smtp_port'])
            if email_settings['use_tls']:
                server.starttls()
            
            server.login(email_settings['smtp_username'], email_settings['smtp_password'])
            server.send_message(msg)
            server.quit()
            
            return True
            
        except Exception as e:
            print(f"[EXPORT] Email send error: {e}")
            return False

# Streamlit UI components for export functionality
def show_export_interface(dashboard_items: List[Dict], dashboard_name: str, username: str):
    """Show export interface in Streamlit - HTML export only (reliable option)"""
    
    if not dashboard_items:
        st.warning("⚠️ Dashboard is empty. Nothing to export.")
        return
    
    exporter = DashboardExporter()
    
    st.markdown("### 📤 Export Dashboard")
    st.info("💡 **Tip**: After downloading HTML, you can convert it to PDF using your browser's 'Print → Save as PDF' option for high-quality results!")
    
    # Export format selection - HTML focused options
    export_format = st.selectbox(
        "Export Format:",
        ["HTML (Interactive Charts)", "Email (HTML)", "Data (CSV)"],
        help="HTML export provides the best quality with interactive charts"
    )
    
    # Additional options
    col1, col2 = st.columns(2)
    with col1:
        include_data = st.checkbox("Include raw data tables", value=False, 
                                 help="Include data tables in the export")
    with col2:
        if export_format is not None and export_format.startswith("Email"):
            recipient_email = st.text_input("Recipient Email:", 
                                           placeholder="user@example.com")
    
    # Export button
    if st.button("🚀 Generate Export", type="primary"):
        
        with st.spinner(f"Generating {export_format} export..."):
            
            try:
                if export_format == "HTML (Interactive Charts)":
                    # HTML Export
                    html_content = exporter.export_to_html(
                        dashboard_items, dashboard_name, username, include_data
                    )
                    
                    # Create download
                    b64_html = base64.b64encode(html_content.encode()).decode()
                    href = f'<a href="data:text/html;base64,{b64_html}" download="{dashboard_name}_dashboard.html">📥 Download HTML Dashboard</a>'
                    st.markdown(href, unsafe_allow_html=True)
                    st.success("✅ HTML export ready for download!")
                    
                    # Show conversion tip
                    with st.expander("💡 How to Convert to PDF"):
                        st.markdown("""
                        **To create a PDF from your HTML export:**
                        
                        1. **Download and open** the HTML file in your browser
                        2. **Print the page** (Ctrl+P or Cmd+P)
                        3. **Select "Save as PDF"** as the destination
                        4. **Adjust settings** if needed (margins, scale, etc.)
                        5. **Save** your high-quality PDF
                        
                        This method gives you the **best quality PDF** with properly formatted charts and layouts!
                        """)

                elif export_format == "Email (HTML)":
                    # Email HTML Export
                    if not recipient_email:
                        st.error("❌ Please enter a recipient email address")
                    else:
                        success = exporter.export_to_email(
                            dashboard_items, dashboard_name, username, 
                            recipient_email, "html"
                        )
                        
                        if success:
                            st.success(f"✅ Dashboard emailed to {recipient_email}!")
                        else:
                            st.error("❌ Email export failed")
                
                elif export_format == "Data (CSV)":
                    # CSV Data Export
                    csv_data = exporter.export_data_to_csv(dashboard_items, dashboard_name)
                    
                    if csv_data:
                        b64_zip = base64.b64encode(csv_data).decode()
                        href = f'<a href="data:application/zip;base64,{b64_zip}" download="{dashboard_name}_data.zip">📥 Download Data (ZIP)</a>'
                        st.markdown(href, unsafe_allow_html=True)
                        st.success("✅ CSV data export ready for download!")
                    else:
                        st.error("❌ CSV export failed")
                
            except Exception as e:
                st.error(f"❌ Export failed: {e}")
                print(f"[EXPORT] Export error: {e}")
            
            finally:
                # Cleanup temporary files
                exporter.cleanup_temp_files()

def get_export_stats(dashboard_items: List[Dict]) -> Dict[str, Any]:
    """Get statistics about exportable content"""
    
    total_items = len(dashboard_items)
    kpi_count = len([item for item in dashboard_items if item.get('chart_type') == 'KPI'])
    chart_count = total_items - kpi_count
    
    total_data_points = 0
    total_columns = 0
    
    for item in dashboard_items:
        if 'data_snapshot' in item and isinstance(item['data_snapshot'], pd.DataFrame):
            df = item['data_snapshot']
            total_data_points += len(df)
            total_columns += len(df.columns)
    
    return {
        'total_items': total_items,
        'kpi_count': kpi_count,
        'chart_count': chart_count,
        'total_data_points': total_data_points,
        'total_columns': total_columns
    } 