"""
Dashboard Export Service
========================

This service handles exporting dashboards to PDF and PNG formats.
Uses Playwright for PNG screenshots and WeasyPrint for PDF generation.
"""

import os
import json
import logging
import tempfile
from datetime import datetime
from typing import Tuple, Optional
from io import BytesIO

from django.conf import settings
from django.template.loader import render_to_string
from django.template import Template, Context

logger = logging.getLogger(__name__)


class DashboardExportService:
    """Service for exporting dashboards to various formats"""
    
    def __init__(self):
        self.temp_dir = tempfile.gettempdir()
    
    def export_dashboard_png(self, dashboard) -> Tuple[bytes, str]:
        """
        Export dashboard as PNG image using Playwright
        
        Args:
            dashboard: Dashboard model instance
            
        Returns:
            tuple: (image_bytes, filename)
        """
        try:
            # Try to use playwright for high-quality screenshots
            return self._export_with_playwright(dashboard, 'png')
        except ImportError:
            logger.warning("Playwright not available, using basic HTML to PNG conversion")
            return self._export_basic_png(dashboard)
        except Exception as e:
            logger.error(f"Error exporting dashboard PNG: {e}")
            return self._export_basic_png(dashboard)
    
    def export_dashboard_pdf(self, dashboard) -> Tuple[bytes, str]:
        """
        Export dashboard as PDF using WeasyPrint
        
        Args:
            dashboard: Dashboard model instance
            
        Returns:
            tuple: (pdf_bytes, filename)
        """
        try:
            return self._export_with_weasyprint(dashboard)
        except ImportError:
            logger.warning("WeasyPrint not available, using basic PDF generation")
            return self._export_basic_pdf(dashboard)
        except Exception as e:
            logger.error(f"Error exporting dashboard PDF: {e}")
            return self._export_basic_pdf(dashboard)
    
    def _export_with_playwright(self, dashboard, format_type='png') -> Tuple[bytes, str]:
        """Export using Playwright for high-quality rendering"""
        try:
            from playwright.sync_api import sync_playwright
            
            # Generate HTML content
            html_content = self._generate_dashboard_html_with_data(dashboard, for_export=True)
            
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page(
                    viewport={'width': 1200, 'height': 800},
                    device_scale_factor=2  # High DPI
                )
                
                # Set content and wait for rendering
                page.set_content(html_content, wait_until='networkidle')
                
                # Take screenshot
                screenshot_bytes = page.screenshot(
                    type=format_type,
                    full_page=True,
                    quality=95 if format_type == 'jpeg' else None
                )
                
                browser.close()
                
                filename = f"dashboard_{dashboard.name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{format_type}"
                return screenshot_bytes, filename
                
        except Exception as e:
            logger.error(f"Playwright export failed: {e}")
            raise
    
    def _export_with_weasyprint(self, dashboard) -> Tuple[bytes, str]:
        """Export PDF using WeasyPrint"""
        try:
            import weasyprint
            
            # Generate enhanced HTML content with proper chart data
            html_content = self._generate_dashboard_html_with_data(dashboard, for_pdf=True)
            
            # Generate PDF with better configuration
            html_doc = weasyprint.HTML(string=html_content, base_url=settings.BASE_DIR)
            pdf_bytes = html_doc.write_pdf(
                optimize_images=True,
                pdf_version='1.7',
                pdf_forms=False
            )
            
            filename = f"dashboard_{dashboard.name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
            return pdf_bytes, filename
            
        except Exception as e:
            logger.error(f"WeasyPrint export failed: {e}")
            raise
    
    def _export_basic_png(self, dashboard) -> Tuple[bytes, str]:
        """Fallback PNG export using Pillow"""
        try:
            from PIL import Image, ImageDraw, ImageFont
            
            # Create a basic image with dashboard info
            img = Image.new('RGB', (800, 600), color='white')
            draw = ImageDraw.Draw(img)
            
            # Try to use a better font
            try:
                font_title = ImageFont.truetype("arial.ttf", 24)
                font_text = ImageFont.truetype("arial.ttf", 14)
            except:
                font_title = ImageFont.load_default()
                font_text = ImageFont.load_default()
            
            # Draw dashboard info
            draw.text((50, 50), f"Dashboard: {dashboard.name}", fill='black', font=font_title)
            draw.text((50, 100), f"Description: {dashboard.description or 'No description'}", fill='black', font=font_text)
            
            # Add chart information
            y_offset = 150
            for i, item in enumerate(dashboard.items.all()[:5]):  # Show up to 5 items
                draw.text((50, y_offset), f"Chart {i+1}: {item.title}", fill='blue', font=font_text)
                draw.text((70, y_offset + 20), f"Type: {item.chart_type}", fill='gray', font=font_text)
                y_offset += 60
            
            # Convert to bytes
            img_buffer = BytesIO()
            img.save(img_buffer, format='PNG', quality=95)
            img_bytes = img_buffer.getvalue()
            
            filename = f"dashboard_{dashboard.name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            return img_bytes, filename
            
        except Exception as e:
            logger.error(f"Basic PNG export failed: {e}")
            # Return a minimal response
            return b'', 'dashboard_export_failed.png'
    
    def _export_basic_pdf(self, dashboard) -> Tuple[bytes, str]:
        """Fallback PDF export using ReportLab"""
        try:
            from reportlab.pdfgen import canvas
            from reportlab.lib.pagesizes import letter, A4
            
            buffer = BytesIO()
            p = canvas.Canvas(buffer, pagesize=A4)
            width, height = A4
            
            # Title
            p.setFont("Helvetica-Bold", 20)
            p.drawString(50, height - 80, f"Dashboard: {dashboard.name}")
            
            # Description
            p.setFont("Helvetica", 12)
            description = dashboard.description or "No description available"
            p.drawString(50, height - 120, f"Description: {description[:80]}")
            
            # Export info
            p.drawString(50, height - 160, f"Exported: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            p.drawString(50, height - 180, f"Owner: {dashboard.owner.username}")
            
            # Chart list
            p.setFont("Helvetica-Bold", 14)
            p.drawString(50, height - 220, "Charts:")
            
            y_position = height - 250
            p.setFont("Helvetica", 10)
            
            for i, item in enumerate(dashboard.items.all()):
                if y_position < 100:  # New page if needed
                    p.showPage()
                    y_position = height - 50
                
                p.drawString(70, y_position, f"{i+1}. {item.title}")
                p.drawString(90, y_position - 15, f"Type: {item.chart_type}")
                if item.query:
                    query_short = item.query[:60] + "..." if len(item.query) > 60 else item.query
                    p.drawString(90, y_position - 30, f"Query: {query_short}")
                
                y_position -= 60
            
            p.save()
            pdf_bytes = buffer.getvalue()
            
            filename = f"dashboard_{dashboard.name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
            return pdf_bytes, filename
            
        except Exception as e:
            logger.error(f"Basic PDF export failed: {e}")
            return b'', 'dashboard_export_failed.pdf'
    
    def _get_dashboard_data(self, dashboard):
        """Get dashboard data with proper error handling"""
        items_data = []
        
        for item in dashboard.items.all():
            try:
                # Try to get real data for this item
                result_data = self._fetch_item_data(item, dashboard.owner)
            except Exception as e:
                logger.warning(f"Could not fetch data for item {item.id}: {e}")
                # Use sample data as fallback
                result_data = [
                    {'Category': 'Sample A', 'Value': 100},
                    {'Category': 'Sample B', 'Value': 80},
                    {'Category': 'Sample C', 'Value': 60}
                ]
            
            item_data = {
                'id': str(item.id),
                'title': item.title,
                'chart_type': item.chart_type,
                'position_x': item.position_x,
                'position_y': item.position_y,
                'width': item.width,
                'height': item.height,
                'query': item.query,
                'data': result_data
            }
            items_data.append(item_data)
        
        return items_data
    
    def _fetch_item_data(self, item, user):
        """Fetch data for a dashboard item"""
        try:
            from django.test import RequestFactory
            from dashboards.views import dashboard_item_data
            
            # Create a mock request
            factory = RequestFactory()
            request = factory.get(f'/dashboards/api/dashboard-item/{item.id}/data/')
            request.user = user
            
            # Get data response
            response = dashboard_item_data(request, item.id)
            
            if response.status_code == 200:
                import json
                data = json.loads(response.content)
                return data.get('result_data', [])
            else:
                return []
                
        except Exception as e:
            logger.warning(f"Failed to fetch item data: {e}")
            return []
    
    def _generate_dashboard_html_with_data(self, dashboard, for_pdf=False, for_export=False) -> str:
        """Generate HTML content for dashboard export with actual data"""
        
        # Get dashboard items with their actual data
        items_data = self._get_dashboard_data(dashboard)
        
        # Generate enhanced HTML template with data tables
        html_template = """
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>{{ dashboard.name }} - Dashboard Export</title>
            <style>
                body {
                    font-family: 'Arial', sans-serif;
                    margin: 20px;
                    background: #f5f7fa;
                    color: #333;
                }
                .dashboard-header {
                    text-align: center;
                    margin-bottom: 30px;
                    padding: 30px;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    border-radius: 15px;
                    box-shadow: 0 8px 25px rgba(0,0,0,0.15);
                }
                .dashboard-header h1 {
                    margin: 0 0 10px 0;
                    font-size: 2.5rem;
                    font-weight: 700;
                }
                .dashboard-grid {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
                    gap: 30px;
                    margin-bottom: 30px;
                }
                .dashboard-item {
                    background: white;
                    border-radius: 15px;
                    padding: 25px;
                    box-shadow: 0 5px 20px rgba(0,0,0,0.1);
                    border: 1px solid #e0e0e0;
                    page-break-inside: avoid;
                }
                .item-title {
                    font-size: 1.4rem;
                    font-weight: 700;
                    margin-bottom: 15px;
                    color: #333;
                    border-bottom: 3px solid #667eea;
                    padding-bottom: 10px;
                }
                .item-content {
                    min-height: 200px;
                }
                .data-table {
                    width: 100%;
                    border-collapse: collapse;
                    margin-top: 15px;
                }
                .data-table th {
                    background: #667eea;
                    color: white;
                    padding: 12px;
                    text-align: left;
                    font-weight: 600;
                }
                .data-table td {
                    padding: 10px 12px;
                    border-bottom: 1px solid #eee;
                }
                .data-table tr:nth-child(even) {
                    background: #f8f9fa;
                }
                .chart-info {
                    background: #e3f2fd;
                    padding: 15px;
                    border-radius: 8px;
                    margin-bottom: 15px;
                    border-left: 4px solid #2196f3;
                }
                .export-info {
                    text-align: center;
                    padding: 20px;
                    background: #e9ecef;
                    border-radius: 10px;
                    color: #666;
                    font-size: 14px;
                    margin-top: 30px;
                }
                .no-data {
                    text-align: center;
                    color: #666;
                    font-style: italic;
                    padding: 40px;
                    background: #f8f9fa;
                    border-radius: 8px;
                }
                @media print {
                    body { margin: 0; background: white; }
                    .dashboard-item { page-break-inside: avoid; margin-bottom: 20px; }
                }
            </style>
        </head>
        <body>
            <div class="dashboard-header">
                <h1>📊 {{ dashboard.name }}</h1>
                <p style="margin: 0; font-size: 1.1rem; opacity: 0.9;">{{ dashboard.description }}</p>
                <p style="margin: 10px 0 0 0; font-size: 0.9rem; opacity: 0.7;">Dashboard Export Report</p>
            </div>
            
            <div class="dashboard-grid">
                {% for item in items %}
                <div class="dashboard-item">
                    <div class="item-title">
                        {% if item.chart_type == 'bar' %}📊{% elif item.chart_type == 'line' %}📈{% elif item.chart_type == 'pie' %}🥧{% else %}📋{% endif %} {{ item.title }}
                    </div>
                    
                    <div class="chart-info">
                        <strong>Chart Type:</strong> {{ item.chart_type|title }}<br>
                        <strong>Query:</strong> {{ item.query|truncatechars:100 }}
                    </div>
                    
                    <div class="item-content">
                        {% if item.data %}
                            <table class="data-table">
                                <thead>
                                    <tr>
                                        {% for key in item.data.0.keys %}
                                        <th>{{ key|title }}</th>
                                        {% endfor %}
                                    </tr>
                                </thead>
                                <tbody>
                                    {% for row in item.data %}
                                    <tr>
                                        {% for value in row.values %}
                                        <td>{{ value }}</td>
                                        {% endfor %}
                                    </tr>
                                    {% endfor %}
                                </tbody>
                            </table>
                            <p style="margin-top: 10px; font-size: 0.85rem; color: #666;">
                                📈 {{ item.data|length }} record{{ item.data|length|pluralize }} displayed
                            </p>
                        {% else %}
                            <div class="no-data">
                                <strong>No data available</strong><br>
                                <small>The query did not return any results</small>
                            </div>
                        {% endif %}
                    </div>
                </div>
                {% endfor %}
            </div>
            
            <div class="export-info">
                📄 Exported on {{ export_date }} by ConvaBI Dashboard System<br>
                🔍 Total Charts: {{ items|length }} | Dashboard Owner: {{ dashboard.owner.username }}
            </div>
        </body>
        </html>
        """
        
        # Create Django template and context
        template = Template(html_template)
        context = Context({
            'dashboard': dashboard,
            'items': items_data,
            'export_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'for_pdf': for_pdf,
            'for_export': for_export
        })
        
        return template.render(context)
    
    def generate_email_html(self, dashboard) -> str:
        """Generate HTML content for email"""
        return self._generate_dashboard_html_with_data(dashboard, for_pdf=False, for_export=True) 