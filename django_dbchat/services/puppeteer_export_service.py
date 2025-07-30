"""
Puppeteer Dashboard Export Service
==================================

This service uses Puppeteer to capture fully rendered dashboards
including all interactive Plotly charts as they appear in the browser.
"""

import os
import json
import logging
import tempfile
import subprocess
from datetime import datetime
from typing import Tuple, Optional
from django.conf import settings

logger = logging.getLogger(__name__)


class PuppeteerExportService:
    """Service for exporting fully rendered dashboards using Puppeteer"""
    
    def __init__(self):
        self.temp_dir = tempfile.gettempdir()
        self.base_url = getattr(settings, 'BASE_URL', 'http://localhost:8000')
    
    def export_dashboard_pdf(self, dashboard, session_cookie=None) -> Tuple[bytes, str]:
        """
        Export dashboard as PDF using Puppeteer to capture fully rendered charts
        
        Args:
            dashboard: Dashboard model instance
            session_cookie: Session cookie dict for authentication
            
        Returns:
            tuple: (pdf_bytes, filename)
        """
        try:
            dashboard_url = f"{self.base_url}/dashboards/{dashboard.id}/"
            
            # Create Puppeteer script with authentication
            script_content = self._create_puppeteer_script('pdf', dashboard_url, dashboard.id, session_cookie)
            
            # Run Puppeteer script
            pdf_bytes = self._execute_puppeteer_script(script_content, 'pdf')
            
            filename = f"dashboard_{dashboard.name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
            return pdf_bytes, filename
            
        except Exception as e:
            logger.error(f"Puppeteer PDF export failed: {e}")
            # Fallback to basic export
            return self._fallback_pdf_export(dashboard)
    
    def export_dashboard_png(self, dashboard, session_cookie=None) -> Tuple[bytes, str]:
        """
        Export dashboard as PNG using Puppeteer to capture fully rendered charts
        
        Args:
            dashboard: Dashboard model instance
            session_cookie: Session cookie dict for authentication
            
        Returns:
            tuple: (png_bytes, filename)
        """
        try:
            dashboard_url = f"{self.base_url}/dashboards/{dashboard.id}/"
            
            # Create Puppeteer script with authentication
            script_content = self._create_puppeteer_script('png', dashboard_url, dashboard.id, session_cookie)
            
            # Run Puppeteer script
            png_bytes = self._execute_puppeteer_script(script_content, 'png')
            
            filename = f"dashboard_{dashboard.name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            return png_bytes, filename
            
        except Exception as e:
            logger.error(f"Puppeteer PNG export failed: {e}")
            # Fallback to basic export
            return self._fallback_png_export(dashboard)
    
    def _create_puppeteer_script(self, format_type, dashboard_url, dashboard_id, session_cookie=None):
        """Create Puppeteer script for capturing dashboard with authentication"""
        
        script = f"""
const puppeteer = require('puppeteer');
const fs = require('fs');

(async () => {{
    console.log('🚀 Starting Puppeteer dashboard export...');
    
    const browser = await puppeteer.launch({{
        headless: true,
        args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-accelerated-2d-canvas',
            '--no-first-run',
            '--no-zygote',
            '--single-process',
            '--disable-gpu'
        ]
    }});
    
    try {{
        const page = await browser.newPage();
        
        // Set viewport for consistent rendering
        await page.setViewport({{ 
            width: 1200, 
            height: 800,
            deviceScaleFactor: 2  // High DPI for better quality
        }});
        
        {self._get_auth_cookie_script(session_cookie)}
        
        console.log('📊 Navigating to dashboard...');
        console.log('URL:', '{dashboard_url}');
        
        // Navigate to dashboard
        await page.goto('{dashboard_url}', {{ 
            waitUntil: 'networkidle0',  // Wait for all network requests to finish
            timeout: 30000 
        }});
        
        console.log('⏳ Waiting for charts to render...');
        
        // Wait for Plotly charts to load
        try {{
            await page.waitForSelector('.plotly-graph-div', {{ timeout: 10000 }});
            console.log('✅ Found Plotly charts');
        }} catch (e) {{
            console.log('⚠️  No Plotly charts found, continuing anyway');
        }}
        
        // Wait for any remaining JavaScript to execute
        await new Promise(resolve => setTimeout(resolve, 3000));
        
        // Check if charts are rendered
        const chartCount = await page.evaluate(() => {{
            const plotlyDivs = document.querySelectorAll('.plotly-graph-div');
            console.log('Found', plotlyDivs.length, 'Plotly chart containers');
            
            // Check if charts have data
            let renderedCharts = 0;
            plotlyDivs.forEach((div, index) => {{
                if (div._plotly_div && div.data && div.data.length > 0) {{
                    renderedCharts++;
                    console.log('Chart', index + 1, 'has data');
                }} else {{
                    console.log('Chart', index + 1, 'is empty or not rendered');
                }}
            }});
            
            return {{ total: plotlyDivs.length, rendered: renderedCharts }};
        }});
        
        console.log(`📈 Charts status: ${{chartCount.rendered}}/${{chartCount.total}} rendered`);
        
        // Additional wait for chart animations to complete
        if (chartCount.total > 0) {{
            console.log('⏳ Waiting for chart animations...');
            await new Promise(resolve => setTimeout(resolve, 2000));
        }}
        
        console.log('📸 Preparing chart-only export...');
        
        // Hide header, navigation, and non-chart elements for clean export
        await page.addStyleTag({{
            content: `
                /* Hide header and navigation */
                .navbar, .header, .dashboard-header, .btn-group, .card-header, .mb-4 {{
                    display: none !important;
                }}
                
                /* Hide buttons and controls */
                .btn, .dropdown, .share-btn, .edit-btn, .delete-btn, .export-btn {{
                    display: none !important;
                }}
                
                /* Hide dashboard title area */
                .dashboard-info, .dashboard-actions {{
                    display: none !important;
                }}
                
                /* Focus on chart containers only */
                .card-body {{
                    padding: 10px !important;
                    border: none !important;
                    box-shadow: none !important;
                }}
                
                /* Clean up chart titles */
                .card-title {{
                    font-size: 16px !important;
                    margin-bottom: 10px !important;
                }}
                
                /* Hide non-essential elements */
                .breadcrumb, .alert, .toast {{
                    display: none !important;
                }}
                
                /* Make charts more prominent */
                .plotly-graph-div {{
                    margin: 5px 0 !important;
                }}
                
                /* Hide page margins for export */
                body {{
                    margin: 0 !important;
                    padding: 10px !important;
                    background: white !important;
                }}
            `
        }});
        
        console.log('✅ Chart-only styling applied');
        console.log('📸 Capturing charts...');
        
        """
        
        if format_type == 'pdf':
            script += f"""
        // Generate chart-focused PDF
        const pdfBuffer = await page.pdf({{
            format: 'A4',
            printBackground: true,
            margin: {{
                top: '1cm',
                right: '1cm', 
                bottom: '1cm',
                left: '1cm'
            }},
            displayHeaderFooter: true,
            headerTemplate: '<div style="font-size: 10px; margin: 0 auto;">Dashboard Export - {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</div>',
            footerTemplate: '<div style="font-size: 10px; margin: 0 auto;">Page <span class="pageNumber"></span> of <span class="totalPages"></span></div>'
        }});
        
        // Write PDF to file
        fs.writeFileSync('/tmp/dashboard_export_{dashboard_id}.pdf', pdfBuffer);
        console.log('✅ PDF saved to /tmp/dashboard_export_{dashboard_id}.pdf');
        """
        else:  # PNG
            script += f"""
        // Generate chart-focused PNG screenshot
        const screenshot = await page.screenshot({{
            fullPage: true,
            type: 'png',
            quality: 100
        }});
        
        // Write PNG to file
        fs.writeFileSync('/tmp/dashboard_export_{dashboard_id}.png', screenshot);
        console.log('✅ PNG saved to /tmp/dashboard_export_{dashboard_id}.png');
        """
        
        script += """
    } catch (error) {
        console.error('❌ Error during export:', error);
        process.exit(1);
    } finally {
        await browser.close();
        console.log('🏁 Puppeteer export complete');
    }
})();
"""
        
        return script
    
    def _get_auth_cookie_script(self, session_cookie):
        """Generate JavaScript code to set authentication cookie"""
        if not session_cookie:
            return "// No authentication cookie provided"
        
        return f"""
        // Set authentication cookie for Django session
        console.log('🔐 Setting authentication cookie...');
        await page.setCookie({{
            name: '{session_cookie['name']}',
            value: '{session_cookie['value']}',
            domain: '{session_cookie['domain']}',
            path: '{session_cookie['path']}'
        }});
        console.log('✅ Authentication cookie set');
        """
    
    def _execute_puppeteer_script(self, script_content, format_type):
        """Execute Puppeteer script and return the generated file bytes"""
        
        # Write script to temporary file
        script_file = os.path.join(self.temp_dir, f'export_script_{datetime.now().strftime("%Y%m%d_%H%M%S")}.js')
        with open(script_file, 'w') as f:
            f.write(script_content)
        
        try:
            # Execute Puppeteer script using Node.js
            logger.info(f"Executing Puppeteer script: {script_file}")
            
            # Set up environment with NODE_PATH for global modules
            env = os.environ.copy()
            env['NODE_PATH'] = '/usr/lib/node_modules'  # Path to global modules
            
            result = subprocess.run(
                ['node', script_file],
                capture_output=True,
                text=True,
                timeout=60,  # 60 second timeout
                env=env
            )
            
            if result.returncode != 0:
                logger.error(f"Puppeteer script failed: {result.stderr}")
                raise Exception(f"Puppeteer execution failed: {result.stderr}")
            
            logger.info(f"Puppeteer output: {result.stdout}")
            
            # Read the generated file
            output_file = f'/tmp/dashboard_export_*.{format_type}'
            import glob
            generated_files = glob.glob(output_file.replace('*', '*'))
            
            if not generated_files:
                raise Exception(f"No {format_type.upper()} file generated")
            
            latest_file = max(generated_files, key=os.path.getctime)
            
            with open(latest_file, 'rb') as f:
                file_bytes = f.read()
            
            # Clean up files
            os.remove(script_file)
            os.remove(latest_file)
            
            return file_bytes
            
        except Exception as e:
            # Clean up script file
            if os.path.exists(script_file):
                os.remove(script_file)
            raise e
    
    def _fallback_pdf_export(self, dashboard):
        """Fallback PDF export using basic HTML generation"""
        try:
            from reportlab.pdfgen import canvas
            from reportlab.lib.pagesizes import A4
            from io import BytesIO
            
            buffer = BytesIO()
            p = canvas.Canvas(buffer, pagesize=A4)
            width, height = A4
            
            # Title
            p.setFont("Helvetica-Bold", 20)
            p.drawString(50, height - 80, f"Dashboard: {dashboard.name}")
            
            # Note about chart rendering
            p.setFont("Helvetica", 12)
            p.drawString(50, height - 120, "Note: Charts could not be rendered. Please ensure Puppeteer is installed.")
            p.drawString(50, height - 140, f"Dashboard URL: /dashboards/{dashboard.id}/")
            
            # Export info
            p.setFont("Helvetica", 10)
            p.drawString(50, height - 180, f"Exported: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            p.save()
            pdf_bytes = buffer.getvalue()
            
            filename = f"dashboard_{dashboard.name.replace(' ', '_')}_fallback.pdf"
            return pdf_bytes, filename
            
        except Exception as e:
            logger.error(f"Fallback PDF export failed: {e}")
            return b'', 'export_failed.pdf'
    
    def _fallback_png_export(self, dashboard):
        """Fallback PNG export using basic image generation"""
        try:
            from PIL import Image, ImageDraw, ImageFont
            from io import BytesIO
            
            # Create basic image
            img = Image.new('RGB', (1200, 800), color='white')
            draw = ImageDraw.Draw(img)
            
            try:
                font_title = ImageFont.truetype("arial.ttf", 24)
                font_text = ImageFont.truetype("arial.ttf", 14)
            except:
                font_title = ImageFont.load_default()
                font_text = ImageFont.load_default()
            
            # Draw content
            draw.text((50, 50), f"Dashboard: {dashboard.name}", fill='black', font=font_title)
            draw.text((50, 100), "Charts could not be rendered", fill='red', font=font_text)
            draw.text((50, 130), "Please ensure Puppeteer is installed", fill='red', font=font_text)
            draw.text((50, 160), f"Dashboard URL: /dashboards/{dashboard.id}/", fill='blue', font=font_text)
            
            # Convert to bytes
            img_buffer = BytesIO()
            img.save(img_buffer, format='PNG')
            img_bytes = img_buffer.getvalue()
            
            filename = f"dashboard_{dashboard.name.replace(' ', '_')}_fallback.png"
            return img_bytes, filename
            
        except Exception as e:
            logger.error(f"Fallback PNG export failed: {e}")
            return b'', 'export_failed.png' 