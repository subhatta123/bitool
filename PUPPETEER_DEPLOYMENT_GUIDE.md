# Puppeteer Dashboard Export - Deployment Guide
=============================================

## 🎯 **Problem Solved**

**Issue:** PDF and PNG exports were showing only dashboard metadata (title, description, chart info) but **NOT the actual rendered charts**.

**Solution:** New Puppeteer-based export service that captures **fully rendered dashboard pages** including all interactive Plotly charts as they appear in the browser.

---

## 🎭 **What is Puppeteer Export?**

Puppeteer export uses a headless Chrome browser to:
1. **Navigate** to the actual dashboard URL
2. **Wait** for all Plotly charts to fully render
3. **Capture** high-quality screenshots or PDFs
4. **Return** the rendered content with visible charts

### **Before (Static HTML):**
```
Dashboard: sales dashboard
Description: Created from query...
Charts:
1. top 3 customer names...
   Type: bar
   Query: SELECT...
```

### **After (Puppeteer):**
```
[Full dashboard screenshot with:]
✅ Actual rendered bar charts
✅ Interactive Plotly visualizations  
✅ Proper styling and colors
✅ All visual elements visible
```

---

## 🚀 **Deployment Steps**

### **Step 1: Rebuild Docker Container**

The container needs Node.js and Puppeteer installed:

```bash
# Stop current containers
docker-compose down

# Rebuild with Puppeteer support
docker-compose build --no-cache

# Start containers
docker-compose up -d
```

### **Step 2: Verify Installation**

Check that Node.js and Puppeteer are installed:

```bash
# Check Node.js
docker-compose exec web node --version

# Check NPM
docker-compose exec web npm --version

# Check Puppeteer
docker-compose exec web node -e "console.log(require('puppeteer'))"
```

Expected output:
```
✅ Node.js: v18.x.x
✅ NPM: 9.x.x  
✅ Puppeteer: [object Object]
```

### **Step 3: Test Export Functionality**

1. **Open Dashboard:** http://localhost:8000/dashboards/
2. **Select Dashboard:** Click on any existing dashboard
3. **Test PDF Export:** Click "Export" → "Export as PDF"
4. **Test PNG Export:** Click "Export" → "Export as PNG"
5. **Verify Content:** Open exported files and verify charts are visible

---

## 🧪 **Testing the Fix**

### **Run Test Script:**

```bash
# Run comprehensive test
python test_puppeteer_export.py
```

Expected results:
```
✅ PASS: INSTALLATION
✅ PASS: SERVICE  
✅ PASS: DASHBOARD
✅ PASS: PDF
✅ PASS: PNG
✅ PASS: REPORT

🎉 PUPPETEER EXPORT WORKING! Most tests passed.
```

### **Manual Testing:**

1. **Access Dashboard:**
   - URL: http://localhost:8000/dashboards/
   - Login with your credentials
   - Open any dashboard with charts

2. **Test PDF Export:**
   - Click "Export" button
   - Select "Export as PDF"
   - Download should start automatically
   - **Verify:** PDF shows rendered charts, not just text

3. **Test PNG Export:**
   - Click "Export" button  
   - Select "Export as PNG"
   - Download should start automatically
   - **Verify:** PNG shows dashboard screenshot

---

## 🏗️ **Technical Implementation**

### **Files Added/Modified:**

1. **`django_dbchat/services/puppeteer_export_service.py`** - New Puppeteer service
2. **`django_dbchat/dashboards/views.py`** - Updated to use Puppeteer
3. **`install_puppeteer.sh`** - Installation script for container
4. **`Dockerfile`** - Updated to include Puppeteer installation

### **How Puppeteer Export Works:**

1. **Script Generation:** Creates a Node.js Puppeteer script
2. **Browser Launch:** Starts headless Chrome with optimized settings
3. **Navigation:** Goes to the dashboard URL
4. **Chart Waiting:** Waits for `.plotly-graph-div` elements to load
5. **Rendering Wait:** Additional time for chart animations
6. **Capture:** Takes PDF or PNG of fully rendered page
7. **File Return:** Returns captured content to Django

### **Fallback System:**

If Puppeteer fails, the system automatically falls back to the static export service:

```python
# Try Puppeteer first
try:
    service = PuppeteerExportService()
    content, filename = service.export_dashboard_pdf(dashboard)
except Exception:
    # Fallback to static export
    service = DashboardExportService() 
    content, filename = service.export_dashboard_pdf(dashboard)
```

---

## 🔧 **Configuration Options**

### **Puppeteer Settings:**

- **Viewport:** 1200x800 with 2x device scale factor
- **Wait Strategy:** `networkidle0` (all network requests finished)
- **Timeout:** 30 seconds for page load, 10 seconds for charts
- **PDF Options:** A4 format with margins and headers/footers
- **PNG Options:** Full page screenshots with high quality

### **Environment Variables:**

```env
# Base URL for dashboard access (optional)
BASE_URL=http://localhost:8000

# Puppeteer timeout settings (optional)
PUPPETEER_TIMEOUT=30000
CHART_RENDER_TIMEOUT=10000
```

---

## 🚨 **Troubleshooting**

### **If exports still show static content:**

1. **Check Puppeteer Installation:**
   ```bash
   docker-compose exec web node -e "require('puppeteer')"
   ```

2. **Verify Dashboard Charts Work:**
   - Open dashboard in browser manually
   - Ensure charts render properly
   - Check for JavaScript errors in console

3. **Check Container Logs:**
   ```bash
   docker-compose logs web
   ```
   Look for Puppeteer-related errors

4. **Memory Issues:**
   - Puppeteer requires additional memory
   - Consider increasing container memory limits

### **Common Issues:**

**Issue:** `Module not found: puppeteer`
**Solution:** Rebuild container with `--no-cache` flag

**Issue:** `Chrome launch failed`  
**Solution:** Verify Chrome dependencies installed in container

**Issue:** `Timeout waiting for charts`
**Solution:** Increase timeout or check chart rendering manually

**Issue:** `Permission denied`
**Solution:** Check file permissions in `/tmp` directory

### **Performance Optimization:**

```bash
# Increase container memory
docker-compose exec web free -h

# Monitor Puppeteer processes
docker-compose exec web ps aux | grep chrome

# Check disk space for temp files
docker-compose exec web df -h /tmp
```

---

## 📊 **Expected Results**

### **PDF Export (Before Fix):**
```
Dashboard: sales dashboard
Exported: 2025-07-23 05:34:56
Owner: jico

Charts:
1. top 3 customer names in consumer segment by sales
   Type: bar
   Query: top 3 customer names in consumer segment by sales
```

### **PDF Export (After Puppeteer Fix):**
```
[Visual dashboard page with:]
- Dashboard header with title and styling
- Actual rendered bar chart showing customer data
- Interactive chart elements and colors
- Professional layout and formatting
- Export timestamp and metadata
```

### **PNG Export (After Fix):**
```
[High-quality screenshot showing:]
- Complete dashboard as it appears in browser
- All charts rendered and visible
- Proper fonts, colors, and layout
- Interactive elements captured
```

---

## ✅ **Success Checklist**

- [ ] Docker container rebuilt with Node.js and Puppeteer
- [ ] Installation test passes: `docker-compose exec web node --version`
- [ ] Puppeteer test passes: `docker-compose exec web node -e "require('puppeteer')"`
- [ ] PDF export shows rendered charts (not just text)
- [ ] PNG export shows dashboard screenshot
- [ ] Export files are substantial size (>10KB typically)
- [ ] No JavaScript errors in browser console
- [ ] Fallback to static export works if Puppeteer fails

---

## 🎯 **Final Verification**

1. **Open:** http://localhost:8000/dashboards/
2. **Export PDF:** Should show rendered charts
3. **Export PNG:** Should show dashboard screenshot
4. **File Sizes:** PDFs >50KB, PNGs >100KB typically
5. **Visual Content:** Charts visible and properly formatted

**🎉 SUCCESS:** When exports show actual rendered charts instead of just text metadata!

---

## 📞 **Support**

If issues persist after following this guide:

1. **Check test report:** `puppeteer_export_test_report.md`
2. **Review logs:** `docker-compose logs web`
3. **Verify manual chart rendering:** Open dashboard in browser
4. **Test fallback:** Static export should still work

**The system is designed to be robust - if Puppeteer fails, it automatically falls back to static export to ensure exports always work.**

---

*Deployment guide generated: 2025-07-23 by ConvaBI Development Team* 