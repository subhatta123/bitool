
# PDF Export Test Instructions
=============================

## 🧪 How to Test PDF Export Fix

### 1. Access Dashboard
1. Navigate to: http://localhost:8000/dashboards/
2. Open any existing dashboard
3. Look for the "Export" button in the dashboard header

### 2. Test PDF Export
1. Click the "Export" button
2. Select "Export as PDF" 
3. Wait for PDF generation
4. Download should start automatically

### 3. Verify PDF Content
✅ **What you should see in the PDF:**
- Dashboard title and description at the top
- Professional styling with gradients and colors
- Each chart section with:
  - Chart title with appropriate emoji (📊 📈 🥧 📋)
  - Chart type and query information
  - **DATA TABLE with actual values** (not blank!)
  - Record count at bottom of each table
- Export information footer with timestamp

❌ **What should be FIXED now:**
- No more blank PDFs
- No more "No data available" messages (unless truly no data)
- Professional styling instead of plain text
- Proper page breaks and formatting

### 4. Expected PDF Structure
```
📊 Dashboard Name
Dashboard Description
Dashboard Export Report

📊 Chart Title 1
Chart Type: Bar | Query: SELECT...
┌─────────────┬─────────────┐
│ Column 1    │ Column 2    │
├─────────────┼─────────────┤
│ Sample A    │ 100         │
│ Sample B    │ 80          │
│ Sample C    │ 60          │
└─────────────┴─────────────┘
📈 3 records displayed

📈 Chart Title 2
[Similar structure for each chart]

📄 Exported on 2025-07-23 10:45:58 by ConvaBI Dashboard System
🔍 Total Charts: 2 | Dashboard Owner: username
```

## 🚨 Troubleshooting

### If PDF is still blank:
1. Check browser console for errors
2. Restart web container: `docker-compose restart web`
3. Check container logs: `docker-compose logs web`
4. Verify WeasyPrint is installed in container

### If export fails:
1. Try PNG export instead to isolate issue
2. Check that dashboard has charts with data
3. Verify user permissions

### If data tables are missing:
1. Verify dashboard charts have queries
2. Check that data sources are accessible
3. Look for sample data fallback in PDF

## ✅ Success Criteria

- [x] PDF downloads successfully
- [x] PDF contains dashboard title and description
- [x] PDF shows data tables for each chart
- [x] PDF has professional styling and layout
- [x] PDF includes export timestamp and metadata
- [x] No blank pages or empty content
- [x] Charts display actual data or sample data
- [x] Proper page breaks and formatting

## 📧 Email Export Testing

You can also test the email functionality:
1. Click "Schedule" button
2. Enter your email address
3. Select "PDF Document" format
4. Choose "Send Once" frequency
5. Click "Schedule Email"
6. Check your email for the PDF attachment

The emailed PDF should have the same enhanced content as the downloaded PDF.
