
# Dashboard Functionality Test Guide
=====================================

## 1. Testing Data Filters in Query Results

1. **Navigate to Query Page**: Go to the main query interface
2. **Run a Query**: Execute any query that returns multiple rows with some null, zero, or empty values
3. **Look for Filter Section**: Scroll down below the chart configuration to find "Data Filters"
4. **Test Filters**:
   - ✓ Check "Remove null values" - rows with null values should disappear
   - ✓ Check "Remove zero values" - rows with zero values should disappear  
   - ✓ Check "Remove empty strings" - rows with empty strings should disappear
   - ✓ Use combinations of filters
   - ✓ Click "Reset Filters" to restore original data
5. **Verify Status**: Check that filter status shows correct removed row counts

## 2. Testing Individual Chart Deletion

1. **Go to Dashboard**: Navigate to any dashboard with charts
2. **Find Delete Buttons**: Look for red trash icon buttons in the top-right of each chart
3. **Delete a Chart**: 
   - ✓ Click the trash button on any chart
   - ✓ Confirm deletion in the popup
   - ✓ Verify chart is removed from dashboard
   - ✓ Verify dashboard still exists with remaining charts

## 3. Testing Dashboard Scheduling

1. **Access Dashboard**: Go to any dashboard
2. **Find Schedule Button**: Look for "Schedule" button in the top dashboard actions
3. **Schedule Email**:
   - ✓ Click "Schedule" button
   - ✓ Enter recipient email address
   - ✓ Select export format (PNG or PDF)
   - ✓ Choose frequency (Once, Daily, Weekly, Monthly)
   - ✓ Click "Schedule Email"
   - ✓ Verify success message shows format and frequency

## 4. Testing Dashboard Export

1. **Access Dashboard**: Go to any dashboard
2. **Find Export Button**: Look for "Export" button in the top dashboard actions
3. **Export Dashboard**:
   - ✓ Click "Export" button
   - ✓ Choose PNG or PDF export
   - ✓ Verify file downloads correctly
   - ✓ Open downloaded file to verify content

## 5. Testing Celery Integration

1. **Check Scheduled Tasks**: 
   - ✓ Use Django admin or database to verify periodic tasks are created
   - ✓ Verify task names follow pattern: "dashboard_email_{dashboard_id}_{user_id}_{frequency}"
2. **Monitor Email Delivery**: Check that scheduled emails are sent according to frequency
3. **Test Attachments**: Verify emails contain correct PDF/PNG attachments

## 6. Testing Browser Functionality

Open your browser and navigate to: http://localhost:8000

### Dashboard Navigation:
- ✓ Go to Dashboards section
- ✓ Verify new buttons are visible: Schedule, Export
- ✓ Verify individual chart delete buttons work
- ✓ Test modal dialogs open correctly

### Query Results:
- ✓ Run a query with diverse data
- ✓ Look for "Data Filters" section below chart configuration
- ✓ Test all filter combinations
- ✓ Verify filter status updates correctly

## Expected Results

✅ **All Features Working**: 
- Data filters remove rows correctly
- Individual charts can be deleted without deleting dashboard
- Email scheduling creates periodic tasks
- Export generates downloadable files
- Celery sends scheduled emails with attachments

❌ **If Issues Found**:
- Check browser console for JavaScript errors
- Verify Docker containers are running
- Check container logs: `docker-compose logs web`
- Ensure database migrations completed

## API Endpoints to Test

- `POST /dashboards/item/<uuid>/delete/` - Delete individual chart
- `POST /dashboards/<uuid>/schedule-email/` - Schedule dashboard email
- `GET /dashboards/<uuid>/export/?format=png|pdf` - Export dashboard
- `GET /dashboards/<uuid>/scheduled-emails/` - Get scheduled emails
- `POST /dashboards/cancel-email/<id>/` - Cancel scheduled email

## Container Verification

```bash
# Check all containers are running
docker-compose ps

# Check web container logs
docker-compose logs --tail=50 web

# Check celery container logs  
docker-compose logs --tail=50 celery

# Restart if needed
docker-compose restart web
```

## Success Criteria

1. ✓ Data filtering works without JavaScript errors
2. ✓ Individual charts can be deleted successfully
3. ✓ Dashboard scheduling creates email tasks
4. ✓ Export downloads functional PDF/PNG files
5. ✓ Celery processes scheduled emails
6. ✓ No console errors in browser
7. ✓ All Docker containers running healthy
