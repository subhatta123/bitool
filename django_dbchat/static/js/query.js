/**
 * Query interface JavaScript for ConvaBI Application
 * Handles natural language to SQL query processing
 */

// Query interface functionality
class QueryInterface {
    constructor() {
        this.currentDataSource = null;
        this.queryHistory = [];
        this.isProcessing = false;
        
        this.init();
    }
    
    init() {
        this.bindEvents();
        this.loadQueryHistory();
        this.setupAutoComplete();
        this.initializeDataSourceState();
    }
    
    bindEvents() {
        // Data source selection
        const dataSourceSelect = document.getElementById('dataSourceSelect');
        if (dataSourceSelect) {
            dataSourceSelect.addEventListener('change', (e) => this.handleDataSourceChange(e));
        }
        
        // Query input events  
        const queryInput = document.getElementById('naturalQueryInput');
        if (queryInput) {
            queryInput.addEventListener('keydown', (e) => this.handleQueryKeydown(e));
            queryInput.addEventListener('input', (e) => this.handleQueryInput(e));
        }
        
        // Ask AI button
        const submitBtn = document.getElementById('submitQueryBtn');
        if (submitBtn) {
            submitBtn.addEventListener('click', (e) => this.handleQuerySubmit(e));
        }
        
        // Clear button
        const clearBtn = document.getElementById('clearQueryBtn');
        if (clearBtn) {
            clearBtn.addEventListener('click', (e) => this.clearQuery(e));
        }
        
        // Data Preview button
        const dataPreviewBtn = document.getElementById('dataPreviewBtn');
        if (dataPreviewBtn) {
            dataPreviewBtn.addEventListener('click', (e) => this.showDataPreview(e));
        }
        
        // Copy Schema button
        const copySchemaBtn = document.getElementById('copySchemaBtn');
        if (copySchemaBtn) {
            copySchemaBtn.addEventListener('click', (e) => this.copySchemaToClipboard(e));
        }
        
        // History items
        this.bindHistoryEvents();
        
        // Reuse query buttons
        const reuseButtons = document.querySelectorAll('.reuse-query-btn');
        reuseButtons.forEach(btn => {
            btn.addEventListener('click', (e) => {
                const query = e.target.closest('.reuse-query-btn').getAttribute('data-query');
                this.reuseQuery(query);
            });
        });
    }
    
    async handleQuerySubmit(e) {
        e.preventDefault();
        
        if (this.isProcessing) {
            return;
        }
        
        const queryInput = document.getElementById('naturalQueryInput');
        const dataSourceSelect = document.getElementById('dataSourceSelect');
        
        const query = queryInput ? queryInput.value.trim() : '';
        const dataSourceId = dataSourceSelect ? dataSourceSelect.value : '';
        
        if (!query) {
            ConvaBI.showAlert('warning', 'Please enter a query');
            return;
        }
        
        if (!dataSourceId) {
            ConvaBI.showAlert('warning', 'Please select a data source');
            return;
        }
        
        this.isProcessing = true;
        this.updateProcessingStatus('Processing your query...');
        
        try {
            await this.processQuery(query, dataSourceId);
        } catch (error) {
            console.error('Query processing failed:', error);
            ConvaBI.showAlert('danger', 'Query processing failed: ' + error.message);
        } finally {
            this.isProcessing = false;
            this.hideProcessingStatus();
        }
    }
    
    async processQuery(query, dataSourceId) {
        console.log('Processing query:', query, 'for data source:', dataSourceId);
        
        // Show progress tracker
        this.showProgressTracker();
        this.updateProgress(1, 'Understanding Query', 'Parsing your natural language request...');
        
        try {
            // Simulate progress steps
            setTimeout(() => {
                this.updateProgress(2, 'Generating SQL', 'Converting to database query...');
            }, 500);
            
            setTimeout(() => {
                this.updateProgress(3, 'Executing Query', 'Running query against your data...');
            }, 1000);
            
            const response = await fetch('/query/', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRFToken': ConvaBI.getCsrfToken()
                },
                body: JSON.stringify({
                    query: query,
                    data_source_id: dataSourceId
                })
            });
            
            const data = await response.json();
            console.log('Query response received:', data);
            
            // Complete progress
            this.updateProgress(4, 'Formatting Results', 'Preparing your results...');
            
            // Hide progress tracker after short delay
            setTimeout(() => {
                this.hideProgressTracker();
            }, 500);
            
            if (data.needs_clarification) {
                console.log('Clarification needed, showing clarification section');
                this.showClarificationSection(data.clarification_question, data.session_id, data.query_context);
            } else if (data.success) {
                console.log('Query successful, redirecting to results');
                // Redirect to results page
                window.location.href = data.redirect_url;
            } else {
                console.log('Query failed with error:', data.error);
                ConvaBI.showAlert('danger', data.error || 'Query failed');
                
                // Show additional error details if available
                if (data.details) {
                    console.error('Error details:', data.details);
                    ConvaBI.showAlert('info', 'Details: ' + data.details);
                }
                
                if (data.suggestion) {
                    ConvaBI.showAlert('info', 'Suggestion: ' + data.suggestion);
                }
            }
            
        } catch (error) {
            console.error('Query API call failed:', error);
            this.hideProgressTracker();
            ConvaBI.showAlert('danger', 'Failed to process query: ' + error.message);
        }
    }
    
    showProgressTracker() {
        const progressCard = document.getElementById('queryProgressCard');
        if (progressCard) {
            progressCard.style.display = 'block';
            // Reset all steps
            document.querySelectorAll('.progress-step').forEach(step => {
                step.classList.remove('active', 'completed');
                const statusIcon = step.querySelector('.progress-step-status i');
                if (statusIcon) {
                    statusIcon.className = 'fas fa-circle text-muted';
                }
            });
            // Reset progress bar
            const progressBar = document.getElementById('overallProgressBar');
            const progressPercentage = document.getElementById('progressPercentage');
            if (progressBar) progressBar.style.width = '0%';
            if (progressPercentage) progressPercentage.textContent = '0%';
        }
    }
    
    hideProgressTracker() {
        const progressCard = document.getElementById('queryProgressCard');
        if (progressCard) {
            progressCard.style.display = 'none';
        }
    }
    
    updateProgress(step, stepTitle, statusText) {
        // Update step status
        const stepElement = document.getElementById(`step-${this.getStepName(step)}`);
        if (stepElement) {
            // Mark previous steps as completed
            for (let i = 1; i < step; i++) {
                const prevStep = document.getElementById(`step-${this.getStepName(i)}`);
                if (prevStep) {
                    prevStep.classList.remove('active');
                    prevStep.classList.add('completed');
                    const statusIcon = prevStep.querySelector('.progress-step-status i');
                    if (statusIcon) {
                        statusIcon.className = 'fas fa-check text-success';
                    }
                }
            }
            
            // Mark current step as active
            stepElement.classList.remove('completed');
            stepElement.classList.add('active');
            const statusIcon = stepElement.querySelector('.progress-step-status i');
            if (statusIcon) {
                statusIcon.className = 'fas fa-spinner fa-spin text-primary';
            }
        }
        
        // Update progress bar
        const progress = (step / 4) * 100;
        const progressBar = document.getElementById('overallProgressBar');
        const progressPercentage = document.getElementById('progressPercentage');
        
        if (progressBar) {
            progressBar.style.width = `${progress}%`;
        }
        if (progressPercentage) {
            progressPercentage.textContent = `${Math.round(progress)}%`;
        }
        
        // Update processing text
        const processingText = document.getElementById('processingText');
        if (processingText) {
            processingText.textContent = statusText;
        }
    }
    
    getStepName(stepNumber) {
        const stepNames = {
            1: 'parse',
            2: 'sql', 
            3: 'execute',
            4: 'results'
        };
        return stepNames[stepNumber] || 'parse';
    }
    
    showClarificationSection(question, sessionId, queryContext = null) {
        this.currentSessionId = sessionId;
        this.currentQueryContext = queryContext; // Store context for quick responses
        
        // Hide query results and processing
        this.hideProcessingStatus();
        const queryResults = document.getElementById('queryResults');
        if (queryResults) {
            queryResults.classList.add('d-none');
        }
        
        // Show clarification section
        const clarificationSection = document.getElementById('clarificationSection');
        const clarificationQuestion = document.getElementById('clarificationQuestion');
        const clarificationInput = document.getElementById('clarificationInput');
        const submitBtn = document.getElementById('submitClarificationBtn');
        
        if (clarificationSection && clarificationQuestion) {
            // Format the question with line breaks
            const formattedQuestion = this.formatClarificationText(question);
            clarificationQuestion.innerHTML = formattedQuestion;
            
            // Generate quick response buttons with context
            this.generateQuickResponses(question, queryContext);
            
            // Clear previous input
            if (clarificationInput) {
                clarificationInput.value = '';
            }
            
            // Show the section
            clarificationSection.classList.remove('d-none');
            
            // Focus on input
            if (clarificationInput) {
                setTimeout(() => clarificationInput.focus(), 100);
            }
            
            // Bind submit button if not already bound
            if (submitBtn && !submitBtn.hasAttribute('data-bound')) {
                submitBtn.addEventListener('click', () => this.submitClarification());
                submitBtn.setAttribute('data-bound', 'true');
            }
            
            // Allow Enter key submission
            if (clarificationInput && !clarificationInput.hasAttribute('data-bound')) {
                clarificationInput.addEventListener('keydown', (e) => {
                    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
                        e.preventDefault();
                        this.submitClarification();
                    }
                });
                clarificationInput.setAttribute('data-bound', 'true');
            }
        }
    }
    
    formatClarificationText(text) {
        // Convert line breaks and bullet points to HTML
        return text
            .replace(/\n\n/g, '</p><p>')
            .replace(/\n/g, '<br>')
            .replace(/• /g, '<br>• ')
            .replace(/^/, '<p>')
            .replace(/$/, '</p>');
    }
    
    generateQuickResponses(question, queryContext = null) {
        const quickButtons = document.getElementById('quickResponseButtons');
        const buttonContainer = document.getElementById('quickButtonsContainer');
        
        if (!quickButtons || !buttonContainer) return;
        
        // Generate context-aware quick responses
        const responses = this.getQuickResponses(question, queryContext);
        
        if (responses.length > 0) {
            buttonContainer.innerHTML = responses.map(response => 
                `<button class="btn btn-sm btn-outline-primary quick-response-btn" 
                         data-response="${response}">${response}</button>`
            ).join('');
            
            // Bind click events
            buttonContainer.querySelectorAll('.quick-response-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const response = e.target.getAttribute('data-response');
                    this.selectQuickResponse(response);
                });
            });
            
            quickButtons.classList.remove('d-none');
        } else {
            quickButtons.classList.add('d-none');
        }
    }
    
    getQuickResponses(question, queryContext = null) {
        const responses = [];
        const questionLower = question.toLowerCase();
        const originalQuery = queryContext?.original_query?.toLowerCase() || '';
        const availableColumns = queryContext?.available_columns || [];
        
        // Helper function to find columns containing specific terms
        const findColumns = (terms) => {
            return availableColumns.filter(col => 
                terms.some(term => col.toLowerCase().includes(term))
            );
        };
        
        // Gender/demographic-specific responses
        if (originalQuery.includes('male') || originalQuery.includes('female') || originalQuery.includes('gender') || originalQuery.includes('sex')) {
            const genderColumns = findColumns(['sex', 'gender']);
            if (genderColumns.length > 0) {
                responses.push('Total count by gender', 'Show percentages', 'Count male passengers', 'Count female passengers');
            } else {
                responses.push('Count by category', 'Show totals', 'Group by type');
            }
        }
        
        // Passenger-specific responses
        if (originalQuery.includes('passenger')) {
            responses.push('Total passenger count', 'Passengers by category', 'Show passenger demographics');
            
            // Add class-related options if available
            const classColumns = findColumns(['class', 'pclass']);
            if (classColumns.length > 0) {
                responses.push('By passenger class', 'Group by class');
            }
        }
        
        // Counting/total queries
        if (questionLower.includes('count') || questionLower.includes('total') || questionLower.includes('number')) {
            responses.push('Show total count', 'Count by category', 'Show unique values');
        }
        
        // Location-based responses
        if (questionLower.includes('location') || questionLower.includes('place')) {
            const locationColumns = findColumns(['city', 'state', 'country', 'region', 'port', 'embark']);
            if (locationColumns.length > 0) {
                responses.push('Group by location', 'Filter by region', 'Show by port');
            }
        }
        
        // Age-related responses
        if (originalQuery.includes('age') || questionLower.includes('age')) {
            responses.push('Group by age range', 'Show age statistics', 'Age categories');
        }
        
        // Survival-related responses (for Titanic-like data)
        if (availableColumns.some(col => col.toLowerCase().includes('surviv'))) {
            responses.push('Survival by gender', 'Survival rates', 'Survival by class');
        }
        
        // Ranking-related responses
        if (questionLower.includes('top') || questionLower.includes('rank') || questionLower.includes('highest')) {
            responses.push('Top 5', 'Top 10', 'Top 20');
            
            // Add specific ranking options based on available numeric columns
            const numericColumns = findColumns(['age', 'fare', 'price', 'amount', 'count']);
            if (numericColumns.length > 0) {
                responses.push(`By ${numericColumns[0]}`, 'Highest to lowest');
            }
        }
        
        // Time-related responses
        if (questionLower.includes('time') || questionLower.includes('date') || questionLower.includes('year')) {
            const dateColumns = findColumns(['date', 'time', 'year', 'month']);
            if (dateColumns.length > 0) {
                responses.push('By year', 'Monthly trends', 'Time periods');
            }
        }
        
        // Category-based responses
        if (questionLower.includes('category') || questionLower.includes('type') || questionLower.includes('group')) {
            const categoryColumns = findColumns(['category', 'type', 'class', 'group']);
            if (categoryColumns.length > 0) {
                responses.push(`Group by ${categoryColumns[0]}`, 'Show categories', 'By type');
            }
        }
        
        // Add column-specific quick responses based on actual data
        if (responses.length < 4) {
            // Suggest options based on most relevant columns
            const relevantColumns = availableColumns.slice(0, 3);
            relevantColumns.forEach(col => {
                if (col.toLowerCase().includes('name')) {
                    responses.push('Show names');
                } else if (col.toLowerCase().includes('class')) {
                    responses.push(`By ${col}`);
                } else if (!responses.some(r => r.includes(col))) {
                    responses.push(`Group by ${col}`);
                }
            });
        }
        
        // Generic helpful responses if nothing specific matched
        if (responses.length === 0) {
            responses.push('Show summary', 'Count all records', 'Group by category', 'Show details');
            
            // Add responses based on available columns
            if (availableColumns.length > 0) {
                responses.push(`Analyze ${availableColumns[0]}`, 'Show breakdown');
            }
        }
        
        // Remove duplicates and limit to 6 buttons
        const uniqueResponses = [...new Set(responses)];
        return uniqueResponses.slice(0, 6);
    }
    
    selectQuickResponse(response) {
        const clarificationInput = document.getElementById('clarificationInput');
        if (clarificationInput) {
            // Add to existing text or replace
            const currentText = clarificationInput.value.trim();
            if (currentText) {
                clarificationInput.value = currentText + ', ' + response;
            } else {
                clarificationInput.value = response;
            }
            clarificationInput.focus();
        }
    }
    
    async submitClarification() {
        const clarificationInput = document.getElementById('clarificationInput');
        const response = clarificationInput ? clarificationInput.value.trim() : '';
        
        if (!response) {
            ConvaBI.showAlert('warning', 'Please provide a response to help me understand your query better.');
            return;
        }
        
        if (!this.currentSessionId) {
            ConvaBI.showAlert('danger', 'Session expired. Please try your query again.');
            return;
        }
        
        // Add to conversation history
        this.addToConversationHistory('user', response);
        
        // Show processing
        this.updateProcessingStatus('Processing your clarification...');
        
        try {
            const clarificationUrl = `/query/clarification/${this.currentSessionId}/`;
            console.log('Submitting clarification to:', clarificationUrl);
            
            const result = await fetch(clarificationUrl, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRFToken': ConvaBI.getCsrfToken()
                },
                body: JSON.stringify({ response: response })
            });
            
            // Check for 404 or other HTTP errors
            if (!result.ok) {
                if (result.status === 404) {
                    document.getElementById('debugInfo')?.classList.remove('d-none');
                    throw new Error(`URL not found (404). Please refresh the page (Ctrl+F5) to clear cache.`);
                } else {
                    throw new Error(`HTTP ${result.status}: ${result.statusText}`);
                }
            }
            
            const data = await result.json();
            
            if (data.success) {
                // Hide clarification section
                this.hideClarificationSection();
                
                // Redirect to results
                window.location.href = data.redirect_url;
            } else if (data.needs_clarification) {
                // Another round of clarification needed
                this.addToConversationHistory('assistant', data.clarification_question);
                // Use stored context from the original query if available
                this.showClarificationSection(data.clarification_question, data.session_id, this.currentQueryContext);
            } else {
                this.hideProcessingStatus();
                ConvaBI.showAlert('danger', data.error || 'Clarification failed. Please try rephrasing your question.');
            }
            
        } catch (error) {
            this.hideProcessingStatus();
            console.error('Clarification submission failed:', error);
            
            // Show debugging info if there's a 404 error
            if (error.message.includes('404') || error.status === 404) {
                document.getElementById('debugInfo')?.classList.remove('d-none');
                ConvaBI.showAlert('danger', 'URL not found. Please refresh the page (Ctrl+F5) to clear cache and try again.');
            } else {
                ConvaBI.showAlert('danger', 'Failed to submit clarification: ' + error.message);
            }
        }
    }
    
    hideClarificationSection() {
        const clarificationSection = document.getElementById('clarificationSection');
        if (clarificationSection) {
            clarificationSection.classList.add('d-none');
        }
    }
    
    addToConversationHistory(sender, message) {
        const conversationHistory = document.getElementById('conversationHistory');
        const conversationItems = document.getElementById('conversationItems');
        
        if (!conversationItems) return;
        
        const senderIcon = sender === 'user' ? 'fa-user' : 'fa-robot';
        const senderColor = sender === 'user' ? 'bg-primary' : 'bg-info';
        const timestamp = new Date().toLocaleTimeString();
        
        const historyItem = document.createElement('div');
        historyItem.className = 'd-flex align-items-start mb-2';
        historyItem.innerHTML = `
            <div class="flex-shrink-0">
                <div class="${senderColor} text-white rounded-circle p-1" style="width: 24px; height: 24px; display: flex; align-items: center; justify-content: center;">
                    <i class="fas ${senderIcon}" style="font-size: 0.7rem;"></i>
                </div>
            </div>
            <div class="flex-grow-1 ms-2">
                <div class="bg-light p-2 rounded" style="font-size: 0.9rem;">
                    ${this.formatClarificationText(message)}
                </div>
                <small class="text-muted">${timestamp}</small>
            </div>
        `;
        
        conversationItems.appendChild(historyItem);
        
        // Show conversation history section
        if (conversationHistory) {
            conversationHistory.classList.remove('d-none');
        }
        
        // Scroll to bottom
        conversationItems.scrollTop = conversationItems.scrollHeight;
    }
    
    initializeDataSourceState() {
        // Check if there's already a data source selected when page loads
        const dataSourceSelect = document.getElementById('dataSourceSelect');
        if (dataSourceSelect && dataSourceSelect.value) {
            // Set current data source and trigger state update
            this.currentDataSource = dataSourceSelect.value;
            this.updateDataSourceState();
        }
    }
    
    updateDataSourceState() {
        // Update UI elements based on current data source selection
        const connectionStatus = document.getElementById('connectionStatus');
        
        if (this.currentDataSource) {
            // Update connection status
            if (connectionStatus) {
                connectionStatus.innerHTML = '<i class="fas fa-circle text-success"></i> Data source connected';
                connectionStatus.className = 'alert alert-success py-2';
            }
            
            // Load data source info
            this.loadDataSourceInfo(this.currentDataSource);
        } else {
            // Update connection status
            if (connectionStatus) {
                connectionStatus.innerHTML = '<i class="fas fa-circle text-secondary"></i> No data source selected';
                connectionStatus.className = 'alert alert-secondary py-2';
            }
            
            // Hide data source info
            const dataSourceInfo = document.getElementById('dataSourceInfo');
            if (dataSourceInfo) {
                dataSourceInfo.style.display = 'none';
            }
        }
        
        // Update submit button state based on both data source and query input
        this.updateSubmitButtonState();
    }

    handleDataSourceChange(e) {
        this.currentDataSource = e.target.value;
        this.updateDataSourceState();
    }
    
    handleQueryKeydown(e) {
        // Ctrl+Enter or Cmd+Enter to submit
        if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
            e.preventDefault();
            const form = e.target.closest('form');
            if (form) {
                form.requestSubmit();
            }
        }
    }
    
    handleQueryInput(e) {
        // Auto-resize textarea
        const textarea = e.target;
        textarea.style.height = 'auto';
        textarea.style.height = textarea.scrollHeight + 'px';
        
        // Update character count if needed
        this.updateCharacterCount(textarea.value.length);
        
        // Update submit button state based on query input and data source
        this.updateSubmitButtonState();
    }
    
    updateSubmitButtonState() {
        const submitBtn = document.getElementById('submitQueryBtn');
        const queryInput = document.getElementById('naturalQueryInput');
        
        if (submitBtn) {
            const hasQuery = queryInput && queryInput.value.trim().length > 0;
            const hasDataSource = this.currentDataSource && this.currentDataSource.length > 0;
            
            submitBtn.disabled = !(hasQuery && hasDataSource);
        }
    }
    
    updateCharacterCount(count) {
        const counter = document.getElementById('characterCount');
        if (counter) {
            counter.textContent = `${count} characters`;
        }
    }
    
    async loadQueryHistory() {
        try {
            const history = await ConvaBI.apiRequest('/query/history/');
            this.queryHistory = history.history || [];
            this.renderQueryHistory();
        } catch (error) {
            console.error('Failed to load query history:', error);
        }
    }
    
    renderQueryHistory() {
        const historyContainer = document.getElementById('queryHistory');
        if (!historyContainer || this.queryHistory.length === 0) {
            return;
        }
        
        const historyHtml = this.queryHistory.map(item => `
            <div class="history-item" data-query-id="${item.id}">
                <div class="d-flex justify-content-between align-items-start">
                    <div class="history-content">
                        <p class="mb-1">${item.query}</p>
                        <small class="text-muted">
                            <span class="badge bg-${item.status === 'SUCCESS' ? 'success' : 'danger'}">
                                ${item.status}
                            </span>
                            ${item.rows_returned} rows • ${ConvaBI.formatDate(item.created_at)}
                        </small>
                    </div>
                    <button class="btn btn-sm btn-outline-primary" onclick="queryInterface.reuseQuery('${item.query}')">
                        <i class="fas fa-redo"></i>
                    </button>
                </div>
            </div>
        `).join('');
        
        historyContainer.innerHTML = historyHtml;
    }
    
    reuseQuery(query) {
        const queryInput = document.getElementById('naturalQueryInput');
        if (queryInput) {
            queryInput.value = query;
            queryInput.focus();
            this.handleQueryInput({ target: queryInput });
        }
    }
    
    clearQuery() {
        const queryInput = document.getElementById('naturalQueryInput');
        if (queryInput) {
            queryInput.value = '';
            queryInput.focus();
        }
    }
    
    async showDataPreview(e) {
        e.preventDefault();
        
        const dataSourceSelect = document.getElementById('dataSourceSelect');
        const dataSourceId = dataSourceSelect ? dataSourceSelect.value : '';
        
        if (!dataSourceId) {
            ConvaBI.showAlert('warning', 'Please select a data source first to preview its data structure');
            return;
        }
        
        // Show the modal
        const modal = new bootstrap.Modal(document.getElementById('dataPreviewModal'));
        modal.show();
        
        // Load data preview
        await this.loadDataPreview(dataSourceId);
    }
    
    async loadDataPreview(dataSourceId) {
        const previewContent = document.getElementById('dataPreviewContent');
        
        try {
            // Show loading state
            previewContent.innerHTML = `
                <div class="text-center py-4">
                    <div class="spinner-border text-primary" role="status">
                        <span class="visually-hidden">Loading...</span>
                    </div>
                    <p class="text-muted mt-2">Loading data preview...</p>
                </div>
            `;
            
            // Fetch data preview
            const response = await fetch(`/api/data-preview/${dataSourceId}/`, {
                method: 'GET',
                headers: {
                    'X-CSRFToken': ConvaBI.getCsrfToken()
                }
            });
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const data = await response.json();
            
            if (data.success) {
                this.renderDataPreview(data);
            } else {
                throw new Error(data.error || 'Failed to load data preview');
            }
            
        } catch (error) {
            console.error('Failed to load data preview:', error);
            previewContent.innerHTML = `
                <div class="alert alert-danger">
                    <i class="fas fa-exclamation-triangle me-2"></i>
                    Failed to load data preview: ${error.message}
                </div>
            `;
        }
    }
    
    renderDataPreview(data) {
        const previewContent = document.getElementById('dataPreviewContent');
        
        let html = `
            <div class="row">
                <div class="col-md-12">
                    <nav>
                        <div class="nav nav-tabs" id="nav-tab" role="tablist">
                            <button class="nav-link active" id="nav-schema-tab" data-bs-toggle="tab" data-bs-target="#nav-schema" type="button" role="tab">
                                <i class="fas fa-database me-1"></i> Schema Information
                            </button>
                            <button class="nav-link" id="nav-sample-tab" data-bs-toggle="tab" data-bs-target="#nav-sample" type="button" role="tab">
                                <i class="fas fa-table me-1"></i> Sample Data
                            </button>
                            <button class="nav-link" id="nav-llm-tab" data-bs-toggle="tab" data-bs-target="#nav-llm" type="button" role="tab">
                                <i class="fas fa-robot me-1"></i> LLM Context
                            </button>
                        </div>
                    </nav>
                    <div class="tab-content" id="nav-tabContent">
        `;
        
        // Schema Information Tab
        html += `
            <div class="tab-pane fade show active" id="nav-schema" role="tabpanel">
                <div class="mt-3">
                    <div class="d-flex justify-content-between align-items-center mb-3">
                        <h6><i class="fas fa-info-circle me-2"></i>Data Source Overview</h6>
                        <span class="badge bg-primary">${data.source_type || 'Unknown'}</span>
                    </div>
                    
                    <div class="row mb-3">
                        <div class="col-md-4">
                            <div class="card border-0 bg-light">
                                <div class="card-body text-center">
                                    <i class="fas fa-table text-primary mb-2" style="font-size: 1.5rem;"></i>
                                    <h5 class="card-title">${data.schema_info?.row_count || 'N/A'}</h5>
                                    <p class="card-text text-muted">Total Rows</p>
                                </div>
                            </div>
                        </div>
                        <div class="col-md-4">
                            <div class="card border-0 bg-light">
                                <div class="card-body text-center">
                                    <i class="fas fa-columns text-success mb-2" style="font-size: 1.5rem;"></i>
                                    <h5 class="card-title">${data.schema_info?.column_count || data.schema_info?.columns?.length || 'N/A'}</h5>
                                    <p class="card-text text-muted">Columns</p>
                                </div>
                            </div>
                        </div>
                        <div class="col-md-4">
                            <div class="card border-0 bg-light">
                                <div class="card-body text-center">
                                    <i class="fas fa-database text-info mb-2" style="font-size: 1.5rem;"></i>
                                    <h5 class="card-title">${data.table_name || 'csv_data'}</h5>
                                    <p class="card-text text-muted">Table Name</p>
                                </div>
                            </div>
                        </div>
                    </div>
                    
                    <h6><i class="fas fa-list me-2"></i>Column Details</h6>
                    <div class="table-responsive">
                        <table class="table table-sm table-hover">
                            <thead class="table-dark">
                                <tr>
                                    <th>Column Name</th>
                                    <th>Data Type</th>
                                    <th>Sample Values</th>
                                    <th>Nullable</th>
                                </tr>
                            </thead>
                            <tbody>
        `;
        
        if (data.schema_info?.columns) {
            data.schema_info.columns.forEach(col => {
                const sampleValues = col.sample_values ? 
                    (Array.isArray(col.sample_values) ? col.sample_values.slice(0, 3).join(', ') : col.sample_values) : 
                    'N/A';
                
                html += `
                    <tr>
                        <td><code>${col.name}</code></td>
                        <td>
                            <span class="badge bg-secondary">${col.type}</span>
                        </td>
                        <td class="text-muted" style="max-width: 200px; overflow: hidden; text-overflow: ellipsis;">${sampleValues}</td>
                        <td>
                            ${col.nullable ? '<i class="fas fa-check text-success"></i>' : '<i class="fas fa-times text-danger"></i>'}
                        </td>
                    </tr>
                `;
            });
        }
        
        html += `
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        `;
        
        // Sample Data Tab
        html += `
            <div class="tab-pane fade" id="nav-sample" role="tabpanel">
                <div class="mt-3">
                    <h6><i class="fas fa-eye me-2"></i>Sample Data Preview</h6>
                    <p class="text-muted">First 10 rows from your data source</p>
        `;
        
        if (data.sample_data && data.sample_data.length > 0) {
            html += `
                <div class="table-responsive">
                    <table class="table table-sm table-striped">
                        <thead class="table-dark">
                            <tr>
            `;
            
            // Column headers
            Object.keys(data.sample_data[0]).forEach(key => {
                html += `<th style="min-width: 120px;">${key}</th>`;
            });
            
            html += `
                            </tr>
                        </thead>
                        <tbody>
            `;
            
            // Data rows
            data.sample_data.slice(0, 10).forEach(row => {
                html += '<tr>';
                Object.values(row).forEach(value => {
                    const displayValue = value === null || value === undefined ? 
                        '<span class="text-muted">null</span>' : 
                        String(value).length > 50 ? 
                            String(value).substring(0, 50) + '...' : 
                            String(value);
                    html += `<td>${displayValue}</td>`;
                });
                html += '</tr>';
            });
            
            html += `
                        </tbody>
                    </table>
                </div>
            `;
        } else {
            html += `
                <div class="alert alert-info">
                    <i class="fas fa-info-circle me-2"></i>
                    No sample data available to display.
                </div>
            `;
        }
        
        html += `
                </div>
            </div>
        `;
        
        // LLM Context Tab
        html += `
            <div class="tab-pane fade" id="nav-llm" role="tabpanel">
                <div class="mt-3">
                    <h6><i class="fas fa-robot me-2"></i>Context Sent to LLM</h6>
                    <p class="text-muted">This is the exact schema information that gets sent to the AI for query generation</p>
                    
                    <div class="mb-3">
                        <label class="form-label">Enhanced Schema Prompt:</label>
                        <pre class="bg-light p-3 border rounded" id="llmSchemaContext" style="max-height: 400px; overflow-y: auto;">${data.llm_context || 'LLM context not available'}</pre>
                    </div>
                    
                    <div class="mb-3">
                        <label class="form-label">Raw Schema JSON:</label>
                        <pre class="bg-light p-3 border rounded" id="rawSchemaJson" style="max-height: 300px; overflow-y: auto;">${JSON.stringify(data.schema_info, null, 2)}</pre>
                    </div>
                </div>
            </div>
        `;
        
        // Close tabs
        html += `
                    </div>
                </div>
            </div>
        `;
        
        previewContent.innerHTML = html;
        
        // Store schema for copy functionality
        this.currentSchemaData = {
            schema_info: data.schema_info,
            llm_context: data.llm_context,
            table_name: data.table_name
        };
    }
    
    copySchemaToClipboard(e) {
        e.preventDefault();
        
        if (!this.currentSchemaData) {
            ConvaBI.showAlert('warning', 'No schema data to copy');
            return;
        }
        
        const schemaText = `
# Data Schema Information

## Table: ${this.currentSchemaData.table_name || 'csv_data'}

## Columns:
${this.currentSchemaData.schema_info?.columns?.map(col => 
    `- ${col.name} (${col.type})${col.sample_values ? ' - Examples: ' + (Array.isArray(col.sample_values) ? col.sample_values.join(', ') : col.sample_values) : ''}`
).join('\n') || 'No column information available'}

## LLM Context:
${this.currentSchemaData.llm_context || 'No LLM context available'}

## Raw Schema JSON:
${JSON.stringify(this.currentSchemaData.schema_info, null, 2)}
        `.trim();
        
        navigator.clipboard.writeText(schemaText).then(() => {
            ConvaBI.showAlert('success', 'Schema information copied to clipboard');
        }).catch(err => {
            console.error('Failed to copy to clipboard:', err);
            ConvaBI.showAlert('danger', 'Failed to copy to clipboard');
        });
    }
    
    updateProcessingStatus(message) {
        const processingDiv = document.getElementById('queryProcessing');
        const processingText = document.getElementById('processingText');
        
        if (processingDiv) {
            processingDiv.classList.remove('d-none');
        }
        
        if (processingText) {
            processingText.textContent = message;
        }
    }
    
    hideProcessingStatus() {
        const processingDiv = document.getElementById('queryProcessing');
        if (processingDiv) {
            processingDiv.classList.add('d-none');
        }
    }
    
    async loadDataSourceInfo(dataSourceId) {
        try {
            const response = await fetch(`/api/data-sources/${dataSourceId}/info/`);
            const data = await response.json();
            
            if (data.success) {
                this.displayDataSourceInfo(data.data_source);
            } else {
                console.error('Failed to load data source info:', data.error);
            }
        } catch (error) {
            console.error('Failed to load data source info:', error);
        }
    }
    
    displayDataSourceInfo(info) {
        const dataSourceInfo = document.getElementById('dataSourceInfo');
        const dataSourceDetails = document.getElementById('dataSourceDetails');
        
        if (dataSourceInfo && dataSourceDetails) {
            // Calculate table count from schema_info
            let tableCount = 0;
            if (info.schema_info && info.schema_info.tables) {
                tableCount = Object.keys(info.schema_info.tables).length;
            } else if (info.schema_info && info.schema_info.columns) {
                tableCount = 1; // Single table (like CSV)
            }
            
            const infoHtml = `
                <div class="row">
                    <div class="col-6">
                        <small class="text-muted">Type</small>
                        <div class="fw-bold">${info.source_type || 'Unknown'}</div>
                    </div>
                    <div class="col-6">
                        <small class="text-muted">Tables</small>
                        <div class="fw-bold">${tableCount}</div>
                    </div>
                </div>
                <div class="row mt-2">
                    <div class="col-6">
                        <small class="text-muted">Status</small>
                        <div>
                            <span class="badge bg-${info.status === 'active' ? 'success' : 'secondary'}">
                                ${info.status || 'Unknown'}
                            </span>
                        </div>
                    </div>
                    <div class="col-6">
                        <small class="text-muted">Connection</small>
                        <div>
                            <span class="badge bg-${info.connection_status === 'connected' ? 'success' : 'warning'}">
                                ${info.connection_status || 'Unknown'}
                            </span>
                        </div>
                    </div>
                </div>
                <div class="mt-2">
                    <small class="text-muted">Name</small>
                    <div class="fw-bold">${info.name || 'Unnamed'}</div>
                </div>
            `;
            
            dataSourceDetails.innerHTML = infoHtml;
            dataSourceInfo.style.display = 'block';
        }
    }
    
    bindHistoryEvents() {
        const historyToggle = document.getElementById('historyToggle');
        if (historyToggle) {
            historyToggle.addEventListener('click', () => {
                const historyPanel = document.getElementById('historyPanel');
                if (historyPanel) {
                    historyPanel.classList.toggle('show');
                }
            });
        }
    }
    
    bindExampleQueries() {
        const exampleButtons = document.querySelectorAll('.example-query');
        exampleButtons.forEach(button => {
            button.addEventListener('click', (e) => {
                const query = e.target.getAttribute('data-query');
                if (query) {
                    this.reuseQuery(query);
                }
            });
        });
    }
    
    updateExampleQueries() {
        // Update example queries based on selected data source
        const examples = this.getExampleQueriesForDataSource(this.currentDataSource);
        const examplesContainer = document.getElementById('exampleQueries');
        
        if (examplesContainer && examples.length > 0) {
            const examplesHtml = examples.map(example => `
                <button class="btn btn-sm btn-outline-secondary example-query me-2 mb-2" data-query="${example}">
                    ${example}
                </button>
            `).join('');
            
            examplesContainer.innerHTML = examplesHtml;
            this.bindExampleQueries();
        }
    }
    
    getExampleQueriesForDataSource(dataSourceId) {
        // Default examples - could be customized per data source
        return [
            'Show me all records',
            'Count total rows',
            'Show unique values',
            'Find top 10 records',
            'Group by category and count',
            'Show recent entries'
        ];
    }
    
    setupAutoComplete() {
        // Basic auto-completion for common SQL patterns
        const queryInput = document.getElementById('queryInput');
        if (!queryInput) return;
        
        const suggestions = [
            'Show me', 'Count', 'List all', 'Find', 'Group by', 'Order by',
            'Filter by', 'Sum of', 'Average', 'Maximum', 'Minimum',
            'Total', 'Records where', 'Data from last', 'Top 10'
        ];
        
        // Simple suggestion system
        queryInput.addEventListener('input', ConvaBI.debounce((e) => {
            const value = e.target.value.toLowerCase();
            if (value.length < 2) return;
            
            const matches = suggestions.filter(s => 
                s.toLowerCase().includes(value)
            );
            
            // Could implement dropdown here
            console.log('Suggestions:', matches);
        }, 300));
    }
}

// Chart visualization helpers
class QueryVisualization {
    constructor() {
        this.currentChart = null;
    }
    
    createChart(data, type = 'bar', options = {}) {
        const chartContainer = document.getElementById('chartContainer');
        if (!chartContainer) return;
        
        try {
            // Use Plotly for visualization
            if (typeof Plotly !== 'undefined') {
                this.createPlotlyChart(data, type, options);
            } else {
                console.warn('Plotly not loaded, chart visualization unavailable');
            }
        } catch (error) {
            console.error('Chart creation failed:', error);
        }
    }
    
    createPlotlyChart(data, type, options) {
        const chartDiv = document.getElementById('chart');
        if (!chartDiv) return;
        
        const plotlyData = this.convertDataForPlotly(data, type);
        const layout = {
            title: options.title || 'Query Results',
            responsive: true,
            ...options.layout
        };
        
        const config = {
            responsive: true,
            displayModeBar: true,
            modeBarButtonsToRemove: ['pan2d', 'lasso2d'],
            ...options.config
        };
        
        Plotly.newPlot(chartDiv, plotlyData, layout, config);
        this.currentChart = { type, data: plotlyData, layout, config };
    }
    
    convertDataForPlotly(data, type) {
        // Convert data based on chart type
        switch (type) {
            case 'bar':
                return this.createBarChart(data);
            case 'line':
                return this.createLineChart(data);
            case 'pie':
                return this.createPieChart(data);
            case 'scatter':
                return this.createScatterChart(data);
            default:
                return this.createBarChart(data);
        }
    }
    
    createBarChart(data) {
        if (!data || data.length === 0) return [];
        
        const keys = Object.keys(data[0]);
        const xKey = keys[0];
        const yKey = keys[1] || keys[0];
        
        return [{
            x: data.map(row => row[xKey]),
            y: data.map(row => row[yKey]),
            type: 'bar'
        }];
    }
    
    createLineChart(data) {
        if (!data || data.length === 0) return [];
        
        const keys = Object.keys(data[0]);
        const xKey = keys[0];
        const yKey = keys[1] || keys[0];
        
        return [{
            x: data.map(row => row[xKey]),
            y: data.map(row => row[yKey]),
            type: 'scatter',
            mode: 'lines+markers'
        }];
    }
    
    createPieChart(data) {
        if (!data || data.length === 0) return [];
        
        const keys = Object.keys(data[0]);
        const labelKey = keys[0];
        const valueKey = keys[1] || keys[0];
        
        return [{
            labels: data.map(row => row[labelKey]),
            values: data.map(row => row[valueKey]),
            type: 'pie'
        }];
    }
    
    createScatterChart(data) {
        if (!data || data.length === 0) return [];
        
        const keys = Object.keys(data[0]);
        const xKey = keys[0];
        const yKey = keys[1] || keys[0];
        
        return [{
            x: data.map(row => row[xKey]),
            y: data.map(row => row[yKey]),
            mode: 'markers',
            type: 'scatter'
        }];
    }
    
    updateChart(type) {
        if (!this.currentChart) return;
        
        const newData = this.convertDataForPlotly(this.currentChart.data, type);
        const chartDiv = document.getElementById('chart');
        
        if (chartDiv) {
            Plotly.newPlot(chartDiv, newData, this.currentChart.layout, this.currentChart.config);
        }
    }
}

// Initialize when DOM is ready
let queryInterface;
let queryVisualization;

document.addEventListener('DOMContentLoaded', function() {
    queryInterface = new QueryInterface();
    queryVisualization = new QueryVisualization();
    
            // Export to global scope
        window.queryInterface = queryInterface;
        window.queryVisualization = queryVisualization;
    }); 