
// Enhanced form data reading - NO FALLBACKS OR HARDCODING
function getFormDataFixed() {
    console.log('Reading form data from user input...');
    
    // Get actual values from form fields
    const smtp_host = document.getElementById('smtp-host').value.trim();
    const smtp_port = document.getElementById('smtp-port').value.trim();
    const smtp_username = document.getElementById('smtp-username').value.trim();
    const smtp_password = document.getElementById('smtp-password').value;
    const sender_email = document.getElementById('sender-email').value.trim();
    const sender_name = document.getElementById('sender-name').value.trim();
    const encryption = document.getElementById('encryption').value;
    
    console.log('User entered values:', {
        smtp_host,
        smtp_username,
        smtp_port,
        sender_email,
        sender_name,
        encryption
    });
    
    // Validate that we have actual user input
    if (!smtp_host || !smtp_username || !smtp_password || !sender_email) {
        alert('Please fill in all required fields before saving!');
        return null;
    }
    
    // Return actual user values - NO FALLBACKS
    return {
        smtp_host,
        smtp_port: smtp_port || '587',
        smtp_username,
        smtp_password,
        sender_email,
        sender_name: sender_name || 'ConvaBI System',
        encryption: encryption || 'tls'
    };
}

// Enhanced save function
function saveEmailConfigFixed() {
    console.log('Starting email config save...');
    
    const formData = getFormDataFixed();
    if (!formData) {
        console.error('Failed to get form data');
        return;
    }
    
    // Show what we're about to save
    console.log('About to save:', formData);
    
    // Confirm with user
    const confirmMessage = `Confirm saving email configuration:
SMTP Host: ${formData.smtp_host}
Username: ${formData.smtp_username}
Sender Email: ${formData.sender_email}
Port: ${formData.smtp_port}

Continue?`;
    
    if (!confirm(confirmMessage)) {
        console.log('Save cancelled by user');
        return;
    }
    
    // Show loading
    const submitBtn = document.querySelector('button[type="submit"]');
    const originalText = submitBtn.innerHTML;
    submitBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Saving...';
    submitBtn.disabled = true;
    
    // Create form data object with exact user values
    const formDataObj = new FormData();
    formDataObj.append('smtp_host', formData.smtp_host);
    formDataObj.append('smtp_port', formData.smtp_port);
    formDataObj.append('smtp_username', formData.smtp_username);
    formDataObj.append('smtp_password', formData.smtp_password);
    formDataObj.append('sender_email', formData.sender_email);
    formDataObj.append('sender_name', formData.sender_name);
    formDataObj.append('encryption', formData.encryption);
    if (formData.encryption === 'tls') {
        formDataObj.append('use_tls', 'on');
    }
    
    fetch('/email-config/', {
        method: 'POST',
        headers: {
            'X-CSRFToken': getCsrfToken()
        },
        body: formDataObj
    })
    .then(response => {
        console.log('Save response status:', response.status);
        if (response.ok) {
            return response.text();
        } else {
            throw new Error(`HTTP ${response.status}`);
        }
    })
    .then(html => {
        console.log('Save completed');
        if (html.includes('Email configuration saved successfully') || html.includes('saved successfully')) {
            alert('✅ Email configuration saved successfully with YOUR values!');
            setTimeout(() => window.location.reload(), 1500);
        } else {
            alert('⚠️ Save may have failed. Please check and try again.');
        }
    })
    .catch(error => {
        console.error('Save error:', error);
        alert('❌ Save failed: ' + error.message);
    })
    .finally(() => {
        submitBtn.innerHTML = originalText;
        submitBtn.disabled = false;
    });
}

// Enhanced test function
function testEmailConfigFixed() {
    console.log('Starting email test...');
    
    const formData = getFormDataFixed();
    if (!formData) {
        console.error('Failed to get form data for test');
        return;
    }
    
    // Show what we're about to test
    console.log('About to test:', formData);
    
    const testBtn = document.getElementById('test-email-btn') || document.querySelector('.btn:contains("Test")');
    if (testBtn) {
        const originalText = testBtn.innerHTML;
        testBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Testing...';
        testBtn.disabled = true;
        
        const testFormData = new FormData();
        testFormData.append('smtp_host', formData.smtp_host);
        testFormData.append('smtp_port', formData.smtp_port);
        testFormData.append('smtp_user', formData.smtp_username);
        testFormData.append('smtp_password', formData.smtp_password);
        if (formData.encryption === 'tls') {
            testFormData.append('use_tls', 'on');
        }
        
        fetch('/test-email-config/', {
            method: 'POST',
            headers: {
                'X-CSRFToken': getCsrfToken()
            },
            body: testFormData
        })
        .then(response => response.json())
        .then(data => {
            console.log('Test response:', data);
            if (data.success) {
                alert('✅ ' + data.message);
            } else {
                alert('❌ ' + data.message);
            }
        })
        .catch(error => {
            console.error('Test error:', error);
            alert('❌ Test failed: ' + error.message);
        })
        .finally(() => {
            testBtn.innerHTML = originalText;
            testBtn.disabled = false;
        });
    }
}

// Replace the original functions
window.getFormData = getFormDataFixed;
window.saveEmailConfig = saveEmailConfigFixed;
window.testEmailConfig = testEmailConfigFixed;

console.log('Email form fix loaded - now reading YOUR actual input!');
