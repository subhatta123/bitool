#!/bin/bash
# Install Node.js and Puppeteer for dashboard export functionality

echo "🚀 Installing Node.js and Puppeteer for dashboard exports..."

# Update package lists
apt-get update

# Install Node.js and npm
echo "📦 Installing Node.js..."
curl -fsSL https://deb.nodesource.com/setup_18.x | bash -
apt-get install -y nodejs

# Verify installation
echo "✅ Node.js version: $(node --version)"
echo "✅ NPM version: $(npm --version)"

# Install Puppeteer globally
echo "🎭 Installing Puppeteer..."
npm install -g puppeteer

# Install dependencies for headless Chrome in container
echo "🔧 Installing Chrome dependencies..."
apt-get install -y \
    ca-certificates \
    fonts-liberation \
    libappindicator3-1 \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libc6 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libexpat1 \
    libfontconfig1 \
    libgbm1 \
    libgcc1 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libstdc++6 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxi6 \
    libxrandr2 \
    libxrender1 \
    libxss1 \
    libxtst6 \
    lsb-release \
    wget \
    xdg-utils

# Create package.json for project dependencies
echo "📝 Creating package.json..."
cat > /app/package.json << 'EOF'
{
  "name": "dashboard-export",
  "version": "1.0.0",
  "description": "Dashboard export with Puppeteer",
  "main": "index.js",
  "scripts": {
    "test": "echo \"Error: no test specified\" && exit 1"
  },
  "dependencies": {
    "puppeteer": "^21.0.0"
  },
  "keywords": ["dashboard", "export", "pdf", "png", "puppeteer"],
  "author": "ConvaBI",
  "license": "MIT"
}
EOF

# Install project dependencies
echo "📦 Installing project dependencies..."
cd /app && npm install

# Clean up
echo "🧹 Cleaning up..."
apt-get autoremove -y
apt-get clean
rm -rf /var/lib/apt/lists/*

echo "✅ Node.js and Puppeteer installation complete!"
echo "🎯 Ready for dashboard exports with fully rendered charts!"

# Test installation
echo "🧪 Testing Puppeteer installation..."
node -e "
const puppeteer = require('puppeteer');
console.log('✅ Puppeteer version:', puppeteer.VERSION || 'installed');
console.log('🎭 Puppeteer ready for dashboard exports!');
" 