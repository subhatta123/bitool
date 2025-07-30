# ConvaBI - Business Intelligence Platform

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.8+-green.svg)
![Django](https://img.shields.io/badge/django-4.2+-blue.svg)

A comprehensive Business Intelligence platform built with Django that provides data visualization, ETL operations, semantic data modeling, and automated scheduling capabilities.

## 🚀 Features

### 📊 **Core Business Intelligence**
- **Interactive Dashboards**: Create and share dynamic dashboards with real-time data
- **Advanced Querying**: Natural language to SQL conversion with LLM integration
- **Data Visualization**: Charts, graphs, and visual analytics
- **Report Generation**: Automated report creation and scheduling

### 🔄 **ETL & Data Integration**
- **Multi-Source Support**: CSV, PostgreSQL, MySQL, Oracle, SQL Server, APIs
- **Data Transformation**: Type conversion, cleaning, and validation
- **Join Operations**: Advanced data joining with visual interface
- **Union & Aggregation**: Combine and summarize data from multiple sources

### ⏰ **Automated ETL Scheduling**
- **Flexible Scheduling**: 15min, 30min, hourly, daily, weekly, monthly intervals
- **Timezone Support**: Global timezone-aware scheduling
- **Automatic Recovery**: Built-in overdue job detection and execution
- **Comprehensive Monitoring**: Success/failure tracking with detailed logs

### 🤖 **AI-Powered Features**
- **Semantic Layer**: Automatic metadata generation and business context
- **LLM Integration**: Support for OpenAI, Ollama, and other providers
- **Smart Query Generation**: Natural language to SQL conversion
- **Intelligent Error Recovery**: Automated problem detection and fixes

### 👥 **Multi-User & Security**
- **User Management**: Role-based access control
- **Data Sharing**: Secure data source and dashboard sharing
- **Licensing System**: Flexible user licensing and permissions
- **Audit Logging**: Comprehensive activity tracking

## 🛠️ Installation

### Prerequisites
- Python 3.8+
- Node.js 14+ (for frontend dependencies)
- PostgreSQL/MySQL (optional, SQLite included)
- Redis (optional, for production Celery)

### Quick Start

1. **Clone the Repository**
```bash
git clone https://github.com/subhatta123/conva.git
cd conva
```

2. **Create Virtual Environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install Dependencies**
```bash
pip install -r requirements.txt
```

4. **Configure Environment**
```bash
cp env.example .env
# Edit .env with your settings
```

5. **Database Setup**
```bash
cd django_dbchat
python manage.py migrate
python manage.py createsuperuser
```

6. **Run Development Server**
```bash
python manage.py runserver
```

Visit `http://localhost:8000` to access the application.

## 📖 Usage Guide

### Getting Started

1. **Login**: Use the superuser account created during setup
2. **Add Data Sources**: Upload CSV files or connect to databases
3. **Create Dashboards**: Build interactive visualizations
4. **Schedule ETL Jobs**: Automate data refresh operations

### Data Source Management

#### Supported Sources
- **CSV Files**: Upload and automatically analyze
- **Databases**: PostgreSQL, MySQL, Oracle, SQL Server
- **APIs**: REST API integration with custom endpoints

#### ETL Operations
- **Data Type Transformation**: Convert and clean data types
- **Join Operations**: Combine data from multiple sources
- **Aggregations**: Sum, count, average, and custom calculations
- **Scheduling**: Automate refresh with flexible timing

### ETL Scheduling System

#### Creating Scheduled Jobs
1. Navigate to **Data Sources → ETL Schedules**
2. Click **"New Schedule"**
3. Select data sources and configure timing
4. Set up notifications and error handling

#### Monitoring & Diagnostics
- **Real-time Status**: Live monitoring of job execution
- **Success Rate Tracking**: Performance metrics and trends
- **Automatic Recovery**: Overdue jobs are automatically detected and executed
- **Comprehensive Diagnostics**: Built-in troubleshooting tools

## 🔧 Configuration

### Environment Variables

Create a `.env` file in the project root:

```env
# Database
DATABASE_URL=sqlite:///db.sqlite3
# DATABASE_URL=postgresql://user:pass@localhost/dbname

# Redis (for production)
USE_REDIS=False
REDIS_URL=redis://localhost:6379

# LLM Configuration
OPENAI_API_KEY=your_openai_key
OLLAMA_BASE_URL=http://localhost:11434

# Email Settings
EMAIL_HOST=smtp.gmail.com
EMAIL_PORT=587
EMAIL_HOST_USER=your_email@gmail.com
EMAIL_HOST_PASSWORD=your_password

# Security
SECRET_KEY=your_secret_key_here
DEBUG=True
ALLOWED_HOSTS=localhost,127.0.0.1
```

### Production Deployment

#### With Redis & Celery
```bash
# Install Redis
sudo apt-get install redis-server

# Start Celery Worker
celery -A dbchat_project worker --loglevel=info

# Start Celery Beat (for scheduling)
celery -A dbchat_project beat --loglevel=info --scheduler django_celery_beat.schedulers:DatabaseScheduler
```

#### With Docker
```bash
docker-compose up -d
```

## 📁 Project Structure

```
conva/
├── django_dbchat/                 # Main Django application
│   ├── accounts/                  # User management
│   ├── core/                      # Core functionality
│   ├── datasets/                  # Data source management
│   ├── dashboards/                # Dashboard system
│   ├── licensing/                 # User licensing
│   ├── services/                  # Business logic services
│   ├── templates/                 # HTML templates
│   ├── static/                    # CSS, JS, images
│   └── manage.py                  # Django management
├── requirements.txt               # Python dependencies
├── .env.example                   # Environment template
└── README.md                      # This file
```

## 🔍 Key Features Deep Dive

### ETL Scheduling System

The advanced ETL scheduling system provides:

- **Automatic Overdue Detection**: Jobs that miss their schedule are automatically detected and executed
- **Fallback Mechanisms**: Works with or without Redis/Celery Beat
- **Smart Recovery**: Failed jobs are retried with exponential backoff
- **Comprehensive Logging**: Detailed execution logs for troubleshooting

### Semantic Data Layer

- **Auto-Discovery**: Automatically detects relationships between tables
- **Business Context**: Adds meaningful descriptions to technical data
- **Query Optimization**: Improves query performance through intelligent caching

### Multi-LLM Support

- **OpenAI Integration**: GPT-3.5/4 for advanced query generation
- **Ollama Support**: Local LLM deployment for privacy
- **Custom Providers**: Easy integration of additional LLM services

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 Documentation

- [Installation Guide](docs/installation.md)
- [User Manual](docs/user-manual.md)
- [API Documentation](docs/api.md)
- [Developer Guide](docs/development.md)

## 🐛 Known Issues

- Large CSV files (>100MB) may require additional memory configuration
- Some advanced SQL features may not be supported in natural language queries
- Real-time dashboard updates require WebSocket support

## 🔒 Security

- All data is encrypted at rest
- User sessions are secured with CSRF protection
- SQL injection prevention through parameterized queries
- Role-based access control for data and features

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/subhatta123/conva/issues)
- **Discussions**: [GitHub Discussions](https://github.com/subhatta123/conva/discussions)
- **Email**: support@conva.ai

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Django community for the excellent framework
- Chart.js for beautiful visualizations
- Bootstrap for responsive UI components
- All contributors who helped build this platform

---

**ConvaBI** - Making Business Intelligence accessible to everyone. 