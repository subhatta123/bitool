# ConvaBI - Conversational Business Intelligence Platform

A powerful Django-based business intelligence platform that enables users to query databases using natural language and create interactive dashboards.

## 🚀 Features

### Core Functionality
- **Natural Language to SQL**: Convert plain English questions into SQL queries using AI
- **Multi-Database Support**: Connect to PostgreSQL, MySQL, Oracle, SQL Server, and CSV files
- **Interactive Dashboards**: Create and share beautiful, interactive data visualizations
- **Real-time Collaboration**: Share dashboards with team members with granular permissions

### Advanced Features
- **ETL Operations**: Join, union, and transform data from multiple sources
- **Semantic Layer**: Auto-generated metadata and column descriptions for better data understanding
- **Permission System**: Role-based access control (Admin, Creator, Viewer)
- **Dashboard Sharing**: Share dashboards with view, edit, or admin permissions
- **Email Integration**: Export and email dashboard reports
- **LLM Integration**: Support for OpenAI and Ollama for natural language processing

## 🛠️ Technology Stack

- **Backend**: Django 4.2+, Python 3.8+
- **Database**: PostgreSQL (primary), DuckDB (analytics)
- **Frontend**: Bootstrap 5, JavaScript, Plotly.js
- **AI/ML**: OpenAI GPT, Ollama support
- **Task Queue**: Celery with Redis
- **Deployment**: Docker, Docker Compose

## 📋 Prerequisites

- Python 3.8 or higher
- PostgreSQL 12+ (for production)
- Redis (for Celery tasks)
- Node.js (for frontend dependencies)

## 🚀 Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/subhatta123/convabifin.git
cd convabifin
```

### 2. Environment Setup
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Database Setup
```bash
# Create PostgreSQL database
createdb convabi_db

# Run migrations
cd django_dbchat
python manage.py migrate

# Create superuser
python manage.py createsuperuser
```

### 4. Configure Environment Variables
Create a `.env` file in the `django_dbchat` directory:
```env
DEBUG=True
SECRET_KEY=your-secret-key-here
DATABASE_URL=postgresql://username:password@localhost:5432/convabi_db

# OpenAI Configuration (optional)
OPENAI_API_KEY=your-openai-api-key

# Ollama Configuration (optional)
OLLAMA_URL=http://localhost:11434

# Redis Configuration
REDIS_URL=redis://localhost:6379/0
```

### 5. Start the Development Server
```bash
python manage.py runserver
```

Visit `http://localhost:8000` to access ConvaBI.

## 🐳 Docker Deployment

### Quick Docker Setup
```bash
# Build and start services
docker-compose up -d

# Run migrations
docker-compose exec web python manage.py migrate

# Create superuser
docker-compose exec web python manage.py createsuperuser
```

### Production Deployment
```bash
# Use production configuration
docker-compose -f docker-compose.prod.yml up -d
```

## 📖 User Guide

### Getting Started
1. **Upload Data Sources**: Connect to databases or upload CSV files
2. **Run ETL Operations**: Clean and transform your data
3. **Generate Semantic Layer**: Auto-generate column descriptions and metadata
4. **Query Data**: Use natural language to ask questions about your data
5. **Create Dashboards**: Build interactive visualizations and dashboards
6. **Share & Collaborate**: Share dashboards with team members

### Permission Levels
- **Admin**: Full system access, user management, configuration
- **Creator**: Can create data sources, dashboards, and share with others
- **Viewer**: Can query shared data sources and view shared dashboards

### Dashboard Sharing
- **View Only**: Read-only access to dashboards
- **Can Edit**: Can add charts and modify dashboard layout
- **Admin**: Full access including sharing permissions

## 🔧 Configuration

### LLM Setup
ConvaBI supports multiple LLM providers:

#### OpenAI Configuration
1. Go to Admin → LLM Configuration
2. Enter your OpenAI API key
3. Select your preferred model (GPT-3.5-turbo, GPT-4, etc.)

#### Ollama Configuration
1. Install and run Ollama locally
2. Pull SQL-focused models: `ollama pull sqlcoder:15b`
3. Configure in Admin → LLM Configuration

### Email Configuration
1. Go to Admin → Email Configuration
2. Configure SMTP settings for dashboard sharing notifications

## 📁 Project Structure

```
django_dbchat/
├── accounts/           # User management and authentication
├── api/               # REST API endpoints
├── core/              # Main application logic and query interface
├── dashboards/        # Dashboard creation and management
├── datasets/          # Data source management and ETL operations
├── licensing/         # User licensing and permissions
├── services/          # Business logic services
├── static/            # CSS, JavaScript, images
├── templates/         # HTML templates
├── utils/             # Utility functions and helpers
├── manage.py          # Django management script
├── requirements.txt   # Python dependencies
└── docker-compose.yml # Docker configuration
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Commit your changes: `git commit -m 'Add amazing feature'`
4. Push to the branch: `git push origin feature/amazing-feature`
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

- **Issues**: Report bugs and feature requests on [GitHub Issues](https://github.com/subhatta123/convabifin/issues)
- **Documentation**: Check the `/docs` folder for detailed documentation
- **Community**: Join our discussions in the GitHub Discussions tab

## 🙏 Acknowledgments

- Built with Django and the amazing Python ecosystem
- UI components powered by Bootstrap 5
- Data visualizations using Plotly.js
- AI capabilities powered by OpenAI and Ollama

---

**ConvaBI** - Making business intelligence conversational and accessible to everyone! 🚀 