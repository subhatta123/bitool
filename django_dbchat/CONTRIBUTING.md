# Contributing to ConvaBI

Thank you for considering contributing to ConvaBI! This document provides guidelines and information for contributors.

## 🚀 Getting Started

### Prerequisites
- Python 3.8+
- PostgreSQL 12+
- Redis (for Celery tasks)
- Git
- Basic knowledge of Django

### Development Setup

1. **Fork and Clone**
   ```bash
   git clone https://github.com/YOUR_USERNAME/convabifin.git
   cd convabifin
   ```

2. **Set Up Virtual Environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. **Configure Environment**
   ```bash
   cd django_dbchat
   cp env.example .env
   # Edit .env with your configuration
   ```

4. **Set Up Database**
   ```bash
   python manage.py migrate
   python manage.py createsuperuser
   ```

5. **Run Development Server**
   ```bash
   python manage.py runserver
   ```

## 📋 Development Guidelines

### Code Style
- Follow PEP 8 Python style guidelines
- Use meaningful variable and function names
- Write docstrings for all classes and functions
- Keep functions small and focused (max 50 lines)
- Use type hints where possible

### Commit Messages
Use conventional commit format:
```
type(scope): description

Examples:
feat(dashboards): add permission level dropdown
fix(core): resolve DataFrame boolean ambiguity error
docs(readme): update installation instructions
```

Types:
- `feat`: New features
- `fix`: Bug fixes
- `docs`: Documentation updates
- `style`: Code style changes
- `refactor`: Code refactoring
- `test`: Adding tests
- `chore`: Maintenance tasks

### Branch Naming
- `feature/feature-name` - New features
- `fix/bug-description` - Bug fixes
- `docs/documentation-update` - Documentation
- `refactor/component-name` - Refactoring

## 🧪 Testing

### Running Tests
```bash
# Run all tests
python manage.py test

# Run tests for specific app
python manage.py test dashboards

# Run with coverage
pip install coverage
coverage run --source='.' manage.py test
coverage report
```

### Writing Tests
- Write tests for all new features
- Include edge cases and error conditions
- Use Django's TestCase class
- Mock external services (OpenAI, email, etc.)

Example test structure:
```python
from django.test import TestCase
from django.contrib.auth import get_user_model
from dashboards.models import Dashboard

User = get_user_model()

class DashboardModelTest(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(
            username='testuser',
            email='test@example.com'
        )
    
    def test_dashboard_creation(self):
        dashboard = Dashboard.objects.create(
            name='Test Dashboard',
            owner=self.user
        )
        self.assertEqual(dashboard.name, 'Test Dashboard')
        self.assertEqual(dashboard.owner, self.user)
```

## 🏗️ Architecture

### Project Structure
```
django_dbchat/
├── accounts/           # User management
├── core/              # Main query interface
├── dashboards/        # Dashboard management
├── datasets/          # Data source management
├── licensing/         # Permission system
├── services/          # Business logic
├── static/            # Frontend assets
├── templates/         # HTML templates
└── utils/             # Utility functions
```

### Key Components
- **Core**: Natural language to SQL conversion
- **Dashboards**: Interactive data visualizations
- **Datasets**: Data source connections and ETL
- **Services**: Reusable business logic
- **Licensing**: Role-based access control

## 🐛 Reporting Issues

### Bug Reports
Include:
- ConvaBI version
- Python version
- Operating system
- Steps to reproduce
- Expected vs actual behavior
- Error messages/logs
- Screenshots (if UI-related)

### Feature Requests
Include:
- Clear description of the feature
- Use case and motivation
- Proposed implementation (if any)
- Screenshots/mockups (if UI-related)

## 🔍 Code Review Process

1. **Create Pull Request**
   - Use descriptive title and description
   - Reference related issues
   - Include testing instructions

2. **Review Checklist**
   - [ ] Code follows style guidelines
   - [ ] Tests are included and passing
   - [ ] Documentation is updated
   - [ ] No breaking changes (or properly documented)
   - [ ] Security considerations addressed

3. **Approval Process**
   - At least one maintainer approval required
   - All CI checks must pass
   - Conflicts must be resolved

## 🚀 Deployment

### Environment-Specific Changes
- Development: Use SQLite, debug mode on
- Staging: Use PostgreSQL, debug mode off
- Production: Full security headers, monitoring

### Database Migrations
```bash
# Create migrations
python manage.py makemigrations

# Apply migrations
python manage.py migrate

# Check migration status
python manage.py showmigrations
```

## 📚 Documentation

### Code Documentation
- Use Google-style docstrings
- Document all public APIs
- Include examples for complex functions

### User Documentation
- Update README.md for user-facing changes
- Create/update API documentation
- Add screenshots for UI changes

## 🤝 Community

### Communication
- GitHub Issues for bugs and features
- GitHub Discussions for questions
- Follow our Code of Conduct

### Getting Help
- Check existing issues and documentation
- Ask questions in GitHub Discussions
- Contact maintainers for security issues

## 📄 License

By contributing to ConvaBI, you agree that your contributions will be licensed under the MIT License.

## 🙏 Recognition

Contributors are recognized in:
- README.md contributors section
- Release notes for significant contributions
- GitHub contributors page

Thank you for contributing to ConvaBI! 🚀 