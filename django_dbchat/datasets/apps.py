from django.apps import AppConfig


class DatasetsConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'datasets'
    verbose_name = 'Data Sources & Integration'
    
    def ready(self):
        import datasets.signals  # noqa 