#!/usr/bin/env python3
"""
Django management command for Ollama model management
Helps with setup, testing, and management of Llama 3.2b models
"""

import json
import requests
import subprocess
import time
from typing import List, Dict, Any, Tuple
from django.core.management.base import BaseCommand
from django.conf import settings
from core.models import LLMConfig
from services.llm_service import LLMService

class Command(BaseCommand):
    help = 'Manage Ollama models and configuration'

    def add_arguments(self, parser):
        parser.add_argument('action', choices=[
            'status', 'list', 'install', 'remove', 'test', 'setup', 'config'
        ], help='Action to perform')
        
        parser.add_argument('--model', type=str, help='Model to install/remove/test')
        parser.add_argument('--url', type=str, default='http://localhost:11434', 
                          help='Ollama server URL')
        parser.add_argument('--force', action='store_true', 
                          help='Force installation/removal')
        parser.add_argument('--verbose', action='store_true', help='Verbose output')

    def handle(self, *args, **options):
        self.verbosity = options.get('verbosity', 1)
        self.verbose = options.get('verbose', False)
        self.ollama_url = options.get('url', 'http://localhost:11434')
        
        action = options['action']
        
        self.stdout.write(self.style.SUCCESS(f"🦙 Ollama Management - {action.upper()}"))
        self.stdout.write("=" * 50)
        
        # Execute the requested action
        if action == 'status':
            self._show_status()
        elif action == 'list':
            self._list_models()
        elif action == 'install':
            self._install_model(options.get('model'), options.get('force', False))
        elif action == 'remove':
            self._remove_model(options.get('model'), options.get('force', False))
        elif action == 'test':
            self._test_model(options.get('model'))
        elif action == 'setup':
            self._setup_llama32()
        elif action == 'config':
            self._show_configuration()
    
    def _show_status(self):
        """Show Ollama server status and configuration"""
        self.stdout.write("📊 Ollama Server Status")
        self.stdout.write("-" * 30)
        
        try:
            response = requests.get(f"{self.ollama_url}/api/tags", timeout=5)
            
            if response.status_code == 200:
                self.stdout.write(self.style.SUCCESS(f"✅ Ollama server is running at {self.ollama_url}"))
                
                data = response.json()
                models = data.get('models', [])
                
                if models:
                    self.stdout.write(f"📦 {len(models)} model(s) installed:")
                    
                    for model in models:
                        name = model.get('name', 'Unknown')
                        size = model.get('size', 0)
                        modified = model.get('modified_at', 'Unknown')
                        
                        # Format size
                        size_str = self._format_size(size)
                        
                        # Check if it's a Llama 3.2 model
                        is_llama32 = name.startswith('llama3.2:')
                        icon = "🦙" if is_llama32 else "📦"
                        
                        self.stdout.write(f"  {icon} {name} ({size_str})")
                        
                        if self.verbose:
                            self.stdout.write(f"    Modified: {modified}")
                else:
                    self.stdout.write(self.style.WARNING("⚠️  No models installed"))
                    
            else:
                self.stdout.write(self.style.ERROR(f"❌ Ollama server returned status {response.status_code}"))
                
        except requests.exceptions.ConnectionError:
            self.stdout.write(self.style.ERROR(f"❌ Cannot connect to Ollama server at {self.ollama_url}"))
            self.stdout.write("💡 Make sure Ollama is installed and running:")
            self.stdout.write("   - Install: https://ollama.ai/")
            self.stdout.write("   - Start: ollama serve")
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ Error checking status: {e}"))
    
    def _list_models(self):
        """List available models with detailed information"""
        self.stdout.write("📋 Available Models")
        self.stdout.write("-" * 30)
        
        try:
            response = requests.get(f"{self.ollama_url}/api/tags", timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                models = data.get('models', [])
                
                if not models:
                    self.stdout.write(self.style.WARNING("⚠️  No models installed"))
                    self.stdout.write("💡 Install Llama 3.2b with: python manage.py manage_ollama install --model llama3.2:3b")
                    return
                
                # Separate Llama 3.2 models from others
                llama32_models = [m for m in models if m.get('name', '').startswith('llama3.2:')]
                other_models = [m for m in models if not m.get('name', '').startswith('llama3.2:')]
                
                if llama32_models:
                    self.stdout.write(self.style.SUCCESS("🦙 Llama 3.2 Models:"))
                    for model in llama32_models:
                        self._display_model_info(model, recommended=True)
                
                if other_models:
                    self.stdout.write(f"\n📦 Other Models:")
                    for model in other_models:
                        self._display_model_info(model, recommended=False)
                        
            else:
                self.stdout.write(self.style.ERROR(f"❌ Failed to get models: {response.status_code}"))
                
        except requests.exceptions.ConnectionError:
            self.stdout.write(self.style.ERROR(f"❌ Cannot connect to Ollama server"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ Error listing models: {e}"))
    
    def _install_model(self, model_name: str, force: bool = False):
        """Install a specific model"""
        if not model_name:
            self.stdout.write(self.style.ERROR("❌ Please specify a model name"))
            self.stdout.write("💡 Recommended: llama3.2:3b or llama3.2:1b")
            return
        
        self.stdout.write(f"📥 Installing {model_name}")
        self.stdout.write("-" * 30)
        
        # Check if model is already installed
        if not force and self._is_model_installed(model_name):
            self.stdout.write(self.style.WARNING(f"⚠️  {model_name} is already installed"))
            self.stdout.write("💡 Use --force to reinstall")
            return
        
        try:
            self.stdout.write(f"⏳ Downloading {model_name}...")
            self.stdout.write("   This may take several minutes depending on model size")
            
            # Use subprocess to call ollama pull
            result = subprocess.run(
                ['ollama', 'pull', model_name],
                capture_output=True,
                text=True,
                timeout=1800  # 30 minutes timeout
            )
            
            if result.returncode == 0:
                self.stdout.write(self.style.SUCCESS(f"✅ Successfully installed {model_name}"))
                
                # Test the model after installation
                self.stdout.write(f"🧪 Testing {model_name}...")
                self._test_model(model_name)
                
                # If it's a Llama 3.2 model, offer to configure it
                if model_name.startswith('llama3.2:'):
                    self.stdout.write(f"\n💡 Would you like to configure {model_name} as your default LLM?")
                    self.stdout.write(f"   Run: python manage.py manage_ollama config --model {model_name}")
                    
            else:
                self.stdout.write(self.style.ERROR(f"❌ Failed to install {model_name}"))
                self.stdout.write(f"Error: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            self.stdout.write(self.style.ERROR(f"❌ Installation timed out"))
        except FileNotFoundError:
            self.stdout.write(self.style.ERROR(f"❌ Ollama command not found"))
            self.stdout.write("💡 Please install Ollama first: https://ollama.ai/")
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ Error installing model: {e}"))
    
    def _remove_model(self, model_name: str, force: bool = False):
        """Remove a specific model"""
        if not model_name:
            self.stdout.write(self.style.ERROR("❌ Please specify a model name"))
            return
        
        self.stdout.write(f"🗑️  Removing {model_name}")
        self.stdout.write("-" * 30)
        
        # Check if model is installed
        if not self._is_model_installed(model_name):
            self.stdout.write(self.style.WARNING(f"⚠️  {model_name} is not installed"))
            return
        
        if not force:
            # Ask for confirmation
            self.stdout.write(f"⚠️  Are you sure you want to remove {model_name}? (y/N)")
            # For management command, we'll skip interactive confirmation
            self.stdout.write("💡 Use --force to skip confirmation")
            return
        
        try:
            result = subprocess.run(
                ['ollama', 'rm', model_name],
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                self.stdout.write(self.style.SUCCESS(f"✅ Successfully removed {model_name}"))
            else:
                self.stdout.write(self.style.ERROR(f"❌ Failed to remove {model_name}"))
                self.stdout.write(f"Error: {result.stderr}")
                
        except FileNotFoundError:
            self.stdout.write(self.style.ERROR(f"❌ Ollama command not found"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ Error removing model: {e}"))
    
    def _test_model(self, model_name: str):
        """Test a specific model"""
        if not model_name:
            self.stdout.write(self.style.ERROR("❌ Please specify a model name"))
            return
        
        self.stdout.write(f"🧪 Testing {model_name}")
        self.stdout.write("-" * 30)
        
        try:
            llm_service = LLMService()
            success, message = llm_service.test_ollama_connection(self.ollama_url, model_name)
            
            if success:
                self.stdout.write(self.style.SUCCESS(f"✅ {message}"))
                
                # Test SQL generation
                self.stdout.write(f"🔍 Testing SQL generation...")
                
                # Create temporary config for testing
                try:
                    if model_name.startswith('llama3.2:'):
                        config = LLMConfig.create_llama32_config(model_name)
                    else:
                        config = LLMConfig.objects.create(
                            provider='local',
                            base_url=self.ollama_url,
                            model_name=model_name,
                            temperature=0.1,
                            max_tokens=500,
                            system_prompt='You are a SQL generator. Convert natural language to SQL.',
                            is_active=True
                        )
                    
                    # Test with a simple query
                    from services.dynamic_llm_service import DynamicLLMService
                    dynamic_service = DynamicLLMService()
                    
                    test_query = "Show total sales"
                    start_time = time.time()
                    sql_success, sql_result = dynamic_service.generate_sql(test_query)
                    response_time = time.time() - start_time
                    
                    if sql_success and sql_result and sql_result.strip() != ';':
                        self.stdout.write(self.style.SUCCESS(f"✅ SQL generation successful ({response_time:.2f}s)"))
                        if self.verbose:
                            self.stdout.write(f"   Generated: {sql_result[:60]}...")
                    else:
                        self.stdout.write(self.style.WARNING(f"⚠️  SQL generation failed: {sql_result}"))
                    
                    # Cleanup
                    config.delete()
                    
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"❌ SQL generation test failed: {e}"))
                    
            else:
                self.stdout.write(self.style.ERROR(f"❌ {message}"))
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ Error testing model: {e}"))
    
    def _setup_llama32(self):
        """Setup Llama 3.2b for optimal SQL generation"""
        self.stdout.write("🚀 Setting up Llama 3.2b for SQL Generation")
        self.stdout.write("=" * 50)
        
        # Step 1: Check Ollama server
        self.stdout.write("1️⃣ Checking Ollama server...")
        
        try:
            response = requests.get(f"{self.ollama_url}/api/tags", timeout=5)
            if response.status_code == 200:
                self.stdout.write(self.style.SUCCESS("   ✅ Ollama server is running"))
            else:
                self.stdout.write(self.style.ERROR(f"   ❌ Ollama server returned {response.status_code}"))
                return
        except requests.exceptions.ConnectionError:
            self.stdout.write(self.style.ERROR("   ❌ Cannot connect to Ollama server"))
            self.stdout.write("   💡 Please start Ollama: ollama serve")
            return
        
        # Step 2: Install recommended model
        self.stdout.write("\n2️⃣ Installing Llama 3.2 3B...")
        recommended_model = 'llama3.2:3b'
        
        if self._is_model_installed(recommended_model):
            self.stdout.write(self.style.SUCCESS(f"   ✅ {recommended_model} is already installed"))
        else:
            self._install_model(recommended_model, force=False)
        
        # Step 3: Configure as default
        self.stdout.write("\n3️⃣ Configuring as default LLM...")
        
        try:
            config = LLMConfig.create_llama32_config(recommended_model)
            self.stdout.write(self.style.SUCCESS("   ✅ Llama 3.2b configured as default LLM"))
            
            # Show configuration details
            model_config = config.get_model_config()
            self.stdout.write(f"   📋 Configuration:")
            self.stdout.write(f"      Model: {config.model_name}")
            self.stdout.write(f"      Display Name: {model_config.get('display_name', 'N/A')}")
            self.stdout.write(f"      Temperature: {config.temperature}")
            self.stdout.write(f"      Max Tokens: {config.max_tokens}")
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"   ❌ Configuration failed: {e}"))
            return
        
        # Step 4: Test the setup
        self.stdout.write("\n4️⃣ Testing setup...")
        self._test_model(recommended_model)
        
        # Step 5: Final recommendations
        self.stdout.write("\n" + "=" * 50)
        self.stdout.write(self.style.SUCCESS("🎉 Llama 3.2b Setup Complete!"))
        self.stdout.write("\n💡 Next steps:")
        self.stdout.write("   • Test with: python manage.py test_llm_sql_generation --provider ollama")
        self.stdout.write("   • Access via web UI: Admin → LLM Configuration")
        self.stdout.write("   • Switch models: python manage.py manage_ollama config --model llama3.2:1b")
        
        # Performance tips
        self.stdout.write("\n⚡ Performance Tips:")
        self.stdout.write("   • llama3.2:3b: Better accuracy, more memory")
        self.stdout.write("   • llama3.2:1b: Faster responses, less memory")
        self.stdout.write("   • Monitor system resources during usage")
    
    def _show_configuration(self):
        """Show current LLM configuration"""
        self.stdout.write("⚙️  Current LLM Configuration")
        self.stdout.write("-" * 30)
        
        try:
            config = LLMConfig.get_active_config()
            
            if config:
                self.stdout.write(f"Provider: {config.provider}")
                self.stdout.write(f"Model: {config.model_name}")
                
                if config.provider == 'local':
                    self.stdout.write(f"Ollama URL: {config.base_url}")
                    self.stdout.write(f"Is Llama 3.2: {config.is_llama32_model()}")
                    
                    if config.is_llama32_model():
                        model_config = config.get_model_config()
                        self.stdout.write(f"Display Name: {model_config.get('display_name', 'N/A')}")
                
                self.stdout.write(f"Temperature: {config.temperature}")
                self.stdout.write(f"Max Tokens: {config.max_tokens}")
                self.stdout.write(f"Last Updated: {config.updated_at}")
                
            else:
                self.stdout.write(self.style.WARNING("⚠️  No active LLM configuration found"))
                self.stdout.write("💡 Set up with: python manage.py manage_ollama setup")
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ Error getting configuration: {e}"))
    
    def _is_model_installed(self, model_name: str) -> bool:
        """Check if a model is installed"""
        try:
            response = requests.get(f"{self.ollama_url}/api/tags", timeout=5)
            if response.status_code == 200:
                data = response.json()
                models = data.get('models', [])
                return any(model.get('name') == model_name for model in models)
        except:
            pass
        return False
    
    def _display_model_info(self, model: Dict[str, Any], recommended: bool = False):
        """Display detailed model information"""
        name = model.get('name', 'Unknown')
        size = model.get('size', 0)
        modified = model.get('modified_at', 'Unknown')
        
        size_str = self._format_size(size)
        
        # Choose icon based on model type
        if name.startswith('llama3.2:'):
            icon = "🦙" if recommended else "🦙"
            status = " (RECOMMENDED)" if recommended else ""
        else:
            icon = "📦"
            status = ""
        
        self.stdout.write(f"  {icon} {name}{status}")
        self.stdout.write(f"     Size: {size_str}")
        
        if self.verbose:
            self.stdout.write(f"     Modified: {modified}")
    
    def _format_size(self, size_bytes: int) -> str:
        """Format size in human readable format"""
        if size_bytes == 0:
            return "Unknown"
        
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f}{unit}"
            size_bytes /= 1024.0
        
        return f"{size_bytes:.1f}PB" 