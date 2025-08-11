#!/usr/bin/env python3
"""
E-commerce Data Warehouse Setup Script

This script automates the entire project setup process including:
- Environment setup and dependency installation
- Database initialization and configuration
- Sample data generation with realistic business patterns
- dbt project initialization and testing
- Documentation generation
"""

import os
import sys
import subprocess
import json
import time
from pathlib import Path
from typing import Dict, List, Optional

class EcommerceDataWarehouseSetup:
    """
    Automated setup and initialization for the e-commerce data warehouse project.
    """
    
    def __init__(self):
        self.project_root = Path.cwd()
        self.dbt_project_dir = self.project_root / "dbt_project"
        self.data_dir = self.project_root / "data"
        self.scripts_dir = self.project_root / "scripts"
        
        # Setup configuration
        self.setup_config = {
            "python_version": "3.8+",
            "postgres_version": "15",
            "dbt_version": "1.6+",
            "sample_data_config": {
                "customers": 10000,
                "products": 5000,
                "orders": 25000,
                "sessions": 50000
            }
        }
        
        print("🏗️  E-commerce Data Warehouse Setup")
        print("=" * 50)
        
    def check_prerequisites(self) -> bool:
        """Check if all required tools are installed."""
        print("🔍 Checking prerequisites...")
        
        requirements = {
            "python": ["python", "--version"],
            "docker": ["docker", "--version"],
            "docker-compose": ["docker-compose", "--version"],
            "git": ["git", "--version"]
        }
        
        missing_tools = []
        
        for tool, command in requirements.items():
            try:
                result = subprocess.run(command, capture_output=True, text=True)
                if result.returncode == 0:
                    version = result.stdout.strip().split('\n')[0]
                    print(f"   ✅ {tool}: {version}")
                else:
                    missing_tools.append(tool)
            except FileNotFoundError:
                missing_tools.append(tool)
                print(f"   ❌ {tool}: Not found")
        
        if missing_tools:
            print(f"\n❌ Missing required tools: {', '.join(missing_tools)}")
            print("Please install the missing tools and run setup again.")
            return False
        
        print("✅ All prerequisites satisfied!")
        return True
    
    def create_directory_structure(self) -> None:
        """Create the project directory structure."""
        print("\n📁 Creating project directory structure...")
        
        directories = [
            "data/raw",
            "data/processed", 
            "data/seeds",
            "dbt_project/models/staging",
            "dbt_project/models/intermediate",
            "dbt_project/models/marts/core",
            "dbt_project/models/marts/analytics",
            "dbt_project/models/utilities",
            "dbt_project/macros",
            "dbt_project/tests",
            "dbt_project/snapshots",
            "dbt_project/analyses",
            "scripts",
            "logs",
            ".github/workflows",
            "docs"
        ]
        
        for directory in directories:
            dir_path = self.project_root / directory
            dir_path.mkdir(parents=True, exist_ok=True)
            print(f"   ✅ Created: {directory}")
    
    def setup_python_environment(self) -> bool:
        """Set up Python virtual environment and install dependencies."""
        print("\n🐍 Setting up Python environment...")
        
        # Check if virtual environment exists
        venv_path = self.project_root / "venv"
        
        if not venv_path.exists():
            print("   Creating virtual environment...")
            result = subprocess.run([sys.executable, "-m", "venv", "venv"], 
                                  cwd=self.project_root)
            if result.returncode != 0:
                print("   ❌ Failed to create virtual environment")
                return False
        
        # Determine pip executable path
        if os.name == 'nt':  # Windows
            pip_exe = venv_path / "Scripts" / "pip"
        else:  # Unix/Linux/Mac
            pip_exe = venv_path / "bin" / "pip"
        
        # Upgrade pip
        print("   Upgrading pip...")
        subprocess.run([str(pip_exe), "install", "--upgrade", "pip"], 
                      capture_output=True)
        
        # Install requirements
        requirements_file = self.project_root / "requirements.txt"
        if requirements_file.exists():
            print("   Installing Python dependencies...")
            result = subprocess.run([str(pip_exe), "install", "-r", "requirements.txt"],
                                  cwd=self.project_root)
            if result.returncode != 0:
                print("   ❌ Failed to install dependencies")
                return False
        else:
            print("   ⚠️  requirements.txt not found, skipping dependency installation")
        
        print("   ✅ Python environment setup complete!")
        return True
    
    def start_database_services(self) -> bool:
        """Start PostgreSQL and related services using Docker Compose."""
        print("\n🐳 Starting database services...")
        
        docker_compose_file = self.project_root / "docker-compose.yml"
        if not docker_compose_file.exists():
            print("   ❌ docker-compose.yml not found")
            return False
        
        # Start services
        print("   Starting PostgreSQL container...")
        result = subprocess.run(["docker-compose", "up", "-d", "postgres"],
                              cwd=self.project_root)
        
        if result.returncode != 0:
            print("   ❌ Failed to start database services")
            return False
        
        # Wait for database to be ready
        print("   Waiting for database to be ready...")
        max_attempts = 30
        for attempt in range(max_attempts):
            try:
                result = subprocess.run([
                    "docker-compose", "exec", "-T", "postgres",
                    "pg_isready", "-U", "dbt_user", "-d", "ecommerce_dw"
                ], capture_output=True, cwd=self.project_root)
                
                if result.returncode == 0:
                    print("   ✅ Database is ready!")
                    return True
                
            except subprocess.CalledProcessError:
                pass
            
            time.sleep(2)
            print(f"   Waiting... (attempt {attempt + 1}/{max_attempts})")
        
        print("   ❌ Database failed to start within timeout")
        return False
    
    def generate_sample_data(self) -> bool:
        """Generate realistic sample data for the data warehouse."""
        print("\n📊 Generating sample data...")
        
        # Check if data generator script exists
        generator_script = self.scripts_dir / "generate_sample_data.py"
        if not generator_script.exists():
            print("   ⚠️  Data generator script not found, skipping data generation")
            return True
        
        # Import and run the data generator
        try:
            # Add scripts directory to Python path
            sys.path.insert(0, str(self.scripts_dir))
            
            # Import the generator
            from generate_sample_data import EcommerceDataGenerator
            
            # Initialize generator with configuration
            config = self.setup_config["sample_data_config"]
            generator = EcommerceDataGenerator(
                base_date='2023-01-01',
                num_customers=config["customers"]
            )
            
            # Generate complete dataset
            print("   Generating comprehensive e-commerce dataset...")
            generator.generate_complete_dataset(
                num_customers=config["customers"],
                num_products=config["products"],
                num_orders=config["orders"],
                num_sessions=config["sessions"]
            )
            
            # Save all data to CSV files
            data_output_dir = self.data_dir / "raw"
            generator.save_all_data(str(data_output_dir))
            
            # Generate data quality report
            quality_report = generator.get_data_quality_report()
            quality_report_path = self.data_dir / "data_quality_report.json"
            
            with open(quality_report_path, 'w') as f:
                json.dump(quality_report, f, indent=2, default=str)
            
            print("   ✅ Sample data generation complete!")
            return True
            
        except Exception as e:
            print(f"   ❌ Failed to generate sample data: {str(e)}")
            return False
    
    def load_data_to_database(self) -> bool:
        """Load generated CSV data into PostgreSQL."""
        print("\n💾 Loading data into PostgreSQL...")
        
        try:
            import pandas as pd
            import psycopg2
            from sqlalchemy import create_engine
            
            # Database connection
            connection_string = "postgresql://dbt_user:dbt_password@localhost:5432/ecommerce_dw"
            engine = create_engine(connection_string)
            
            # Data files to load
            data_files = {
                'customers': 'customers.csv',
                'products': 'products.csv',
                'orders': 'orders.csv',
                'order_items': 'order_items.csv',
                'web_sessions': 'web_sessions.csv',
                'marketing_spend': 'marketing_spend.csv'
            }
            
            data_dir = self.data_dir / "raw"
            
            for table_name, filename in data_files.items():
                file_path = data_dir / filename
                
                if file_path.exists():
                    print(f"   Loading {filename} to {table_name}...")
                    df = pd.read_csv(file_path)
                    
                    # Handle datetime columns
                    datetime_columns = ['created_at', 'updated_at', 'order_date', 'session_date', 
                                      'spend_date', 'birth_date', 'last_order_date']
                    
                    for col in datetime_columns:
                        if col in df.columns:
                            df[col] = pd.to_datetime(df[col], errors='coerce')
                    
                    # Load data to database
                    df.to_sql(table_name, engine, if_exists='replace', index=False, 
                             method='multi', chunksize=1000)
                    print(f"   ✅ Loaded {len(df):,} records to {table_name}")
                else:
                    print(f"   ⚠️  File not found: {filename}")
            
            # Create indexes for better performance
            print("   Creating database indexes...")
            with engine.connect() as conn:
                index_queries = [
                    "CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);",
                    "CREATE INDEX IF NOT EXISTS idx_orders_order_date ON orders(order_date);",
                    "CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);",
                    "CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id);",
                    "CREATE INDEX IF NOT EXISTS idx_web_sessions_customer_id ON web_sessions(customer_id);",
                    "CREATE INDEX IF NOT EXISTS idx_web_sessions_date ON web_sessions(session_date);",
                    "CREATE INDEX IF NOT EXISTS idx_marketing_spend_date ON marketing_spend(spend_date);"
                ]
                
                for query in index_queries:
                    try:
                        conn.execute(query)
                        conn.commit()
                    except Exception as e:
                        print(f"   ⚠️  Index creation warning: {e}")
            
            print("   ✅ Data loading complete!")
            return True
            
        except Exception as e:
            print(f"   ❌ Failed to load data: {str(e)}")
            print(f"   Make sure PostgreSQL is running and accessible")
            return False
    
    def create_profiles_yml(self) -> bool:
        """Create dbt profiles.yml in the correct location."""
        print("   Creating dbt profiles configuration...")
        
        profiles_content = """ecommerce_dw:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: dbt_user
      password: dbt_password
      port: 5432
      dbname: ecommerce_dw
      schema: dbt_dev
      threads: 4
      keepalives_idle: 0
      search_path: "dbt_dev,public"
    
    ci:
      type: postgres
      host: localhost
      user: dbt_user
      password: dbt_password
      port: 5432
      dbname: ecommerce_dw
      schema: dbt_ci
      threads: 2
      keepalives_idle: 0
"""
        
        # Create profiles directory in user home
        profiles_dir = Path.home() / ".dbt"
        profiles_dir.mkdir(exist_ok=True)
        
        profiles_file = profiles_dir / "profiles.yml"
        
        # Check if profiles.yml already exists
        if profiles_file.exists():
            print("   ⚠️  profiles.yml already exists, backing up...")
            backup_file = profiles_dir / "profiles.yml.backup"
            profiles_file.rename(backup_file)
        
        with open(profiles_file, 'w') as f:
            f.write(profiles_content)
        
        print(f"   ✅ Created dbt profiles at {profiles_file}")
        return True

    def validate_setup(self) -> bool:
        """Validate that the setup completed successfully."""
        print("\n🔍 Validating setup completion...")
        
        validation_checks = []
        
        # Check if data files exist
        data_dir = self.data_dir / "raw"
        expected_files = ['customers.csv', 'products.csv', 'orders.csv', 
                         'order_items.csv', 'web_sessions.csv', 'marketing_spend.csv']
        
        for filename in expected_files:
            file_path = data_dir / filename
            if file_path.exists():
                validation_checks.append(f"✅ Data file: {filename}")
            else:
                validation_checks.append(f"❌ Missing data file: {filename}")
        
        # Check database connection
        try:
            import psycopg2
            conn = psycopg2.connect(
                host="localhost",
                database="ecommerce_dw",
                user="dbt_user",
                password="dbt_password",
                port="5432"
            )
            conn.close()
            validation_checks.append("✅ Database connection successful")
        except Exception as e:
            validation_checks.append(f"❌ Database connection failed: {e}")
        
        # Check dbt project structure
        dbt_files = ['dbt_project.yml', 'models/staging/_sources.yml']
        for dbt_file in dbt_files:
            file_path = self.dbt_project_dir / dbt_file
            if file_path.exists():
                validation_checks.append(f"✅ dbt file: {dbt_file}")
            else:
                validation_checks.append(f"❌ Missing dbt file: {dbt_file}")
        
        # Print validation results
        print("\n   Validation Results:")
        for check in validation_checks:
            print(f"   {check}")
        
        # Count successful checks
        successful_checks = len([c for c in validation_checks if c.startswith("✅")])
        total_checks = len(validation_checks)
        
        success_rate = successful_checks / total_checks
        print(f"\n   Setup Success Rate: {successful_checks}/{total_checks} ({success_rate*100:.1f}%)")
        
    def setup_dbt_project(self) -> bool:
        """Initialize and configure the dbt project."""
        print("\n🔧 Setting up dbt project...")
        
        if not self.dbt_project_dir.exists():
            print("   ❌ dbt project directory not found")
            return False
        
        # Create dbt profiles
        if not self.create_profiles_yml():
            print("   ❌ Failed to create dbt profiles")
            return False
        
        # Install dbt dependencies
        print("   Installing dbt packages...")
        result = subprocess.run(["dbt", "deps"], cwd=self.dbt_project_dir, 
                              capture_output=True, text=True)
        if result.returncode != 0:
            print("   ❌ Failed to install dbt dependencies")
            print(f"   Error: {result.stderr}")
            return False
        
        # Test database connection
        print("   Testing database connection...")
        result = subprocess.run(["dbt", "debug"], cwd=self.dbt_project_dir,
                              capture_output=True, text=True)
        if result.returncode != 0:
            print("   ❌ dbt connection test failed")
            print(f"   Error: {result.stderr}")
            return False
        
        # Load seed data (if any)
        print("   Loading seed data...")
        result = subprocess.run(["dbt", "seed"], cwd=self.dbt_project_dir,
                              capture_output=True, text=True)
        if result.returncode != 0:
            print("   ⚠️  dbt seed had issues, but continuing...")
        
        # Run staging models first
        print("   Running staging models...")
        result = subprocess.run(["dbt", "run", "--select", "staging"],
                              cwd=self.dbt_project_dir, capture_output=True, text=True)
        if result.returncode != 0:
            print("   ❌ Failed to run staging models")
            print(f"   Error: {result.stderr}")
            return False
        
        # Run all models
        print("   Running complete dbt transformation...")
        result = subprocess.run(["dbt", "run"], cwd=self.dbt_project_dir,
                              capture_output=True, text=True)
        if result.returncode != 0:
            print("   ❌ Failed to run dbt models")
            print(f"   Error: {result.stderr}")
            return False
        
        # Run tests
        print("   Running data quality tests...")
        result = subprocess.run(["dbt", "test"], cwd=self.dbt_project_dir,
                              capture_output=True, text=True)
        if result.returncode != 0:
            print("   ⚠️  Some tests failed, but continuing setup...")
            print("   Review test failures after setup completion")
        
        print("   ✅ dbt project setup complete!")
        return True
        """Initialize and configure the dbt project."""
        print("\n🔧 Setting up dbt project...")
        
        if not self.dbt_project_dir.exists():
            print("   ❌ dbt project directory not found")
            return False
        
        # Install dbt dependencies
        print("   Installing dbt packages...")
        result = subprocess.run(["dbt", "deps"], cwd=self.dbt_project_dir)
        if result.returncode != 0:
            print("   ❌ Failed to install dbt dependencies")
            return False
        
        # Test database connection
        print("   Testing database connection...")
        result = subprocess.run(["dbt", "debug"], cwd=self.dbt_project_dir,
                              capture_output=True, text=True)
        if result.returncode != 0:
            print("   ❌ dbt connection test failed")
            print(f"   Error: {result.stderr}")
            return False
        
        # Load seed data (if any)
        print("   Loading seed data...")
        subprocess.run(["dbt", "seed"], cwd=self.dbt_project_dir)
        
        # Run staging models first
        print("   Running staging models...")
        result = subprocess.run(["dbt", "run", "--select", "staging"],
                              cwd=self.dbt_project_dir)
        if result.returncode != 0:
            print("   ❌ Failed to run staging models")
            return False
        
        # Run all models
        print("   Running complete dbt transformation...")
        result = subprocess.run(["dbt", "run"], cwd=self.dbt_project_dir)
        if result.returncode != 0:
            print("   ❌ Failed to run dbt models")
            return False
        
        # Run tests
        print("   Running data quality tests...")
        result = subprocess.run(["dbt", "test"], cwd=self.dbt_project_dir)
        if result.returncode != 0:
            print("   ⚠️  Some tests failed, but continuing setup...")
        
        print("   ✅ dbt project setup complete!")
        return True
    
    def generate_documentation(self) -> bool:
        """Generate dbt documentation and lineage."""
        print("\n📚 Generating project documentation...")
        
        try:
            # Generate dbt docs
            print("   Generating dbt documentation...")
            result = subprocess.run(["dbt", "docs", "generate"],
                                  cwd=self.dbt_project_dir)
            
            if result.returncode != 0:
                print("   ❌ Failed to generate dbt documentation")
                return False
            
            print("   ✅ Documentation generated!")
            print("   📖 To view docs, run: cd dbt_project && dbt docs serve")
            return True
            
        except Exception as e:
            print(f"   ❌ Documentation generation failed: {str(e)}")
            return False
    
    def create_env_file(self) -> None:
        """Create environment file with default configurations."""
        print("\n🔧 Creating environment configuration...")
        
        env_content = """# E-commerce Data Warehouse Environment Configuration
# Database Configuration
DBT_USER=dbt_user
DBT_PASSWORD=dbt_password
POSTGRES_DB=ecommerce_dw

# PostgreSQL Configuration for Docker
POSTGRES_USER=dbt_user
POSTGRES_PASSWORD=dbt_password

# Optional: Production database configurations
# STAGING_HOST=your-staging-host
# STAGING_USER=your-staging-user
# STAGING_PASSWORD=your-staging-password

# PROD_HOST=your-production-host
# PROD_USER=your-production-user
# PROD_PASSWORD=your-production-password
"""
        
        env_file = self.project_root / ".env"
        with open(env_file, 'w') as f:
            f.write(env_content)
        
        print("   ✅ Environment file created (.env)")
    
    def print_success_message(self) -> None:
        """Print setup completion message with next steps."""
        print("\n" + "=" * 60)
        print("🎉 SETUP COMPLETE!")
        print("=" * 60)
        
        print("\n📊 Your e-commerce data warehouse is ready!")
        print(f"   • Generated {self.setup_config['sample_data_config']['customers']:,} customers")
        print(f"   • Generated {self.setup_config['sample_data_config']['products']:,} products")
        print(f"   • Generated {self.setup_config['sample_data_config']['orders']:,} orders")
        print(f"   • Generated {self.setup_config['sample_data_config']['sessions']:,} web sessions")
        
        print("\n🔗 Access Points:")
        print("   • Database: localhost:5432 (ecommerce_dw)")
        print("   • pgAdmin: http://localhost:8080")
        print("   • Metabase: http://localhost:3000")
        
        print("\n📚 Next Steps:")
        print("   1. View dbt documentation:")
        print("      cd dbt_project && dbt docs serve")
        print("   2. Explore the data:")
        print("      docker-compose up -d metabase")
        print("   3. Run additional analyses:")
        print("      cd dbt_project && dbt run --select analytics")
        print("   4. Set up CI/CD:")
        print("      Configure GitHub Actions with your repository")
        
        print("\n📖 Documentation:")
        print("   • README.md - Complete project documentation")
        print("   • data/data_quality_report.json - Data validation results")
        print("   • dbt_project/ - All transformation logic and tests")
        
        print("\n🚀 Happy analyzing! Your data warehouse is production-ready.")
    
    def run_setup(self) -> bool:
        """Execute the complete setup process."""
        setup_steps = [
            ("Checking prerequisites", self.check_prerequisites),
            ("Creating directories", lambda: (self.create_directory_structure(), True)[1]),
            ("Setting up Python environment", self.setup_python_environment),
            ("Creating environment config", lambda: (self.create_env_file(), True)[1]),
            ("Starting database services", self.start_database_services),
            ("Generating sample data", self.generate_sample_data),
            ("Loading data to database", self.load_data_to_database),
            ("Setting up dbt project", self.setup_dbt_project),
            ("Generating documentation", self.generate_documentation),
            ("Validating setup", self.validate_setup),
        ]
        
        for step_name, step_function in setup_steps:
            print(f"\n⏳ {step_name}...")
            try:
                success = step_function()
                if not success:
                    print(f"❌ Setup failed at step: {step_name}")
                    print("   Please check the error messages above and try again.")
                    return False
            except Exception as e:
                print(f"❌ Setup failed at step: {step_name}")
                print(f"   Error: {str(e)}")
                print("   Please check your environment and try again.")
                return False
        
        self.print_success_message()
        return True

    def quick_setup(self) -> bool:
        """Run a quick setup with minimal data for testing."""
        print("🚀 Running Quick Setup (minimal data for testing)...")
        
        # Override configuration for quick setup
        self.setup_config["sample_data_config"] = {
            "customers": 1000,
            "products": 500, 
            "orders": 2000,
            "sessions": 5000
        }
        
        return self.run_setup()

    def production_setup(self) -> bool:
        """Run full production setup with large dataset."""
        print("🏭 Running Production Setup (large dataset)...")
        
        # Override configuration for production
        self.setup_config["sample_data_config"] = {
            "customers": 50000,
            "products": 10000,
            "orders": 200000, 
            "sessions": 500000
        }
        
        return self.run_setup()


def main():
    """Main setup function with command line options."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="E-commerce Data Warehouse Setup Script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python setup.py                    # Standard setup
  python setup.py --quick           # Quick setup with minimal data
  python setup.py --production      # Production setup with large dataset
  python setup.py --validate-only   # Only validate existing setup
        """
    )
    
    parser.add_argument(
        '--quick', 
        action='store_true',
        help='Run quick setup with minimal data for testing'
    )
    
    parser.add_argument(
        '--production',
        action='store_true', 
        help='Run production setup with large dataset'
    )
    
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Only validate existing setup without running full setup'
    )
    
    parser.add_argument(
        '--skip-data-generation',
        action='store_true',
        help='Skip data generation step (use existing data)'
    )
    
    args = parser.parse_args()
    
    setup = EcommerceDataWarehouseSetup()
    
    try:
        if args.validate_only:
            print("🔍 Validating existing setup...")
            success = setup.validate_setup()
        elif args.quick:
            success = setup.quick_setup()
        elif args.production:
            success = setup.production_setup()
        else:
            success = setup.run_setup()
        
        if success:
            print("\n🎉 Setup completed successfully!")
            sys.exit(0)
        else:
            print("\n❌ Setup failed. Please check error messages above.")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Setup interrupted by user")
        print("You can resume setup by running the script again.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error during setup: {str(e)}")
        print("Please report this issue with the full error trace.")
        sys.exit(1)


if __name__ == "__main__":
    main()