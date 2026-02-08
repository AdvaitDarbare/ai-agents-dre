"""
Project Setup Script

This script sets up the entire project directory structure for the
Agentic Data Quality application.

Run this once to scaffold the project:
    python setup_project.py
"""

import os
from pathlib import Path


def create_directory_structure():
    """Create the complete project directory structure."""
    
    print("=" * 70)
    print("🔧 PROJECT SETUP - Agentic Data Quality")
    print("=" * 70)
    
    # Define directory structure
    directories = [
        "config/expectations",
        "data/landing",
        "data/system",
        "src/tools",
        "tests",
        "examples",
        "schemas",
        "sample_data"
    ]
    
    # Create directories
    print("\n📁 Creating directory structure...")
    for directory in directories:
        path = Path(directory)
        path.mkdir(parents=True, exist_ok=True)
        print(f"   ✓ {directory}")
    
    # Create __init__.py files for Python packages
    print("\n🐍 Creating Python package files...")
    init_files = [
        "src/__init__.py",
        "src/tools/__init__.py",
        "tests/__init__.py"
    ]
    
    for init_file in init_files:
        path = Path(init_file)
        if not path.exists():
            path.touch()
            print(f"   ✓ {init_file}")
        else:
            print(f"   ⏭️  {init_file} (already exists)")
    
    # Create .gitignore if it doesn't exist
    gitignore_path = Path(".gitignore")
    if not gitignore_path.exists():
        print("\n📝 Creating .gitignore...")
        gitignore_content = """# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
venv/
.venv/
ENV/
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# DuckDB
*.db
*.duckdb
*.sqlite
data/*.db
data/*.duckdb
data/*.sqlite

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# Testing
.pytest_cache/
.coverage
htmlcov/

# OS
.DS_Store
Thumbs.db

# Temporary files
*.tmp
*.temp
temp/
tmp/

# Config (if contains sensitive data)
# config/secrets.yaml
"""
        gitignore_path.write_text(gitignore_content)
        print("   ✓ .gitignore")
    else:
        print("\n⏭️  .gitignore (already exists)")
    
    # Create README if it doesn't exist
    readme_path = Path("README.md")
    if not readme_path.exists():
        print("\n📄 Creating README.md...")
        readme_content = """# Agentic Data Quality Application

A deterministic data quality monitoring application that acts as a preloader check 
before data enters Apache Doris Data Warehouse.

## Features

- **Schema Validation**: Validates data against ODCS v3.1.0 contracts
- **DuckDB Integration**: Fast, efficient data introspection
- **Formatted Reports**: Clear, emoji-based validation reports
- **Type Compatibility**: Intelligent type matching and compatibility checks
- **Schema Drift Detection**: Warns about unexpected columns

## Project Structure

```
my_project/
├── config/
│   └── expectations/          # ODCS contracts
├── data/
│   ├── landing/              # Input data files
│   └── system/               # DuckDB files
├── src/
│   └── tools/                # Core validation tools
├── tests/                    # Test suite
├── examples/                 # Usage examples
└── schemas/                  # Schema definitions
```

## Quick Start

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Generate test data:
   ```bash
   python tests/generate_chaos.py
   ```

3. Run validation tests:
   ```bash
   pytest tests/
   ```

## Documentation

- `SCHEMA_GUIDE.md` - Schema definition guide
- `ODCS_INTEGRATION.md` - ODCS v3.1.0 integration guide

## License

Proprietary
"""
        readme_path.write_text(readme_content)
        print("   ✓ README.md")
    else:
        print("\n⏭️  README.md (already exists)")
    
    print("\n" + "=" * 70)
    print("✅ PROJECT SETUP COMPLETE!")
    print("=" * 70)
    
    print("\n📋 Next Steps:")
    print("   1. Install dependencies: pip install -r requirements.txt")
    print("   2. Generate test data: python tests/generate_chaos.py")
    print("   3. Run tests: pytest tests/")
    print("   4. Review documentation: SCHEMA_GUIDE.md, ODCS_INTEGRATION.md")
    
    print("\n📁 Directory Structure:")
    os.system("tree -L 2 -I '__pycache__|*.pyc' . 2>/dev/null || find . -maxdepth 2 -not -path '*/\.*' -not -path '*/__pycache__*' | head -30")


if __name__ == '__main__':
    create_directory_structure()
