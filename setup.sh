#!/bin/bash

echo "Starting E-Commerce ETL Pipeline setup..."

# Check Python
python3 --version || { echo "Python 3 is not installed"; exit 1; }

# Create virtual environment
echo "Creating virtual environment..."
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install dependencies
if [ -f requirements.txt ]; then
    pip install -r requirements.txt
else
    echo "requirements.txt not found"
    exit 1
fi

echo "Environment setup completed successfully!"
