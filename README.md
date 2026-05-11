# E-Commerce Data Pipeline with PySpark

A comprehensive e-commerce data pipeline project that generates fake customer, product, and order data, then analyzes it using PySpark to derive business insights.

## Project Structure

```
genai-pyspark-pipeline/
├── README.md
├── requirements.txt
├── .gitignore
├── src/
│   ├── __init__.py
│   ├── config.py
│   ├── data_generator.py
│   └── spark_analytics.py
├── data/
│   ├── raw/              # Generated fake data
│   └── processed/        # Analyzed results
├── tests/                # Test scripts
└── notebooks/            # Jupyter notebooks
```

## Features

- **Data Generation**: Create realistic fake customer, product, and order data
- **PySpark Analytics**: Analyze data to find business insights
- **Type Hints**: Full type annotations for all functions
- **Logging**: Comprehensive logging for debugging
- **Tests**: Unit tests for all components

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run data generation:
```bash
python -m src.data_generator
```

3. Run Spark analytics:
```bash
python -m src.spark_analytics
```

## Usage

The project generates fake e-commerce data and performs various analyses including:
- Customer segmentation
- Product performance analysis
- Order trends and patterns
- Revenue analysis

## Technologies Used

- Python 3.8+
- Apache Spark (PySpark)
- Faker (for fake data generation)
- Pandas
- NumPy
