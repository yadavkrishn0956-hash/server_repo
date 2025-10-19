import pandas as pd
import numpy as np
from typing import Dict, List, Any, Tuple
from datetime import datetime, timedelta
import random
import string

class CategoryTemplates:
    """Predefined schemas and generators for different data categories"""
    
    @staticmethod
    def get_medical_template(rows: int, columns: int) -> pd.DataFrame:
        """Generate medical dataset with realistic columns"""
        base_columns = [
            'patient_id', 'age', 'gender', 'blood_pressure_systolic', 
            'blood_pressure_diastolic', 'heart_rate', 'temperature', 
            'diagnosis', 'treatment', 'admission_date'
        ]
        
        # Adjust columns to match requested count
        columns_to_use = base_columns[:min(columns, len(base_columns))]
        if columns > len(base_columns):
            columns_to_use.extend([f'custom_field_{i}' for i in range(columns - len(base_columns))])
        
        data = {}
        
        for col in columns_to_use:
            if col == 'patient_id':
                data[col] = [f'P{str(i).zfill(6)}' for i in range(1, rows + 1)]
            elif col == 'age':
                data[col] = np.random.randint(18, 90, rows)
            elif col == 'gender':
                data[col] = np.random.choice(['Male', 'Female', 'Other'], rows, p=[0.48, 0.48, 0.04])
            elif col == 'blood_pressure_systolic':
                data[col] = np.random.normal(120, 15, rows).astype(int)
            elif col == 'blood_pressure_diastolic':
                data[col] = np.random.normal(80, 10, rows).astype(int)
            elif col == 'heart_rate':
                data[col] = np.random.normal(72, 12, rows).astype(int)
            elif col == 'temperature':
                data[col] = np.round(np.random.normal(98.6, 1.5, rows), 1)
            elif col == 'diagnosis':
                diagnoses = ['Hypertension', 'Diabetes', 'Asthma', 'Arthritis', 'Migraine', 'Healthy']
                data[col] = np.random.choice(diagnoses, rows)
            elif col == 'treatment':
                treatments = ['Medication', 'Surgery', 'Therapy', 'Observation', 'Lifestyle Change']
                data[col] = np.random.choice(treatments, rows)
            elif col == 'admission_date':
                start_date = datetime.now() - timedelta(days=365)
                data[col] = [start_date + timedelta(days=random.randint(0, 365)) for _ in range(rows)]
            else:
                # Custom fields with random numeric data
                data[col] = np.random.normal(50, 15, rows)
        
        return pd.DataFrame(data)
    
    @staticmethod
    def get_finance_template(rows: int, columns: int) -> pd.DataFrame:
        """Generate financial dataset with realistic columns"""
        base_columns = [
            'transaction_id', 'account_id', 'amount', 'transaction_type',
            'merchant', 'category', 'date', 'balance', 'credit_score', 'risk_level'
        ]
        
        columns_to_use = base_columns[:min(columns, len(base_columns))]
        if columns > len(base_columns):
            columns_to_use.extend([f'financial_metric_{i}' for i in range(columns - len(base_columns))])
        
        data = {}
        
        for col in columns_to_use:
            if col == 'transaction_id':
                data[col] = [f'TXN{str(i).zfill(8)}' for i in range(1, rows + 1)]
            elif col == 'account_id':
                data[col] = [f'ACC{str(random.randint(100000, 999999))}' for _ in range(rows)]
            elif col == 'amount':
                data[col] = np.round(np.random.lognormal(3, 1.5, rows), 2)
            elif col == 'transaction_type':
                types = ['Purchase', 'Transfer', 'Deposit', 'Withdrawal', 'Payment']
                data[col] = np.random.choice(types, rows)
            elif col == 'merchant':
                merchants = ['Amazon', 'Walmart', 'Starbucks', 'Shell', 'Target', 'McDonald\'s']
                data[col] = np.random.choice(merchants, rows)
            elif col == 'category':
                categories = ['Food', 'Gas', 'Shopping', 'Entertainment', 'Bills', 'Healthcare']
                data[col] = np.random.choice(categories, rows)
            elif col == 'date':
                start_date = datetime.now() - timedelta(days=90)
                data[col] = [start_date + timedelta(days=random.randint(0, 90)) for _ in range(rows)]
            elif col == 'balance':
                data[col] = np.round(np.random.normal(5000, 2000, rows), 2)
            elif col == 'credit_score':
                data[col] = np.random.randint(300, 850, rows)
            elif col == 'risk_level':
                data[col] = np.random.choice(['Low', 'Medium', 'High'], rows, p=[0.6, 0.3, 0.1])
            else:
                data[col] = np.random.normal(100, 25, rows)
        
        return pd.DataFrame(data)
    
    @staticmethod
    def get_business_template(rows: int, columns: int) -> pd.DataFrame:
        """Generate business dataset with realistic columns"""
        base_columns = [
            'employee_id', 'department', 'position', 'salary', 'hire_date',
            'performance_score', 'projects_completed', 'training_hours', 'location', 'manager_id'
        ]
        
        columns_to_use = base_columns[:min(columns, len(base_columns))]
        if columns > len(base_columns):
            columns_to_use.extend([f'business_metric_{i}' for i in range(columns - len(base_columns))])
        
        data = {}
        
        for col in columns_to_use:
            if col == 'employee_id':
                data[col] = [f'EMP{str(i).zfill(5)}' for i in range(1, rows + 1)]
            elif col == 'department':
                departments = ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance', 'Operations']
                data[col] = np.random.choice(departments, rows)
            elif col == 'position':
                positions = ['Manager', 'Senior', 'Junior', 'Lead', 'Associate', 'Director']
                data[col] = np.random.choice(positions, rows)
            elif col == 'salary':
                data[col] = np.random.normal(75000, 25000, rows).astype(int)
            elif col == 'hire_date':
                start_date = datetime.now() - timedelta(days=2000)
                data[col] = [start_date + timedelta(days=random.randint(0, 2000)) for _ in range(rows)]
            elif col == 'performance_score':
                data[col] = np.round(np.random.normal(3.5, 0.8, rows), 1)
            elif col == 'projects_completed':
                data[col] = np.random.poisson(5, rows)
            elif col == 'training_hours':
                data[col] = np.random.randint(0, 100, rows)
            elif col == 'location':
                locations = ['New York', 'San Francisco', 'Chicago', 'Austin', 'Seattle', 'Boston']
                data[col] = np.random.choice(locations, rows)
            elif col == 'manager_id':
                data[col] = [f'MGR{str(random.randint(1, 50)).zfill(3)}' for _ in range(rows)]
            else:
                data[col] = np.random.normal(50, 15, rows)
        
        return pd.DataFrame(data)
    
    @staticmethod
    def get_retail_template(rows: int, columns: int) -> pd.DataFrame:
        """Generate retail dataset with realistic columns"""
        base_columns = [
            'product_id', 'product_name', 'category', 'price', 'cost',
            'quantity_sold', 'revenue', 'profit_margin', 'supplier', 'launch_date'
        ]
        
        columns_to_use = base_columns[:min(columns, len(base_columns))]
        if columns > len(base_columns):
            columns_to_use.extend([f'retail_metric_{i}' for i in range(columns - len(base_columns))])
        
        data = {}
        
        for col in columns_to_use:
            if col == 'product_id':
                data[col] = [f'PRD{str(i).zfill(6)}' for i in range(1, rows + 1)]
            elif col == 'product_name':
                products = ['Widget A', 'Gadget B', 'Tool C', 'Device D', 'Item E', 'Product F']
                data[col] = [f'{random.choice(products)} {random.randint(1, 100)}' for _ in range(rows)]
            elif col == 'category':
                categories = ['Electronics', 'Clothing', 'Home', 'Sports', 'Books', 'Toys']
                data[col] = np.random.choice(categories, rows)
            elif col == 'price':
                data[col] = np.round(np.random.lognormal(3, 0.8, rows), 2)
            elif col == 'cost':
                # Cost is typically 60-80% of price
                prices = np.random.lognormal(3, 0.8, rows)
                data[col] = np.round(prices * np.random.uniform(0.6, 0.8, rows), 2)
            elif col == 'quantity_sold':
                data[col] = np.random.poisson(50, rows)
            elif col == 'revenue':
                # Will be calculated based on price and quantity
                data[col] = np.random.normal(1000, 500, rows)
            elif col == 'profit_margin':
                data[col] = np.round(np.random.normal(0.25, 0.1, rows), 3)
            elif col == 'supplier':
                suppliers = ['Supplier A', 'Supplier B', 'Supplier C', 'Supplier D']
                data[col] = np.random.choice(suppliers, rows)
            elif col == 'launch_date':
                start_date = datetime.now() - timedelta(days=1000)
                data[col] = [start_date + timedelta(days=random.randint(0, 1000)) for _ in range(rows)]
            else:
                data[col] = np.random.normal(25, 10, rows)
        
        return pd.DataFrame(data)

class StructuredDataGenerator:
    """Main class for generating structured synthetic datasets"""
    
    def __init__(self):
        self.templates = CategoryTemplates()
    
    def generate_dataset(self, category: str, rows: int, columns: int) -> pd.DataFrame:
        """Generate dataset based on category and parameters"""
        
        if category == "Medical":
            return self.templates.get_medical_template(rows, columns)
        elif category == "Finance":
            return self.templates.get_finance_template(rows, columns)
        elif category == "Business":
            return self.templates.get_business_template(rows, columns)
        elif category == "Retail":
            return self.templates.get_retail_template(rows, columns)
        else:
            # Generic dataset for unknown categories
            return self._generate_generic_dataset(rows, columns)
    
    def _generate_generic_dataset(self, rows: int, columns: int) -> pd.DataFrame:
        """Generate generic dataset with random data"""
        data = {}
        
        for i in range(columns):
            col_name = f'column_{i+1}'
            
            # Mix of data types
            if i % 4 == 0:  # Numeric
                data[col_name] = np.random.normal(50, 15, rows)
            elif i % 4 == 1:  # Categorical
                categories = [f'Category_{j}' for j in range(1, 6)]
                data[col_name] = np.random.choice(categories, rows)
            elif i % 4 == 2:  # Boolean-like
                data[col_name] = np.random.choice(['Yes', 'No'], rows)
            else:  # ID-like
                data[col_name] = [f'ID_{str(j).zfill(6)}' for j in range(1, rows + 1)]
        
        return pd.DataFrame(data)
    
    def add_noise_and_missing_values(self, df: pd.DataFrame, 
                                   missing_rate: float = 0.05,
                                   noise_rate: float = 0.02) -> pd.DataFrame:
        """Add realistic missing values and noise to make data more realistic"""
        df_noisy = df.copy()
        
        # Add missing values
        if missing_rate > 0:
            for col in df_noisy.columns:
                if df_noisy[col].dtype in ['int64', 'float64']:
                    # Add missing values to numeric columns
                    missing_indices = np.random.choice(
                        df_noisy.index, 
                        size=int(len(df_noisy) * missing_rate), 
                        replace=False
                    )
                    df_noisy.loc[missing_indices, col] = np.nan
        
        # Add noise to numeric columns
        if noise_rate > 0:
            for col in df_noisy.columns:
                if df_noisy[col].dtype in ['int64', 'float64']:
                    noise = np.random.normal(0, df_noisy[col].std() * noise_rate, len(df_noisy))
                    df_noisy[col] = df_noisy[col] + noise
        
        return df_noisy
    
    def get_dataset_preview(self, df: pd.DataFrame, sample_size: int = 5) -> Dict[str, Any]:
        """Generate preview data for frontend display"""
        sample_data = df.head(sample_size).to_dict('records')
        
        # Convert datetime objects to strings for JSON serialization
        for record in sample_data:
            for key, value in record.items():
                if isinstance(value, (datetime, pd.Timestamp)):
                    record[key] = value.strftime('%Y-%m-%d %H:%M:%S')
                elif pd.isna(value):
                    record[key] = None
        
        return {
            'sample_data': sample_data,
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'column_names': list(df.columns),
            'data_types': {col: str(dtype) for col, dtype in df.dtypes.items()},
            'file_size_mb': round(df.memory_usage(deep=True).sum() / (1024 * 1024), 2)
        }

# Global instance
data_generator = StructuredDataGenerator()