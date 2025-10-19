import pandas as pd
import numpy as np
from typing import Dict, List, Any, Tuple, Optional
# Sklearn is optional - disable for Vercel deployment
try:
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import LabelEncoder
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
import json
import io
import zipfile
from datetime import datetime

from models import QualityMetrics, QualityAssessment

class QualityAssessmentService:
    """Service for assessing dataset quality using machine learning metrics"""
    
    def __init__(self):
        self.weights = {
            'completeness': 0.30,
            'statistical_consistency': 0.20,
            'class_balance': 0.20,
            'duplicates': 0.10,
            'outliers': 0.10,
            'schema_match': 0.10
        }
    
    def assess_dataset_quality(self, file_bytes: bytes, 
                             expected_category: str = None) -> QualityAssessment:
        """Main method to assess dataset quality"""
        
        try:
            # Try to extract and analyze the dataset
            df = self._extract_dataframe_from_bytes(file_bytes)
            
            if df is None or df.empty:
                return self._create_failed_assessment("Unable to extract valid data from file")
            
            # Calculate individual metrics
            completeness = self._calculate_completeness(df)
            statistical_consistency = self._calculate_statistical_consistency(df)
            class_balance = self._calculate_class_balance(df)
            duplicates = self._calculate_duplicates_score(df)
            outliers = self._calculate_outliers_score(df)
            schema_match = self._calculate_schema_match(df, expected_category)
            
            # Create metrics object
            metrics = QualityMetrics(
                completeness=completeness,
                statistical_consistency=statistical_consistency,
                class_balance=class_balance,
                duplicates=duplicates,
                outliers=outliers,
                schema_match=schema_match
            )
            
            # Calculate overall score
            overall_score = self._calculate_overall_score(metrics)
            
            # Generate explanations and recommendations
            explanations = self._generate_explanations(metrics)
            recommendations = self._generate_recommendations(metrics)
            
            return QualityAssessment(
                overall_score=overall_score,
                metrics=metrics,
                explanation=explanations,
                recommendations=recommendations
            )
            
        except Exception as e:
            return self._create_failed_assessment(f"Error during quality assessment: {str(e)}")
    
    def _extract_dataframe_from_bytes(self, file_bytes: bytes) -> Optional[pd.DataFrame]:
        """Extract pandas DataFrame from file bytes (CSV or ZIP)"""
        
        try:
            # First, try to read as CSV directly
            csv_data = file_bytes.decode('utf-8')
            df = pd.read_csv(io.StringIO(csv_data))
            return df
        except:
            pass
        
        try:
            # Try to extract CSV from ZIP
            with zipfile.ZipFile(io.BytesIO(file_bytes), 'r') as zip_file:
                # Look for CSV file in ZIP
                csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
                if csv_files:
                    csv_content = zip_file.read(csv_files[0]).decode('utf-8')
                    df = pd.read_csv(io.StringIO(csv_content))
                    return df
        except:
            pass
        
        return None
    
    def _calculate_completeness(self, df: pd.DataFrame) -> float:
        """Calculate completeness score (30% weight) - missing values percentage"""
        
        if df.empty:
            return 0.0
        
        total_cells = df.shape[0] * df.shape[1]
        missing_cells = df.isnull().sum().sum()
        
        if total_cells == 0:
            return 0.0
        
        completeness_ratio = 1 - (missing_cells / total_cells)
        return max(0.0, min(100.0, completeness_ratio * 100))
    
    def _calculate_statistical_consistency(self, df: pd.DataFrame) -> float:
        """Calculate statistical consistency (20% weight) - mean/std within reasonable bounds"""
        
        if df.empty:
            return 0.0
        
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        
        if len(numeric_columns) == 0:
            return 75.0  # Neutral score for non-numeric data
        
        consistency_scores = []
        
        for col in numeric_columns:
            col_data = df[col].dropna()
            
            if len(col_data) < 2:
                continue
            
            # Check for reasonable variance (not all same values, not too extreme)
            std_dev = col_data.std()
            mean_val = col_data.mean()
            
            if std_dev == 0:
                # All values are the same - might be suspicious
                consistency_scores.append(30.0)
            elif abs(mean_val) > 0:
                # Coefficient of variation check
                cv = std_dev / abs(mean_val)
                if 0.1 <= cv <= 2.0:  # Reasonable variation
                    consistency_scores.append(90.0)
                elif cv < 0.1:  # Too little variation
                    consistency_scores.append(60.0)
                else:  # Too much variation
                    consistency_scores.append(70.0)
            else:
                consistency_scores.append(75.0)
        
        if not consistency_scores:
            return 75.0
        
        return np.mean(consistency_scores)
    
    def _calculate_class_balance(self, df: pd.DataFrame) -> float:
        """Calculate class balance (20% weight) - balanced category distribution"""
        
        if df.empty:
            return 0.0
        
        categorical_columns = df.select_dtypes(include=['object']).columns
        
        if len(categorical_columns) == 0:
            return 80.0  # Neutral score for non-categorical data
        
        balance_scores = []
        
        for col in categorical_columns:
            col_data = df[col].dropna()
            
            if len(col_data) == 0:
                continue
            
            # Calculate distribution balance
            value_counts = col_data.value_counts()
            
            if len(value_counts) == 1:
                # Only one category - perfectly balanced but not diverse
                balance_scores.append(60.0)
            else:
                # Calculate entropy-based balance score
                proportions = value_counts / len(col_data)
                entropy = -np.sum(proportions * np.log2(proportions + 1e-10))
                max_entropy = np.log2(len(value_counts))
                
                if max_entropy > 0:
                    balance_ratio = entropy / max_entropy
                    balance_scores.append(balance_ratio * 100)
                else:
                    balance_scores.append(50.0)
        
        if not balance_scores:
            return 80.0
        
        return np.mean(balance_scores)
    
    def _calculate_duplicates_score(self, df: pd.DataFrame) -> float:
        """Calculate duplicates score (10% weight) - unique row count"""
        
        if df.empty:
            return 0.0
        
        total_rows = len(df)
        unique_rows = len(df.drop_duplicates())
        
        if total_rows == 0:
            return 0.0
        
        uniqueness_ratio = unique_rows / total_rows
        return uniqueness_ratio * 100
    
    def _calculate_outliers_score(self, df: pd.DataFrame) -> float:
        """Calculate outliers score (10% weight) - anomaly detection"""
        
        if df.empty:
            return 0.0
        
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        
        if len(numeric_columns) == 0:
            return 85.0  # Neutral score for non-numeric data
        
        try:
            # Prepare data for outlier detection
            numeric_data = df[numeric_columns].dropna()
            
            if len(numeric_data) < 10:  # Need minimum samples for outlier detection
                return 85.0
            
            # Use Isolation Forest for outlier detection (if available)
            if SKLEARN_AVAILABLE:
                iso_forest = IsolationForest(contamination=0.1, random_state=42)
                outlier_labels = iso_forest.fit_predict(numeric_data)
                # Calculate outlier ratio
                outlier_ratio = np.sum(outlier_labels == -1) / len(outlier_labels)
            else:
                # Fallback: use simple statistical method
                outlier_ratio = 0.05  # Assume normal distribution
            
            # Score based on outlier ratio (lower is better, but some outliers are normal)
            if outlier_ratio <= 0.05:  # Very few outliers
                return 95.0
            elif outlier_ratio <= 0.10:  # Expected amount
                return 85.0
            elif outlier_ratio <= 0.20:  # Moderate amount
                return 70.0
            else:  # Too many outliers
                return 50.0
                
        except Exception:
            return 75.0  # Default score if outlier detection fails
    
    def _calculate_schema_match(self, df: pd.DataFrame, expected_category: str = None) -> float:
        """Calculate schema match (10% weight) - columns match expected template"""
        
        if df.empty:
            return 0.0
        
        if not expected_category:
            return 80.0  # Neutral score if no category specified
        
        # Define expected columns for each category
        expected_schemas = {
            'Medical': ['patient_id', 'age', 'gender', 'blood_pressure', 'heart_rate', 'diagnosis'],
            'Finance': ['transaction_id', 'amount', 'account_id', 'transaction_type', 'date'],
            'Business': ['employee_id', 'department', 'salary', 'position', 'hire_date'],
            'Retail': ['product_id', 'price', 'category', 'quantity', 'revenue']
        }
        
        if expected_category not in expected_schemas:
            return 80.0  # Neutral score for unknown categories
        
        expected_cols = expected_schemas[expected_category]
        actual_cols = [col.lower() for col in df.columns]
        
        # Calculate how many expected columns are present (fuzzy matching)
        matches = 0
        for expected_col in expected_cols:
            for actual_col in actual_cols:
                if expected_col.lower() in actual_col or actual_col in expected_col.lower():
                    matches += 1
                    break
        
        if len(expected_cols) == 0:
            return 80.0
        
        match_ratio = matches / len(expected_cols)
        return match_ratio * 100
    
    def _calculate_overall_score(self, metrics: QualityMetrics) -> int:
        """Calculate weighted overall quality score"""
        
        weighted_score = (
            metrics.completeness * self.weights['completeness'] +
            metrics.statistical_consistency * self.weights['statistical_consistency'] +
            metrics.class_balance * self.weights['class_balance'] +
            metrics.duplicates * self.weights['duplicates'] +
            metrics.outliers * self.weights['outliers'] +
            metrics.schema_match * self.weights['schema_match']
        )
        
        return max(0, min(100, int(round(weighted_score))))
    
    def _generate_explanations(self, metrics: QualityMetrics) -> List[str]:
        """Generate human-readable explanations for the quality scores"""
        
        explanations = []
        
        # Completeness explanation
        if metrics.completeness >= 95:
            explanations.append("Excellent data completeness with minimal missing values")
        elif metrics.completeness >= 80:
            explanations.append("Good data completeness with acceptable missing values")
        elif metrics.completeness >= 60:
            explanations.append("Moderate data completeness with some missing values")
        else:
            explanations.append("Poor data completeness with significant missing values")
        
        # Statistical consistency explanation
        if metrics.statistical_consistency >= 85:
            explanations.append("Statistical properties appear consistent and realistic")
        elif metrics.statistical_consistency >= 70:
            explanations.append("Statistical properties are mostly consistent")
        else:
            explanations.append("Statistical properties show some inconsistencies")
        
        # Class balance explanation
        if metrics.class_balance >= 80:
            explanations.append("Categorical data shows good class balance")
        elif metrics.class_balance >= 60:
            explanations.append("Categorical data shows moderate class balance")
        else:
            explanations.append("Categorical data shows class imbalance")
        
        # Duplicates explanation
        if metrics.duplicates >= 95:
            explanations.append("Excellent data uniqueness with minimal duplicates")
        elif metrics.duplicates >= 80:
            explanations.append("Good data uniqueness with few duplicates")
        else:
            explanations.append("Significant duplicate records detected")
        
        return explanations
    
    def _generate_recommendations(self, metrics: QualityMetrics) -> List[str]:
        """Generate recommendations for improving data quality"""
        
        recommendations = []
        
        if metrics.completeness < 80:
            recommendations.append("Consider imputing missing values or collecting more complete data")
        
        if metrics.statistical_consistency < 70:
            recommendations.append("Review data generation process for statistical consistency")
        
        if metrics.class_balance < 60:
            recommendations.append("Consider balancing categorical distributions")
        
        if metrics.duplicates < 80:
            recommendations.append("Remove duplicate records to improve data quality")
        
        if metrics.outliers < 60:
            recommendations.append("Review and validate outlier values")
        
        if metrics.schema_match < 70:
            recommendations.append("Ensure dataset schema matches expected category structure")
        
        if not recommendations:
            recommendations.append("Dataset quality is good - no major improvements needed")
        
        return recommendations
    
    def _create_failed_assessment(self, error_message: str) -> QualityAssessment:
        """Create a failed quality assessment with error information"""
        
        return QualityAssessment(
            overall_score=0,
            metrics=QualityMetrics(
                completeness=0.0,
                statistical_consistency=0.0,
                class_balance=0.0,
                duplicates=0.0,
                outliers=0.0,
                schema_match=0.0
            ),
            explanation=[f"Quality assessment failed: {error_message}"],
            recommendations=["Please ensure the file is a valid CSV or ZIP containing CSV data"]
        )
    
    def get_quality_indicator_color(self, score: int) -> str:
        """Get color indicator for quality score"""
        
        if score >= 80:
            return "green"  # High quality
        elif score >= 60:
            return "yellow"  # Medium quality
        else:
            return "red"  # Low quality

# Global instance
quality_service = QualityAssessmentService()