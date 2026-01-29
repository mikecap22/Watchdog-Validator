#watchdog_validator

"""
Watchdog Validator - Core Validation Engine
Backend module for data quality validation using Great Expectations
Supports CSV, Excel, and SQL database sources
"""

import pandas as pd
import great_expectations as gx
from typing import Dict, List, Tuple, Optional, Union
import sqlalchemy


class WatchdogValidator:
    """
    Core validation engine for data quality checks.
    Implements the quarantine pattern to separate clean from failed data.
    Supports multiple data sources: CSV, Excel, SQL databases
    """
    
    def __init__(self, dataframe: pd.DataFrame):
        """
        Initialize the validator with a pandas DataFrame.
        
        Args:
            dataframe: Input data to validate
        """
        self.df = dataframe
        self.context = None
        self.validator = None
        self.results = None
        self.df_clean = None
        self.df_failed = None
        self.bad_indices = set()
        self.failure_details = {}
    
    @classmethod
    def from_csv(cls, filepath: str, **kwargs):
        """
        Create validator from a CSV file.
        
        Args:
            filepath: Path to CSV file
            **kwargs: Additional arguments passed to pd.read_csv()
            
        Returns:
            WatchdogValidator instance
        """
        df = pd.read_csv(filepath, **kwargs)
        return cls(df)
    
    @classmethod
    def from_excel(cls, filepath: str, sheet_name: Union[str, int] = 0, **kwargs):
        """
        Create validator from an Excel file (.xlsx, .xls).
        
        Args:
            filepath: Path to Excel file
            sheet_name: Sheet name or index (default: 0 - first sheet)
            **kwargs: Additional arguments passed to pd.read_excel()
            
        Returns:
            WatchdogValidator instance
        """
        df = pd.read_excel(filepath, sheet_name=sheet_name, **kwargs)
        return cls(df)
    
    @classmethod
    def from_sql(cls, query: str, connection_string: str):
        """
        Create validator from a SQL database query.
        
        Args:
            query: SQL query to execute
            connection_string: Database connection string
                Examples:
                - PostgreSQL: "postgresql://user:password@localhost:5432/dbname"
                - MySQL: "mysql+pymysql://user:password@localhost:3306/dbname"
                - SQLite: "sqlite:///path/to/database.db"
                - SQL Server: "mssql+pyodbc://user:password@server/dbname?driver=ODBC+Driver+17+for+SQL+Server"
            
        Returns:
            WatchdogValidator instance
        """
        engine = sqlalchemy.create_engine(connection_string)
        df = pd.read_sql(query, engine)
        return cls(df)
    
    @classmethod
    def from_sql_table(cls, table_name: str, connection_string: str, schema: Optional[str] = None):
        """
        Create validator from an entire SQL table.
        
        Args:
            table_name: Name of the table to read
            connection_string: Database connection string
            schema: Database schema (optional)
            
        Returns:
            WatchdogValidator instance
        """
        engine = sqlalchemy.create_engine(connection_string)
        df = pd.read_sql_table(table_name, engine, schema=schema)
        return cls(df)
        
    def _initialize_validator(self):
        """Initialize Great Expectations context and validator."""
        try:
            # Initialize the Data Context
            self.context = gx.get_context()
            
            # Create the validator using modern GX workflow
            data_source = self.context.data_sources.add_pandas("pandas_datasource")
            data_asset = data_source.add_dataframe_asset(name="validation_data")
            batch_definition = data_asset.add_batch_definition_whole_dataframe("batch_def")
            batch = batch_definition.get_batch(batch_parameters={"dataframe": self.df})
            self.validator = self.context.get_validator(batch=batch)
            
            return True
        except Exception as e:
            raise Exception(f"Failed to initialize validator: {str(e)}")
    
    def add_price_validation(self, column: str, min_value: float = 0, max_value: Optional[float] = None):
        """
        Validate that price values are within acceptable range.
        
        Args:
            column: Column name containing price data
            min_value: Minimum acceptable price (default: 0)
            max_value: Maximum acceptable price (optional)
        """
        if self.validator is None:
            self._initialize_validator()
        
        if max_value is not None and max_value > 0:
            self.validator.expect_column_values_to_be_between(
                column, 
                min_value=min_value, 
                max_value=max_value
            )
        else:
            self.validator.expect_column_values_to_be_between(
                column, 
                min_value=min_value
            )
    
    def add_not_null_validation(self, column: str):
        """
        Validate that column does not contain null values.
        
        Args:
            column: Column name to check for nulls
        """
        if self.validator is None:
            self._initialize_validator()
        
        self.validator.expect_column_values_to_not_be_null(column)
    
    def add_unique_validation(self, column: str):
        """
        Validate that column contains only unique values.
        
        Args:
            column: Column name to check for uniqueness
        """
        if self.validator is None:
            self._initialize_validator()
        
        self.validator.expect_column_values_to_be_unique(column)
    
    def add_set_validation(self, column: str, allowed_values: List):
        """
        Validate that column values are within an allowed set.
        
        Args:
            column: Column name to validate
            allowed_values: List of acceptable values
        """
        if self.validator is None:
            self._initialize_validator()
        
        self.validator.expect_column_values_to_be_in_set(column, allowed_values)
    
    def add_custom_range_validation(self, column: str, min_value: Optional[float] = None, 
                                   max_value: Optional[float] = None):
        """
        Add a custom numeric range validation.
        
        Args:
            column: Column name to validate
            min_value: Minimum acceptable value (optional)
            max_value: Maximum acceptable value (optional)
        """
        if self.validator is None:
            self._initialize_validator()
        
        self.validator.expect_column_values_to_be_between(
            column,
            min_value=min_value,
            max_value=max_value
        )
    
    def run_validation(self) -> Dict:
        """
        Execute all configured validations and process results.
        
        Returns:
            Dictionary containing validation results and statistics
        """
        if self.validator is None:
            raise Exception("No validation rules configured. Add rules before running validation.")
        
        # Run validation
        self.results = self.validator.validate()
        
        # Extract failed indices and failure details
        all_results = self.results['results']
        self.bad_indices = set()
        self.failure_details = {}
        
        for res in all_results:
            if not res['success']:
                expectation_type = res['expectation_config']['expectation_type']
                column = res['expectation_config']['kwargs'].get('column', 'N/A')
                failed_index_list = res['result'].get('unexpected_index_list', [])
                
                # Build human-readable failure messages
                failure_message = self._format_failure_message(expectation_type, column)
                
                for idx in failed_index_list:
                    if idx not in self.failure_details:
                        self.failure_details[idx] = []
                    self.failure_details[idx].append(failure_message)
                
                self.bad_indices.update(failed_index_list)
        
        # Perform quarantine - split data into clean and failed
        self._quarantine_data()
        
        # Return summary
        return self._generate_summary()
    
    def _format_failure_message(self, expectation_type: str, column: str) -> str:
        """
        Format a human-readable failure message.
        
        Args:
            expectation_type: Type of expectation that failed
            column: Column name
            
        Returns:
            Formatted failure message
        """
        message_map = {
            'expect_column_values_to_not_be_null': f'{column} is null',
            'expect_column_values_to_be_between': f'{column} out of range',
            'expect_column_values_to_be_unique': f'{column} is not unique',
            'expect_column_values_to_be_in_set': f'{column} not in allowed set'
        }
        
        return message_map.get(
            expectation_type,
            expectation_type.replace('expect_column_values_to_', '').replace('_', ' ').title()
        )
    
    def _quarantine_data(self):
        """
        Split data into clean (passed) and failed (quarantined) DataFrames.
        """
        # Create failed DataFrame with failure reasons
        if self.bad_indices:
            self.df_failed = self.df.iloc[list(self.bad_indices)].copy()
            self.df_failed['Failure_Reason'] = self.df_failed.index.map(
                lambda x: '; '.join(self.failure_details.get(x, ['Unknown']))
            )
        else:
            self.df_failed = pd.DataFrame()
        
        # Create clean DataFrame
        self.df_clean = self.df.drop(index=list(self.bad_indices))
    
    def _generate_summary(self) -> Dict:
        """
        Generate validation summary statistics.
        
        Returns:
            Dictionary with validation summary
        """
        return {
            'success': self.results.success,
            'total_rows': len(self.df),
            'clean_rows': len(self.df_clean),
            'failed_rows': len(self.df_failed),
            'pass_rate': len(self.df_clean) / len(self.df) * 100 if len(self.df) > 0 else 0,
            'results': self.results
        }
    
    def get_clean_data(self) -> pd.DataFrame:
        """
        Get the clean (validated) data.
        
        Returns:
            DataFrame containing only validated records
        """
        if self.df_clean is None:
            raise Exception("Validation has not been run yet. Call run_validation() first.")
        return self.df_clean
    
    def get_failed_data(self) -> pd.DataFrame:
        """
        Get the failed (quarantined) data with failure reasons.
        
        Returns:
            DataFrame containing failed records with failure reasons
        """
        if self.df_failed is None:
            raise Exception("Validation has not been run yet. Call run_validation() first.")
        return self.df_failed
    
    def save_results(self, clean_path: str = 'clean_transactions.csv', 
                    failed_path: str = 'failed_transactions.csv'):
        """
        Save validation results to CSV files.
        
        Args:
            clean_path: File path for clean data
            failed_path: File path for failed data
        """
        if self.df_clean is None or self.df_failed is None:
            raise Exception("Validation has not been run yet. Call run_validation() first.")
        
        self.df_clean.to_csv(clean_path, index=False)
        self.df_failed.to_csv(failed_path, index=False)
        
        return {
            'clean_file': clean_path,
            'failed_file': failed_path,
            'clean_rows': len(self.df_clean),
            'failed_rows': len(self.df_failed)
        }


def validate_ecommerce_data(csv_path: str) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
    """
    Convenience function for standard e-commerce validation.
    
    Args:
        csv_path: Path to the CSV file
        
    Returns:
        Tuple of (clean_data, failed_data, summary)
    """
    # Load data
    df = pd.read_csv(csv_path)
    
    # Initialize validator
    validator = WatchdogValidator(df)
    
    # Add standard business rules
    validator.add_price_validation("Price", min_value=0)
    validator.add_not_null_validation("Quantity")
    validator.add_not_null_validation("Customer ID")
    
    # Run validation
    summary = validator.run_validation()
    
    # Save results
    validator.save_results()
    
    return validator.get_clean_data(), validator.get_failed_data(), summary


# CLI entry point
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python watchdog_validator.py <csv_file_path>")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    
    print(f"🐕‍🦺 Watchdog Validator - Processing {csv_file}")
    print("-" * 60)
    
    try:
        clean_data, failed_data, summary = validate_ecommerce_data(csv_file)
        
        if summary['success']:
            print("✅ Project 1: Data passed the Quality Gate!")
        else:
            print("⚠️ Project 1: Issues detected; validation failed.")
        
        print(f"\nQuarantine Complete:")
        print(f"  Clean Rows: {summary['clean_rows']}")
        print(f"  Flagged Rows: {summary['failed_rows']}")
        print(f"  Pass Rate: {summary['pass_rate']:.2f}%")
        print(f"\n📁 Files saved:")
        print(f"  - clean_transactions.csv")
        print(f"  - failed_transactions.csv")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        sys.exit(1)