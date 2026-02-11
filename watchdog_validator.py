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
    """
    
    def __init__(self, dataframe: pd.DataFrame):
        """Initialize the validator with a pandas DataFrame."""
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
        df = pd.read_csv(filepath, **kwargs)
        return cls(df)

    @classmethod
    def from_excel(cls, filepath: str, sheet_name: Union[str, int] = 0, **kwargs):
        """
        Loads Excel data automatically selecting the engine for .xls or .xlsx formats.
        """
        if filepath.lower().endswith(".xls"):
            # The xlrd engine is required for legacy .xls files
            df = pd.read_excel(filepath, sheet_name=sheet_name, engine='xlrd', **kwargs)
        else:
            # openpyxl is the default for modern .xlsx files
            df = pd.read_excel(filepath, sheet_name=sheet_name, **kwargs)
        return cls(df)

    @classmethod
    def from_sql(cls, query: str, connection_string: str):
        engine = sqlalchemy.create_engine(connection_string)
        df = pd.read_sql(query, engine)
        return cls(df)

    def _initialize_validator(self):
        """Initialize Great Expectations context and validator using the Ephemeral Data Context."""
        try:
            self.context = gx.get_context()
            # Modern GX 1.0 workflow for Pandas
            data_source = self.context.data_sources.add_pandas("watchdog_source")
            data_asset = data_source.add_dataframe_asset(name="input_data")
            batch_definition = data_asset.add_batch_definition_whole_dataframe("batch_def")
            batch = batch_definition.get_batch(batch_parameters={"dataframe": self.df})
            self.validator = self.context.get_validator(batch=batch)
            return True
        except Exception as e:
            raise Exception(f"Failed to initialize GX validator: {str(e)}")

    def add_price_validation(self, column: str, min_value: float = 0, max_value: Optional[float] = None):
        if self.validator is None: self._initialize_validator()
        self.validator.expect_column_values_to_be_between(column, min_value=min_value, max_value=max_value)

    def add_not_null_validation(self, column: str):
        if self.validator is None: self._initialize_validator()
        self.validator.expect_column_values_to_not_be_null(column)

    def add_unique_validation(self, column: str):
        if self.validator is None: self._initialize_validator()
        self.validator.expect_column_values_to_be_unique(column)

    def add_set_validation(self, column: str, allowed_values: List):
        if self.validator is None: self._initialize_validator()
        self.validator.expect_column_values_to_be_in_set(column, allowed_values)

    def run_validation_all(self) -> Tuple[Dict, pd.DataFrame, pd.DataFrame]:
        """
        Execute all configured validations and process results.
        Updated to handle GX 1.0 result objects.
        """
        if self.validator is None:
            raise Exception("No validation rules configured.")
        
        self.results = self.validator.validate()
        self.bad_indices = set()
        self.failure_details = {}
        
        # Extract failures from the Modern GX Validation Result object
        for res in self.results.results:
            if not res.success:
                # Capture metadata for error logging
                column = res.expectation_config.kwargs.get('column', 'Unknown')
                etype = res.expectation_config.expectation_type
                failed_list = res.result.get('unexpected_index_list', [])
                
                # Format a friendly error message
                msg = self._format_failure_message(etype, column)
                
                for idx in failed_list:
                    if idx not in self.failure_details:
                        self.failure_details[idx] = []
                    self.failure_details[idx].append(msg)
                    self.bad_indices.add(idx)
        
        self._quarantine_data()
        summary = self._generate_summary()
        return summary, self.df_clean, self.df_failed

    def _format_failure_message(self, etype: str, column: str) -> str:
        message_map = {
            'expect_column_values_to_not_be_null': f'{column} is null',
            'expect_column_values_to_be_between': f'{column} out of range',
            'expect_column_values_to_be_unique': f'{column} is duplicate',
            'expect_column_values_to_be_in_set': f'{column} invalid category'
        }
        return message_map.get(etype, f'{column} failed {etype}')

    def _quarantine_data(self):
        """Splits the main dataframe into Clean and Failed buckets."""
        bad_idx_list = list(self.bad_indices)
        
        if bad_idx_list:
            # Failed Data
            self.df_failed = self.df.loc[bad_idx_list].copy()
            self.df_failed['Failure_Reason'] = self.df_failed.index.map(
                lambda x: '; '.join(self.failure_details.get(x, ['Unknown']))
            )
        else:
            self.df_failed = pd.DataFrame(columns=list(self.df.columns) + ['Failure_Reason'])
        
        # Clean Data
        self.df_clean = self.df.drop(index=bad_idx_list)

    def _generate_summary(self) -> Dict:
        total = len(self.df)
        clean = len(self.df_clean)
        return {
            'success': self.results.success,
            'total_rows': total,
            'clean_rows': clean,
            'failed_rows': len(self.df_failed),
            'pass_rate': (clean / total * 100) if total > 0 else 0
        }

    def save_results(self, clean_path: str = 'clean_transactions.csv', failed_path: str = 'failed_transactions.csv'):
        if self.df_clean is None: raise Exception("Run validation first.")
        self.df_clean.to_csv(clean_path, index=False)
        self.df_failed.to_csv(failed_path, index=False)

# CLI Helper functions
def validate_ecommerce_data(csv_path: str):
    df = pd.read_csv(csv_path)
    validator = WatchdogValidator(df)
    validator.add_price_validation("Price", min_value=0)
    validator.add_not_null_validation("Quantity")
    validator.add_not_null_validation("Customer ID")
    return validator.run_validation_all()

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python watchdog_validator.py <csv_file>")
        sys.exit(1)
    
    try:
        summary, clean, failed = validate_ecommerce_data(sys.argv[1])
        print(f"🐕‍🦺 Watchdog Quarantine Complete: {summary['clean_rows']} Clean / {summary['failed_rows']} Flagged")
    except Exception as e:
        print(f"❌ Error: {e}")