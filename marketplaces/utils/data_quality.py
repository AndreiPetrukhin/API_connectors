import logging

from utils.logs import Logs

class DataQualityCheck:
    def __init__(self, logger_name: str, log_level: int = logging.INFO):
        # Set up logging
        self.logging_instance = Logs(name=logger_name, log_level=log_level)
        self.logger = self.logging_instance.get_logger()

    def check_missing_values(self, df):
        """Checks for missing values in crucial columns."""
        missing_values_count = df.isnull().sum()
        for column, missing_count in missing_values_count.items():
            if missing_count > 0:
                self.logger.warning(event="check_missing_values_failed", text=f"Column {column} has {missing_count} missing values")

    def check_data_positive(self, df, columns_to_check):
        """
        Verifies that numerical data in specified columns are greater than 0.

        Parameters:
        df (DataFrame): The DataFrame containing the data to check.
        columns_to_check (list): A list of column names to check for values greater than 0.
        """
        for column in columns_to_check:
            if column in df.columns:
                # Check if any values in the column are more than or equal to 0
                if (df[column] < 0).any():
                    self.logger.warning(event="check_data_positive_failed", text=f"Non-positive values found in '{column}'")
            else:
                self.logger.warning(event="check_data_positive_failed", text=f"Column '{column}' not found in DataFrame")

    def check_for_duplicates(self, df, columns_to_check):
        """
        Ensures specified column values are unique within the DataFrame.

        Parameters:
        df (DataFrame): The DataFrame containing the data to check.
        columns_to_check (list): A list of column names to check for duplicate values.
        """
        for column in columns_to_check:
            if column in df.columns:
                # Check if any values in the column are duplicated
                if df[column].duplicated().any():
                    self.logger.error(event="check_for_duplicates_failed", text=f"Duplicate values found in '{column}'")
                    exit()
            else:
                self.logger.warning(event="check_for_duplicates_failed", text=f"Column '{column}' not found in DataFrame")