import pandas as pd
from datetime import datetime
from utils.data_quality import DataQualityCheck

# Creating a sample DataFrame to simulate the test data
test_data = {
    'operation_id': [1, 2, 3, 4, 7],  # Intentional duplicate to test the check_for_duplicates function
    'operation_date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-02'],
    'operation_type_name': ['Sale', 'Return', 'Sale', 'Sale', 'Return'],
    'delivery_charge': [100, 0, 100, 100, 0],
    'return_delivery_charge': [0, 50, 0, 0, 50],
    'accruals_for_sale': [500, 0, 600, 700, 0],
    'sale_commission': [50, 0, 60, 70, 0],
    'amount': [450, -50, 540, 630, -50],
    'type': ['sale', 'return', 'sale', 'sale', 'return'],
    'items_name': ['Item A', 'Item B', 'Item C', 'Item D', 'Item B'],
    'items_sku': [1001, 1002, 1003, 1004, 1002],  # Intentional duplicate to test the check_for_duplicates function
    'posting_order_date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-02']
}

# Convert string dates to datetime objects
for key in ['operation_date', 'posting_order_date']:
    test_data[key] = [datetime.strptime(date, '%Y-%m-%d').date() for date in test_data[key]]

df_test = pd.DataFrame(test_data)

df_test

# Example usage
if __name__ == "__main__":
    dqc = DataQualityCheck(logger_name='DataChecker')
    dqc.check_missing_values(df_test)
    dqc.check_data_positive(df_test, ['operation_id', 'items_sku'])
    dqc.check_for_duplicates(df_test, ['operation_id'])