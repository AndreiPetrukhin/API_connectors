CREATE TABLE IF NOT EXISTS {schema}.{table} 
(
    operation_id BIGINT,
    operation_date DATE,
    operation_type_name TEXT,
    delivery_charge INTEGER,
    return_delivery_charge INTEGER,
    accruals_for_sale INTEGER,
    sale_commission INTEGER,
    amount FLOAT,
    type TEXT,
    items_name TEXT,
    items_sku BIGINT,
    posting_order_date DATE,
    datetime_upload TIMESTAMP
);
