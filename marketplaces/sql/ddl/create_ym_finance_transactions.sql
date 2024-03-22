CREATE TABLE IF NOT EXISTS {schema}.{table} 
(
    datetime_service TIMESTAMP,
    datetime_upload TIMESTAMP,
    service_model TEXT,         
    service_type TEXT,
    costs_rub NUMERIC,
    customer_paid NUMERIC
);