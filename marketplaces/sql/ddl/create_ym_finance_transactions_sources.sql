CREATE TABLE IF NOT EXISTS {schema}.{table}
(
	report_date_from DATE,
	report_date_to DATE,
    datetime_upload DATE,
    file_data BYTEA
);