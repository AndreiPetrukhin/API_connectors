DELETE FROM {schema}.{table}
    WHERE date(datetime_service) = '{report_date_from}';