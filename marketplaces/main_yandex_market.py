import os
import pandas as pd
from datetime import datetime, timedelta
import argparse
import logging
from dotenv import load_dotenv

from utils.logs import Logs
from utils.data_quality import DataQualityCheck
from api_yandex_market.costs_report import YandexMarket
from utils.general import GeneralUtils
from greenplum.sqlalchemy_manager import GreenPlum

# Set up argument parsing
def parse_arguments():
    parser = argparse.ArgumentParser(description="Run Yandex Market report script with custom date and timedelta.")
    parser.add_argument("--date-to", type=str, help="The reference date in YYYY-MM-DD format.", default=datetime.now().strftime('%Y-%m-%d'))
    parser.add_argument("--days-to-shift", type=int, help="Date_to shift like date_to - timedelta(days=1).", default=1)
    parser.add_argument("--days-window", type=int, help="Window width to set up the date range for report.", default=4)
    return parser.parse_args()


def main(report_date_to, report_date_from, datetime_upload):
    load_dotenv()# Load the .env file
    main_file_path = os.path.abspath(__file__) # Get the absolute path of the main.py file
    main_dir_path = os.path.dirname(main_file_path) # Get the directory containing main.py
    dbname = os.getenv('GP_DB_NAME')
    schema = "dynamic_data_staging"
    host = "c-c9q76dq48o2rnogp4f29.rw.mdb.yandexcloud.net"
    port = "5432"
    source_data_table_name = 'ym_finance_transactions_sources'
    transformed_data_table_name = 'ym_finance_transactions'
    logger_name = f"{report_date_from.strftime('%Y-%m-%d')}_Yandex_market"

    # Init logging instance
    logging_instance = Logs(name=logger_name)
    logger = logging_instance.get_logger()

    dqc = DataQualityCheck(logger_name=logger_name)
    ym = YandexMarket(oauth_token=os.getenv("YM_OAUTH_TOKEN"), 
                    logger_name=logger_name)
    gu = GeneralUtils(logger_name=logger_name)
    gp = GreenPlum(dbname=dbname, 
                    user=os.getenv("GP_USER_NAME"), 
                    password=os.getenv("GP_USER_PASSWORD"), 
                    host=host, 
                    port=port,
                    logger_name=logger_name)
    
    report_id = ym.generate_report(f"{report_date_from.strftime('%Y-%m-%d')}T00:00:00Z", f"{report_date_to.strftime('%Y-%m-%d')}T23:59:59Z", os.getenv("YM_BUSINESS_ID"))

    url = ym.get_link_to_report(report_id)

    result = gu.read_excel_from_url(url)

    # Create source storage
    filepath = f"{main_dir_path}/sql/ddl/create_{source_data_table_name}.sql"
    query = gu.read_sql_file(filepath=filepath, schema=schema, table=source_data_table_name)
    gp.execute_query(query)

    # Delete from source retro data
    filepath = f"{main_dir_path}/sql/dml/delete_from_{source_data_table_name}.sql"
    query = gu.read_sql_file(filepath=filepath, schema=schema, table=source_data_table_name, report_date_from=report_date_from)
    gp.execute_query(query)

    # Fullfil source storage
    gp.insert_file_data(result, schema, source_data_table_name, report_date_from.strftime('%Y-%m-%d'), report_date_to.strftime('%Y-%m-%d'), datetime_upload.strftime('%Y-%m-%d'))
    
    result_stg = gp.read_file_data(schema, source_data_table_name, report_date_from.strftime('%Y-%m-%d'), report_date_to.strftime('%Y-%m-%d'), datetime_upload.strftime('%Y-%m-%d'))

    # Check-up of insertation data to GP from API
    if result_stg is None:
        logger.error(event="no_data_was_read_from_GP_cource")
        exit()
    if result == result_stg:
        logger.info(event="insertation_from_API_to_staging_is_correct")
    else:
        logger.error(event="insertation_from_API_to_staging_is_NOT_correct")
        exit()

    df = pd.DataFrame()
    df = ym.data_processing(result, report_date_from)

    # Data quality check
    dqc.check_missing_values(df)
    dqc.check_data_positive(df, ['customer_paid'])

    # Create data modification storage
    filepath = f"{main_dir_path}/sql/ddl/create_{transformed_data_table_name}.sql"
    query = gu.read_sql_file(filepath=filepath, schema=schema, table=transformed_data_table_name)
    gp.execute_query(query)

    # Delete from source retro data
    filepath = f"{main_dir_path}/sql/dml/delete_from_{transformed_data_table_name}.sql"
    query = gu.read_sql_file(filepath=filepath, schema=schema, table=transformed_data_table_name, report_date_from=report_date_from)
    gp.execute_query(query)

    # Fullfil data modification storage
    gp.insert_from_pd_df_to_table(df, transformed_data_table_name, schema)


if __name__ == "__main__":
    args = parse_arguments() # Parse the arguments
    report_date_to = (datetime.strptime(args.date_to, '%Y-%m-%d') - timedelta(days=args.days_to_shift)).date()
    report_date_from = (report_date_to - timedelta(days=args.days_window))
    datetime_upload = datetime.now().date()

    while report_date_to >= report_date_from:

        main(report_date_from, report_date_from, datetime_upload)
        report_date_from = (report_date_from + timedelta(days=1))