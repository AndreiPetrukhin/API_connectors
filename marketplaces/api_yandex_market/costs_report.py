import requests
import time
import re
import pandas as pd
from utils.logs import Logs
import logging

class YandexMarket:
    """
    Class for handling Yandex Market report generation and retrieval, with integrated logging.
    """
    
    def __init__(self, oauth_token: str, logger_name: str, log_level: int = logging.INFO):
        self.logging_instance = Logs(name=logger_name, log_level=log_level)
        self.logger = self.logging_instance.get_logger()
        
        if not oauth_token:
            self.logger.error(event="oauth_token_is_empty")
            raise
        
        try:
            self.oauth_token = f'Bearer {oauth_token}'
            self.api_url = 'https://api.partner.market.yandex.ru/reports/united-marketplace-services/generate'
            self.headers = {
                'Authorization': self.oauth_token,
                'Content-Type': 'application/json',
            }
            self.logger.info(event="yandex_market_initialized")

        except Exception as e:
            self.logger.error(event="yandex_market_initialize_failed", error=str(e))
            raise 
    
    def generate_report(self, date_from: str, date_to: str, business_id: int) -> str:
        """
        Generates a report and returns the report ID.

        :param business_id: Business identifier for the report.
        :param date_from: Start date for the report in 'YYYY-MM-DDTHH:MM:SSZ' format.
        :param date_to: End date for the report in 'YYYY-MM-DDTHH:MM:SSZ' format.
        :return: Report ID or None in case of failure.
        """
        post_params = {
            'businessId': business_id,
            'dateTimeFrom': date_from,
            'dateTimeTo': date_to,
        }

        try:
            response = requests.post(self.api_url, headers=self.headers, json=post_params)
            response.raise_for_status()
            report_info = response.json()
            report_id = report_info['result']['reportId']
            if report_id:
                self.logger.info(event="generate_report_successful", report_id=report_id)
                return report_id
            else:
                self.logger.error(event="generate_report_failed_no_id", response_text=response.text)
                raise
        except requests.RequestException as e:
            self.logger.error(event="generate_report_failed", error=str(e))
            raise
    
    def get_link_to_report(self, report_id: str) -> str:
        """
        Retrieves the download link for the generated report.

        :param report_id: The ID of the report.
        :return: Download URL for the report or None in case of failure.
        """
        status_check_url = f'https://api.partner.market.yandex.ru/reports/info/{report_id}'
        tries_count = 0
        delta_sec = 10

        while True:
            try:
                status_response = requests.get(status_check_url, headers=self.headers)
                status_response.raise_for_status()
                report_status = status_response.json().get('result', {}).get('status')

                if report_status == 'DONE':
                    report_url = status_response.json().get('result', {}).get('file')
                    if report_url:
                        self.logger.info(event="report_ready", report_id=report_id, report_url=report_url)
                        return report_url
                    else:
                        self.logger.error(event="report_is_empty", report_id=report_id, report_url=report_url)
                        exit()
                elif report_status in ['PENDING', 'PROCESSING']:
                    tries_count += 1
                    self.logger.info(event="report_processing", report_id=report_id, status=report_status)
                    time.sleep(delta_sec)  # Wait before the next status check
                    if tries_count * delta_sec > 300:
                        self.logger.error(event="report_generation_too_long", report_id=report_id, status=report_status, text=status_response.json())
                        exit()
                elif report_status == 'FAILED':
                    self.logger.error(event="report_generation_failed", report_id=report_id, status=report_status, text=status_response.json())
                    exit()
            except requests.RequestException as e:
                self.logger.error(event="error_checking_report_status", report_id=report_id, error=str(e))
                raise
    
    def data_processing(self, file: dict, report_date_from: time) -> pd.DataFrame:
        # Define the sheets and their respective columns
        sheets_columns = {
            'Размещение товаров на витрин': ['Дата и время оказания услуги', 'Модели работы', 'Стоимость услуги (гр.46=гр. 34-гр.36+гр.41+гр.43-гр.44-гр.45), ₽'],
            'Складская обработк': ['Дата и время оказания услуги', 'Модели работы', 'Стоимость услуги, ₽'],
            'Программа лояльности и отзывы': ['Дата и время оказания услуги', 'Модели работы', 'Стоимость услуги, ₽'],
            'Буст продаж': ['Дата оказания услуги', 'Модели работы', 'Предоплата, ₽', 'Постоплата, ₽'],
            'Полк': ['Дата оказания услуги', 'Модели работы', 'Стоимость услуги, ₽'],
            'Баннер': ['Дата оказания услуги', 'Модели работы', 'Стоимость услуги, ₽'],
            'Доставка покупател': ['Дата и время оказания услуги', 'Модели работы', 'Стоимость услуги, ₽'],
            'Приём платеж': ['Дата и время оказания услуги', 'Модели работы', 'Стоимость услуги, ₽'], 
            'Перевод платеж': ['Дата и время оказания услуги', 'Модели работы', 'Покупатель заплатил, ₽', 'Стоимость услуги, ₽'],
            'Платное хранен': ['Дата оказания услуги', 'Модели работы', 'Стоимость платного хранения, ₽'],
            'Обработка заказ': ['Дата оказания услуги', 'Модели работы', 'Стоимость услуги, ₽']
        }

        result = pd.DataFrame()

        for base_sheet_name, expected_cols in sheets_columns.items():
            for actual_sheet_name, sheet_data in file.items():
                if re.match(f"{base_sheet_name}.*", actual_sheet_name):
                    df = pd.DataFrame(sheet_data)
                    # Assuming the row with 'ID бизнес-аккаунта' is the header
                    row_number = df[df.isin(['ID бизнес-аккаунта']).any(axis=1)].index[0]
                    df.columns = df.iloc[row_number]
                    df = df[row_number + 1:]

                    # Find actual columns that contain the expected column names as substrings
                    actual_cols = []
                    for col in df.columns:
                        for expected_col in expected_cols:
                            if col is not None and expected_col in col:
                                actual_cols.append(col)
                                break

                    df = df[actual_cols].copy()

                    if actual_sheet_name == 'Буст продаж':
                        df.fillna(0, inplace=True)
                        df["Стоимость услуги руб"] = df["Предоплата, ₽"] + df["Постоплата, ₽"]
                        df.drop(columns=["Предоплата, ₽", "Постоплата, ₽"], inplace=True)

                    # Rename columns
                    df.rename(columns={df.columns[-2]: 'datetime_service', df.columns[-1]: 'costs_rub'}, inplace=True)
                    df.rename(columns={'Модели работы': 'service_model', 'Покупатель заплатил, ₽': 'customer_paid'}, inplace=True)
                    df['service_type'] = actual_sheet_name
                    df['datetime_upload'] = pd.to_datetime('now')

                    # Concatenate to the result DataFrame
                    if not df.empty:
                        result = pd.concat([result, df], ignore_index=True)

        # Additional processing
        if 'customer_paid' not in result.columns:
            result['customer_paid'] = 0
        result.fillna(0, inplace=True)
        result['costs_rub'] = pd.to_numeric(result['costs_rub'], errors='coerce')
        result['customer_paid'] = pd.to_numeric(result['customer_paid'], errors='coerce')
        result['service_model'] = result['service_model'].astype(str)
        result['service_type'] = result['service_type'].astype(str)
        result['datetime_service'] = pd.to_datetime(result['datetime_service']).astype('datetime64[us]')
        result['datetime_upload'] = pd.to_datetime(result['datetime_upload']).astype('datetime64[us]')
        
        pattern = r'^\d+([.,]\d+)?$'
        result = result[~result['service_model'].str.match(pattern)]
        result = result[(result['datetime_service'].dt.date >= report_date_from) & (result['datetime_service'].dt.date <= report_date_from)]
        result = result[['datetime_service', 'datetime_upload', 'service_model', 'service_type', 'costs_rub', 'customer_paid']]

        if not result.empty:
            self.logger.info(event="report_processed", report_date_from=report_date_from, report_date_to=report_date_from)
            return result
        else:
            self.logger.error(event="report_is_empty_after_procession", report_date_from=report_date_from, report_date_to=report_date_from)
            exit()