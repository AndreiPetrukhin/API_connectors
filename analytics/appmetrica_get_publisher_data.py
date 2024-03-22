import os
import requests
from typing import Optional
from datetime import datetime, timedelta, timezone
import time
import pandas as pd

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from esn.hooks import ESNClickHouseHook as ClickHouseHook
from esn.utils import create_clickhouse_table_if_not_exists


class GetTrafficAppmetricaOperator(BaseOperator):
    """
    Оператор для извлечения данных об установках приложений с использованием API AppMetrica и сохранения в целевой таблице
    (traffic_source) в схеме (punch/chips) ClickHouse.

    Атрибуты:
    appmetrica_api_key (str): Ключ API для авторизации запросов в AppMetrica.
    application_id (str): Идентификатор приложения в AppMetrica, для которого извлекаются данные.
    endpoint (str): URL-адрес конечной точки API AppMetrica для экспорта данных об установках.

    Методы:
    __init__(self, ...):
        Инициализирует новый экземпляр класса для извлечения данных об установках приложений.

        Аргументы:
        clickhouse_conn_id (str): Идентификатор соединения с ClickHouse.
        clickhouse_database (str): Название базы данных в ClickHouse.
        target_schema (str): Название схемы в ClickHouse для целевой таблицы.
        target_table_name (str): Название таблицы в ClickHouse для сохранения данных.

    check_api_response(cls, response_data):
        Проверяет, содержит ли ответ API данные.

        Аргументы:
        response_data (dict): Ответ API в формате словаря.

    get_publisher_data(self, start_date: str, end_date: str) -> list:
        Извлекает данные издателей за определенный период.

        Аргументы:
        start_date (str): Начальная дата периода выборки данных.
        end_date (str): Конечная дата периода выборки данных.

        Возвращает:
        list: Список данных об установках.

    execute(self, context):
        Выполняет оператор, создает таблицу в ClickHouse, если она не существует, и вставляет данные об установках приложений.

        Аргументы:
        context (dict): Контекст выполнения.

    """
    app_metrica_token = os.getenv('APPMETRICA_API_ACCESS_TOKEN')
    appmetrica_api_key = f'OAuth {app_metrica_token}'
    endpoint = f'https://api.appmetrica.yandex.ru/logs/v1/export/installations.json'

    def __init__(self, clickhouse_conn_id, clickhouse_database, target_schema, target_table_name, application_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.clickhouse_conn_id = clickhouse_conn_id
        self.clickhouse_database = clickhouse_database
        self.target_schema = target_schema
        self.target_table_name = target_table_name
        self.application_id = application_id # ID приложения в AppMetrica

    @classmethod
    def check_api_response(cls, response_data):
        # Проверка ответа API на наличие данных
        if not response_data:
            raise ValueError('No data found in the response from API')


    def get_publisher_data(self, start_date: str, end_date: str) -> list:

        headers = {
            'Authorization': self.appmetrica_api_key,
            'Content-Type': 'application/json'
        }
        # Параметры для запроса данных (см. документацию в AppMetrica: Working with the API/Logs API/API resources/App installations)
        params = {
            'application_id': self.application_id,
            'date_since': start_date, #Дата начала (включительно)
            'date_until': end_date, #Дата окончания (включительно)
            'fields': 'publisher_name,tracker_name,install_datetime,appmetrica_device_id'
        }

        installations_data = {}

        try:
            response = requests.get(self.endpoint, headers=headers, params=params)
            
            # Обработка статуса ответа. Занимает время, поэтому реализован цикл ожидания ответа от API
            if response.status_code == 202:
                self.log.info('Request to API accepted. Waiting for processing to complete.') 
                
                while True:
                    # Интервал между запросами на проверку статуса
                    time.sleep(10)
                    status_response = requests.get(self.endpoint, headers=headers, params=params)
                    # Проверка на успешное завершение обработки
                    if status_response.status_code == 200:
                        self.log.info('Request processing complete.')
                        installations_data = status_response.json().get('data', [])
                        self.check_api_response(installations_data)
                        break
                    # Проверка на отсутствие успеха в ответе
                    elif status_response.status_code != 202:
                        raise AirflowException(f'Failed to get data: {status_response.status_code}, {status_response.text}')
                    else:
                        self.log.info('Still processing. Waiting a bit longer.')
            # Проверка на успешное завершение обработки
            elif response.status_code == 200:
                self.log.info('Request processing complete.')
                installations_data = response.json().get('data', [])
                self.check_api_response(installations_data)
            # Проверка на отсутствие успеха в ответе
            else:
                raise AirflowException(f'Failed to retrieve data: {response.status_code}, {response.text}')
        # Обработка исключений, связанных с запросами
        except requests.RequestException as e:
            raise AirflowException(f'Request failed: {e}')
        # Обработка ошибок, связанных с запросами
        except ValueError as e:
            raise AirflowException(f'Value error: {e}')

        # Проверка структуры данных в ответе от API
        for record in installations_data:
            if not all(k in record for k in ['publisher_name', 'tracker_name', 'install_datetime', 'appmetrica_device_id']):
                raise AirflowException('One or more records are missing required keys')
        
        return installations_data
    

    def execute(self, context, is_increment=False):
        
        self.click_conn_hook = ClickHouseHook(clickhouse_conn_id=self.clickhouse_conn_id, database=self.clickhouse_database)
        
        # Создание таблицы, если она не существует
        ddl_publisher_data = (f"""
        CREATE TABLE IF NOT EXISTS {self.target_schema}.{self.target_table_name}
        (
            publisher_name String,
            tracker_name String,
            install_datetime DateTime,
            appmetrica_device_id String
        )
        ENGINE = MergeTree()
        ORDER BY (install_datetime, appmetrica_device_id);
        """)
        create_clickhouse_table_if_not_exists(
                                                clickhouse_hook=self.click_conn_hook,
                                                schema=self.target_schema,
                                                table=self.target_table_name,
                                                ddl_query=ddl_publisher_data
                                                )

        # Определение временных рамок для загрузки данных
        if not is_increment:
            start_date = datetime(2023, 4, 20)
            end_date = datetime.now() - timedelta(days=1)
            current_date = start_date
            batch_size = 9
        else:
            end_date = datetime.now().date()
            start_date = datetime.now().date() - timedelta(days=2)
            current_date = start_date
            batch_size = 2

        try:
            while current_date < end_date:
                self.log.info(current_date)
                batch_end_date = min(current_date + timedelta(days=batch_size), end_date)
                start_date_str = current_date.strftime('%Y-%m-%d')
                end_date_str = batch_end_date.strftime('%Y-%m-%d')

                # Получение и вставка данных за батч
                data = self.get_publisher_data(start_date_str, end_date_str)
                self.log.info(data)
                values = []
                for d in data:
                    publisher_name = d['publisher_name'] if d['publisher_name'] else 'organic'
                    install_datetime = datetime.strptime(d['install_datetime'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                    values.append((publisher_name, d['tracker_name'], install_datetime, d['appmetrica_device_id']))

                if not is_increment:
                    if values:
                        self.click_conn_hook.run(f'INSERT INTO {self.target_schema}.{self.target_table_name} VALUES', values, append_settings=False)
                    else:
                        self.log.info('No data to insert for batch {} - {}'.format(start_date_str, end_date_str))

                else:

                    filtered_data = [item for item in values if item[1].date() >= current_date]

                    delete_query = f"DELETE FROM {self.target_schema}.{self.target_table_name} WHERE install_datetime >= '{current_date.strftime('%Y-%m-%d')}'"
                    self.click_conn_hook.run(delete_query)
                    
                    if values:
                        self.click_conn_hook.run(f'INSERT INTO {self.target_schema}.{self.target_table_name} VALUES', filtered_data, append_settings=False)
                    else:
                        self.log.info('No data to insert for batch {} - {}'.format(start_date_str, end_date_str))

                # Переход к следующему батчу
                current_date = batch_end_date + timedelta(days=1)

        except AirflowException as e:
            self.log.info(str(e))