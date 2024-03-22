import pandas as pd
import requests
import os
import json
from utils.logs import Logs
import logging

class Ozon:
    """
    Class for handling Ozon Market report generation and retrieval, with integrated logging.
    """
    
    def __init__(self, ozon_api_key: str, ozon_client_id: str, date_from: str, date_to: str, logger_name: str, log_level: int = logging.INFO):
        # Set up logging
        self.logging_instance = Logs(name=logger_name, log_level=log_level)
        self.logger = self.logging_instance.get_logger()

        # Initialize a session for HTTP requests. Usefull when we use requests. a several times
        self.session = requests.Session()
        
        if not ozon_api_key:
            raise ValueError("Ozon API key is required but was not provided.")

        
        self.api_url = "https://api-seller.ozon.ru/v3/finance/transaction/list"
        self.headers = {
            "Client-Id": ozon_client_id,
            "Api-Key": ozon_api_key,
            "Content-Type": "application/json"
            }
        self.logger.info(event="ozon_market_initialized")
        
        self.post_params = {
            "filter": {
                "date": {
                "from": f"{date_from}T00:00:00.000Z",
                "to": f"{date_to}T23:59:59.999Z"
                },
                "operation_type": [],
                "posting_number": "",
                "transaction_type": "all"
            },
            "page": 1,
            "page_size": 1000
            }
    
    def get_finance_transactions(self) -> dict:
        """
        Send a POST request to the Ozon API to retrieve transaction data based on the specified parameters.
        Using the initialized session, headers, and post parameters.

        Returns:
            dict: A dictionary object containing the JSON response from the Ozon API. This includes details
            of the finance transactions that match the filter criteria. The structure of the response is
            determined by the Ozon API's response format for the finance transaction list endpoint.
        """
        
        try:
            response = self.session.post(self.api_url, headers=self.headers, json=self.post_params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(event="ozon_market_request_failed", error=str(e))
            raise
        except Exception as e:
            self.logger.error(event="ozon_market_request_unexpected_error", error=str(e))
            raise 
    
    def get_all_finance_transactions(self) -> dict:
        """
        Retrieves all finance transactions from the Ozon API across multiple pages.
        This method first fetches the initial page of finance transactions and then iterates through any
        additional pages, as indicated by the 'page_count' in the response.

        Returns:
            dict: A dictionary containing the combined finance transactions from all fetched pages. The structure
            of the dictionary is based on the Ozon API's response format for finance transactions.
        """
        results = {}

        try:
            self.logger.info(event="start_fetch_all_finance_transactions")
            finance_transactions = self.get_finance_transactions()

            if finance_transactions and 'result' in finance_transactions and 'page_count' in finance_transactions['result']:
                results.update(finance_transactions)
                pages = finance_transactions['result']['page_count']
                self.logger.info(event=f"total_pages: {pages}")

                for page in range(2, pages + 1):
                    self.logger.debug(event=f"fetching_for: {page}")
                    self.post_params["page"] = page
                    try:
                        page_data = self.get_finance_transactions()
                        if page_data:
                            results.update(page_data)
                        else:
                            self.logger.warning(event=f"empty_response_on_page_{page}")
                            break
                    except Exception as e:
                        self.logger.error(event=f"error_fetching_for_page_{page}", error=str(e))
                        raise

            else:
                self.logger.error(event="No finance transactions found on the initial request.")
                exit()

        except Exception as e:
            self.logger.error(event="fetch_initial_finance_transactions_failed", error=str(e))
            raise

        return results
    
    def simplify_response(self, data):
        """
        Simplifies the response from the OZON API by extracting necessary fields.

        Parameters:
        data (str): The JSON response from the OZON API as a string.

        Returns:
        DataFrame: A DataFrame of simplified operation data.
        """
        try:

            # Extract relevant information
            simplified_operations = []
            for operation in data.get('result', {}).get('operations', []):
                simplified_operation = {
                    'operation_id': operation.get('operation_id'),
                    'operation_date': operation.get('operation_date'),
                    'operation_type_name': operation.get('operation_type_name'),
                    'delivery_charge': operation.get('delivery_charge', 0),
                    'return_delivery_charge': operation.get('return_delivery_charge', 0),
                    'accruals_for_sale': operation.get('accruals_for_sale', 0),
                    'sale_commission': operation.get('sale_commission', 0),
                    'amount': operation.get('amount', 0),
                    'type': operation.get('type'),
                    'items_name': operation['items'][0]['name'] if operation.get('items') else None,
                    'items_sku': operation['items'][0]['sku'] if operation.get('items') else None,
                    'posting_order_date': operation['posting']['order_date'] if 'posting' in operation else None
                }
                simplified_operations.append(simplified_operation)

            if not simplified_operations:
                self.logger.error(event="simplify_response", text="no_data_in_response")
                exit()

            # Create DataFrame
            df = pd.DataFrame(simplified_operations)
            df['datetime_upload'] = pd.to_datetime('now')

            # Specify columns
            columns_str = ['operation_type_name', 'type', 'items_name']
            columns_numeric = ['amount', 'delivery_charge', 'return_delivery_charge', 'accruals_for_sale', 'sale_commission']
            columns_date = ['operation_date', 'posting_order_date']
            columns_integers = ['operation_id', 'items_sku']

            # Convert data types
            for column in columns_str:
                df[column] = df[column].astype(str)
            for column in columns_numeric:
                df[column] = pd.to_numeric(df[column], errors='coerce')
            for column in columns_date:
                df[column] = pd.to_datetime(df[column], errors='coerce').dt.date.fillna(pd.Timestamp('1945-05-09'))
            for column in columns_integers:
                df[column] = df[column].fillna(0).astype('int64')
            df['datetime_upload'] = pd.to_datetime(df['datetime_upload']).astype('datetime64[us]')

            return df

        except Exception as e:
            self.logger.error(event="simplify_response_failed", error=str(e))
            raise