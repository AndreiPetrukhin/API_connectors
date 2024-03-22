from sqlalchemy import create_engine, exc
from sqlalchemy.sql import text
from sqlalchemy import create_engine
import pickle
from utils.logs import Logs
import logging

class GreenPlum:
    def __init__(self, dbname, user, password, host, port, logger_name, log_level=logging.INFO):
        self.logging_instance = Logs(name=logger_name, log_level=log_level)
        self.logger = self.logging_instance.get_logger()
        
        try:
            self.engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}?sslmode=require", pool_pre_ping=True) # Add at debuging , echo=True
            # Test the database connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self.logger.info(event="database_connection_initialized", dbname=dbname, host=host)
        except exc.SQLAlchemyError as e:
            self.logger.error(event="database_connection_failed", dbname=dbname, host=host, error=str(e))
            raise
        except Exception as e:
            self.logger.error(event="unexpected_error_database_connection", dbname=dbname, host=host, error=str(e))
            raise

    def execute_dml_query(self, query): # need to be tested
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                try:
                    fetched = result.fetchall()
                    self.logger.info(event="dml_query_executed", query=query, rows_fetched=len(fetched) if fetched else 0)
                    return fetched if fetched else "Query executed successfully, but no rows returned."
                except exc.ResourceClosedError:
                    self.logger.info(event="dml_query_executed_no_result", query=query)
                    return "Query executed successfully."
        except exc.SQLAlchemyError as e:
            self.logger.error(event="dml_query_execution_failed", query=query, error=str(e))
            return None

    def execute_query(self, query):
        try:
            with self.engine.connect() as conn:
                conn.execute(text(query))
                conn.commit()
                self.logger.info(event="query_executed", query=query)
        except exc.SQLAlchemyError as e:
            self.logger.error(event="query_execution_failed", query=query, error=str(e))
            raise
        except Exception as e:
            self.logger.error(event="unexpected_error_query_execution", error=str(e))
            raise

    def insert_from_pd_df_to_table(self, dataframe, table_name, schema):
        try:
            dataframe.to_sql(table_name, self.engine, if_exists='append', index=False, schema=schema)
            self.logger.info(event="data_inserted_successfully", table_name=table_name, schema=schema)
        except exc.SQLAlchemyError as e:
            self.logger.error(event="error_inserting_data_to_table", table_name=table_name, schema=schema, error=str(e))
            raise
        except Exception as e:
            self.logger.error(event="unexpected_error_inserting_data", error=str(e))
            raise

    def insert_file_data(self, result, schema, table_name, report_date_from, report_date_to, datetime_upload):
        try:
            # Serialize the dictionary into a binary format using pickle
            serialized_data = pickle.dumps(result)

            query = text(f"""INSERT INTO {schema}.{table_name} 
                        (file_data, report_date_from, report_date_to, datetime_upload) 
                        VALUES (:file_data, :report_date_from, :report_date_to, :datetime_upload);""")
                    
            # Parameters for the query
            params = {
                'file_data': serialized_data,
                'report_date_from': report_date_from,
                'report_date_to': report_date_to,
                'datetime_upload': datetime_upload
            }
            
            # Execute the query with the serialized data
            with self.engine.connect() as conn:
                conn.execute(query, params)
                conn.commit()  # Explicitly commit the transaction
            self.logger.info(event="serialized_data_inserted", table_name=table_name, schema=schema)

        except exc.SQLAlchemyError as e:
            self.logger.error(event="database_error_inserting_serialized_data", table_name=table_name, schema=schema, error=str(e))
            raise
        except pickle.PicklingError as e:
            self.logger.error(event="serialization_error", error=str(e))
            raise
        except Exception as e:
            self.logger.error(event="unexpected_error_inserting_serialized_data", error=str(e))
            raise

    def read_file_data(self, schema, table_name, report_date_from, report_date_to, datetime_upload):
        try:
            query = f"""SELECT file_data FROM {schema}.{table_name} 
                        WHERE report_date_from = '{report_date_from}'
                        AND report_date_to = '{report_date_to}'
                        AND datetime_upload = '{datetime_upload}';"""

            with self.engine.connect() as conn:
                result = conn.execute(text(query)).fetchone()

            if result:
                file_data = pickle.loads(result[0])
                self.logger.info(event="file_data_retrieved", table_name=table_name, schema=schema)
                return file_data
            else:
                self.logger.warning(event="no_data_found", table_name=table_name, schema=schema, report_date_from=report_date_from, report_date_to=report_date_to, datetime_upload=datetime_upload)
                return None
        except exc.SQLAlchemyError as e:
            self.logger.error(event="database_error_reading_file_data", table_name=table_name, schema=schema, error=str(e))
            raise
        except pickle.UnpicklingError as e:
            self.logger.error(event="deserialization_error", error=str(e))
            raise
        except Exception as e:
            self.logger.error(event="unexpected_error_reading_file_data", error=str(e))
            raise