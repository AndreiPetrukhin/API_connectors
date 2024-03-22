import psycopg2
import os

try:
    conn = psycopg2.connect(
        dbname="holding_dev_zsn",
        user=os.getenv("GP_USER"),
        password=os.getenv("GP_PASSWORD"),
        host="c-c9q76dq48o2rnogp4f29.rw.mdb.yandexcloud.net",
        port="5432",
        sslmode="require"
    )
    conn.close()
    print("Connection successful")
except psycopg2.OperationalError as e:
    print(f"Connection failed: {e}")