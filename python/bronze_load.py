import os
import json
import snowflake.connector

DATA_DIR = "data"
STAGE_NAME = "BRONZE_STAGE"

TABLE_COLUMNS = {
    "CUSTOMERS": ["CUSTOMER_ID", "FIRST_NAME", "LAST_NAME", "EMAIL", "PHONE", "COUNTRY", "CREATED_AT", "IS_ACTIVE"],
    "PRODUCTS": ["PRODUCT_ID", "PRODUCT_NAME", "CATEGORY", "PRICE", "CURRENCY", "IS_ACTIVE", "CREATED_AT"],
    "ORDERS": ["ORDER_ID", "CUSTOMER_ID", "ORDER_DATE", "ORDER_STATUS", "TOTAL_AMOUNT", "CURRENCY"],
    "ORDER_ITEMS": ["ORDER_ITEM_ID", "ORDER_ID", "PRODUCT_ID", "QUANTITY", "UNIT_PRICE"],
    "PAYMENTS": ["PAYMENT_ID", "ORDER_ID", "PAYMENT_DATE", "PAYMENT_METHOD", "AMOUNT", "CURRENCY", "PAYMENT_STATUS"],
    "SHIPMENTS": ["SHIPMENT_ID", "ORDER_ID", "SHIPMENT_DATE", "DELIVERY_DATE", "CARRIER", "SHIPMENT_STATUS"]
}

with open("config/snowflake_config.json") as f:
    cfg = json.load(f)

conn = snowflake.connector.connect(
    user=cfg["user"],
    password=cfg["password"],
    account=cfg["account"],
    warehouse=cfg["warehouse"],
    database=cfg["database"],
    role=cfg["role"],
    schema="BRONZE"
)

cursor = conn.cursor()

def execute(sql):
    print(sql)
    cursor.execute(sql)

try:
    # Upload files
    for file in os.listdir(DATA_DIR):
        if file.endswith(".csv"):
            abs_path = os.path.abspath(os.path.join(DATA_DIR, file))
            execute(f"""
                PUT file://{abs_path}
                @{STAGE_NAME}
                AUTO_COMPRESS = TRUE
                OVERWRITE = TRUE
            """)

    # Copy per table with PATTERN
    for table, columns in TABLE_COLUMNS.items():
        col_list = ", ".join(columns)
        pattern = f".*{table.lower()}.*"

        execute(f"""
            COPY INTO BRONZE.{table}
            ({col_list})
            FROM @{STAGE_NAME}
            PATTERN = '{pattern}'
            FILE_FORMAT = (
                TYPE = 'CSV'
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            )
            FORCE = TRUE;
        """)

    print("Bronze ingestion SUCCESS")

except Exception as e:
    print("Bronze ingestion failed")
    print(e)

finally:
    cursor.close()
    conn.close()
