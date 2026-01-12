import json
import snowflake.connector
import os

with open("config/snowflake_config.json") as f:
    cfg = json.load(f)

conn = snowflake.connector.connect(
    user=cfg["user"],
    password=cfg["password"],
    account=cfg["account"],
    warehouse=cfg["warehouse"],
    database=cfg["database"],
    role=cfg["role"],
    schema="GOLD"
)

cursor = conn.cursor()

try:
    print("\nGOLD LAYER - Dimensional Model")
    print("="*70)

    gold_files = [
        "sql/gold/01_dim_date.sql",
        "sql/gold/02_dim_customer.sql",
        "sql/gold/03_dim_product.sql",
        "sql/gold/04_fact_orders.sql",
        "sql/gold/05_fact_order_items.sql",
        "sql/gold/06_fact_payments.sql",
        "sql/gold/07_fact_shipments.sql",
    ]

    for sql_file in gold_files:
        table_name = os.path.basename(sql_file).replace('.sql', '').split('_', 1)[1].upper()
        print(f"Creating {table_name}...")
        with open(sql_file, 'r') as f:
            cursor.execute(f.read())

    print("\nGOLD LAYER SUMMARY")
    print("="*70)

    print("Dimension Tables:")
    for table in ['DIM_DATE', 'DIM_CUSTOMER', 'DIM_PRODUCT']:
        cursor.execute(f"SELECT COUNT(*) FROM GOLD.{table}")
        count = cursor.fetchone()[0]
        print(f"  {table:20s}: {count:>10,}")

    print("\nFact Tables:")
    for table in ['FACT_ORDERS', 'FACT_ORDER_ITEMS', 'FACT_PAYMENTS', 'FACT_SHIPMENTS']:
        cursor.execute(f"SELECT COUNT(*) FROM GOLD.{table}")
        count = cursor.fetchone()[0]
        print(f"  {table:20s}: {count:>10,}")

    print("\n" + "="*70)
    print("Gold layer transformation completed successfully")
    print("="*70 + "\n")

except Exception as e:
    print(f"\n✗ ERROR: {e}\n")
    raise

finally:
    cursor.close()
    conn.close()
