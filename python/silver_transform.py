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
    schema="SILVER"
)

cursor = conn.cursor()

try:
    sql_files = [
        "sql/silver/01_customers.sql",
        "sql/silver/02_products.sql",
        "sql/silver/03_orders.sql",
        "sql/silver/04_order_items.sql",
        "sql/silver/05_payments.sql",
        "sql/silver/06_shipments.sql",
        "sql/silver/07_exchange_rates.sql",
    ]

    for sql_file in sql_files:
        table_name = os.path.basename(sql_file).replace('.sql', '').split('_', 1)[1].upper()
        print(f"Transforming {table_name}...")
        with open(sql_file, 'r') as f:
            cursor.execute(f.read())

    print("\nData Quality Summary:")

    tables = ['CUSTOMERS', 'PRODUCTS', 'ORDERS', 'ORDER_ITEMS', 'PAYMENTS', 'SHIPMENTS']
    total_records = 0
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM SILVER.{table}")
        count = cursor.fetchone()[0]
        total_records += count
        print(f"{table:20s}: {count:>10,}")

    print("\nReferential Integrity:")
    cursor.execute("SELECT COUNT(*) FROM SILVER.ORDERS WHERE IS_CUSTOMER_ORPHANED = TRUE")
    orphaned_orders = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM SILVER.ORDER_ITEMS WHERE IS_ORDER_ORPHANED = TRUE OR IS_PRODUCT_ORPHANED = TRUE")
    orphaned_items = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM SILVER.PAYMENTS WHERE IS_ORDER_ORPHANED = TRUE")
    orphaned_payments = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM SILVER.SHIPMENTS WHERE IS_ORDER_ORPHANED = TRUE")
    orphaned_shipments = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM SILVER.SHIPMENTS WHERE IS_DELIVERY_DATE_ILLOGICAL = TRUE")
    illogical_delivery_dates = cursor.fetchone()[0]
    
    total_orphaned = orphaned_orders + orphaned_items + orphaned_payments + orphaned_shipments
    status = "PASS" if total_orphaned == 0 else f"✗ FAIL ({total_orphaned:,} orphaned records)"
    print(f"  {status}")

    print("\nData Quality Issues:")
    cursor.execute("SELECT COUNT(*) FROM SILVER.CUSTOMERS WHERE IS_EMAIL_MISSING = TRUE")
    missing_emails = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM SILVER.PRODUCTS WHERE IS_PRICE_INVALID = TRUE")
    invalid_prices = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM SILVER.ORDER_ITEMS WHERE IS_QUANTITY_INVALID = TRUE")
    invalid_quantities = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM SILVER.SHIPMENTS WHERE IS_DELIVERY_DATE_MISSING = TRUE")
    missing_delivery_dates = cursor.fetchone()[0]
    
    total_issues = missing_emails + invalid_prices + invalid_quantities + total_orphaned + illogical_delivery_dates
    quality_score = ((total_records - total_issues) / total_records * 100) if total_records > 0 else 0
    print(f"  Missing emails: {missing_emails:,} | Invalid prices: {invalid_prices:,} | Invalid quantities: {invalid_quantities:,}")
    print(f"  Missing delivery dates: {missing_delivery_dates:,} | Illogical delivery dates: {illogical_delivery_dates:,}")
    print(f"  Quality Score: {quality_score:.1f}%")

    print("\nBusiness Metrics:")
    cursor.execute("SELECT COUNT(*), SUM(CASE WHEN PAYMENT_STATUS='FAILED' THEN 1 ELSE 0 END) FROM SILVER.PAYMENTS")
    total_payments, failed_payments = cursor.fetchone()
    success_rate = ((total_payments - failed_payments) / total_payments * 100) if total_payments > 0 else 0
    print(f"  Payment Success Rate: {success_rate:.1f}%")
    
    cursor.execute("SELECT AVG(DELIVERY_DAYS) FROM SILVER.SHIPMENTS WHERE DELIVERY_DAYS IS NOT NULL")
    avg_delivery = cursor.fetchone()[0]
    print(f"  Avg Delivery Time: {avg_delivery:.1f} days")

    print("\n" + "="*70)
    print("Silver layer transformation completed successfully")
    print("="*70 + "\n")

except Exception as e:
    print(f"\n✗ ERROR: {e}\n")
    raise

finally:
    cursor.close()
    conn.close()