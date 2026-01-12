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
    schema="MART"
)

cursor = conn.cursor()

try:
    print("\nMART LAYER - Business Views")
    print("="*70)

    mart_files = [
        "sql/mart/01_daily_revenue.sql",
        "sql/mart/02_monthly_revenue.sql",
        "sql/mart/03_customer_lifetime_value.sql",
        "sql/mart/04_product_performance.sql",
        "sql/mart/05_category_performance.sql",
        "sql/mart/06_payment_analysis.sql",
        "sql/mart/07_shipment_performance.sql",
    ]

    for sql_file in mart_files:
        view_name = os.path.basename(sql_file).replace('.sql', '').split('_', 1)[1].upper()
        print(f"Creating {view_name}...")
        with open(sql_file, 'r') as f:
            cursor.execute(f.read())

    print("\nMART LAYER SUMMARY")
    print("="*70)

    views = [
        'DAILY_REVENUE',
        'MONTHLY_REVENUE', 
        'CUSTOMER_LIFETIME_VALUE',
        'PRODUCT_PERFORMANCE',
        'CATEGORY_PERFORMANCE',
        'PAYMENT_ANALYSIS',
        'SHIPMENT_PERFORMANCE'
    ]

    print("Business Views Created:")
    for view in views:
        print(f"  ✓ {view}")

    print("\nSample Metrics:")
    cursor.execute("SELECT COUNT(*) FROM MART.DAILY_REVENUE")
    daily_count = cursor.fetchone()[0]
    print(f"  Days with revenue data: {daily_count:,}")

    cursor.execute("SELECT COUNT(*) FROM MART.CUSTOMER_LIFETIME_VALUE")
    customer_count = cursor.fetchone()[0]
    print(f"  Customers analyzed: {customer_count:,}")

    cursor.execute("SELECT COUNT(*) FROM MART.PRODUCT_PERFORMANCE")
    product_count = cursor.fetchone()[0]
    print(f"  Products tracked: {product_count:,}")

    print("\n" + "="*70)
    print("✓ Mart layer transformation completed successfully")
    print("="*70 + "\n")

except Exception as e:
    print(f"\n✗ ERROR: {e}\n")
    raise

finally:
    cursor.close()
    conn.close()
