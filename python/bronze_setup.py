import snowflake.connector
import json
import os

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'snowflake_config.json')
    with open(config_path, 'r') as f:
        return json.load(f)

def execute_sql_file(cursor, filepath, description):
    print(f"Creating {description}...")
    with open(filepath, 'r') as f:
        sql = f.read()
        for statement in sql.split(';'):
            statement = statement.strip()
            if statement:
                cursor.execute(statement)

def main():

    config = load_config()
    conn = snowflake.connector.connect(
        user=config['user'],
        password=config['password'],
        account=config['account'],
        warehouse=config.get('warehouse', 'DE_WH'),
        database=config.get('database', 'ECOMMERCE_ANALYTICS'),
        schema='BRONZE'
    )

    cursor = conn.cursor()

    try:
        base_path = os.path.join(os.path.dirname(__file__), '..', 'sql', 'bronze')
        
        # Setup infrastructure
        execute_sql_file(
            cursor,
            os.path.join(base_path, '00_setup_infrastructure.sql'),
            'Infrastructure'
        )

        # Create Bronze tables
        table_files = [
            ('01_customers.sql', 'CUSTOMERS'),
            ('02_products.sql', 'PRODUCTS'),
            ('03_orders.sql', 'ORDERS'),
            ('04_order_items.sql', 'ORDER_ITEMS'),
            ('05_payments.sql', 'PAYMENTS'),
            ('06_shipments.sql', 'SHIPMENTS'),
        ]

        for filename, description in table_files:
            execute_sql_file(
                cursor,
                os.path.join(base_path, filename),
                description
            )

        print("Bronze setup completed")

    except Exception as e:
        print(f"ERROR: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    main()
