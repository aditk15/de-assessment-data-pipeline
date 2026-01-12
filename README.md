# E-Commerce Analytics Data Pipeline

A production-grade, layered data engineering pipeline built with Snowflake, SQL, and Python. This project transforms raw e-commerce operational data into analytics-ready datasets using a medallion architecture (Bronze → Silver → Gold → Mart).

## Business Problem

An e-commerce company generates data across multiple operational systems (customer management, order processing, payments, shipments). The business requires clean, reliable datasets to answer:

- How much revenue is generated daily, weekly, and monthly?
- Who are the most valuable customers?
- Which products and categories perform best?
- How successful are payment transactions?
- How long does order fulfillment take?
- What discount patterns exist across products and categories?

## Architecture Overview

### Layered Data Warehouse Model

```
┌─────────────────────────────────────────────────────────────────┐
│                         MART LAYER                              │
│         (Business Views - Aggregated Metrics)                   │
│  • Daily/Monthly Revenue   • Customer LTV                       │
│  • Product Performance     • Payment Analysis                   │
│  • Category Performance    • Shipment Performance              │
└─────────────────────────────────────────────────────────────────┘
                              ▲
┌─────────────────────────────────────────────────────────────────┐
│                        GOLD LAYER                               │
│        (Dimensional Model - Star Schema)                        │
│                                                                 │
│  Dimensions:              Facts:                                │
│  • DIM_DATE               • FACT_ORDERS                         │
│  • DIM_CUSTOMER           • FACT_ORDER_ITEMS                    │
│  • DIM_PRODUCT            • FACT_PAYMENTS                       │
│                           • FACT_SHIPMENTS                      │
└─────────────────────────────────────────────────────────────────┘
                              ▲
┌─────────────────────────────────────────────────────────────────┐
│                       SILVER LAYER                              │
│     (Cleansed & Conformed Data)                                 │
│  • Type standardization    • Deduplication                      │
│  • Business validation     • Referential integrity              │
│  • Data quality flags      • Null handling                      │
└─────────────────────────────────────────────────────────────────┘
                              ▲
┌─────────────────────────────────────────────────────────────────┐
│                       BRONZE LAYER                              │
│            (Raw Data Ingestion)                                 │
│  • Customers  • Products  • Orders                              │
│  • Order Items  • Payments  • Shipments                         │
│  • Preserves original structure with metadata                  │
└─────────────────────────────────────────────────────────────────┘
```

## Data Model

### Star Schema Design (Gold Layer)

**Dimension Tables:**

1. **DIM_DATE** (Calendar dimension)

   - Grain: One row per date
   - Attributes: year, quarter, month, week, day_of_week, is_weekend
   - Range: Dynamically generated from order dates

2. **DIM_CUSTOMER**

   - Grain: One row per customer
   - Attributes: customer_id (PK), full_name, email, phone, country, is_active
   - Source: SILVER.CUSTOMERS

3. **DIM_PRODUCT**
   - Grain: One row per product
   - Attributes: product_id (PK), product_name, category, price, currency, is_active
   - Source: SILVER.PRODUCTS

**Fact Tables:**

1. **FACT_ORDERS**

   - Grain: One row per order
   - Keys: order_id (PK), customer_id (FK), order_date_key (FK)
   - Metrics: order_revenue, order_item_count, total_quantity, total_discount, list_price_total
   - Flags: is_paid, is_delivered, is_fulfilled
   - Business Logic: Aggregates line items, joins payment/shipment status

2. **FACT_ORDER_ITEMS**

   - Grain: One row per line item (order + product combination)
   - Keys: order_item_id (PK), order_id (FK), product_id (FK), customer_id (FK), order_date_key (FK)
   - Metrics: quantity, list_price, actual_price, revenue, discount_per_unit, total_discount, discount_percentage
   - Business Logic: Calculates discounts by comparing list price vs actual price

3. **FACT_PAYMENTS**

   - Grain: One row per payment transaction
   - Keys: payment_id (PK), order_id (FK), customer_id (FK), payment_date_key (FK)
   - Metrics: payment_amount, order_total, payment_difference
   - Attributes: payment_method, payment_status, is_payment_successful

4. **FACT_SHIPMENTS**
   - Grain: One row per shipment
   - Keys: shipment_id (PK), order_id (FK), customer_id (FK), shipment_date_key (FK), delivery_date_key (FK)
   - Metrics: delivery_days, days_to_ship, days_to_deliver, order_total
   - Attributes: carrier, shipment_status, is_delivered

### Mart Layer Views

Business-friendly aggregated views:

- **DAILY_REVENUE**: Daily order counts, revenue, discounts, fulfillment metrics
- **MONTHLY_REVENUE**: Monthly trends with unique customers, payment/delivery rates
- **CUSTOMER_LIFETIME_VALUE**: Per-customer revenue, order frequency, discount usage
- **PRODUCT_PERFORMANCE**: Product-level sales, revenue, discount analysis
- **CATEGORY_PERFORMANCE**: Category-level aggregations
- **PAYMENT_ANALYSIS**: Success rates by payment method
- **SHIPMENT_PERFORMANCE**: Carrier performance, delivery times

## Project Structure

```
de-assessment-data-pipeline/
├── config/
│   └── snowflake_config.json       # Snowflake connection credentials
├── data/
│   ├── customers.csv
│   ├── products.csv
│   ├── orders.csv
│   ├── order_items.csv
│   ├── payments.csv
│   └── shipments.csv
├── sql/
│   ├── bronze/
│   │   ├── 00_setup_infrastructure.sql
│   │   ├── 01_customers.sql
│   │   ├── 02_products.sql
│   │   ├── 03_orders.sql
│   │   ├── 04_order_items.sql
│   │   ├── 05_payments.sql
│   │   └── 06_shipments.sql
│   ├── silver/
│   │   ├── 01_customers.sql
│   │   ├── 02_products.sql
│   │   ├── 03_orders.sql
│   │   ├── 04_order_items.sql
│   │   ├── 05_payments.sql
│   │   └── 06_shipments.sql
│   ├── gold/
│   │   ├── 01_dim_date.sql
│   │   ├── 02_dim_customer.sql
│   │   ├── 03_dim_product.sql
│   │   ├── 04_fact_orders.sql
│   │   ├── 05_fact_order_items.sql
│   │   ├── 06_fact_payments.sql
│   │   └── 07_fact_shipments.sql
│   └── mart/
│       ├── 01_daily_revenue.sql
│       ├── 02_monthly_revenue.sql
│       ├── 03_customer_lifetime_value.sql
│       ├── 04_product_performance.sql
│       ├── 05_category_performance.sql
│       ├── 06_payment_analysis.sql
│       └── 07_shipment_performance.sql
├── python/
│   ├── bronze_setup.py             # Creates infrastructure & tables
│   ├── bronze_load.py              # Loads CSV data to Bronze
│   ├── silver_transform.py         # Silver transformations + DQ checks
│   ├── gold_transform.py           # Gold dimensional model
│   ├── mart_transform.py           # Mart business views
│   └── run_pipeline.py             # End-to-end orchestration
├── venv/                           # Python virtual environment (not in git)
└── README.md
```

## Setup Instructions

### Prerequisites

- **Snowflake Account**: Free trial account ([signup.snowflake.com](https://signup.snowflake.com))
- **Python**: Version 3.8 or higher
- **Git**: For version control

### 1. Clone Repository

```bash
git clone <your-repo-url>
cd de-assessment-data-pipeline
```

### 2. Set Up Python Environment

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # macOS/Linux
# OR
venv\Scripts\activate     # Windows

# Install dependencies
pip install snowflake-connector-python
```

### 3. Configure Snowflake Connection

Create `config/snowflake_config.json` with your credentials:

```json
{
  "user": "YOUR_USERNAME",
  "password": "YOUR_PASSWORD",
  "account": "YOUR_ACCOUNT_IDENTIFIER",
  "warehouse": "DE_WH",
  "database": "ECOMMERCE_ANALYTICS",
  "role": "ACCOUNTADMIN"
}
```

**Note:** Replace placeholders with your actual Snowflake credentials. The account identifier format is typically `ABC12345.us-east-1` (varies by region).

### 4. Prepare Data Files

Ensure all CSV files are in the `data/` directory:

- customers.csv
- products.csv
- orders.csv
- order_items.csv
- payments.csv
- shipments.csv

## How to Run the Pipeline

### Option 1: Run Full Pipeline (Recommended)

```bash
source venv/bin/activate
python python/run_pipeline.py
```

This executes all layers sequentially:

1. Bronze Setup (infrastructure)
2. Bronze Load (CSV ingestion)
3. Silver Transform (data cleansing)
4. Gold Transform (dimensional model)
5. Mart Transform (business views)

### Option 2: Run Individual Layers

```bash
source venv/bin/activate

# Bronze Layer
python python/bronze_setup.py   # One-time setup
python python/bronze_load.py    # Load CSV data

# Silver Layer
python python/silver_transform.py

# Gold Layer
python python/gold_transform.py

# Mart Layer
python python/mart_transform.py
```

### Expected Output

Each layer will print progress messages:

```
Creating CUSTOMERS...
Creating PRODUCTS...
...
Transforming CUSTOMERS...
Data Quality Summary:
  CUSTOMERS            :        999
  Quality Score: 98.5%
Silver layer completed
...
Creating DIM_DATE...
Dimension Tables:
  DIM_DATE            :        633
Gold layer completed
...
Creating DAILY_REVENUE...
Mart layer completed
```

## Assumptions Made

### Data Quality Assumptions

1. **Duplicates**: Handled using `ROW_NUMBER() OVER (PARTITION BY primary_key ORDER BY ingestion_ts DESC)` - keeps most recent record
2. **Missing Primary Keys**: Records with NULL primary keys are excluded from Silver layer
3. **Orphaned Records**: Child records without matching parent records are flagged but retained with `IS_*_ORPHANED` columns for analysis
4. **Invalid Business Values**:
   - Negative prices → Set to NULL, flagged with `IS_PRICE_INVALID`
   - Quantity ≤ 0 → Set to NULL, flagged with `IS_QUANTITY_INVALID`
   - Negative payment amounts → Set to NULL, flagged with `IS_AMOUNT_INVALID`

### Business Logic Assumptions

1. **Discounts**: Calculated as difference between product's list price and actual unit price charged
   - Assumes product.price is the list/catalog price
   - order_items.unit_price is the actual selling price (may include discounts)
2. **Order Fulfillment**: Order is considered fulfilled when:
   - Order status = 'COMPLETED'
   - Payment is successful
   - Shipment is delivered
3. **Revenue Calculation**: Uses actual selling price × quantity (line_total), not list price
4. **Date Dimension**: Generated dynamically from MIN/MAX order dates in Silver layer
5. **Currency**: No currency conversion applied; assumes single currency (USD) or downstream tool handles conversion

### Data Relationships

1. **One-to-One**:
   - Order → Payment (one payment per order)
   - Order → Shipment (one shipment per order)
2. **One-to-Many**:
   - Order → Order Items (multiple line items per order)
   - Customer → Orders (customer can have multiple orders)
   - Product → Order Items (product can appear in multiple orders)

### Silver Layer Transformations

1. **Type Casting**:
   - Dates: String → TIMESTAMP using `TO_TIMESTAMP_NTZ()`
   - Prices/Amounts: String → DECIMAL(10,2)
   - Quantities: String → INTEGER
   - Booleans: String ('True'/'False') → BOOLEAN
2. **Text Standardization**:
   - Country names: UPPER() for consistency
   - Email/Phone: TRIM() to remove whitespace
   - Status fields: Keep original casing from source

## Known Limitations

### Scope Limitations

1. **No Foreign Exchange**: Multi-currency support not implemented; assumes all transactions in same currency
2. **No Slowly Changing Dimensions**: Dimension tables are Type 1 (overwrite); historical changes not tracked
3. **No Incremental Loading**: Pipeline performs full refresh each run; not optimized for incremental updates
4. **Single Source System**: Assumes all data comes from one operational database
5. **No Data Lineage**: Column-level lineage tracking not implemented

### Data Limitations

1. **Delivery Days Calculation**: Assumes shipment_date and delivery_date are provided; does not calculate if missing
2. **Payment Reconciliation**: Identifies payment_amount vs order_total mismatches but doesn't auto-reconcile
3. **Product Price Changes**: Uses current product price from catalog; historical pricing not preserved
4. **Duplicate Detection**: Based on primary key only; does not detect semantic duplicates (e.g., same customer with different IDs)

### Technical Limitations

1. **Error Handling**: Basic exception handling; production system would need detailed error logging and alerting
2. **Performance**: No partitioning or clustering keys defined; suitable for datasets up to ~10M rows
3. **Data Quality Reporting**: Prints to console; production would need persistent DQ metrics storage
4. **Configuration Management**: Single config file; production would use environment-specific configs
5. **Testing**: No automated unit/integration tests included

### Silver Layer Edge Cases

1. **Missing Referential Keys**: Child records with invalid foreign keys are retained with orphaned flags
2. **Negative Metrics**: Invalid negative values set to NULL rather than erroring entire load
3. **Null Timestamps**: Records with NULL dates are kept; downstream analysis must handle appropriately

## Data Quality Features

### Silver Layer Quality Checks

The pipeline includes automated data quality validation:

```
Data Quality Summary:
  CUSTOMERS            :        999
  PRODUCTS             :      1,000
  ORDERS               :      1,500
  ORDER_ITEMS          :      4,000
  PAYMENTS             :      1,500
  SHIPMENTS            :      1,500

Referential Integrity:
  PASS (or shows count of orphaned records)

Data Quality Issues:
  Missing emails: 15 | Invalid prices: 3 | Invalid quantities: 2
  Quality Score: 99.7%
```

### Quality Flags in Silver Tables

Each Silver table includes boolean flags:

**CUSTOMERS**:

- `IS_EMAIL_MISSING`: TRUE if email is NULL

**PRODUCTS**:

- `IS_PRICE_INVALID`: TRUE if price < 0 or NULL

**ORDERS**:

- `IS_CUSTOMER_ORPHANED`: TRUE if customer_id not in CUSTOMERS

**ORDER_ITEMS**:

- `IS_ORDER_ORPHANED`: TRUE if order_id not in ORDERS
- `IS_PRODUCT_ORPHANED`: TRUE if product_id not in PRODUCTS
- `IS_QUANTITY_INVALID`: TRUE if quantity ≤ 0
- `IS_PRICE_INVALID`: TRUE if unit_price < 0

**PAYMENTS**:

- `IS_ORDER_ORPHANED`: TRUE if order_id not in ORDERS
- `IS_AMOUNT_INVALID`: TRUE if amount < 0

**SHIPMENTS**:

- `IS_ORDER_ORPHANED`: TRUE if order_id not in ORDERS

## Key Design Decisions

### Why This Architecture?

1. **Medallion Pattern**: Industry-standard for data lakehouses

   - Bronze: Maintains data lineage and auditability
   - Silver: Single source of truth for clean data
   - Gold: Optimized for analytics queries
   - Mart: Business-specific reporting

2. **SQL-First Approach**: Transformations in SQL files, not embedded in Python

   - Version control friendly
   - Easy to review and modify
   - Portable across orchestration tools

3. **Idempotent Design**: All scripts use `CREATE OR REPLACE`

   - Can rerun pipeline multiple times
   - No manual cleanup required

4. **Data Quality Flags**: Preserve problematic records with flags vs hard filtering
   - Enables data quality trend analysis
   - Allows business to make filtering decisions

### Technology Choices

- **Snowflake**: Cloud-native, handles semi-structured data, built-in optimization
- **Python**: Industry standard for data engineering orchestration
- **Star Schema**: Optimized for analytical queries, intuitive for business users

## Sample Queries

### Query 1: Top 10 Customers by Revenue

```sql
SELECT
    customer_id,
    full_name,
    lifetime_revenue,
    total_orders,
    avg_order_value
FROM MART.CUSTOMER_LIFETIME_VALUE
ORDER BY lifetime_revenue DESC
LIMIT 10;
```

### Query 2: Monthly Revenue Trends

```sql
SELECT
    year,
    month_name,
    total_revenue,
    total_orders,
    unique_customers,
    avg_discount_percentage
FROM MART.MONTHLY_REVENUE
ORDER BY year DESC, month DESC;
```

### Query 3: Product Performance with Discounting

```sql
SELECT
    product_name,
    category,
    list_price,
    total_revenue,
    total_quantity_sold,
    avg_discount_percentage,
    unique_customers
FROM MART.PRODUCT_PERFORMANCE
WHERE total_quantity_sold > 10
ORDER BY total_revenue DESC;
```

## Success Metrics

Pipeline is production-ready when:

- ✅ Runs end-to-end without manual intervention
- ✅ Data quality score consistently > 95%
- ✅ All fact tables have matching dimension keys (no orphans)
- ✅ Business metrics reconcile with source systems
- ✅ Execution time < 5 minutes for sample dataset
- ✅ Clear error messages for any failures
