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
   - Metrics: order_total_usd, order_revenue_usd, list_price_total_usd, total_discount_usd, payment_amount_usd, order_item_count, total_quantity
   - Flags: is_paid, is_delivered, is_fulfilled
   - Business Logic: Aggregates line items with USD conversion; uses payment_summary CTE to aggregate multiple payments per order (handles split payments/retries)

2. **FACT_ORDER_ITEMS**

   - Grain: One row per line item (order + product combination)
   - Keys: order_item_id (PK), order_id (FK), product_id (FK), customer_id (FK), order_date_key (FK)
   - Metrics: quantity, list_price_usd, actual_price_usd, revenue_usd, discount_per_unit_usd, total_discount_usd, discount_percentage
   - Business Logic: Calculates discounts by comparing list price vs actual price; converts all amounts to USD

3. **FACT_PAYMENTS**

   - Grain: One row per payment transaction
   - Keys: payment_id (PK), order_id (FK), customer_id (FK), payment_date_key (FK)
   - Metrics: payment_amount_usd, order_total_usd, payment_difference_usd
   - Attributes: payment_method, payment_status, is_payment_successful, original currencies

4. **FACT_SHIPMENTS**
   - Grain: One row per shipment
   - Keys: shipment_id (PK), order_id (FK), customer_id (FK), shipment_date_key (FK), delivery_date_key (FK)
   - Metrics: delivery_days, days_to_ship, days_to_deliver, order_total_usd
   - Attributes: carrier, shipment_status, is_delivered

### Mart Layer Views

Business-friendly aggregated views with USD-normalized metrics:

- **DAILY_REVENUE**: Daily order counts, total_revenue_usd, avg_order_value_usd, total_discounts_usd, fulfillment metrics
- **MONTHLY_REVENUE**: Monthly trends with unique customers, payment/delivery rates, avg_discount_percentage
- **CUSTOMER_LIFETIME_VALUE**: Per-customer lifetime_revenue_usd, order frequency, total_discounts_received_usd
- **PRODUCT_PERFORMANCE**: Product-level sales, total_revenue_usd, total_discounts_given_usd, avg_discount_percentage
- **CATEGORY_PERFORMANCE**: Category-level aggregations in USD
- **PAYMENT_ANALYSIS**: Success rates by payment method, total_amount_usd
- **SHIPMENT_PERFORMANCE**: Carrier performance, average delivery times, on-time delivery rates

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
│   │   ├── 06_shipments.sql
│   │   └── 07_exchange_rates.sql
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
5. **Currency Conversion**: Multi-currency support implemented via SILVER.EXCHANGE_RATES table with static conversion rates to USD base currency; all Gold fact tables and Mart views use USD-normalized metrics (\_usd suffix)

### Data Relationships

1. **One-to-One**:
   - Order → Shipment (one shipment per order)
2. **One-to-Many**:
   - Order → Payments (multiple payments per order; handles split payments and retries)
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
3. **Currency Conversion Setup**:
   - EXCHANGE_RATES table with static conversion rates (USD=1.0, EUR=1.085, GBP=1.27, CAD=0.74, AUD=0.66, JPY=0.0068, CNY=0.139, INR=0.012)
   - Joined in Gold layer to convert all monetary values to USD
4. **Enhanced Shipments Logic**:
   - Sets delivery_date to NULL for IN_TRANSIT/CANCELLED/PENDING statuses (prevents illogical dates)
   - Calculates delivery_days only for DELIVERED shipments with valid dates
   - Flags illogical delivery dates (status doesn't match date presence)
   - Validates delivery_date is not before shipment_date

## Known Limitations

### Scope Limitations

1. **Static Exchange Rates**: Currency conversion uses static rates in EXCHANGE_RATES table; real-time FX rates not implemented
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
- `IS_DELIVERY_DATE_MISSING`: TRUE if delivery_date is NULL
- `IS_DELIVERY_DATE_ILLOGICAL`: TRUE if shipment status is IN_TRANSIT/CANCELLED/PENDING but delivery_date is populated
- `IS_DELIVERY_BEFORE_SHIPMENT`: TRUE if delivery_date is before shipment_date

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

5. **Payment Aggregation Strategy**: Handles multiple payments per order using CTE pattern

   - payment_summary CTE aggregates all payments before joining to orders
   - Prevents row multiplication in GROUP BY clauses
   - Uses SUM() for total payment amounts, BOOL_OR() for success flags
   - Supports split payments, partial payments, and payment retries

6. **Currency Normalization**: All monetary metrics converted to USD in Gold/Mart layers
   - Enables cross-currency analytics and aggregations
   - Preserves original currency information alongside USD values
   - Static exchange rates stored in SILVER.EXCHANGE_RATES table

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
    lifetime_revenue_usd,
    total_orders,
    avg_order_value_usd
FROM MART.CUSTOMER_LIFETIME_VALUE
ORDER BY lifetime_revenue_usd DESC
LIMIT 10;
```

### Query 2: Monthly Revenue Trends

```sql
SELECT
    year,
    month_name,
    total_revenue_usd,
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
    list_price_usd,
    total_revenue_usd,
    total_quantity_sold,
    avg_discount_percentage,
    unique_customers
FROM MART.PRODUCT_PERFORMANCE
WHERE total_quantity_sold > 10
ORDER BY total_revenue_usd DESC;
```

### Data Validation Queries

Check for data quality issues:

```sql
-- Verify currency conversion is working
SELECT ORIGINAL_CURRENCY, COUNT(*)
FROM GOLD.FACT_ORDERS
GROUP BY ORIGINAL_CURRENCY;

-- Check for orders with multiple payments
SELECT ORDER_ID, COUNT(*) AS payment_count
FROM SILVER.PAYMENTS
GROUP BY ORDER_ID
HAVING COUNT(*) > 1
ORDER BY payment_count DESC;

-- Validate shipment delivery dates
SELECT
    SHIPMENT_STATUS,
    COUNT(*) AS total_shipments,
    SUM(CASE WHEN IS_DELIVERY_DATE_ILLOGICAL THEN 1 ELSE 0 END) AS illogical_dates
FROM SILVER.SHIPMENTS
GROUP BY SHIPMENT_STATUS;
```
