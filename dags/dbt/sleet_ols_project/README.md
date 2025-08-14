Snowflake Data Engineering Project – ELT Pipeline with dbt & Airflow
Project Overview

This project implements a complete data engineering pipeline using Snowflake as the data warehouse, dbt for transformation and testing, and Airflow for orchestration.

The pipeline ingests raw data into Snowflake, transforms it into business-ready datasets using dbt, and schedules automated runs using Airflow. The entire process is version-controlled with GitHub for collaborative development and CI/CD.

Business Requirement

The business requirement is to analyze customer purchasing behavior and identify the top 10 customers with the highest total revenue and number of orders.

The organization’s datasets include:

Customers

Orders

Order Items

Stores

Employees

Products

The goal is to:

Consolidate raw transactional data.

Create a fact table for orders.

Calculate the top 10 customers by revenue and order count.

Store the results in a consumption layer for analytics and reporting.

### Data Architecture
#### - Snowflake Schemas

landing – Raw ingested data from source systems (CSV, API, etc.).

Tables: customers, orders, orderitems, stores, employees, products.

processing – Transformed and staged datasets, including fact tables.

Example: order_facts table created from order_stg and orderitems_stg.

consumption – Business-ready datasets for BI & analytics.

Example: Top 10 customers by revenue and number of orders.

ELT Process
1. Extract & Load (EL)

Source Data: Customer, order, and product-related datasets are ingested into Snowflake’s landing schema.

Data ingestion can be performed via bulk load (Snowpipe) or manual file uploads.

2. Transform (T) with dbt

Transformation logic is handled by dbt, following the modular SQL approach:

a. Staging Layer (staging folder)

Each raw table in landing is transformed into a cleaned staging table with standardized column names, data types, and filters.

Example:

order_stg – cleans and standardizes order data.

orderitems_stg – cleans and standardizes order item data.

customer_stg – cleans and standardizes customer data.

b. Fact Table Creation

order_facts table is built by joining order_stg and orderitems_stg.

Metrics calculated:

Order revenue (unit price × quantity).

Order count per customer.

c. Business Computations

order_facts is joined with customer_stg to compute:

Top 10 customers by revenue.

Top 10 customers by number of orders.

The results are stored in the consumption schema.

dbt Features Used

source.yml – Defines all source tables and their relationships.

Macros & Jinja – Reusable SQL logic for revenue calculation and customer ranking.

Tests:

Singular tests – Validate specific business rules.

Generic tests – Check for not_null, unique, and accepted_values.

Snapshots – Implements Slowly Changing Dimension Type 2 (SCD2) to track historical changes in customer data.

Orchestration with Airflow

Airflow DAG schedules dbt runs to execute transformations daily.

DAG Tasks:

Check data availability in Snowflake landing tables.

Run dbt models (dbt run).

Run dbt tests (dbt test).

Load results into consumption schema.

The pipeline is monitored and logged in Airflow UI.

Version Control & CI/CD

GitHub repository stores dbt project code, including models, macros, and tests.

Branching strategy ensures feature development is merged into main only after review.

Future scope: Implement Azure DevOps/GitHub Actions for automated deployment on commit to main.

Folder Structure
.
├── dags/                   # Airflow DAG files
├── dbt_project.yml         # dbt project configuration
├── models/
│   ├── staging/            # Staging models
│   ├── Processing/         # Fact & dimension models
│   ├── consumption/        # Business logic models
│   └── snapshots/          # SCD2 snapshots
├── macros/                 # Reusable SQL & Jinja macros
├── seeds/                  # Optional static datasets
├── tests/                  # Singular test SQL files
└── source.yml              # Source table definitions

#### Lineage
![alt text](image.png)


Key Benefits of This Pipeline

Separation of concerns – Raw, processing, and consumption schemas clearly isolate data states.

Reproducibility – dbt’s SQL-based transformations ensure transparent logic.

Scalability – Airflow handles automated orchestration and scheduling.

Data Quality – dbt tests and snapshots maintain accuracy over time.
