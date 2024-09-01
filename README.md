# Retail Analysis with Spark and Airflow

This project performs retail data analysis using Apache Spark and schedules the analysis job using Apache Airflow. It processes retail transaction data stored in a PostgreSQL database, performs various analyses, and saves the results back to the database and as CSV files.

## Project Structure

```
retail-analysis/
│
├── spark-scripts/
│   └── retail_analysis.py
│
├── dags/
│   └── retail_analysis_dag.py
│
├── data/
│   ├── customer_order_summary.csv
│   └── churn_retention_analysis.csv
│
└── README.md
```

## Components

1. **Spark Script (`retail_analysis.py`)**: 
   - Reads retail data from PostgreSQL
   - Performs data cleaning and analysis
   - Calculates customer order summaries and churn-retention analysis
   - Saves results to PostgreSQL and CSV files

2. **Airflow DAG (`retail_analysis_dag.py`)**: 
   - Schedules the Spark job to run weekly
   - Uses the SparkSubmitOperator to execute the Spark script

## Setup Instructions

1. **Environment Variables**: 
   Set the following environment variables:
   - `POSTGRES_USER`
   - `POSTGRES_PASSWORD`
   - `POSTGRES_DW_DB`
   - `POSTGRES_CONTAINER_NAME`

2. **PostgreSQL**: 
   - Ensure PostgreSQL is running and accessible
   - Create a database named as per `POSTGRES_DW_DB`
   - Create a table named `public.retail` with the appropriate schema

3. **Spark**: 
   - Install Apache Spark
   - Ensure the PostgreSQL JDBC driver is available in the Spark classpath

4. **Airflow**: 
   - Install Apache Airflow
   - Configure the Spark connection in Airflow (conn_id: 'spark_main')

## Usage

1. Place the Spark script (`retail_analysis.py`) in the `/spark-scripts/` directory.
2. Place the Airflow DAG file (`retail_analysis_dag.py`) in your Airflow DAGs folder.
3. Start the Airflow scheduler and webserver.
4. The DAG will run automatically every week, or you can trigger it manually from the Airflow UI.

## Analysis Performed

1. **Customer Order Summary**: 
   - Aggregates total orders and total spent by country

2. **Churn-Retention Analysis**: 
   - Calculates days since last transaction for each customer
   - Classifies customers as Active, Churned, or One-Time Customer
   - Includes total spent and transaction count for each customer

## Output

1. Results are saved to PostgreSQL tables:
   - `country_order_summary`
   - `churn_retention_analysis`

2. CSV files are generated in the `data/` directory:
   - `country_order_summary.csv`
   - `churn_retention_analysis.csv`

## Dependencies

- Apache Spark
- Apache Airflow
- PostgreSQL JDBC Driver (42.6.0)
- Python libraries: pyspark, os

## Notes

- The churn threshold is set to 90 days and can be adjusted in the Spark script.
- Ensure proper network connectivity between Airflow, Spark, and PostgreSQL.
- Monitor Airflow logs for any execution issues.

## Future Improvements

- Add data quality checks
- Implement error handling and alerting
- Extend analysis with more advanced metrics and visualizations