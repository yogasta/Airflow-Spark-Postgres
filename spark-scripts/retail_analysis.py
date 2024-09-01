from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import datediff, max, when
import os

def main():
    # Read environment variables
    postgres_user = os.environ.get('POSTGRES_USER', 'user')
    postgres_password = os.environ.get('POSTGRES_PASSWORD', 'password')
    postgres_db = os.environ.get('POSTGRES_DW_DB', 'warehouse')
    postgres_port = '5432'
    postgres_container_name = os.environ.get('POSTGRES_CONTAINER_NAME', 'dataeng-postgres')

    # Create Spark session with PostgreSQL JDBC driver
    spark = SparkSession.builder \
        .appName("RetailAnalysis") \
        .getOrCreate()

    # JDBC URL for PostgreSQL
    jdbc_url = f"jdbc:postgresql://{postgres_container_name}:{postgres_port}/{postgres_db}"

    # Read data from PostgreSQL
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "public.retail") \
        .option("user", postgres_user) \
        .option("password", postgres_password) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    # Convert InvoiceDate to date type if it's not already
    df = df.withColumn("InvoiceDate", to_date(df["InvoiceDate"]))
    # Remove Quantities below 0 and Unit Price below 0
    df = df.filter((col("Quantity") > 0) & (col("UnitPrice") > 0))

    # Aggregate the total spending by Each Country 
    country_order_summary = df.groupBy("Country") \
        .agg(count("InvoiceNo").alias("total_orders"),
             sum(col("Quantity") * col("UnitPrice")).alias("total_spent"))

    # Calculate the last transaction date for each customer
    most_recent_date = df.agg(max("InvoiceDate")).collect()[0][0]

    print(f"Most recent date in the dataset: {most_recent_date}")

    # Calculate the last transaction date for each customer
    last_transaction_date = df.groupBy("CustomerID").agg(max("InvoiceDate").alias("LastTransactionDate"))

    # Calculate the number of days since the last transaction
    days_since_last_transaction = last_transaction_date.withColumn(
        "DaysSinceLastTransaction", 
        datediff(lit(most_recent_date), last_transaction_date["LastTransactionDate"])
    )

    # Determine customer status (adjust the churn threshold as needed)
    customer_status = days_since_last_transaction.withColumn(
        "Status",
        when(days_since_last_transaction["DaysSinceLastTransaction"] > 90, "Churned")
        .otherwise("Active")
    )

    # Identify one-time customers
    # Identify one-time customers and calculate total spent
    customer_summary = df.groupBy("CustomerID").agg(
        count("InvoiceNo").alias("TransactionCount"),
        sum(col("Quantity") * col("UnitPrice")).alias("TotalSpent")
)

    churn_retention = customer_status.join(customer_summary, "CustomerID", "left").withColumn(
        "Status",
        when(customer_summary["TransactionCount"] == 1, "One-Time Customer")
        .otherwise(customer_status["Status"])
    )

    # Output results
    print("Country Order Summary:")
    country_order_summary.show()
    
    print("Churn-Retention Analysis:")
    churn_retention.show()

    # Save results back to PostgreSQL
    country_order_summary.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "country_order_summary") \
        .option("user", postgres_user) \
        .option("password", postgres_password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    churn_retention.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "churn_retention_analysis") \
        .option("user", postgres_user) \
        .option("password", postgres_password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    
    country_order_summary_pd = country_order_summary.toPandas()
    churn_retention_pd = churn_retention.toPandas()

    country_order_summary_pd.to_csv(os.path.join("data","country_order_summary.csv"), index=False)
    churn_retention_pd.to_csv(os.path.join("data","churn_retention_analysis.csv"), index=False)
    spark.stop()

if __name__ == "__main__":
    main()