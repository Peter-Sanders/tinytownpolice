"""
Delaware North: Take-Home Assignment

Technical Prerequisites:
  - Python 3+
  - Spark: pip install delta-spark
  - Java Runtime: https://java.com/en/download/manual.jsp

Assignment Background:
    - You are a freelance analytics consultant who has partnered with the TTPD (Tiny Town Police Department)
      to analyze speeding tickets that have been given to the adult citizens of Tiny Town over the 2020-2023 period.
    - Inside the folder "ttpd_data" you will find a directory of data for Tiny Town. This dataset will need to be "ingested" for analysis.
    - The solutions must use the Dataframes API.
    - You will need to ingest this data into a PySpark environment and answer the following three questions for the TTPD.

Questions:
    1. Which police officer was handed the most speeding tickets?
        - Police officers are recorded as citizens. Find in the data what differentiates an officer from a non-officer.
    2. What 3 months (year + month) had the most speeding tickets? 
        - Bonus: What overall month-by-month or year-by-year trends, if any, do you see?
    3. Using the ticket fee table below, who are the top 10 people who have spent the most money paying speeding tickets overall?

Ticket Fee Table:
    - Ticket (base): $30
    - Ticket (base + school zone): $60
    - Ticket (base + construction work zone): $60
    - Ticket (base + school zone + construction work zone): $120
"""
from delta import *
from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    """Retrieves or creates an active Spark Session for Delta operations
    
    Returns:
        spark (SparkSession): the active Spark Session
    """
    builder = SparkSession \
        .builder \
        .appName('takehome') \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    return configure_spark_with_delta_pip(builder).getOrCreate()

def main():
    spark: SparkSession = get_spark_session()

    ...


if __name__ == '__main__':
    main()
