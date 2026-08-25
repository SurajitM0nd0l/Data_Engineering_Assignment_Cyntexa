from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table
def sales_cleaned() :
    df = spark.readStream.csv(
        "/Volumes/dev/bronze/raw/sales.csv",
        header = True,
        inferSchema = True
    )

    df_cleaned = (
        df.dropna(how="all")
        .withColumn("order_date", F.coalesce(F.col("order_date"), F.current_date()))
        .fillna({
            "order_id": -1,
            "customer_id": -1,
            "transaction_id": -1,
            "product_id": -1,
            "quantity": 0,
            "discount_amount": 0,
            "total_amount": 0.00
        })
        .dropDuplicates()
        .orderBy("order_date")
    )

    return df_cleaned