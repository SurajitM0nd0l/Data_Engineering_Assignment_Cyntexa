from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table
def sales_cleaned_pipeline():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load("/Volumes/dev/bronze/raw/sales/sales_csv/")
    )

    df_cleaned = (
        df.drop("_rescued_data")
        .dropna(how="any")
        .dropDuplicates()
        .withColumn("sale_date", F.coalesce(F.col("sale_date"), F.current_date()))
    )

    return df_cleaned