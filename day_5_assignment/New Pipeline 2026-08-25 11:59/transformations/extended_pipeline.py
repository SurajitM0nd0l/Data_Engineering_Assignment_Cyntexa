from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table
def sales_cleaned_extended():
    df = spark.read.csv(
        "/Volumes/dev/bronze/raw/sales-3.csv",
        header=True,
        inferSchema=True
    )

    df_cleaned = (
        df.dropna(how="all")
        
        # 1. Clean Identifiers: Remove whitespace and standardize case
        .withColumn("order_id", F.upper(F.trim(F.col("order_id"))))
        .withColumn("customer_id", F.upper(F.trim(F.col("customer_id"))))
        .withColumn("product_id", F.upper(F.trim(F.col("product_id"))))
        
        # 2. Clean Quantity: Handle textual numbers and cast to integer
        .withColumn("quantity", F.lower(F.trim(F.col("quantity"))))
        .withColumn("quantity", 
            F.when(F.col("quantity") == "one", "1")
             .when(F.col("quantity") == "two", "2")
             .when(F.col("quantity") == "three", "3")
             .when(F.col("quantity") == "four", "4")
             .when(F.col("quantity") == "five", "5")
             .otherwise(F.col("quantity"))
        )
        .withColumn("quantity", F.col("quantity").cast("integer"))
        
        # 3. Clean Financials: Use RegEx to strip currency symbols and text, then cast to double
        .withColumn("total_amount", F.regexp_replace(F.col("total_amount"), r"[^\d\.\-]", "").cast("double"))
        .withColumn("discount_amount", F.regexp_replace(F.col("discount_amount"), r"[^\d\.\-]", "").cast("double"))
        
        # 4. Clean Dates: Use coalesce to attempt multiple date formats
        .withColumn("order_date_parsed", F.coalesce(
            F.to_date(F.col("order_date"), "yyyy-MM-dd"),
            F.to_date(F.col("order_date"), "MM/dd/yyyy"),
            F.to_date(F.col("order_date"), "yyyy/MM/dd"),
            F.to_date(F.col("order_date"), "MM-dd-yyyy"),
            F.to_date(F.col("order_date"), "MMM d, yyyy"),
            F.to_date(F.col("order_date"), "dd-MM-yy")
        ))
        # Fallback to current_date() if the date was completely unparseable (e.g., "Unknown", "TBD")
        .withColumn("order_date", F.coalesce(F.col("order_date_parsed"), F.current_date()))
        .drop("order_date_parsed")

        # 5. Handle Nulls: Use string defaults for ID columns
        .fillna({
            "order_id": "UNKNOWN",
            "customer_id": "UNKNOWN",
            "transaction_id": "UNKNOWN",
            "product_id": "UNKNOWN",
            "quantity": 0,
            "discount_amount": 0.0,
            "total_amount": 0.0
        })
        .dropDuplicates()
        # Note: orderBy is generally not supported in readStream without a watermark, 
        # but it is kept here to match your original pipeline structure.
        .orderBy("order_date")
    )

    return df_cleaned