from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp

s3_file_path = 's3://food-delivery-bucket-fn/food_delivery_data/' #sys.argv[1]
# Create a Spark session
spark = SparkSession.builder.appName("Transforming Food Delivery Data").getOrCreate()


# Read the data from S3
# df = spark.read.csv(s3_file_path, schema=schema, header=True)
df = spark.read.option("inferSchema", "true").option("header", "true").option("multiline", "true").csv(s3_file_path)
df.printSchema()

# Validate the data
    # 1. convert string to timestamp
df = df.withColumn("order_time", to_timestamp(df["order_time"], "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("delivery_time", to_timestamp(df["delivery_time"], "yyyy-MM-dd HH:mm:ss"))
    # 2. drop null values
df = df.na.drop()
    # 3. valid ratings and order_values
df = df.filter(
            (df.rating > 0) &
            (df.order_value > 0)
        )

    # 4. Transform the data
df2 = df.withColumn('Difference_In_Seconds',col("delivery_time").cast("long") - col('order_time').cast("long"))\
    .withColumn("Difference_In_Hours",round(col("Difference_in_Seconds")/3600))
df2.show()

    # 5. Categorize orders based on value
low_value = 50
high_value = 300

df_transformed = df2.withColumn('order_category', when(col('order_value') <= low_value, "Low").when((col('order_value') > low_value) & (col('order_value') <= high_value), 'Medium').otherwise("High"))
    # 6. drop columns not needed
df_transformed = df_transformed.drop("Difference_In_Seconds")

df_transformed.show(5)

        # Writing the transformed data into a staging area in Amazon S3
output_s3_path = 's3://food-delivery-bucket-fn/output_files/transformed_file.csv'
df_transformed.write.csv(output_s3_path, mode="overwrite")

username = 'awsuser'
password ='AWSuser12'
jdbc_url = f"jdbc:redshift://redshift-cluster-spark-load.cyy1gmfmb9hv.us-east-1.redshift.amazonaws.com:5439/dev?user={username}&password={password}"
aws_iam_role = "arn:aws:iam::590183810146:role/redshift-role"
temp_dir="s3://food-delivery-bucket-fn/temp-folder/"
target_table = 'food_delivery'
table_schema = "order_id string, customer_id string, restaurant_id string, order_time timestamp, customer_location string, restaurant_location string, order_value double, rating double, delivery_time timestamp"
create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {target_table} ({table_schema})
    USING io.github.spark_redshift_community.spark.redshift
    OPTIONS (
        url '{jdbc_url}',
        tempdir '{temp_dir}',
        dbtable '{target_table}',
        aws_iam_role '{aws_iam_role}'
    )
"""


# Now you can execute the create_table_query using Spark SQL
spark.sql(create_table_query).show()
#writing data to redshift with options
df_transformed.write \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url",jdbc_url) \
        .option("dbtable",target_table) \
        .option("tempdir",temp_dir) \
        .option("aws_iam_role", aws_iam_role) \
        .mode("overwrite") \
        .save()
print(f"Data written to Redshift table: {target_table}")
spark.stop()