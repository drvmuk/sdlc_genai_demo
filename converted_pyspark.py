from pyspark.sql import functions as F

# Read the source table and apply a predicate with pushdown
employees_df = spark.table("employees")
result_df = employees_df.filter(F.col("emp_id") == F.lit(1))
