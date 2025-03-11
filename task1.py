from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round

# Initialize Spark session
spark = SparkSession.builder.appName("EmployeeSatisfactionEngagement").getOrCreate()

# Load the data into a DataFrame
df = spark.read.csv("employee_data.csv", header=True, inferSchema=True)

# Relaxed Filter Criteria: Employees with SatisfactionRating >= 4 and EngagementLevel in ['High', 'Medium']
filtered_df = df.filter((col("SatisfactionRating") >= 4) & (col("EngagementLevel").isin("High", "Medium")))

# Count total employees in each department
total_count_df = df.groupBy("Department").agg(count("*").alias("total_count"))

# Count filtered employees in each department
filtered_count_df = filtered_df.groupBy("Department").agg(count("*").alias("filtered_count"))

# Join the total and filtered counts
percentage_df = total_count_df.join(filtered_count_df, on="Department", how="left")

# Calculate the percentage of employees meeting the criteria
percentage_df = percentage_df.withColumn("percentage", round((col("filtered_count") / col("total_count")) * 100, 2))

# Filter departments where the percentage exceeds 50%
result_df = percentage_df.filter(col("percentage") > 50)

# Save the result to CSV in the output folder
result_df.select("Department", "percentage").write.csv("output/department_high_satisfaction.csv", header=True)

# Stop the Spark session
spark.stop()
