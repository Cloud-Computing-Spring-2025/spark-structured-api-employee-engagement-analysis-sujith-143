from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg

# Initialize Spark session
spark = SparkSession.builder.appName("EmployeeSatisfactionEngagement").getOrCreate()

# Load the data into a DataFrame
df = spark.read.csv("employee_data.csv", header=True, inferSchema=True)

# Map Engagement Levels to numerical values: 'Low' = 1, 'Medium' = 2, 'High' = 3
engagement_mapped_df = df.withColumn(
    "EngagementLevelNum",
    when(col("EngagementLevel") == "Low", 1)
    .when(col("EngagementLevel") == "Medium", 2)
    .when(col("EngagementLevel") == "High", 3)
)

# Group by JobTitle and calculate the average EngagementLevel
avg_engagement_df = engagement_mapped_df.groupBy("JobTitle").agg(
    avg("EngagementLevelNum").alias("AvgEngagementLevel")
)

# Sort the results by the average Engagement Level in descending order
avg_engagement_df = avg_engagement_df.orderBy("AvgEngagementLevel", ascending=False)

# Show the results
avg_engagement_df.show()

# Save the result to CSV in the output folder
avg_engagement_df.write.csv("output/avg_engagement_by_jobtitle.csv", header=True)

# Stop the Spark session
spark.stop()
