from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("EmployeeSatisfactionEngagement").getOrCreate()

# Load the data into a DataFrame
df = spark.read.csv("employee_data.csv", header=True, inferSchema=True)

# Filter employees who feel valued (SatisfactionRating >= 4) but did not provide suggestions
valued_non_contributors_df = df.filter((col("SatisfactionRating") >= 4) & (col("ProvidedSuggestions") == False))

# Show the number of such employees
print(f"Number of employees feeling valued but not providing suggestions: {valued_non_contributors_df.count()}")

# Calculate the proportion of these employees in the organization
total_employees = df.count()
proportion = (valued_non_contributors_df.count() / total_employees) * 100

# Output the proportion result
print(f"Proportion of employees feeling valued but not providing suggestions: {proportion:.2f}%")

# Save the result to CSV in the output folder
valued_non_contributors_df.write.csv("output/valued_non_contributors.csv", header=True)

# Stop the Spark session
spark.stop()
