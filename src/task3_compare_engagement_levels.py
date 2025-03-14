from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round as spark_round, when

def initialize_spark(app_name="Task3_Compare_Engagement_Levels"):
    """
    Initialize and return a SparkSession.
    """
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def load_data(spark, file_path):
    """
    Load the employee data from a CSV file into a Spark DataFrame.
    """
    schema = "EmployeeID INT, Department STRING, JobTitle STRING, SatisfactionRating INT, EngagementLevel STRING, ReportsConcerns BOOLEAN, ProvidedSuggestions BOOLEAN"
    df = spark.read.csv(file_path, header=True, schema=schema)
    return df

def map_engagement_level(df):
    """
    Map EngagementLevel from categorical to numerical values.
    """
    df = df.withColumn("EngagementScore", when(col("EngagementLevel") == "Low", 1)
                                        .when(col("EngagementLevel") == "Medium", 2)
                                        .when(col("EngagementLevel") == "High", 3)
                                        .otherwise(None))
    return df

def compare_engagement_levels(df):
    """
    Compare engagement levels across different job titles and identify the top-performing job title.
    """
    result_df = df.groupBy("JobTitle").agg(spark_round(avg("EngagementScore"), 2).alias("AvgEngagementLevel"))
    return result_df

def write_output(result_df, output_path):
    """
    Write the result DataFrame to a CSV file.
    """
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    """
    Main function to execute Task 3.
    """
    spark = initialize_spark()
    input_file = "/workspaces/spark-structured-api-employee-engagement-analysis-sujith-143/input/employee_data.csv"
    output_file = "/workspaces/spark-structured-api-employee-engagement-analysis-sujith-143/outputs/task3/engagement_levels_job_titles.csv"
    
    df = load_data(spark, input_file)
    df_mapped = map_engagement_level(df)
    result_df = compare_engagement_levels(df_mapped)
    
    result_df.show()  # Debugging output before writing
    write_output(result_df, output_file)
    
    spark.stop()

if __name__ == "__main__":
    main()