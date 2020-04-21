"# SparkJavaSnowFlake" 
Lots of stray code on Internet for Python and Scala to connect to Snowflake but not much in Java.

This code will help you to connect Spark(in Java) to Snowflake Table - read the data and print it.

It's using Apache Spark 2.4.4 with Java 1.8 and using Snowflake JDBC driver 3.11.1

When submitting on Spark on EMR cluster using Spark-submit use this command:
"spark-submit --class com.deepthinkingbrains.spark.SnowFlakeDataFrame spark-0.0.1-SNAPSHOT.jar"

If you want to see how the output in local Eclispe DEV environment, please refer Output.txt
