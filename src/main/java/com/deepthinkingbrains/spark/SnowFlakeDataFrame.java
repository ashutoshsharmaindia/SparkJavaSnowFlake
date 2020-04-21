package com.deepthinkingbrains.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.Properties;
import org.apache.spark.sql.SparkSession;

public class SnowFlakeDataFrame
{

	public static void main(String[] args)
	{
		//First get handle to Spark Session which is the first step
		SparkSession spark = SparkSession
	    	      .builder()
	    	      .appName("SnowFlakeDataFrame")
	    	      .config("spark.master", "local")//change this line or override it from spark-submit. This work fines on AWS EMR clusters as well as Spark Dev environment on Eclipse
	    	      .getOrCreate();
	    		   		
		//First you need to create the properties for Snowflake configuration 
		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "YourSnowFlakeUserName"); //SnowFlake UserName
		connectionProperties.put("password", "YourSnowFlakePassword"); //SnowFlake password
		connectionProperties.put("account", "YourSnowFlakeAccount"); //SnowFlake Account
		/*
		 * Now here you need to pass the full JDBC URL
		 * I have the free 30 days Snowflake Account - i am using that. I am not hiding or redacting it for your help.
		 * You need to specify the exact schema.Table from where you are reading the data from
		 * Here I am reading the data from CALL_CENTER table under TPCDS_SF100TCL which is under SNOWFLAKE_SAMPLE_DATA.
		 * This comes by default in Snowflake.
		 */
		Dataset<Row> jdbcDF1 = spark.read().jdbc("jdbc:snowflake://DYA91493.us-east-1.snowflakecomputing.com/",
		"SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CALL_CENTER", connectionProperties);
		
		/*
		 * Now i am going to just groupBy CC_NAME and show the count of the regions.
		 */
		Dataset<Row> jdbcDF2 = jdbcDF1.groupBy("CC_NAME").count(); 
		//Now show this on the console
		jdbcDF2.show();
		 
	   
	}

}
