package project2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import sys.process._
import java.util._
import java.io._
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SaveMode



object obj12 {

	def main (args:Array[String]): Unit={
			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
					val sc = new SparkContext(conf)

					sc.setLogLevel("ERROR")

					val spark = SparkSession
					.builder()
					.config(conf)
					.getOrCreate()
					import spark.implicits._

					val data = spark.read.format("json").option("multiline", true)
					.load("file:///c:/data/zomato.json")
					
					data.show()
					data.printSchema()

					var exprest = data
					.withColumn("restaurants_new", explode(col("restaurants")))
										
					exprest.show()
					exprest.printSchema()
					
				 exprest = exprest
					.withColumn("offers_new", col("restaurants_new.restaurant")
					    .getItem("offers"))
				   .withColumn("offers_new", explode(col("offers_new")))
																
					exprest.show()
					exprest.printSchema()
					
					exprest = exprest
					.withColumn("zomoto_new", col("restaurants_new.restaurant")
					    .getItem("zomato_events"))
					.withColumn("zomoto_new", explode(col("zomoto_new")))
					   
					exprest.show()
					exprest.printSchema()
				

	}
}