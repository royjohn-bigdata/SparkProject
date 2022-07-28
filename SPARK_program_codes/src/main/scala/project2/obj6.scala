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


object obj6 {
	def main (args:Array[String]): Unit={
			val config = new SparkConf().setAppName("first").setMaster("local[*]")
					val sc = new SparkContext(config)
					sc.setLogLevel("ERROR")
					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._

					val data = spark.read
					.format("csv")
					.option("header", true)
					.load("file:///c:/data/dept.csv")
					println("\n=========Raw Data=========")
					data.persist()
					data.show()

					val deptorder = Window
					.partitionBy(col("department"))
					.orderBy(col("salary").desc)
					
					val df = data.withColumn("Rank", dense_rank().over(deptorder))
					.filter("Rank=2")
					df.show()

					

	}
}