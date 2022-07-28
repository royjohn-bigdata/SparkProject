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

object obj8 {
	def main (args:Array[String]): Unit={

			println("====started=====")

			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
			val sc = new SparkContext(conf)

			sc.setLogLevel("ERROR")

			val spark = SparkSession
			.builder()
			.config(conf)
			.getOrCreate()
			import spark.implicits._

			val df = spark.read
			.format("json")
			.option("multiline",true)
			.load("file:///c:/data/jc.json")

			df.show()
			df.printSchema()

			val flatdf = df.select(

					col("doorno"),
					col("orgname"),
					col("trainer"),
					col("address.permanent_city"),
					col("address.temporary_city")

					)

			val flatdf1 = df.withColumn("permenant_city", expr("address.permanent_city"))
			.withColumn("temporary_city",expr("address.temporary_city")).drop("address")

			flatdf.show()
			flatdf.printSchema()

			flatdf1.show()
			flatdf1.printSchema()
	}
}