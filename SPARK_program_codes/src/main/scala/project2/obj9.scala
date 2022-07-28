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

object obj9 {

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
			.load("file:///c:/data/place.json")

			df.show()
			df.printSchema()

			val flat = df.select(
					col("place"),
					col("user.name"),
					col("user.address.number"),
					col("user.address.pin"),
					col("user.address.street")

					)

			flat.show(false)
			flat.printSchema()

	}
}