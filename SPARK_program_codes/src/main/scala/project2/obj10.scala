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


object obj10 {
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
			.load("file:///c:/data/donut.json")

			df.show()
			df.printSchema()

			val flatdf = df.select(

					col("id"),
					col("name"),
					col("type"),
					col("image.height").alias("iheight"),
					col("image.url").alias("iurl"),
					col("image.width").alias("iwidth"),
					col("thumbnail.height").alias("theight"),
					col("thumbnail.url").alias("turl"),
					col("thumbnail.width").alias("twidth")
					)


			flatdf.show()
			flatdf.printSchema()

			val comdf = flatdf.select(
					col("id"),
					col("name"),
					col("type"),

					struct(

							struct(
									col("iheight"),
									col("iwidth"),
									col("iheight")
									).alias("image"),


							struct(
									col("theight"),
									col("turl"),
									col("twidth")
									).alias("thumbnail")       



							).alias("details")
					)
			comdf.persist()
			comdf.show()
			comdf.printSchema()

			comdf.write.format("json").mode("overwrite").save("file:///c:/data/donutcomplex")


	}
}