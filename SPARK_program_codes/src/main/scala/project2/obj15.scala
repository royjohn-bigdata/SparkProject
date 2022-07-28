package project2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import sys.process._
import java.io._

object obj15 {



	def main (args:Array[String]): Unit={

			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
					val sc = new SparkContext(conf)

					sc.setLogLevel("ERROR")

					val spark = SparkSession
					.builder()
					.config(conf)
					.getOrCreate()
					import spark.implicits._

					val df  = spark.read.format("json")
					.option("multiLine","true")
					.load("file:///C:/data/actors.json")

					df.show()
					df.printSchema()

					val flat1 = df.withColumn("Actors", explode(col("Actors")))
					flat1.show()
					flat1.printSchema()

					val flat2 =flat1.withColumn("children1", explode(col("Actors.children")))
					flat2.show()
					flat2.printSchema()

					println("\n========Flattened Data ===========\n")

					val flat3 = flat2.select(
							col("Actors.*"),
							col("country"),
							col("version"),
							col("children1")
							).drop("children").withColumnRenamed("children1", "children")
					flat3.persist()
					flat3.show()
					flat3.printSchema()

					println("\n========Complex Data ReGeneration ===========\n")

					val comp = flat3.groupBy(
							"Birthdate",
							"Born At",
							"BornAt",
							"age",
							"hasChildren",
							"hasGreyHair",
							"name",
							"photo",
							"weight",
							"wife",
							"country",
							"version"
							).agg(collect_list("children").alias("children"))
					comp.show()
					comp.printSchema()

					val comp1 = comp.groupBy("country", "version")
					.agg(collect_list(
							struct(
									"Birthdate",
									"Born At",
									"BornAt",
									"age",
									"children",
									"hasChildren",
									"hasGreyHair",
									"name",
									"photo",
									"weight",
									"wife"
									)
							).alias("Actors"))
					comp1.show()
					comp1.printSchema()

					val comp2 = comp1.select(
							"Actors",
							"country",
							"version"
							)

					comp2.show()
					comp2.printSchema()

	}

}