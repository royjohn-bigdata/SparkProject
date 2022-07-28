package project2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.io.Source


object obj13 {
	def main (args:Array[String]): Unit={

			println("====started=====")

			println("====started=====")



			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			val spark = SparkSession
			.builder()
			.config(conf)
			.getOrCreate()
			import spark.implicits._




			val html = Source.fromURL("https://randomuser.me/api/0.8/?results=5")
			val urldata= html.mkString
			println(urldata)          // url datafetch

			val rdd = sc.parallelize(List(urldata)) // rdd conversion
			val df = spark.read.json(rdd)     // dataframe conversion
			df.show()
			df.printSchema()

			val flattendf = df.withColumn("results",explode(col("results")))
			val flattendf2= flattendf.select(
					col("nationality"),
					col("results.user.cell"),
					col("results.user.dob"),
					col("results.user.email"),
					col("results.user.gender"),
					col("results.user.location.*"),
					col("results.user.md5"),
					col("results.user.name.*"),
					col("results.user.password"),
					col("results.user.phone"),
					col("results.user.picture.*"),
					col("results.user.registered"),
					col("results.user.salt"),
					col("results.user.sha1"),
					col("results.user.sha256"),
					col("results.user.username"),
					col("seed"),
					col("version")

					)

			flattendf2.show(false)
			flattendf2.printSchema()

			flattendf2.write.format("csv").option("header","true").partitionBy("nationality")
			.mode("append").save("file:///C:/data/urldata1")

	}
}