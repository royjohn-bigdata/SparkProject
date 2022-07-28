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

object SPARK_Program_obj4 {
	def main (args:Array[String]): Unit={
			val config = new SparkConf().setAppName("first").setMaster("local[*]")
					val sc = new SparkContext(config)
					sc.setLogLevel("ERROR")
					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._

					val sch = StructType(Array(
							StructField("id",IntegerType),
							StructField("name",StringType),
							StructField("comp_name",StringType),
							StructField("corrupt_rec",StringType)))


					val data = spark.read
					.format("csv")
					.schema(sch)
					.option("mode","PERMISSIVE")
					.option("columnNameOfCorruptRecord", "corrupt_rec")
					.option("header", true)
					.load("file:///c:/data/rd.csv")
					println("\n==Raw Data==")
					data.show(false)
					data.persist()

					println("\n==Valid Records==")
					val gooddata = data.filter(
							(col("corrupt_rec").isNull)    
							).drop("corrupt_rec")

					gooddata.show(false)

					println("\n==Bad Records==")
					val baddata = data.select(col("corrupt_rec"))
					.where(col("corrupt_rec").isNotNull)
					baddata.show(false)

					println("\n==Bad Records converted to Good Records==")
					val valid = baddata.
					select(
							split(col("corrupt_rec"),",").getItem(0).as("id"),
							split(col("corrupt_rec"),",").getItem(1).as("name"),
							concat(split(col("corrupt_rec"),",").getItem(2),
									lit(','),
									split(col("corrupt_rec"),",").getItem(3))
							.as("comp_name")
							).drop("corrupt_rec")

					valid.show(false)	

					val combine = gooddata.union(valid)
					println("\n==validated records==")
					combine.show(false)	

	}
}