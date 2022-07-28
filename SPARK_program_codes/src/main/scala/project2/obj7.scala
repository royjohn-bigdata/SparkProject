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


object obj7 {
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

			val sqldf = spark.read.format("jdbc")
			.option("url", "jdbc:mysql://192.168.1.102:3306/roy")
			.option("driver", "com.mysql.jdbc.Driver")
			.option("dbtable", "employees")
			.option("user", "root")
			.option("password", "cloudera")
			.load()
			sqldf.show(sqldf.count().toInt)

			val agg_window = Window.partitionBy("dept","position")

			val rnk_window = Window.partitionBy("dept","position")
			.orderBy(col("salary").desc)

			val df = sqldf.withColumn("Min_Salary", min("salary").over(agg_window))
			.withColumn("Max_Salary",max("salary").over(agg_window))
			.select("dept","position", "Min_Salary","Max_Salary").dropDuplicates()
			.orderBy(col("dept").desc)
			df.show()

			val df1 = sqldf.withColumn("Higest_Salary",dense_rank().over(rnk_window))
			.filter(col("Higest_Salary") === 1).orderBy(col("dept").desc)
			df1.show()

			df.write.format("jdbc").mode(SaveMode.Overwrite)
			.option("url", "jdbc:mysql://192.168.1.102:3306/roy")
			.option("driver", "com.mysql.jdbc.Driver")
			.option("dbtable", "emp_sal_range")
			.option("user", "root")
			.option("password", "cloudera")
			.save()

			df1.write.format("jdbc").mode(SaveMode.Overwrite)
			.option("url", "jdbc:mysql://192.168.1.102:3306/roy")
			.option("driver", "com.mysql.jdbc.Driver")
			.option("dbtable", "emp_highpay")
			.option("user", "root")
			.option("password", "cloudera")
			.save()


			println("============Completed=========")

	}
}