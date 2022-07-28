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
import java.util.Calendar


object obj11 {
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
	
		println("==========Raw Data=========")			 
		 val data = spark.read.format("json").option("multiline",true)
		 .load("file:///c:/data/medication.json")
		data.show() 	
		data.printSchema()
		
		val exp1 = data.withColumn("medications", explode(col("medications")))
		exp1.printSchema()
		
		val exp2 =   exp1.withColumn("medications.aceInhibitors", explode(col("medications.aceInhibitors"))).withColumnRenamed("medications.aceInhibitors", "aceInhibitors")
		             .withColumn("medications.antianginal",explode(col("medications.antianginal"))).withColumnRenamed("medications.antianginal", "antianginal")
		             .withColumn("medications.anticoagulants", explode(col("medications.anticoagulants"))).withColumnRenamed("medications.anticoagulants", "anticoagulants")
		             .withColumn("medications.betaBlocker", explode(col("medications.betaBlocker"))).withColumnRenamed("medications.betaBlocker", "betaBlocker")
		             .withColumn("medications.diuretic", explode(col("medications.diuretic"))).withColumnRenamed("medications.diuretic", "diuretic")
		             .withColumn("medications.mineral", explode(col("medications.mineral"))).withColumnRenamed("medications.mineral", "mineral")
		             .drop("medications")
		exp2.printSchema()        
		
		val exp3 = exp2.select(
            		            col("aceInhibitors.dose").alias("aceInhibitors.dose"),
            		            col("aceInhibitors.name").alias("aceInhibitors.name"),
            		            col("aceInhibitors.pillCount").alias("aceInhibitors.pillCount"),
            		            col("aceInhibitors.refills").alias("aceInhibitors.refills"),
            		            col("aceInhibitors.route").alias("aceInhibitors.route"),
            		            col("aceInhibitors.sig").alias("aceInhibitors.sig"),
            		            col("aceInhibitors.strength").alias("aceInhibitors.strength"),
            		            
            		            col("antianginal.dose").alias("antianginal.dose"),
            		            col("antianginal.name").alias("antianginal.name"),
            		            col("antianginal.pillCount").alias("antianginal.pillCount"),
            		            col("antianginal.refills").alias("antianginal.refills"),
            		            col("antianginal.route").alias("antianginal.route"),
            		            col("antianginal.sig").alias("antianginal.sig"),
            		            col("antianginal.strength").alias("antianginal.strength"),
            		            
            		            col("anticoagulants.dose").alias("anticoagulants.dose"),
            		            col("anticoagulants.name").alias("anticoagulants.name"),
            		            col("anticoagulants.pillCount").alias("anticoagulants.pillCount"),
            		            col("anticoagulants.refills").alias("anticoagulants.refills"),
            		            col("anticoagulants.route").alias("anticoagulants.route"),
            		            col("anticoagulants.sig").alias("anticoagulants.sig"),
            		            col("anticoagulants.strength").alias("anticoagulants.strength"),
            		            
            		            col("betaBlocker.dose").alias("betaBlocker.dose"),
            		            col("betaBlocker.name").alias("betaBlocker.name"),
            		            col("betaBlocker.pillCount").alias("betaBlocker.pillCount"),
            		            col("betaBlocker.refills").alias("betaBlocker.refills"),
            		            col("betaBlocker.route").alias("betaBlocker.route"),
            		            col("betaBlocker.sig").alias("betaBlocker.sig"),
            		            col("betaBlocker.strength").alias("betaBlocker.strength"),
            		            
            		            col("diuretic.dose").alias("diuretic.dose"),
            		            col("diuretic.name").alias("diuretic.name"),
            		            col("diuretic.pillCount").alias("diuretic.pillCount"),
            		            col("diuretic.refills").alias("diuretic.refills"),
            		            col("diuretic.route").alias("diuretic.route"),
            		            col("diuretic.sig").alias("diuretic.sig"),
            		            col("diuretic.strength").alias("diuretic.strength"),
            		            
            		            col("mineral.dose").alias("mineral.dose"),
            		            col("mineral.name").alias("mineral.name"),
            		            col("mineral.pillCount").alias("mineral.pillCount"),
            		            col("mineral.refills").alias("mineral.refills"),
            		            col("mineral.route").alias("mineral.route"),
            		            col("mineral.sig").alias("mineral.sig"),
            		            col("mineral.strength").alias("mineral.strength")
            		            
		            
		                    )
		                              
		                    exp3.printSchema()
		  
		                      
	}

}