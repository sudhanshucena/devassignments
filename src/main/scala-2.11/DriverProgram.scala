/**
  * Created by sudha on 12/30/2015.
  */

import org.apache.spark.{SparkContext, SparkConf}

object DriverProgram {

  def main(args: Array[String]){
    val conf = new SparkConf().setMaster("local").setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val res2= sqlContext.sql("FROM genre SELECT id, name")
    res2.write.parquet("hdfs:///user/guest/people.parquet")
    val parquetFile = sqlContext.read.parquet("hdfs:///user/guest/people.parquet")
    parquetFile.registerTempTable("parquetFile")
    sqlContext.sql("SELECT * FROM parquetFile ").collect().foreach(println)
  }
}
