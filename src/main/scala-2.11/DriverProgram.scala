/**
  * Created by sudha on 12/30/2015.
  */

import org.apache.spark.SparkConf
import org.apache.spark.{SparkContext, SparkConf}
object DriverProgram {

  def main(args: Array[String]){
    val conf = new SparkConf().setMaster("local").setAppName("Simple Application")
    val sc = new SparkContext(conf)

    val logData = sc.textFile("/home/sudhanshu/Desktop/devassignments/resources/mock_apache_pool-1-thread-1.data")
    val splitLogData = logData.flatMap(line => line.split(" +")).filter(_.matches("[0-9]{3}"))
    splitLogData.take(20).foreach(println)
  }

}
