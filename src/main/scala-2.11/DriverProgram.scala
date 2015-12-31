/**
  * Created by sudha on 12/30/2015.
  */

import org.apache.spark.SparkConf
import org.apache.spark.{SparkContext, SparkConf}
class DriverProgram {

  def main(args: Array[String]){
    val conf = new SparkConf().setMaster("local").setAppName("Simple Application")
    val sc = new SparkContext(conf)


  }

}
