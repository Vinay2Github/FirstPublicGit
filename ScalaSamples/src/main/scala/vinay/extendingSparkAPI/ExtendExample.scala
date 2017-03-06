package vinay.extendingSparkAPI

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import vinay.extendingSparkAPI.SalesRecord

import vinay.extendingSparkAPI.CustomFunctionsImport._





object ExtendExample {



  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[1]", "extendingspark")
    val dataRDD = sc.textFile("file:///home/cloudera/SparkWorkSpace/ExtendingSparkAPI/extendingSparkAPI_sampleData.csv")

    val salesRecordRDD = dataRDD.map(row => {
      val colValues = row.split(",")
      new SalesRecord(colValues(0),colValues(1),colValues(2),colValues(3).toDouble)
    })
    //val x=salesRecordRDD.map(_.itemValue)
    //println("dataRDD is "+dataRDD.sum());
    //println("salesRecordRDD is "+salesRecordRDD.sum());
    //println("X is "+x.sum());
    println("Out Put="+salesRecordRDD.totalSales)




  }


  }