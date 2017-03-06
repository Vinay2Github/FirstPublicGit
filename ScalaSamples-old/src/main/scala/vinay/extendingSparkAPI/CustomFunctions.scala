  package com.tesco.iit.extendingSparkAPI

import org.apache.spark.rdd.RDD
  import vinay.extendingSparkAPI.SalesRecord


  /**
  * Created by cloudera on 2/26/17.
  */

  class CustomFunctions(rdd:RDD[SalesRecord]) {
    def totalSales = rdd.map(_.itemValue).sum

  }