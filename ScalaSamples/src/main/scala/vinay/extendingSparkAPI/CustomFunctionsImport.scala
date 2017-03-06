
package vinay.extendingSparkAPI

import org.apache.spark.rdd.RDD
import vinay.extendingSparkAPI.{CustomFunctions, SalesRecord}

/**
  * Created by cloudera on 2/27/17.
  */
object CustomFunctionsImport {

   implicit def addCustomFunctions(rdd: RDD[SalesRecord])= {

     println("Inside Implict function")
     new CustomFunctions(rdd)

   }

  }