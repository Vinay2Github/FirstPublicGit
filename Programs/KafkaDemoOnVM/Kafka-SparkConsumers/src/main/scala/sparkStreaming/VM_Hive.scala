package sparkStreaming

/**
  * Created by YJ02 on 4/25/2017.
  */

import java.util.HashMap


import _root_.kafka.MySqlExecutor
import _root_.kafka.serializer.StringDecoder
import org.apache.hadoop.io.{LongWritable, IntWritable, Text}
import org.apache.hadoop.mapred.{SequenceFileOutputFormat, TextOutputFormat}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.{TaskContext, SparkContext, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.kafka.clients.consumer.ConsumerRecord

object VM_Hive {

  val schema = StructType(Array(
    StructField("Col1", StringType, true),
    StructField("Col2",StringType, true)))

  def main(args: Array[String]) {


    val Array(brokerlist,zookeper, group, topics, numThreads, hdfslocation) = args
    var kafkaParams = Map(
      "bootstrap.servers"->brokerlist,
      "zookeeper.connect"->zookeper,
      "group.id"-> group,
      "security.protocol"->"SASL_PLAINTEXT",
      "auto.offset.reset"-> "smallest",
    "auto.commit.offset"->"false")



    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    val ssc = new StreamingContext(sc, Seconds(100))



    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder] (ssc, kafkaParams, Set(topics))
    //kafkaStream.checkpoint("checkpoint")
    /*kafkaStream.map(rec=> {
      val rec1:(String, String) = rec
    rec1._2
    }
    )*/

    kafkaStream.foreachRDD(rdd=>{

      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges



     rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        val offsets=s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}"
       val sql:String="insert into test.kafka_msg_offsets (col) values(\""+offsets+"\")"
       //MySqlExecutor.insert2MySQL("jdbc:mysql://localhost:3306/test","root","",sql)

      }

    //  val hiveContext = new HiveContext(rdd.sparkContext)
   //   val sqlContext = new SQLContext(rdd.sparkContext)
//      sqlContext.

      val a1=rdd.map(x=> {
          val record: (String, String) = x
        try{val y = record._2.split(",")
        //if(y.length==1){Row("old and Dummy","old and Dummy")}
        Row(y(0), y(1))
      }

    catch {case e :Exception=> println(record.toString()+"****************Exception**********"+e.getMessage+"**********************"); Row("null","null")
    case _ => Row("null","null")
            }
      } )

      val dF = hiveContext.createDataFrame(a1, schema)
      dF.registerTempTable("Temp_kafka")
      hiveContext.sql("insert into kafka_test select * from Temp_kafka")



    })

    ssc.start()
    ssc.awaitTermination()
  }




}
