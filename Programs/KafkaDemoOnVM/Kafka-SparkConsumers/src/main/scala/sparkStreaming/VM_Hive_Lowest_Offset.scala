package sparkStreaming

/**
  * Created by YJ02 on 4/25/2017.
  * spark-submit --class sparkStreaming.VM_Hive_Lowest_Offset /root/kafka/kafka-1.0-jar-with-dependencies.jar sandbox.hortonworks.com:6667 samplekafkaconsumer KVJ1 test.KVJ_Topic_Offsets
  */

import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, HashMap}


import _root_.kafka.MySqlExecutor
import _root_.kafka.common.TopicAndPartition
import _root_.kafka.message.MessageAndMetadata
import _root_.kafka.serializer.StringDecoder
import org.apache.hadoop.io.{LongWritable, IntWritable, Text}
import org.apache.hadoop.mapred.{SequenceFileOutputFormat, TextOutputFormat}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.{TaskContext, SparkContext, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{InputDStream, DStream}
import org.apache.spark.streaming.kafka._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.explode
import scala.collection.JavaConversions._

import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.spark.streaming.kafka.LocationStrategies.PreferConsistent

object VM_Hive_Lowest_Offset {


  def main(args: Array[String]) {

    val Array(brokerlist,group, topics, offsetTable) = args

    // }
    // else{fromOffsets.add(TopicAndPartition(topics,0), "0".toLong);fromOffsets.add(TopicAndPartition(topics,1), "0".toLong);fromOffsets.add(TopicAndPartition(topics,2), "0".toLong)}



    var kafkaParams = Map(
      "bootstrap.servers"->brokerlist,
      "group.id"-> group,
      "security.protocol"->"SASL_PLAINTEXT",
     "auto.offset.reset"-> "smallest")/*,
      "auto.commit.offset"->"false")*/


    //val fromOffsets= Map(TopicAndPartition(topics,0)->messageOffset.toLong)



    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    val ssc = new StreamingContext(sc, Seconds(100))



    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder] (ssc, kafkaParams, Set(topics))


    kafkaStream.foreachRDD(rdd=>{

      if (rdd.count()>0) {

        val pickedOrderMessage = rdd.map(x => {
          val record: (String, String) = x
          record._2
        }
        )
        val pickedOrderDF = hiveContext.read.json(pickedOrderMessage)

        //val flattenedPickedOrder = pickedOrderDF.withColumn("lintem_flat",explode($"lineItems"))//)select($"orderId",$"recommendedFulfillmentTime",$"tenantId", explode($"lineItems").as("lineItems_flat"))
        val flattenedPickedOrder = pickedOrderDF.select($"orderId", $"recommendedFulfillmentTime", $"tenantId", explode($"lineItems").as("lineItems_flat"))
        val pickedOrderWithIndividualColumns = flattenedPickedOrder.select($"orderId", $"recommendedFulfillmentTime", $"tenantId", $"lineItems_flat.lineItemId", $"lineItems_flat.tpnb")
        pickedOrderWithIndividualColumns.registerTempTable("tmp_pickedorders");
        hiveContext.sql("insert into tmp_picked_order select * from tmp_pickedorders")

        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        rdd.foreachPartition { iter =>
          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
          println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
          val topic: String = s"${o.topic}"
          val partition: String = s"${o.partition}"
          val fromOffset: String = s"${o.fromOffset}"
          val untilOffset: String = s"${o.untilOffset}"

          val dateTimeFormat = new SimpleDateFormat("yyy:MM:dd:hh:mm:ss")
          val now = Calendar.getInstance().getTime()
          dateTimeFormat.format(now)
          val offsets = s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}"
          val sql: String = "insert into  " + offsetTable + " (insert_date_time,topic,partition,fromoffset, untilloffset) values(\"" + dateTimeFormat.format(now) + "\",\"" + topic + "\",\"" + partition + "\",\"" + fromOffset + "\",\"" + untilOffset + "\")"
          println("<<<<<<<<<<<<<<<<<<" + sql + ">>>>>>>>>>>>>>>>>>>>>>>>>")
          MySqlExecutor.insert2MySQL("jdbc:mysql://localhost:3306/test", "root", "", sql)

        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }




}
