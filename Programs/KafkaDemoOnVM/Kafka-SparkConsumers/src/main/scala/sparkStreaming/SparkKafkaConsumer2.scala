package sparkStreaming

/**
  * Created by YJ02 on 4/25/2017.
  */

import java.util.HashMap


import _root_.kafka.serializer.StringDecoder
import org.apache.hadoop.io.{LongWritable, IntWritable, Text}
import org.apache.hadoop.mapred.{SequenceFileOutputFormat, TextOutputFormat}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._


object SparkKafkaConsumer2 {

  def main(args: Array[String]) {


    val Array(brokerlist,zookeper, group, topics, numThreads, hdfslocation) = args
    var kafkaParams = Map(
      "bootstrap.servers"->brokerlist,
      "zookeeper.connect"->zookeper,
      "group.id"-> group,
      "security.protocol"->"SASL_PLAINTEXT",
       "auto.offset.reset"-> "smallest")



    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(100))
    //ssc.checkpoint("checkpoint")
    // val lines = createKafkaStream(ssc, "mytopic", "my-kafka.example.com:9092")

     val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder] (ssc, kafkaParams, Set(topics))

     /* val words = lines.flatMap(_.split(" "))
       val wordCounts = words.map(x => (x, 1L))
         .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    //lines.saveAsHadoopFiles ("/user/kafka/streaming_output","org.apache.hadoop.mapred.TextOutputFormat")*/
    kafkaStream.print()
    kafkaStream.saveAsTextFiles(hdfslocation)
   // kafkaStream.saveAsHadoopFiles(hdfslocation, "txt")
    ssc.start()
    ssc.awaitTermination()
  }

  /*
    def createKafkaStream(ssc: StreamingContext, kafkaTopics: String, brokers: String): DStream[(String, String)] = {
      val topicsSet = kafkaTopics.split(",").toSet
      val props = Map(
        "bootstrap.servers" -> brokers,
        "metadata.broker.list" -> brokers,
        "serializer.class" -> "kafka.serializer.StringEncoder",
        "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
        "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
        "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
        "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
      )
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, props, topicsSet)

    }*/

}
