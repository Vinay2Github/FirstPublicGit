package sparkStreaming

/**
  * Created by YJ02 on 4/25/2017.
  */

import java.util.HashMap


import org.apache.hadoop.io.{LongWritable, IntWritable, Text}
import org.apache.hadoop.mapred.{SequenceFileOutputFormat, TextOutputFormat}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._


object SparkKafkaConsumer {

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum><group> <topics> <numThreads>")
      System.exit(1)
    }

    val zkQuorum=args(0)
    val group=args(1)
    val topics=args(2)
    val numThreads = args(3)
    val hdfslocation= args(4)

    println("zkQuorum = "+zkQuorum)
    println("group = "+group)
    println("topics = "+topics)
    println("numThreads = "+numThreads )

    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(30))
    //ssc.checkpoint("checkpoint")
   // val lines = createKafkaStream(ssc, "mytopic", "my-kafka.example.com:9092")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    topicMap.foreach(x=>println("Topic map values="+x))

    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
 /*   val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2) */
    //lines.saveAsHadoopFiles ("/user/kafka/streaming_output","org.apache.hadoop.mapred.TextOutputFormat")
    lines.print()
    lines.saveAsHadoopFiles(hdfslocation,"txt");
    lines.saveAsTextFiles(hdfslocation)


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
