package kafka;
/* THis code reads messages from a file and publishes to Kafka. Each line in a file is treated as single message.
* Sample command to run this code.
* java -cp kafka-1.0-jar-with-dependencies.jar kafka.SimpleProducer -numMsgs 10 -topic test -brokers sandbox.hortonworks.com:6667 -file /root/kafka/picked_order_messages/picked_order_messagebody.txt -messagingMode Sync
spark-submit --class sparkStreaming.VM_Hive_Checkpoint /root/kafka/kafka-1.0-jar-with-dependencies.jar sandbox.hortonworks.com:6667 localhost:2181 samplekafkaconsumer KVJ1 1 /warehouse_dev/yj02/kafka test.KVJ_Topic_Offsets

*
* */

//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class SimpleProducer_VM {
    public static void main(String[] args) {

        int timeOut=1000; // in Milli Second.
        String messagingMode="FandF";
        int events=10;
        String topic=null;
        String brokers=null;
        String ipFile="";
        String msg_key="Dummy_Key";

        for (int k=0; k<args.length;k++){
            if(args[k].equals("-topic")){k++; topic=args[k]; }
            else if(args[k].equals("-brokers")){k++; brokers=args[k]; }
            else if(args[k].equals("-timeOut")){k++; timeOut=Integer.parseInt(args[k]); }
            else if(args[k].equals("-numMsgs")){k++; events=Integer.parseInt(args[k]); }
            else if(args[k].equals("-messagingMode")){k++; messagingMode=args[k]; }
            else if(args[k].equals("-messagKay")){k++; msg_key=args[k]; }
            else if(args[k].equals("-file")){k++; ipFile=args[k]; }
        }

        System.out.println("Supplied Arguments are ");
        System.out.println("Message Key Provided= "+msg_key);
        System.out.println(" number of messages to publish="+events);
        System.out.println(" Topic to publish = " +topic);
        System.out.println(" Known broker List = "+brokers);
        System.out.println(" TimeOut in miliSeconds = "+timeOut);
        System.out.println(" Messaging Mode = "+messagingMode);

        if (topic.equals(null)||brokers.equals(null)){

            System.out.println("Error: Need to specify valid topic name and Broker List");
            System.exit(1);

              }
/*
* buffer.memory : This sets the amount of memory the producer will use to buffer messages waiting to be sent to brokers.
* compression.type: By default, messages are sent uncompressed. This parameter can be set to snappy, gzip or lz4. Sanpy provides decent compression and needs low cpu. GZIP provides better compression but needs more CPU and time.
*batch.size:When multiple records are sent to the same partition, the producer will batch them together. This parameter controls the amount of memory in bytes (not messages!) that will be used for each batch.
* linger.ms:linger.ms control the amount of time we wait for additional messages before sending the current batch. KafkaProducer sends a batch of messages either when the current batch is full or when linger.ms limit is reached.
*max.in.flight.requests.per.connection: This controls how many messages the producer will send to the server without receiving responses. Setting this high can increase memory usage while improving throughput, although setting it too high can reduce throughput as batching becomes less efficient. Setting this to 1 will guarantee that messages will be written to the broker in the order they were sent, even when retries occure.
*
* */

        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
       // props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(ProducerConfig.ACKS_CONFIG, "all");//The acks parameter controls how many partition replicas must receive the record before the producer can consider the write successful.
        props.put("client.id", "ProducerExample");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("timeout.ms",timeOut );




        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        if (messagingMode.equals("Sync")) {
            try {
                BufferedReader br = null;
                br = new BufferedReader(new FileReader(ipFile));
                String sCurrentLine;
                //for (int i = 0; i < events; i++) {
                while ((sCurrentLine = br.readLine()) != null) {


                    // long t = System.currentTimeMillis();

                    // (Topic_Name,Key, Value)
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic,msg_key, sCurrentLine);


                    System.out.println("Before Publishing Sync message ");
                  //  System.out.println("<<<<<"+sCurrentLine+">>>>>>>>>");
                    producer.send(record).get();
                    System.out.println("Published message ");

                }}catch(Exception ex){
                    ex.printStackTrace();
                }


        }

        else if (messagingMode.equals("Async")) {

            for (int i = 0; i < events; i++) {
                long t = System.currentTimeMillis();

                // (Topic_Name,Key, Value)
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, "Sample_Key", i + "th Sample_Value=" + t);
                try {

                    System.out.println("Before Publishing Sync message " + i);
                    producer.send(record, new SampleProducerCallback());
                    System.out.println("Published message " + i);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }

        else {
            for (int i = 0; i < events; i++) {
                long t = System.currentTimeMillis();

                // (Topic_Name,Key, Value)
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, "Sample_Key", i + "th Sample_Value=" + t);
                try {

                    System.out.println("Before Publishing Fire and Forget message " + i);
                    producer.send(record).get();
                    System.out.println("Published message " + i);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }

        }

         producer.close();
    }


    private static class SampleProducerCallback implements Callback {
           public void onCompletion(RecordMetadata recordMetadata, Exception e) {
               if (e != null) {
                   e.printStackTrace();
               }
           }}


}