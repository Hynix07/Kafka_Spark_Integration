package spark_kafka_integration;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;

import java.util.*;

public class sparkStreamConsumer{
    public static void main(String[]args) throws InterruptedException{
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        
        String appName = "my-app";
        String master = "local[2]";

        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaStreamingContext jssc = new JavaStreamingContext(conf,Durations.seconds(2));

        Map<String,Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers","your_brokerID");
        kafkaParams.put("group.id","1");
        kafkaParams.put("key.deserializer","org.apache.kafka.common.serialization.IntegerDeserializer");
        kafkaParams.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        Set<String> topic = Collections.singleton("my-topic");

        JavaInputDStream<ConsumerRecord<String,String>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topic,kafkaParams),
        );

        JavaDStream<String> lines  = messages.map(ConsumerRecord::value);
        lines.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
