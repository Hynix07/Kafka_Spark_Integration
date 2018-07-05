package spark_kafka_integration;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;

import java.util.*;

public class sparkStreamingConsumer{
    public static void main(String[]args) throws InterruptedException{
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        //get the stream context
        JavaStreamingContext jssc  = sparkConnection.getStreamConnection(3);

        //setup the stream configurations
        Map<String,Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers","127.0.0.1:9092");
        kafkaParams.put("group.id","1");
        kafkaParams.put("key.deserializer","org.apache.kafka.common.serialization.IntegerDeserializer");
        kafkaParams.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        Set<String> topic = Collections.singleton("my-topic");

        //build the stream
        JavaInputDStream<ConsumerRecord<String,String>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topic,kafkaParams)
                );

        //print the messages from the stream
        JavaDStream<String> lines  = messages.map(ConsumerRecord::value);
        lines.print();

        //run the stream
        jssc.start();
        jssc.awaitTermination();
    }
}
        
