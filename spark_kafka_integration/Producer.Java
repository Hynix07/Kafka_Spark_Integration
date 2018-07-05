package spark_kafka_integration;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class producer {
    public static void main(String[]args){
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        //get the spark context
        JavaSparkContext sc  = sparkConnection.getContext();

        //preprocess the file spark.txt
        JavaRDD<String> lines = sc.textFile("spark.txt");
        lines  = lines.map(str->str.replace(".","\n"));
        lines  = lines.map(str->str.replace("!","\n"));
        lines  = lines.map(str->str.replace("?","\n"));
        System.out.println("Preprocessing done successfully");

        List<String> s = new ArrayList<>();
        s.addAll(lines.collect());

        //setup the producer configurations
        Properties props = new Properties();
        props.put("bootstrap.servers","127.0.0.1:9092");
        props.put("key.serializer","org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        Producer<Integer,String> producer =  new KafkaProducer<>(props);

        //send the messages
        int n=s.size();
        for(int i=0;i<n;i++){
            producer.send(new ProducerRecord<>("my-topic",0,i,s.get(i)));
        }
        producer.close();
        System.out.println("Successfully published all messages");
    }
}


        
        
