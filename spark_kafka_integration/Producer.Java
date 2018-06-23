package spark_kafka_integration;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ProducerJava {
    public static void main(String[]args){
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        String appName = "my-app";
        String master = "local[2]";

        SparkConf conf  = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("spark.txt");
        lines  = lines.map(str->str.replace(".","\n"));
        lines  = lines.map(str->str.replace("!","\n"));
        lines  = lines.map(str->str.replace("?","\n"));
        System.out.println("Preprocessing done successfully");

        List<String> s = new ArrayList<>();
        s.addAll(lines.collect());

        Properties props =  new Properties();
        props.put("bootstrap.servers","your_brokerID");
        props.put("key.serializer","org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        Producer<Integer,String> producer =  new KafkaProducer<>(props);

        int n=s.size();
        for(int i=0;i<n;i++){
            producer.send(new ProducerRecord<>("my-topic",0,i,s.get(i)));
        }
        producer.close();
        System.out.println("Successfully published all messages");
    }
}
