package spark_kafka_integration;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerJava {
    public static void main(String[]args){
        Properties props =  new Properties();
        props.put("bootstrap.servers","your_brokerID");
        props.put("group-id","1");
        props.put("key.deserializer","org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<Integer,String> consumer = new KafkaConsumer<>(props);
        String topic = "my-topic";
        TopicPartition partition0 = new TopicPartition(topic,0);
        consumer.assign(Arrays.asList(partition0));

        while(true){
            ConsumerRecords<Integer,String> records = consumer.poll(100);
            for(ConsumerRecord<Integer,String>record:records){
                System.out.println(record.value());
            }
        }
    }
}
