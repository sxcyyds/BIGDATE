package day_4_10;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.HashMap;

public class MyProducer_MyPartition {
    public static void main(String[] args) {
        HashMap<String, Object> map = new HashMap<>();
        map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop11:9092,hadoop12:9092,hadoop13:9092");
        map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        //自定义分区策略
        map.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"day_4_10.MyPartition");
        //开启生产者幂等性
        map.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);

        //创建一个生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(map);
        for (int i = 0; i <20 ; i++) {
            producer.send(new ProducerRecord<>("topicF","Hello"+i));
        }
        producer.close();


    }
}
