package day_4_10;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.HashMap;

public class MyConsumer_MyPartition {
    public static void main(String[] args) {
        HashMap<String, Object> map = new HashMap<>();
        map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop11:9092,hadoop12:9092,hadoop13:9092");
        map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);

        //设置组
        map.put(ConsumerConfig.GROUP_ID_CONFIG,"MyPartition");
        //轮询循环
//        map.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,"org.apache.kafka.clients.consumer.RoundRobinAssignor");




        //创建Consumer
        KafkaConsumer<String, String> consumerOne = new KafkaConsumer<>(map);
        //订阅topic数据
        ArrayList<String> list = new ArrayList<>();
        list.add("topicF");
        consumerOne.subscribe(list);

        while (true){
            ConsumerRecords<String, String> poll = consumerOne.poll(2000);
            for (ConsumerRecord p:poll){
                System.out.println("值为"+p.value()+"分区号为"+p.partition());
            }
        }




    }
}
