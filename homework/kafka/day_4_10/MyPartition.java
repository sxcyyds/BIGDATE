package day_4_10;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class MyPartition implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //获取value的值
        String s = value.toString();
        String trim = s.replaceAll("[^(0-9)]", "").trim();
        System.out.println("trim的值为"+trim);

        if(NumberUtils.isNumber(trim)){
            int a = Integer.parseInt(trim);
            if(a < 5){
                return 0;
            }
            if(a > 5 ){
                return 1;
            }

        }

        return 2;
    }

    @Override
    public void close() {
        //不管

    }

    @Override
    public void configure(Map<String, ?> configs) {
        //不管

    }
}
