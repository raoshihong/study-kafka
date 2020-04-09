package com.rao.study.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class MyPartition implements Partitioner {

    //获取配置
    public void configure(Map<String, ?> configs) {
        configs.forEach((s, o) -> {
            System.out.println("key="+s+",value="+o);
        });
    }

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //根据不同的值设置不同的分区
        Integer v = (Integer) value;
        return v%3;
    }

    public void close() {

    }

}
