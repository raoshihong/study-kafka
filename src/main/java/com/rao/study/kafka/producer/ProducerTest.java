package com.rao.study.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.util.Properties;

public class ProducerTest {

    @Test
    public void test(){

        // bin/kafka-console-producer.sh --broker-list hadoop102:9092 --topic mytopic

        //创建一个配置属性
        Properties properties = new Properties();

        //设置broker-list,指明连接kafka服务器
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");

        //指定key的序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        //指定value的序列化器
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        //指定acks级别0,1,-1(all)
        properties.put(ProducerConfig.ACKS_CONFIG,"all");

        //设置重试次数retries
        properties.put(ProducerConfig.RETRIES_CONFIG,1);

        //设置批量发送消息的大小 batch.size,当达到这个大小时才发送
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);

        //linger.ms设置延迟时间,超过这个时间,消息条数还未达到batch.size,则将被发送
        properties.put(ProducerConfig.LINGER_MS_CONFIG,1);

        //buffer.memory
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);//RecordAccumulator缓冲区大小

        KafkaProducer producer = new KafkaProducer(properties);

        for(int i=0;i<10;i++) {
            //指定主题,指定分区,如果没有指定分区,则会根据key来进行hash求分区
            ProducerRecord<String,String> producerRecord = new ProducerRecord<String, String>("mytopic",0,"name","bbb");
            //发送消息
            producer.send(producerRecord);
        }

        producer.close();
    }

}
