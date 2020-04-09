package com.rao.study.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;
import java.util.Arrays;
import java.util.Properties;

public class CommitOffsetTest {
    @Test
    public void test(){

        Properties properties = new Properties();
        //设置bootstrapServer
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092");
        //group.id  设置消费者组id,group.id相同,则表示是同一个组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"mygroup");
        //设置自动提交offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);

        //设置key 反序列化器key.deserializer
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        //设置value 反序列化器value.deserializer  需要跟生产者的序列化器一一对应
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        //创建消费者客户端
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //订阅topic主题,可以同时订阅多个主题,如果主题不存在,不会自动创建
        consumer.subscribe(Arrays.asList("mytopic","mytopic"));

        //通过死循环,不断去主动拉取分区中的消息
        while (true) {

            //主动拉取消息,可以一次消费多条
            ConsumerRecords<String,String> records = consumer.poll(1000);
            records.forEach(record -> {
                System.out.println("topic="+record.topic()+",partition="+record.partition()
                        +",key="+record.key()+",value="+record.value()+",offset="+record.offset());
            });

//            if(!records.isEmpty()){
//                //手动提交offset
//                consumer.commitAsync();
//            }

        }
    }
}
