package com.rao.study.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.junit.Test;
import java.util.Properties;

public class PartitionTest {

    @Test
    public void test() throws InterruptedException {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        properties.put(ProducerConfig.LINGER_MS_CONFIG,1);
        properties.put(ProducerConfig.RETRIES_CONFIG,1);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.IntegerSerializer");

        //设置自定义分区器 partitioner.class
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.rao.study.kafka.producer.MyPartition");

        KafkaProducer<String, Integer> producer = new KafkaProducer<String, Integer>(properties);

        for (int i=0;i<10;i++) {
            ProducerRecord<String,Integer> record = new ProducerRecord<String,Integer>("mytopic","number",i);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception==null) {
                        System.out.println("topic="+metadata.topic()+",partition="+metadata.partition()+",offset="+metadata.offset());
                    }else{
                        exception.printStackTrace();
                    }
                }
            });
        }

        //测试上面配置linger_ms延迟1s提交
        Thread.sleep(3000);

        //一定要调用close方法,如果不调用close方法,拦截器等的close方法也不会被调用,且不会立马把消息刷出去
        producer.close();
    }
}
