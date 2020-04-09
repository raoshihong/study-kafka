package com.rao.study.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class InterceptorTest {

    @Test
    public void test(){

        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        properties.put(ProducerConfig.LINGER_MS_CONFIG,1);
        properties.put(ProducerConfig.RETRIES_CONFIG,1);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        //设置拦截器,多个拦截器使用List,根据加入的顺序形成拦截器链
        List<String> interceptors = new ArrayList<String>();
        interceptors.add("com.rao.study.kafka.producer.TimeInterceptor");
        interceptors.add("com.rao.study.kafka.producer.CountInterceptor");
        //配置拦截器interceptor.classes
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptors);

        //创建kafka producer客户端
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0;i<10;i++) {
            ProducerRecord record = new ProducerRecord<String, String>("mytopic","aaa","abc");

            //发送消息
            producer.send(record);
        }

        //一定要调用close方法,如果不调用close方法,拦截器等的close方法
        producer.close();
    }
}
