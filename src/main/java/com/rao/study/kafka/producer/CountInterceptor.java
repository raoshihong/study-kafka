package com.rao.study.kafka.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CountInterceptor implements ProducerInterceptor<String,String> {

    private int successCount = 0;
    private int errorCount = 0;

    public void configure(Map<String, ?> configs) {

    }

    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        //拦截器这块必须返回record,如果返回null,则表示过滤掉该消息了
        return record;
    }

    //每条消息发送到broker后都会调用这个方法
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        //在消息发送到broker作出ack应答或者发送中失败时调用
        if (exception == null) {
            successCount++;
        }else{
            errorCount++;
        }
    }

    //关闭方法,只有producer调用了close方法才会被调用
    public void close() {
        System.out.println("successCount="+successCount+",errorCount="+errorCount);

    }
}
