package com.zxk.kafka.produce;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Describe: kafka提供者示例
 *
 * @author : ZhuXiaokang
 * @mail : xiaokang.zhu@pactera.com
 * @date : 2018/7/2 11:06
 * Attention:
 * Modify:
 */
public class KafkaProducer {

    static final String brokers = System.getProperty("brokers");
    static final String topic = "spark-test";

    Producer<String, String> producer = null;

    public  KafkaProducer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);
    }

    public void sendMessage(String msg){
        try{
            System.out.println("Send:" + msg);
            producer.send(new ProducerRecord<String, String>(topic, msg));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void closeProducer(){
        producer.close();
    }

    public static void main(String[] args){
        KafkaProducer kafkaProducer = new KafkaProducer();
        for (int i = 0; i < 100; i++) {
            String msg = "wolegequ " + i;
            kafkaProducer.sendMessage(msg);
        }
        kafkaProducer.closeProducer();
    }
}
