package com.zxk.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Describe:  kafka消费者示例
 *
 * @author : ZhuXiaokang
 * @mail : xiaokang.zhu@pactera.com
 * @date : 2018/7/2 11:07
 * Attention:
 * Modify:
 */
public class KafkaConsumer {

    static final String brokers = System.getProperty("brokers");
    static final String topic = "spark-test";
    static final String groupId= "gspark-1";

    org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = null;

    public  KafkaConsumer(){
        java.util.Properties props = new java.util.Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", groupId);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
    }

    public void subscribe(){
        consumer.subscribe(java.util.Arrays.asList(topic));
        System.err.println("kafka订阅成功");
        while(true){
            ConsumerRecords<String, String> records =null;
            try {
                records = consumer.poll(1000);
            }catch (Exception e){
                e.printStackTrace();
            }
            for (ConsumerRecord<String, String> record : records) {
                System.err.println("offset = "+ record.offset() + ", value= "+record.value());
            }
        }
    }

    public void closeConsumer() {
        consumer.close();
    }

    public static void main(String[] args) {
        KafkaConsumer kafkaConsumer = new KafkaConsumer();
        kafkaConsumer.subscribe();
        kafkaConsumer.closeConsumer();
    }
}
