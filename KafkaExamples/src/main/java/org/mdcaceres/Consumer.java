package org.mdcaceres;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    public static void main (String[] args){
        Properties props = new Properties();

        props.setProperty("bootstrap.servers","127.0.0.1:9092");
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());
        props.setProperty("group.id","test");
        //props.setProperty("enable.auto.commit","false");
        props.setProperty("enable.auto.commit","true");
        props.setProperty("auto.commit.interval.ms","1000");
        props.setProperty("auto.offset.reset","earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //configure consumer to know what read from!
        consumer.subscribe(Arrays.asList("second_topic"));

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> consumerRecord : records ) {
                consumerRecord.value();
                consumerRecord.key();
                consumerRecord.offset();
                consumerRecord.partition();
                consumerRecord.topic();
                consumerRecord.timestamp();

                System.out.println(String.format("Partion %s \n Offset: %s \n key: %s \n value: %s \n timestamp: %s",
                        consumerRecord.partition(),
                        consumerRecord.offset(),
                        consumerRecord.key(),
                        consumerRecord.value(),
                        consumerRecord.timestamp())
                );
                //if you want to commit the offset
                //set auto commit as false
                //in this case intervals.ms is not needed
                //consumer.commitSync();
            }
        }

    }
}
