package org.mdcaceres;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Hello world!
 *
 */
public class KafkaProducer
{
    public static void main( String[] args )
    {
        Properties properties = new Properties();

        //kafka bootstrap server
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        //kafka key serializer
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        // producer acks
        properties.setProperty("acks","1");
        properties.setProperty("retries","3");

        //or every 1 milisecond send the record
        properties.setProperty("linger.ms","1");


        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);

        for(int i = 0; i < 10; i++){
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("second_topic", Integer.toString(i),"another message test, key: " + i);
            producer.send(producerRecord);
            // or flush the records and send then ok
            //producer.flush();
        }
        producer.close();
        System.out.println( "Hello Kafka!" );
    }
}
