package kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.util.Properties;

public class ProducerFactory {
    public static Producer getProducer(){
        Properties properties=new Properties();
        properties.put("zookeeper.connect","192.168.1.2:2181,192.168.1.3:2181,192.168.1.4:2181");
        properties.put("serializer.class", StringEncoder.class.getName());
        properties.put("metadata.broker.list","192.168.1.2:9092,192.168.1.3:9092,192.168.1.4:9092");
        return new Producer<Integer,String>(new ProducerConfig(properties));
    }
}