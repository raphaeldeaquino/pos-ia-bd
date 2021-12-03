package kafka.basics;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProdutorDemo {

    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";

        // cria as propriedades do Produtor
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // cria o produtor
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // cria o registo a ser produzido
        ProducerRecord<String, String> record =
                new ProducerRecord<>("primeiro_topico", "hello world");

        // envia o registo de maneira assíncrona
        producer.send(record);

        // flush data
        producer.flush();
        // flush e encerra produtor
        producer.close();
    }
}
