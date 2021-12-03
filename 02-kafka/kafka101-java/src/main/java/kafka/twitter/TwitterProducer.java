package kafka.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    // use suas próprias credenciais - não as compartilhe com ninguém
    Properties properties = new Properties();

    String consumerKey;
    String consumerSecret;
    String token;
    String secret;

    List<String> terms = Lists.newArrayList("bitcoin", "usa", "politics", "sport", "soccer");

    public TwitterProducer() {
        try {
            properties.load(TwitterProducer.class.getClassLoader().getResourceAsStream("twitter.env"));
            this.consumerKey = properties.getProperty("TWITTER_API_KEY");
            this.consumerSecret = properties.getProperty("TWITTER_SECRET_KEY");
            this.token = properties.getProperty("TWITTER_TOKEN");
            this.secret = properties.getProperty("TWITTER_TOKEN_SECRET");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        logger.info("Setup");

        /**
         * Configure suas filas de bloqueio: certifique-se de dimensioná-las corretamente com
         * base no TPS esperado de sua transmissão
         */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

        // crie um cliente twitter
        Client client = createTwitterClient(msgQueue);

        // Tenta estabelecer uma conexão.
        client.connect();

        // criar um produtor kafka
        KafkaProducer<String, String> producer = createKafkaProducer();

        // adicionar um hook de desligamento
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("parando o aplicativo...");
            logger.info("desligando o cliente do twitter...");
            client.stop();
            logger.info("fechando o produtor...");
            producer.close();
            logger.info("done!");
        }));

        // loop para enviar tweets para kafka em um tópico diferente
        // ou em vários tópicos diferentes ....
        while (!client.isDone()) {
            String msg = null;

            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if (msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Something bad happened", e);
                        }
                    }
                });
            }
        }
        logger.info("End of application");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /**
         * Declare o host ao qual deseja se conectar, o endpoint e a autenticação
         * (autenticação básica ou oauth)
         */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(terms);

        // Esses segredos devem ser lidos de um arquivo de configuração
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // opcional: principalmente para os logs
                .hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        String bootstrapServers = "localhost:9092";

        // criar propriedades do produtor
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // cria o produtor
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        return producer;
    }
}
