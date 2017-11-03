package kafka.simple;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * When bootstrap, please add parameters as follow:
 * -Dproduct_path=/xxx/producer.properties
 * 
 * After sending message, view the info of queue by command:
 * kafka-consumer-groups.sh --bootstrap-server localhost:9094 --describe --group group1
 * 
 * @see https://kafka.apache.org/0100/documentation.html#theproducer
 * 
 * Created by jangz on 11/03/17
 */
public class Producer {
	private static final Logger log = LoggerFactory.getLogger(Producer.class);

	private static String consumerProPath;

	public static void main(String[] args) {
		// set up the producer
		consumerProPath = System.getProperty("product_path");
		KafkaProducer<String, String> producer = null;

		try {
			FileInputStream inStream = new FileInputStream(new File(consumerProPath));
			Properties properties = new Properties();
			properties.load(inStream);
			producer = new KafkaProducer<>(properties);
		} catch (IOException e) {
			log.error("load config error", e);
		}

		try {
			// set lots of messages
			for (int i = 0; i < 100; i++) {
				producer.send(new ProducerRecord<String, String>("topic_optimization", i + "", i + ""));
			}
		} catch (Throwable throwable) {
			System.out.printf("%s", throwable.getStackTrace().toString());
		} finally {
			producer.close();
		}
	}
}
