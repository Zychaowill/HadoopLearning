package kafka.simple;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jodd.util.StringUtil;

/**
 * When bootstrap, please enter commands as follows:
 * -Dlog_path=/log/consumer.log -Dtopic=test -Dconsumer_pro_path=consumer.properties
 * 
 * Created by jangz on
 */
public class KafkaOfficialConsumer {
	private static final Logger log = LoggerFactory.getLogger(KafkaOfficialConsumer.class);

	private static String logPath;

	private static String topic;

	private static String consumerProPath;

	private static boolean initCheck() {
		topic = System.getProperty("topic");
		logPath = System.getProperty("log_path");
		consumerProPath = System.getProperty("consumer_pro_path");
		if (StringUtil.isEmpty(topic) || logPath.isEmpty()) {
			log.error("system property topic, consumer_pro_path, log_path is required!");
			return true;
		}
		return false;
	}

	private static KafkaConsumer<String, String> initKafkaConsumer() {
		KafkaConsumer<String, String> consumer = null;
		try {
			FileInputStream inStream = new FileInputStream(new File(consumerProPath));
			Properties properties = new Properties();
			properties.load(inStream);
			consumer = new KafkaConsumer<>(properties);
			consumer.subscribe(Arrays.asList(topic));
		} catch (IOException e) {
			log.error("load consumer.prop error", e);
		}
		return consumer;
	}
	
	public static void main(String[] args) {
		if (initCheck()) {
			return;
		}
		
		int totalCount = 0;
		long totalMin = 0L;
		int count = 0;
		KafkaConsumer<String, String> consumer = initKafkaConsumer();
		
		long startTime = System.currentTimeMillis();
		
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(200);
			if (records.count() <= 0) {
				continue;
			}
			log.debug("get " + records.count() + " records");
			count += records.count();
			
			long endTime = System.currentTimeMillis();
			log.debug("count = " + count);
			if (count >= 10000) {
				totalCount += count;
				log.info("this consumer {} record, use {} milliseconds", count, endTime - startTime);
				totalMin += (endTime - startTime);
				startTime = System.currentTimeMillis();
				count = 0;
			}
			log.debug("end totalCount={}, min={}", totalCount, totalMin);
		}
	}
}
