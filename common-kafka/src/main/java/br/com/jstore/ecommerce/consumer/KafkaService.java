package br.com.jstore.ecommerce.consumer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import br.com.jstore.ecommerce.Message;
import br.com.jstore.ecommerce.dispatcher.GsonSerializer;
import br.com.jstore.ecommerce.dispatcher.KafkaDispatcher;

public class KafkaService<T> implements Closeable {

	private final KafkaConsumer<String, Message<T>> consumer;
	private final ConsumerFunction<T> parse;
	

	public KafkaService(String groupId, String topic, ConsumerFunction<T> parse,
			Map<String, String> overrideProperties) {
		this(parse, groupId, overrideProperties);
		consumer.subscribe(Collections.singletonList(topic));
	}

	public KafkaService(String groupId, Pattern topic, ConsumerFunction<T> parse,
			Map<String, String> overrideProperties) {
		this(parse, groupId, overrideProperties);
		consumer.subscribe(topic);
	}

	private KafkaService(ConsumerFunction<T> parse, String groupId,
			Map<String, String> overrideProperties) {
		this.parse = parse;
		this.consumer = new KafkaConsumer<String, Message<T>>(properties(groupId, overrideProperties));
	}

	public void run() throws InterruptedException, ExecutionException {
		try(var deadLetter = new KafkaDispatcher<>()){			
			while (true) {
				var records = consumer.poll(Duration.ofMillis(100));
				if (!records.isEmpty()) {
					System.out.println("Found " + records.count() + " records");
					for (var record : records) {
						try {
							this.parse.consume(record);
						} catch (Exception e) {
							// only catches exception to be able to recover and parse the next ones
							e.printStackTrace();
							var message = record.value();
							deadLetter.send("ECOMMERCE_DEADLETTER", message.getId().toString(),
									message.getId().continueWith("DeadLetter"),
									new GsonSerializer().serialize("", message));
						}
					}
				}
			}
		}
	}

	private Properties properties(String groupId, Map<String, String> overrideProperties) {
		var properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		properties.putAll(overrideProperties);
		return properties;
	}

	@Override
	public void close() {
		this.consumer.close();
	}

}
