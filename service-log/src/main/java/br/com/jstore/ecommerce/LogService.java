package br.com.jstore.ecommerce;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import br.com.jstore.ecommerce.consumer.KafkaService;

public class LogService {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		var logService = new LogService();
		try (var service = new KafkaService(LogService.class.getSimpleName(), Pattern.compile("ECOMMERCE.*"),
				logService::parse, Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
			service.run();
		}
	}

	private void parse(ConsumerRecord<String, Message<String>> record) {
		System.out.println("------------");
		System.out.println("LOG: " + record.topic());
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());

	}
}
