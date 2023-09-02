package br.com.alura.ecommerce;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		
		var fraudService = new FraudDetectorService();
		try(var service = new KafkaService(FraudDetectorService.class.getSimpleName(),"ECOMMERCE_NEW_ORDER", fraudService::parse, Order.class, Map.of())){
			service.run();			
		}
	}
	
	private void parse(ConsumerRecord<String, Order> record) {
		System.out.println("------------");
		System.out.println("Processing new order, checking for fraud");
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());
		System.out.println(record.offset());
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Order processed");
	}

}