package br.com.jstore.ecommerce;


import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.jstore.ecommerce.consumer.ConsumerService;
import br.com.jstore.ecommerce.consumer.ServiceRunner;

public class EmailService implements ConsumerService<String> {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		new ServiceRunner<>(EmailService::new).start(5);		
	}
	
	public String getTopic() {
		return "ECOMMERCE_SEND_EMAIL";
	}
	public String getConsumerGroup() {
		return EmailService.class.getSimpleName();
	}
	
	public void parse(ConsumerRecord<String, Message<String>> record) {
		System.out.println("------------");
		System.out.println("Sending email...");
		System.out.println(record.key());
		System.out.println(record.value().getPayload());
		System.out.println(record.partition());
		System.out.println(record.offset());
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Email sent.");
	}
}
