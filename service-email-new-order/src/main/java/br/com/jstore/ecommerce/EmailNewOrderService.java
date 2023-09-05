package br.com.jstore.ecommerce;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.jstore.ecommerce.consumer.ConsumerService;
import br.com.jstore.ecommerce.consumer.ServiceRunner;
import br.com.jstore.ecommerce.dispatcher.KafkaDispatcher;

public class EmailNewOrderService implements ConsumerService<Order> {

	public static void main(String[] args) {
		new ServiceRunner<>(EmailNewOrderService::new).start(1);
	}

	private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();
	
	@Override
	public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
		System.out.println("------------");
		System.out.println("Processing new order, preparing email");
		System.out.println(record.value());
		
		
		var emailContent = "Thank you for your order, it is currently being processed.";
		var order = record.value().getPayload();
		emailDispatcher.send("ECOMMERCE_SEND_EMAIL", order.getEmail(), record.value().getId().continueWith(EmailNewOrderService.class.getSimpleName()),emailContent);
	}

	@Override
	public String getTopic() {
		return "ECOMMERCE_NEW_ORDER";
	}


	@Override
	public String getConsumerGroup() {
		return EmailNewOrderService.class.getSimpleName();
	}

}
