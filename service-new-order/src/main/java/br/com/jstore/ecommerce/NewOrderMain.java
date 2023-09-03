package br.com.jstore.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import br.com.jstore.ecommerce.KafkaDispatcher;

public class NewOrderMain {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		try (var orderDispatcher = new KafkaDispatcher<Order>()) {
			try (var emailDispatcher = new KafkaDispatcher<String>()) {
				var email = Math.random() + "@email.com";
				for (var i = 0; i < 10; i++) {
					var orderId = UUID.randomUUID().toString();
					var amount = new BigDecimal(Math.random() * 5000 + 1);
					
					var order = new Order(orderId, amount, email);
					orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, order);

					var emailContent = "Thank you for your order, it is currently being processed.";
					emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email, emailContent);
				}
			}
		}
	}
}
