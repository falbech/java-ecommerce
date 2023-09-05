package br.com.jstore.ecommerce;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.jstore.ecommerce.consumer.ConsumerService;
import br.com.jstore.ecommerce.consumer.ServiceRunner;
import br.com.jstore.ecommerce.database.LocalDatabase;
import br.com.jstore.ecommerce.dispatcher.KafkaDispatcher;

public class FraudDetectorService implements ConsumerService<Order> {

	private final LocalDatabase database;

	public FraudDetectorService() throws SQLException {
		this.database = new LocalDatabase("frauds_database");
		this.database.createIfNotExists(
				" create table if not exists Orders ( " + "uuid varchar(200) primary key, is_fraud boolean" + "email varchar(200))");
	}

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		new ServiceRunner<>(FraudDetectorService::new).start(1);
	}

	private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

	public void parse(ConsumerRecord<String, Message<Order>> record)
			throws InterruptedException, ExecutionException, SQLException {
		System.out.println("------------");
		System.out.println("Processing new order, checking for fraud");
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());
		System.out.println(record.offset());
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		var message = record.value();
		var order = message.getPayload();

		if (wasProcessed(order)) {
			System.out.println(String.format("Order {} was already processed", order.getOrderId()));

		}

		if (isFraud(order)) {
			// simulates fraud occurrence
			database.update("insert into Orders (uuid, is_fraud) values (?,true)", order.getOrderId());
			System.out.println("Order is a fraud!");
			orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(),
					message.getId().continueWith(FraudDetectorService.class.getSimpleName()), order);
		} else {
			database.update("insert into Orders (uuid, is_fraud) values (?,false)", order.getOrderId());
			System.out.println("Approved: " + order);
			orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(),
					message.getId().continueWith(FraudDetectorService.class.getSimpleName()), order);
		}
	}

	private boolean wasProcessed(Order order) throws SQLException {
		var results = database.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId());
		return results.next();
	}

	private boolean isFraud(Order order) {
		return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
	}

	@Override
	public String getTopic() {
		return "ECOMMERCE_NEW_ORDER";
	}

	@Override
	public String getConsumerGroup() {
		return FraudDetectorService.class.getSimpleName();
	}

}
