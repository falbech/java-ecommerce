package br.com.jstore.ecommerce;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.swing.plaf.basic.BasicTableHeaderUI;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.jstore.ecommerce.consumer.KafkaService;
import br.com.jstore.ecommerce.dispatcher.KafkaDispatcher;

public class BatchSendMessageService {

	private Connection connection;

	public BatchSendMessageService() throws SQLException {
		String url = "jdbc:sqlite:target/users_database.db";
		this.connection = DriverManager.getConnection(url);
		try {
			connection.createStatement().execute(
					" create table if not exists Users ( " + "uuid varchar(200) primary key," + "email varchar(200))");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws InterruptedException, ExecutionException, SQLException {
		var batchService = new BatchSendMessageService();
		try (var service = new KafkaService<>(BatchSendMessageService.class.getSimpleName(),
				"ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS", batchService::parse, Map.of())) {
			service.run();
		}
	}

	private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<>();

	private void parse(ConsumerRecord<String, Message<String>> record)
			throws SQLException, InterruptedException, ExecutionException {
		System.out.println("--------------------------------------------");
		System.out.println("Processing new batch");
		var message = record.value();
		System.out.println("Topic: " + message.getPayload());

		for (User user : getAllUsers()) {
			userDispatcher.sendAsync(message.getPayload(), user.getUuid(), message.getId().continueWith(BatchSendMessageService.class.getSimpleName()),
					user);
		}
	}

	private List<User> getAllUsers() throws SQLException {
		var results = connection.prepareStatement("select uuid from Users").executeQuery();
		List<User> users = new ArrayList<>();
		while (results.next()) {
			users.add(new User(results.getString(1)));
		}
		return users;
	}

}
