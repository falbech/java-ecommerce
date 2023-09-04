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

public class BatchSendMessageService {
	
	private Connection connection;

	public BatchSendMessageService() throws SQLException {
		String url = "jdbc:sqlite:target/users_database.db";
		this.connection = DriverManager.getConnection(url);
		try {
			connection.createStatement().execute(" create table if not exists Users ( "+ 
					"uuid varchar(200) primary key," +
					"email varchar(200))");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws InterruptedException, ExecutionException, SQLException {
		var batchService = new BatchSendMessageService();
		try (var service = new KafkaService<>(BatchSendMessageService.class.getSimpleName(), "SEND_MESSAGE_TO_ALL_USERS",
				batchService::parse, String.class, Map.of())) {
			service.run();
		}
	}
	
	private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<>();

	private void parse(ConsumerRecord<String, String> record) throws SQLException, InterruptedException, ExecutionException {
		System.out.println("--------------------------------------------");
		System.out.println("Processing new batch");
		System.out.println("Topic: "+record.value());
		
		for(User user : getAllUsers()) {
			userDispatcher.send(record.value(), user.getUuid(), user);
		}
	}

	private List<User> getAllUsers() throws SQLException {
		var results =  connection.prepareStatement("select uuid from Users").executeQuery();
		List<User> users = new ArrayList<>();
		while(results.next()) {
			users.add(new User(results.getString(1)));
		}
		return users;
	}

}
