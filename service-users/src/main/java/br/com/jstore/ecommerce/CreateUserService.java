package br.com.jstore.ecommerce;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.jstore.ecommerce.KafkaService;

public class CreateUserService {
	
	private Connection connection;

	public CreateUserService() throws SQLException {
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
		var createUserService = new CreateUserService();
		try (var service = new KafkaService<>(CreateUserService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER",
				createUserService::parse, Order.class, Map.of())) {
			service.run();
		}
	}

	private void parse(ConsumerRecord<String, Order> record) throws SQLException {
		System.out.println("--------------------------------------------");
		System.out.println("Processing new order, checking for new user");
		System.out.println(record.value());
		var order = record.value();
		
		if(isNewUser(order.getEmail())) {
			insertNewUser(order.getEmail());
		}
	}

	private void insertNewUser(String email) throws SQLException {
		var insert = connection.prepareStatement("insert into Users(uuid, email) "+
				"values (?,?)");
		var uuid = UUID.randomUUID().toString();
		insert.setString(1, uuid);
		insert.setString(2, email);
		insert.execute();
		System.out.println("Usuario "+ uuid + " e email " + email + " adicionado");
	}

	private boolean isNewUser(String email) throws SQLException {
		var exists = connection.prepareStatement("select uuid from Users " + 
				"where email = ? limit 1;");
		exists.setString(1, email);
		var results = exists.executeQuery();
		return !results.next();
	}
}
