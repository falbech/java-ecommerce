package br.com.jstore.ecommerce.database;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LocalDatabase implements Closeable {

	private Connection connection;

	public LocalDatabase(String name) throws SQLException {
		String url = "jdbc:sqlite:target/" + name + ".db";
		this.connection = DriverManager.getConnection(url);
	}

	// intentionally fragile by sql injection just for demo purpose
	public void createIfNotExists(String sql) {
		try {
			connection.createStatement().execute(sql);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void update(String statement, String... params) throws SQLException {
		prepare(statement, params).execute();
	}

	public ResultSet query(String query, String... params) throws SQLException {
		return prepare(query, params).executeQuery();
	}

	private PreparedStatement prepare(String statement, String... params) throws SQLException {
		var preparedStatement = connection.prepareStatement(statement);
		for (int i = 0; i < params.length; i++) {
			preparedStatement.setString(i + 1, params[i]);
		}
		return preparedStatement;
	}

	@Override
	public void close(){
		try {
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
