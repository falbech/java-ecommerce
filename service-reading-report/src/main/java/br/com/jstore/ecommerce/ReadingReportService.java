package br.com.jstore.ecommerce;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.jstore.ecommerce.consumer.ConsumerService;
import br.com.jstore.ecommerce.consumer.KafkaService;
import br.com.jstore.ecommerce.consumer.ServiceRunner;

public class ReadingReportService implements ConsumerService<User> { 
	
	private static final Path SOURCE = new File("src/main/resources/report.txt").toPath();

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		new ServiceRunner<>(ReadingReportService::new).start(5);
	}

	@Override
	public void parse(ConsumerRecord<String, Message<User>> record) throws IOException {
		System.out.println("------------");
		System.out.println("Processing report for " + record.value());
		
		var user = record.value().getPayload();
		var target = new File(user.getReportPath());
		IO.copyTo(SOURCE, target);
		IO.append(target, "Created for " + user.getUuid());
		
		System.out.println("File created: " + target.getAbsolutePath());
	}

	@Override
	public String getTopic() {
		return "ECOMMERCE_USER_GENERATE_READING_REPORT";
	}

	@Override
	public String getConsumerGroup() {
		// TODO Auto-generated method stub
		return null;
	}
}
