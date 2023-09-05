package br.com.jstore.ecommerce.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.jstore.ecommerce.Message;

public interface ConsumerService<T> {
	
	String getTopic();
	
	void parse(ConsumerRecord<String, Message<T>> record) throws Exception;
	
	String getConsumerGroup();

}
