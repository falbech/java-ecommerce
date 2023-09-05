package br.com.jstore.ecommerce.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.jstore.ecommerce.Message;

public interface ConsumerFunction<T> {
	void consume(ConsumerRecord<String, Message<T>> record) throws Exception;
}
