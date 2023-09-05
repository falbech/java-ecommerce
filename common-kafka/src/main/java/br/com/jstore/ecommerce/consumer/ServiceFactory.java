package br.com.jstore.ecommerce.consumer;

public interface ServiceFactory<T> {
	ConsumerService<T> create() throws Exception;
}
