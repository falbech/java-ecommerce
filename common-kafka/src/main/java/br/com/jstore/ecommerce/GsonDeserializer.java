package br.com.jstore.ecommerce;

import org.apache.kafka.common.serialization.Deserializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonDeserializer implements Deserializer<Message<?>>{
	
	private final Gson gson = new GsonBuilder().registerTypeAdapter(Message.class, new MessageAdapter()).create();

	@Override
	public Message<?> deserialize(String topic, byte[] bytes) {
		return gson.fromJson(new String(bytes), Message.class);
	}

}
