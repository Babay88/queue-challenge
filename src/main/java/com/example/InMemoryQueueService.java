package com.example;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;

public class InMemoryQueueService implements QueueService {

	private Queue<String> q = new LinkedList<>();
	private Map<String, Message> invisibleMessages = new HashMap<String, Message>();

	@Override
	public void push(String messageBody) {
		q.add(messageBody);
	}

	@Override
	public Message pull() {
		String messageBody = q.poll();
		String receiptHandle = UUID.randomUUID().toString();
		Message message = new Message(messageBody, receiptHandle);
		invisibleMessages.put(receiptHandle, message);
		return message;
	}

	@Override
	public void delete(String receiptHandle) {
		invisibleMessages.remove(receiptHandle);
	}
}
