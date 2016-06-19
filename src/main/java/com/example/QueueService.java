package com.example;

public interface QueueService {

	void push(String messageBody);

	Message pull();

	boolean delete(String receiptHandle);
}

class Message {
	private final String body;
	private final String receiptHandle;

	public Message(String body, String receiptHandle) {
		this.body = body;
		this.receiptHandle = receiptHandle;
	}

	public String getBody() {
		return body;
	}

	public String getReceiptHandle() {
		return receiptHandle;
	}
}
