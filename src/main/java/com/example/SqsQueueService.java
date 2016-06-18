package com.example;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class SqsQueueService implements QueueService {

	private AmazonSQSClient sqs;

	private String TEMPORARY_DEFAULT_URL = "default";

	public SqsQueueService(AmazonSQSClient sqsClient) {
		this.sqs = sqsClient;
	}

	@Override
	public void push(String messageBody) {
		sqs.sendMessage(new SendMessageRequest(TEMPORARY_DEFAULT_URL, messageBody));
	}

	@Override
	public Message pull() {
		ReceiveMessageResult sqsMsg = sqs.receiveMessage(TEMPORARY_DEFAULT_URL);
		com.amazonaws.services.sqs.model.Message message = sqsMsg.getMessages().get(0);// TODO

		String messageBody = message.getBody();
		String receiptHandle = message.getReceiptHandle();

		return new Message(messageBody, receiptHandle);
	}

	@Override
	public void delete(String receiptHandle) {
		sqs.deleteMessage(TEMPORARY_DEFAULT_URL, receiptHandle);
	}
}
