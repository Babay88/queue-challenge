package com.example;

public class InMemoryQueueService implements QueueService {
	//
	// Task 2: Implement me.
	//

	@Override
	public String push(String messageBody) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Message pull() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void delete(String receiptHandle) {
		throw new UnsupportedOperationException();
	}
}
