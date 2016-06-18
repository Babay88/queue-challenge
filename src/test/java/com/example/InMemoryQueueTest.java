package com.example;

import org.junit.Assert;
import org.junit.Test;

public class InMemoryQueueTest {

	@Test
	public void testOneThread() {
		QueueService qs = new InMemoryQueueService();

		qs.push("TEST_MSG_1");
		qs.push("TEST_MSG_2");

		Message message = qs.pull();
		Assert.assertTrue(message.getBody().equals("TEST_MSG_1"));

		message = qs.pull();
		Assert.assertTrue(message.getBody().equals("TEST_MSG_2"));
	}

	@Test
	public void testOneThreadWithDelay() throws InterruptedException {
		QueueService qs = new InMemoryQueueService();

		qs.push("TEST_MSG_1");
		qs.push("TEST_MSG_2");

		Message message = qs.pull();
		Assert.assertTrue(message.getBody().equals("TEST_MSG_1"));

		Thread.sleep(110);

		message = qs.pull();
		Assert.assertTrue(message.getBody().equals("TEST_MSG_1"));
		message = qs.pull();
		Assert.assertTrue(message.getBody().equals("TEST_MSG_2"));
	}
}
