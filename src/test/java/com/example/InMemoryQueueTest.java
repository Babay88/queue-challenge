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
		qs.delete(message.getReceiptHandle());

		message = qs.pull();
		Assert.assertTrue(message.getBody().equals("TEST_MSG_2"));
		qs.delete(message.getReceiptHandle());
	}

	@Test
	public void testOneThreadWithDelay() throws InterruptedException {
		QueueService qs = new InMemoryQueueService(100);

		qs.push("TEST_MSG_1");
		qs.push("TEST_MSG_2");

		Message message = qs.pull();
		Assert.assertTrue(message.getBody().equals("TEST_MSG_1"));
		String receiptHandleFirst = message.getReceiptHandle();

		Thread.sleep(110);

		message = qs.pull();
		String receiptHandleSecond = message.getReceiptHandle();
		Assert.assertTrue(message.getBody().equals("TEST_MSG_1"));

		Assert.assertFalse(receiptHandleFirst.equals(receiptHandleSecond));

		message = qs.pull();
		Assert.assertTrue(message.getBody().equals("TEST_MSG_2"));
	}

	@Test
	public void testDelete() throws InterruptedException {
		QueueService qs = new InMemoryQueueService(100);

		qs.push("TEST_MSG_1");
		qs.push("TEST_MSG_2");

		Message message = qs.pull();
		Assert.assertTrue(message.getBody().equals("TEST_MSG_1"));
		String receiptHandleFirst = message.getReceiptHandle();
		qs.delete(receiptHandleFirst);

		message = qs.pull();
		String receiptHandleSecond = message.getReceiptHandle();
		Assert.assertTrue(message.getBody().equals("TEST_MSG_2"));

		Assert.assertFalse(receiptHandleFirst.equals(receiptHandleSecond));

		Thread.sleep(200);
		qs.delete(receiptHandleSecond);
	}
}
