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
		Assert.assertTrue(
				message == null || message.getBody().equals("TEST_MSG_1") || message.getBody().equals("TEST_MSG_2"));
		if (message != null) {
			qs.delete(message.getReceiptHandle());
		}

		message = qs.pull();
		Assert.assertTrue(
				message == null || message.getBody().equals("TEST_MSG_1") || message.getBody().equals("TEST_MSG_2"));
		if (message != null) {
			qs.delete(message.getReceiptHandle());
		}
	}

	@Test
	public void testDelete() throws InterruptedException {
		QueueService qs = new InMemoryQueueService(100);

		qs.push("TEST_MSG_1");
		qs.push("TEST_MSG_2");

		Message message = qs.pull();

		Assert.assertTrue(
				message == null || message.getBody().equals("TEST_MSG_1") || message.getBody().equals("TEST_MSG_2"));

		if (message != null) {
			String receiptHandleFirst = message.getReceiptHandle();
			qs.delete(receiptHandleFirst);
		}

		message = qs.pull();

		Assert.assertTrue(
				message == null || message.getBody().equals("TEST_MSG_1") || message.getBody().equals("TEST_MSG_2"));

		if (message != null) {
			String receiptHandleSecond = message.getReceiptHandle();
			Thread.sleep(200);
			qs.delete(receiptHandleSecond);
		}
	}

	@Test
	public void testLateDelete() throws InterruptedException {
		QueueService qs = new InMemoryQueueService(100);

		qs.push("TEST_MSG_1");

		Message message = qs.pull();
		Assert.assertTrue(message == null || message.getBody().equals("TEST_MSG_1"));
		if (message != null) {
			String receiptHandleFirst = message.getReceiptHandle();
			Thread.sleep(200);
			qs.delete(receiptHandleFirst);
		}
	}

	@Test
	public void testEarlyDelete() throws InterruptedException {
		QueueService qs = new InMemoryQueueService(1_000);

		qs.push("TEST_MSG_1");

		Message message = qs.pull();
		Assert.assertTrue(message == null || message.getBody().equals("TEST_MSG_1"));
		if (message != null) {
			String receiptHandleFirst = message.getReceiptHandle();
			Thread.sleep(200);
			qs.delete(receiptHandleFirst);
		}
	}

	@Test
	public void testPullFromEmptyQueue() {
		QueueService qs = new InMemoryQueueService();
		qs.pull();
	}

	@Test
	public void testPushLoop() {
		QueueService qs = new InMemoryQueueService();

		for (int i = 0; i < 100_000; ++i) {
			qs.push(new Object().toString());
		}
	}

	@Test
	public void testPullLoop() {
		QueueService qs = new InMemoryQueueService();

		for (int i = 0; i < 100_000; ++i) {
			qs.pull();
		}
	}

	@Test
	public void testDeleteLoop() {
		QueueService qs = new InMemoryQueueService();

		for (int i = 0; i < 100_000; ++i) {
			qs.delete("" + i);
		}
	}

	@Test
	public void testPushAndPullLoop() {
		QueueService qs = new InMemoryQueueService(3_000);

		for (int i = 0; i < 100_000; ++i) {
			qs.push(new Object().toString());
			qs.pull();
		}
	}

	@Test
	public void testPushAndPullAndDeleteLoop() {
		QueueService qs = new InMemoryQueueService(3_000);

		for (int i = 0; i < 100_000; ++i) {
			qs.push(new Object().toString());
			Message msg = qs.pull();
			if (msg != null) {
				qs.delete(msg.getReceiptHandle());
			}
		}
	}

	@Test
	public void testWrongReceiptHandle() {
		QueueService qs = new InMemoryQueueService(3_000);

		qs.push(new Object().toString());
		Message msg = qs.pull();
		qs.delete("INVALID_RECEIPT_HANDLE" + (msg != null ? msg.getBody() : "!@#_garbage_#$%"));
	}

	@Test
	public void testPushNull() {
		QueueService qs = new InMemoryQueueService(3_000);

		qs.push(null);
	}

	@Test
	public void testPushAndDeleteRandomObjects() {
		QueueService qs = new InMemoryQueueService(3_000);

		qs.push(new Object().toString());
		qs.delete(new Object().toString());
	}

	@Test
	public void testDeleteNull() {
		QueueService qs = new InMemoryQueueService(3_000);

		qs.delete(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testWrongConstructorCall0() {
		QueueService qs = new InMemoryQueueService(-100);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testWrongConstructorCall1() {
		QueueService qs = new InMemoryQueueService(0);
	}
}
