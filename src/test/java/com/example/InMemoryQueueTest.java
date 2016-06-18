package com.example;

import org.junit.Assert;
import org.junit.Test;

public class InMemoryQueueTest {

	@Test
	public void testOneThread() {
		QueueService qs = new InMemoryQueueService();
		Assert.assertTrue(qs != null);
		qs.push("TEST_MSG_1");
		qs.push("TEST_MSG_2");

		Message message1 = qs.pull();
		Assert.assertTrue(message1.getBody().equals("TEST_MSG_1"));
		Message message2 = qs.pull();
		Assert.assertTrue(message2.getBody().equals("TEST_MSG_2"));
	}
}
