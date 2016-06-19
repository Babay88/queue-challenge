package com.example;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class InMemoryQueueTest {

    @Test
    public void testOneThread() {
        InMemoryQueueService qs = new InMemoryQueueService();

        qs.push("TEST_MSG_1");
        qs.push("TEST_MSG_2");

        Message message1 = qs.pull();

        assertTrue(message1.getBody().equals("TEST_MSG_1") || message1.getBody().equals("TEST_MSG_2"));
        assertEquals(1, qs.getVisibleMessagesCount());

        qs.delete(message1.getReceiptHandle());

        Message message2 = qs.pull();
        assertTrue(message2.getBody().equals("TEST_MSG_1") || message2.getBody().equals("TEST_MSG_2"));
        assertFalse(message1.getBody().equals(message2.getBody()));
        assertEquals(0, qs.getVisibleMessagesCount());

        qs.delete(message2.getReceiptHandle());
    }

    @Test
    public void testDelete() throws InterruptedException {
        InMemoryQueueService qs = new InMemoryQueueService(100);

        qs.push("TEST_MSG_1");
        qs.push("TEST_MSG_2");

        Message message1 = qs.pull();
        Message message2 = qs.pull();

        assertTrue(message1.getBody().equals("TEST_MSG_1") || message1.getBody().equals("TEST_MSG_2"));
        assertTrue(message2.getBody().equals("TEST_MSG_1") || message2.getBody().equals("TEST_MSG_2"));
        assertFalse(message1.getBody().equals(message2.getBody()));
        assertEquals(2, qs.getInvisibleMessagesCount());

        qs.delete(message1.getReceiptHandle());

        assertEquals(1, qs.getInvisibleMessagesCount());

        qs.delete(message2.getReceiptHandle());

        assertEquals(0, qs.getInvisibleMessagesCount());
        assertEquals(0, qs.getVisibleMessagesCount());
    }

    @Test
    public void testLateDelete() throws InterruptedException {
        InMemoryQueueService qs = new InMemoryQueueService(50);

        qs.push("TEST_MSG_1");

        Message message = qs.pull();
        assertTrue(message.getBody().equals("TEST_MSG_1"));

        String receiptHandleFirst = message.getReceiptHandle();
        assertEquals(1, qs.getInvisibleMessagesCount());
        Thread.sleep(100);
        assertEquals(0, qs.getInvisibleMessagesCount());
        boolean result = qs.delete(receiptHandleFirst);

        assertTrue(result);
        assertNull(qs.pull());
    }

    @Test
    public void testEarlyDelete() throws InterruptedException {
        InMemoryQueueService qs = new InMemoryQueueService(1_000);

        qs.push("TEST_MSG_1");

        Message message = qs.pull();
        assertTrue(message.getBody().equals("TEST_MSG_1"));
        String receiptHandleFirst = message.getReceiptHandle();
        Thread.sleep(50);
        assertEquals(1, qs.getInvisibleMessagesCount());
        qs.delete(receiptHandleFirst);
        assertEquals(0, qs.getInvisibleMessagesCount());
        assertEquals(0, qs.getVisibleMessagesCount());
    }

    @Test
    public void testPullFromEmptyQueue() {
        QueueService qs = new InMemoryQueueService();
        Message message = qs.pull();
        assertNull(message);
    }

    @Test
    public void testPushLoop() {
        InMemoryQueueService qs = new InMemoryQueueService();

        for (int i = 0; i < 1000; ++i) {
            qs.push(new Object().toString());
        }
        assertEquals(1000, qs.getVisibleMessagesCount());
        assertEquals(0, qs.getInvisibleMessagesCount());
    }

    @Test
    public void testPullLoop() {
        QueueService qs = new InMemoryQueueService();
        boolean[] receivedMessages = new boolean[10_000];

        for (int i = 0; i < 1000; ++i) {
            qs.push(Integer.toString(i));
        }

        for (int i = 0; i < 1000; ++i) {
            Message message = qs.pull();
            int num = Integer.parseInt(message.getBody());
            receivedMessages[num] = true;
        }

        for (int i = 0; i < 1000; ++i) {
            assertTrue(receivedMessages[i]);
        }
        assertNull(qs.pull());
    }

    @Test
    public void testDeleteLoop() throws InterruptedException {
        InMemoryQueueService qs = new InMemoryQueueService(5);

        for (int i = 0; i < 1000; ++i) {
            qs.push(Integer.toString(i));
        }
        for (int i = 0; i < 1000; ++i) {
            Message message = qs.pull();
            assertTrue(qs.delete(message.getReceiptHandle()));
        }

        assertEquals(0, qs.getVisibleMessagesCount());
        assertEquals(0, qs.getInvisibleMessagesCount());

        Thread.sleep(20);

        assertNull(qs.pull());
    }

    @Test
    public void testPushAndPullLoop() {
        QueueService qs = new InMemoryQueueService(3_000);

        for (int i = 0; i < 1000; ++i) {
            qs.push(new Object().toString());
            qs.pull();
        }
    }

    @Test
    public void testPushAndPullAndDeleteLoop() throws InterruptedException {
        InMemoryQueueService qs = new InMemoryQueueService(10);

        for (int i = 0; i < 1000; ++i) {
            qs.push(new Object().toString());
            Message msg = qs.pull();
            boolean deleted = qs.delete(msg.getReceiptHandle());
            assertTrue(deleted);
        }

        assertEquals(0, qs.getVisibleMessagesCount());
        assertEquals(0, qs.getInvisibleMessagesCount());

        Thread.sleep(30);

        assertNull(qs.pull());
    }

    @Test
    public void testWrongReceiptHandle() {
        QueueService qs = new InMemoryQueueService(3_000);

        qs.push(new Object().toString());
        Message msg = qs.pull();
        boolean result = qs.delete("INVALID_RECEIPT_HANDLE" + (msg != null ? msg.getBody() : "!@#_garbage_#$%"));
        assertFalse(result);
    }

    @Test
    public void testPushNull() {
        QueueService qs = new InMemoryQueueService(3_000);

        qs.push(null);
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

    @Test
    public void testMultipleProducersConsumers() throws InterruptedException {
        InMemoryQueueService qs = new InMemoryQueueService(100);

        final int received[] = new int[10];

        final Thread senderThread1 = new Thread(() -> {
            for (int i = 0; i < 10; i += 2) {
                qs.push("" + i);
            }
        });

        final Thread senderThread2 = new Thread(() -> {
            for (int i = 1; i < 10; i += 2) {
                qs.push("" + i);
            }
        });

        Thread consumerThread1 = new Thread(() -> {
            int i = 0;
            while (i < 50 || senderThread1.isAlive() || senderThread2.isAlive()) {
                Message pull = qs.pull();
                if (pull != null) {
                    String body = pull.getBody();
                    int number = Integer.parseInt(body);
                    assertTrue(number >= 0 && number < 10);
                    assertTrue(qs.delete(pull.getReceiptHandle()));
                    received[number] = 1;
                }
                ++i;
            }
        });



        Thread consumerThread2 = new Thread(() -> {
            int i = 0;
            while (i < 50 || senderThread1.isAlive() || senderThread2.isAlive()) {
                Message pull = qs.pull();
                if (pull != null) {
                    String body = pull.getBody();
                    int number = Integer.parseInt(body);
                    assertTrue(number >= 0 && number < 10);
                    assertTrue(qs.delete(pull.getReceiptHandle()));
                    received[number] = 2;
                }
                ++i;
            }
        });

        senderThread1.start();
        senderThread2.start();
        consumerThread1.start();
        consumerThread2.start();

        consumerThread1.join();
        consumerThread2.join();

        for (int i=0; i < 10; i++){
            assertTrue(received[i] > 0);
        }

        assertEquals(0, qs.getInvisibleMessagesCount());
        assertEquals(0, qs.getVisibleMessagesCount());

        //fail(Arrays.toString(received));
    }
}
