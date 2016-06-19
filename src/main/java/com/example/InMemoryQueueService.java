package com.example;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class InMemoryQueueService implements QueueService {

    private static final long DEFAULT_VISIBILITY_TIMEOUT = 30_000;

    private final long visibilityTimeoutMillis;

    private final Deque<Message> q = new ConcurrentLinkedDeque<>();
    private final Map<String, Message> reactivatedMessagesMap = new ConcurrentHashMap<>();
    private final Map<String, TimerTask> invisibleMessageReactivationTasks = new ConcurrentHashMap<>();
    private final Timer timer = new Timer();

    public InMemoryQueueService() {
        this(DEFAULT_VISIBILITY_TIMEOUT);
    }

    public InMemoryQueueService(long visibilityTimeoutMillis) {
        if (visibilityTimeoutMillis <= 0 || visibilityTimeoutMillis > 43_200_000) {
            throw new IllegalArgumentException("Illegal visibilityTimeoutMillis: " + visibilityTimeoutMillis);
        }
        this.visibilityTimeoutMillis = visibilityTimeoutMillis;
    }

    @Override
    public void push(String messageBody) {
        if (messageBody == null)
            return;
        q.addLast(new Message(messageBody, null));
    }

    @Override
    public Message pull() {
        Message queueMessage = q.pollFirst();
        if (queueMessage == null) {
            return null;
        }

        if (queueMessage.getReceiptHandle() != null) {
            reactivatedMessagesMap.remove(queueMessage.getReceiptHandle());
        }

        String receiptHandle = UUID.randomUUID().toString();
        Message message = new Message(queueMessage.getBody(), receiptHandle);

        ReactivateMessageTask task = new ReactivateMessageTask(message);
        timer.schedule(task, visibilityTimeoutMillis);
        invisibleMessageReactivationTasks.put(receiptHandle, task);

        return message;
    }

    @Override
    public boolean delete(String receiptHandle) {
        if (receiptHandle == null)
            return false;

        boolean removed = false;
        TimerTask task = invisibleMessageReactivationTasks.get(receiptHandle);
        if (task != null) {
            task.cancel();
            invisibleMessageReactivationTasks.remove(receiptHandle);
            removed = true;
        }

        Message msg = reactivatedMessagesMap.remove(receiptHandle);
        if (msg != null) {
            q.remove(msg);
            removed = true;
        }

        return removed;
    }

    public int getVisibleMessagesCount() {
        return q.size();
    }

    public int getInvisibleMessagesCount() {
        return invisibleMessageReactivationTasks.size();
    }

    public boolean hasAnyMessages() {
        return q.size() > 0 || invisibleMessageReactivationTasks.size() > 0;
    }

    private class ReactivateMessageTask extends TimerTask {

        private final Message msg;

        public ReactivateMessageTask(Message msg) {
            this.msg = msg;
        }

        @Override
        public void run() {
            reactivatedMessagesMap.put(msg.getReceiptHandle(), msg);
            q.addFirst(msg);
            invisibleMessageReactivationTasks.remove(msg.getReceiptHandle());
        }
    }
}
