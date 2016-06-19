package com.example;

import java.util.Deque;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
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

        if (queueMessage.getReceiptHandle() != null){
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

        TimerTask task = invisibleMessageReactivationTasks.get(receiptHandle);
        if (task != null) {
            task.cancel();
            invisibleMessageReactivationTasks.remove(receiptHandle);
            return true;
        } else {
            Message msg = reactivatedMessagesMap.remove(receiptHandle);
            if (msg != null){
                q.remove(msg);
                return true;
            }
        }
        return false;
    }

    public int getVisibleMessagesCount() {
        return q.size();
    }

    public int getInvisibleMessagesCount() {
        return invisibleMessageReactivationTasks.size();
    }

    private class ReactivateMessageTask extends TimerTask {

        private final Message msg;

        public ReactivateMessageTask(Message msg) {
            this.msg = msg;
        }

        @Override
        public void run() {
            invisibleMessageReactivationTasks.remove(msg.getReceiptHandle());
            reactivatedMessagesMap.put(msg.getReceiptHandle(), msg);
            q.addFirst(msg);
        }
    }
}
