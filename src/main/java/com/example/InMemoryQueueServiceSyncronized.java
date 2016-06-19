package com.example;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class InMemoryQueueServiceSyncronized implements QueueService {

    private static final long DEFAULT_VISIBILITY_TIMEOUT = 30_000;

    private final long visibilityTimeoutMillis;

    private final LinkedList<Message> q = new LinkedList<>();
    private final Map<String, Message> reactivatedMessagesMap = new HashMap<>();
    private final Map<String, TimerTask> invisibleMessageReactivationTasks = new HashMap<>();
    private final Timer timer = new Timer();

    private final Object sync = new Object();

    public InMemoryQueueServiceSyncronized() {
        this(DEFAULT_VISIBILITY_TIMEOUT);
    }

    public InMemoryQueueServiceSyncronized(long visibilityTimeoutMillis) {
        if (visibilityTimeoutMillis <= 0 || visibilityTimeoutMillis > 43_200_000) {
            throw new IllegalArgumentException("Illegal visibilityTimeoutMillis: " + visibilityTimeoutMillis);
        }
        this.visibilityTimeoutMillis = visibilityTimeoutMillis;
    }

    @Override
    public void push(String messageBody) {
        if (messageBody == null)
            return;
        synchronized (sync) {
            q.addLast(new Message(messageBody, null));
        }
    }

    @Override
    public Message pull() {

        synchronized (sync) {
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
    }

    @Override
    public boolean delete(String receiptHandle) {
        if (receiptHandle == null)
            return false;

        boolean removed = false;

        synchronized (sync) {
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
        }

        return removed;
    }

    public int getVisibleMessagesCount() {
        synchronized (sync) {
            return q.size();
        }
    }

    public int getInvisibleMessagesCount() {
        synchronized (sync) {
            return invisibleMessageReactivationTasks.size();
        }
    }

    public boolean hasAnyMessages(){
        synchronized (sync){
            return q.size() > 0 || invisibleMessageReactivationTasks.size() > 0;
        }
    }

    private class ReactivateMessageTask extends TimerTask {

        private final Message msg;

        public ReactivateMessageTask(Message msg) {
            this.msg = msg;
        }

        @Override
        public void run() {
            synchronized (sync) {
                reactivatedMessagesMap.put(msg.getReceiptHandle(), msg);
                q.addFirst(msg);
                invisibleMessageReactivationTasks.remove(msg.getReceiptHandle());
            }
        }
    }
}
