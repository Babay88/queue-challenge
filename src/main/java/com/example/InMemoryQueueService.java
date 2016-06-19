package com.example;

import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

public class InMemoryQueueService implements QueueService {

	private static final long DEFAULT_VISIBILITY_TIMEOUT = 30_000;

	private final long visibilityTimeoutMillis;

	private final Deque<String> q = new LinkedList<>();
	private final Map<String, TimerTask> invisibleMessageReactivationTasks = new HashMap<>();
	private final Timer timer = new Timer();

	private Object lock = new Object();

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
		synchronized (lock) {
			q.addLast(messageBody);
		}
	}

	@Override
	public Message pull() {
		synchronized (lock) {
			String messageBody = q.pollFirst();
			if (messageBody == null) {
				return null;
			}

			String receiptHandle = UUID.randomUUID().toString();
			Message message = new Message(messageBody, receiptHandle);

			ReactivateMessageTask task = new ReactivateMessageTask(message);
			timer.schedule(task, visibilityTimeoutMillis);
			invisibleMessageReactivationTasks.put(receiptHandle, task);

			return message;
		}
	}

	@Override
	public void delete(String receiptHandle) {
		synchronized (lock) {
			if (invisibleMessageReactivationTasks.containsKey(receiptHandle)) {
				invisibleMessageReactivationTasks.get(receiptHandle).cancel();
				invisibleMessageReactivationTasks.remove(receiptHandle);
			}
		}
	}

	private class ReactivateMessageTask extends TimerTask {

		private final Message msg;

		public ReactivateMessageTask(Message msg) {
			this.msg = msg;
		}

		@Override
		public void run() {
			synchronized (lock) {
				q.addFirst(msg.getBody());
				invisibleMessageReactivationTasks.remove(msg.getReceiptHandle());
			}
		}
	}
}
