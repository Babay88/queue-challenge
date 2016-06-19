package com.example;

import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

public class InMemoryQueueService implements QueueService {

	private static final long DEFAULT_VISIBILITY_TIMEOUT = 30_000;

	private final long visibilityTimeoutMillis;

	private Queue<String> q = new LinkedList<>();
	private Map<String, TimerTask> invisibleMessageReactivationTasks = new HashMap<String, TimerTask>();
	private Timer timer = new Timer();

	public InMemoryQueueService() {
		this(DEFAULT_VISIBILITY_TIMEOUT);
	}

	public InMemoryQueueService(long visibilityTimeoutMillis) {
		this.visibilityTimeoutMillis = visibilityTimeoutMillis;
	}

	@Override
	public void push(String messageBody) {
		q.add(messageBody);
	}

	@Override
	public Message pull() {
		String messageBody = q.poll();
		if (messageBody == null) {
			return null;
		}

		String receiptHandle = UUID.randomUUID().toString();

		ReactivateMessageTask task = new ReactivateMessageTask(messageBody, receiptHandle);

		timer.schedule(task, visibilityTimeoutMillis);
		invisibleMessageReactivationTasks.put(receiptHandle, task);

		return new Message(messageBody, receiptHandle);
	}

	@Override
	public void delete(String receiptHandle) {
		if (invisibleMessageReactivationTasks.containsKey(receiptHandle)) {
			invisibleMessageReactivationTasks.get(receiptHandle).cancel();
			invisibleMessageReactivationTasks.remove(receiptHandle);
		}
	}

	private class ReactivateMessageTask extends TimerTask {

		final String messageBody;
		final String receiptHandle;

		public ReactivateMessageTask(String messageBody, String receiptHandle) {
			this.messageBody = messageBody;
			this.receiptHandle = receiptHandle;
		}

		@Override
		public void run() {
			Deque<String> d = (Deque<String>) q;
			d.addFirst(messageBody);
			invisibleMessageReactivationTasks.remove(receiptHandle);
		}
	}
}
