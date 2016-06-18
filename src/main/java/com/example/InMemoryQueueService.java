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

	private final static int VISIBILITY_TIMEOUT_MILLIS = 100;

	private Queue<String> q = new LinkedList<>();
	private Map<String, String> invisibleMessages = new HashMap<String, String>();
	private Map<String, TimerTask> invisibleMessageRemovalTasks = new HashMap<String, TimerTask>();
	private Timer timer = new Timer();

	@Override
	public void push(String messageBody) {
		q.add(messageBody);
	}

	@Override
	public Message pull() {
		String messageBody = q.poll();
		String receiptHandle = UUID.randomUUID().toString();
		invisibleMessages.put(receiptHandle, messageBody);

		timer.schedule(new TimerTask() {

			@Override
			public void run() {
				Deque<String> d = (Deque<String>) q;
				d.addFirst(messageBody);

				invisibleMessages.remove(receiptHandle);
				invisibleMessageRemovalTasks.remove(receiptHandle);
			}

		}, VISIBILITY_TIMEOUT_MILLIS);

		return new Message(messageBody, receiptHandle);
	}

	@Override
	public void delete(String receiptHandle) {
		invisibleMessageRemovalTasks.get(receiptHandle).cancel();

		invisibleMessages.remove(receiptHandle);
		invisibleMessageRemovalTasks.remove(receiptHandle);
	}
}
