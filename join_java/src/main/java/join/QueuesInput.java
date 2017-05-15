package join;

import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class QueuesInput implements Queues {

	private HashMap<Integer, ConcurrentLinkedQueue<Tuple>> queues;
	private int numberOfQueues;

	public QueuesInput(int numberOfQueues, boolean keepStats, String path,
			String id) {

		this.numberOfQueues = numberOfQueues;
		queues = new HashMap<Integer, ConcurrentLinkedQueue<Tuple>>();

		for (int i = 0; i < numberOfQueues; i++) {
			queues.put(i, new ConcurrentLinkedQueue<Tuple>());

		}

	}

	public void writeStats() {

	}

	public void addTuple(Tuple tuple, int queue) {
		queues.get(queue).add(tuple);
	}

	public void addBatch(TuplesBatch batch, int queue) {
		for (Tuple t : batch.getTuples()) {
			addTuple(t, queue);
		}

	}

	public Tuple getTuple(int queue) {
		return queues.get(queue).poll();
	}

	public Tuple peekTuple(int queue) {
		return queues.get(queue).peek();
	}

	public int getNumberOfQueues() {
		return numberOfQueues;
	}

}
