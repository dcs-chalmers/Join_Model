package join;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import stat.CountStat;

public class QueuesOutputSorting implements Queues, Runnable {

	private ConcurrentHashMap<Integer, ConcurrentLinkedQueue<Tuple>> writerQueues;
	private int writers;

	private CountStat rateStat;
	private CountStat delayStat;

	public QueuesOutputSorting(int writers, boolean keepStats, String path,
			String id) {

		this.writers = writers;
		writerQueues = new ConcurrentHashMap<Integer, ConcurrentLinkedQueue<Tuple>>();
		for (int i = 0; i < writers; i++) {
			writerQueues.put(i, new ConcurrentLinkedQueue<Tuple>());
		}

		this.rateStat = new CountStat(path
				+ System.getProperty("file.separator") + id
				+ "_0_outQueueRate.csv", true);
		this.delayStat = new CountStat(path
				+ System.getProperty("file.separator") + id + "_0_delay.csv",
				true);

	}

	public void writeStats() {
		this.rateStat.writeStats();
		this.delayStat.writeStats();
	}

	public void addTuple(Tuple tuple, int queue) {

		writerQueues.get(queue).add(tuple);

	}

	public void addBatch(TuplesBatch batch, int queue) {
		for (Tuple tuple : batch.getTuples()) {
			addTuple(tuple, queue);
		}
	}

	public Tuple getTuple(int queue) {
		throw new RuntimeException("Cannot invoke getTuple on OutputQueues!");
	}

	public int getNumberOfQueues() {
		return 1;
	}

	public Tuple peekTuple(int queue) {
		throw new RuntimeException("Cannot invoke peekTuple on OutputQueues!");
	}

	private boolean continueRunning = true;

	public void stopRunning() {
		continueRunning = false;
	}

	public void run() {
		while (continueRunning) {
			boolean thereIsATupleFromEachStream = true;
			Tuple thisTuple = null;
			Tuple selectedTuple = null;
			int selectedIndex = -1;
			for (int i = 0; i < writers; i++) {
				thisTuple = writerQueues.get(i).peek();
				thereIsATupleFromEachStream &= thisTuple != null;
				if (!thereIsATupleFromEachStream)
					break;
				if (i == 0 || thisTuple.getTS() < selectedTuple.getTS()) {
					selectedTuple = thisTuple;
					selectedIndex = i;
				}
			}
			if (thereIsATupleFromEachStream) {
				this.delayStat.increase(System.nanoTime()
						- selectedTuple.getSystemInputTS());
				this.rateStat.increase(1);
				writerQueues.get(selectedIndex).poll();

			}
		}
	}

}
