package join;

import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import stat.CountStat;
import usecase.centralized.KeepStats;

public class QueuesInputSorting implements Queues, Runnable {

	private HashMap<Integer, ConcurrentLinkedQueue<Tuple>> writerQueues;
	private HashMap<Integer, ConcurrentLinkedQueue<Tuple>> readerQueues;
	private int writers;
	private int readers;

	private CountStat rateStat;
	private CountStat delayStat;

	public QueuesInputSorting(int writers, int readers, String path, String id) {

		this.writers = writers;
		this.readers = readers;
		writerQueues = new HashMap<Integer, ConcurrentLinkedQueue<Tuple>>();
		readerQueues = new HashMap<Integer, ConcurrentLinkedQueue<Tuple>>();
		for (int i = 0; i < writers; i++) {
			writerQueues.put(i, new ConcurrentLinkedQueue<Tuple>());
		}
		for (int i = 0; i < readers; i++)
			readerQueues.put(i, new ConcurrentLinkedQueue<Tuple>());

		if (KeepStats.keepStats) {
			this.rateStat = new CountStat(path
					+ System.getProperty("file.separator") + id
					+ "_0_inputReadyDelayCount.csv", true);
			this.delayStat = new CountStat(path
					+ System.getProperty("file.separator") + id
					+ "_0_inputReadyDelaySum.csv", true);
		}

	}

	public void writeStats() {

	}

	public void addTuple(Tuple tuple, int queue) {

		writerQueues.get(queue).add(tuple);

	}

	public void addBatch(TuplesBatch batch, int queue) {
		for (Tuple t : batch.getTuples()) {
			addTuple(t, queue);
		}

	}

	public Tuple getTuple(int queue) {
		return readerQueues.get(queue).poll();

	}

	public Tuple peekTuple(int queue) {
		assert (false);
		return null;
	}

	public int getNumberOfQueues() {
		assert (false);
		return 0;
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

				if (KeepStats.keepStats) {
					long timeNow = System.nanoTime();
					this.delayStat.increase(timeNow
							- selectedTuple.getSystemInputTS());
					this.rateStat.increase(1);
				}
				for (int j = 0; j < readers; j++) {
					readerQueues.get(j).add(selectedTuple);
				}
				writerQueues.get(selectedIndex).poll();

			}
		}
	}

}
