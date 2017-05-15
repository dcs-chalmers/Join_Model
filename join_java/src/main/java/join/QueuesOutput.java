package join;

import stat.CountStat;

public class QueuesOutput implements Queues {

	private CountStat rateStat;
	private CountStat delayStat;

	public QueuesOutput(boolean storeOutput, boolean keepStats, String path,
			String id) {

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
		this.delayStat.increase(System.nanoTime() - tuple.getSystemInputTS());
		this.rateStat.increase(1);
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

}
