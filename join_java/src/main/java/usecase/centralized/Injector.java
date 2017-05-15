package usecase.centralized;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import join.Queues;
import join.Tuple;
import join.TuplesBatch;
import stat.CountStat;

abstract class Injector implements Runnable {

	private int queueNumber;
	private long referenceNanoSecond;
	private long currentDecreaseWait;
	private int periodIndex;
	private List<Long> periods;
	private List<Long> waits;
	private boolean sleepOnlyForBatches;
	private Queues queues;
	private boolean deterministicTSs;
	private long tupleCounter;
	private CountStat inputRate;
	Random random;

	Injector(Queues queues, int queueNumber, long referenceNanoSecond,
			String periodsFile, int numberOfGenerators,
			boolean sleepOnlyForBatches, boolean deterministicTSs,
			boolean logTuples, String logFile, String statFile) {

		this.queueNumber = queueNumber;
		this.queues = queues;
		this.inputRate = new CountStat(statFile, true);
		this.sleepOnlyForBatches = sleepOnlyForBatches;
		this.referenceNanoSecond = referenceNanoSecond;
		this.random = new Random(this.queueNumber);
		this.periods = new LinkedList<Long>();
		this.waits = new LinkedList<Long>();
		this.periodIndex = 0;
		this.currentDecreaseWait = System.currentTimeMillis() / 1000;
		this.deterministicTSs = deterministicTSs;
		this.tupleCounter = 0;

		try {
			BufferedReader br = new BufferedReader(new FileReader(periodsFile));
			String line;
			while ((line = br.readLine()) != null) {

				long overallInputRate = Long.valueOf(line.split(",")[0]);
				long thisInputRate = overallInputRate / numberOfGenerators;
				if (this.queueNumber == numberOfGenerators - 1) {
					thisInputRate = overallInputRate - thisInputRate
							* (numberOfGenerators - 1);
				}

				double thisSleepPeriod = 1000000 / thisInputRate;

				this.periods.add((long) Math.floor(thisSleepPeriod));

				this.waits.add(Long.valueOf(line.split(",")[1]));

			}
			br.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	protected abstract Tuple getNextTuple(long ts, long systemInputTS);

	public void run() {

		System.out.println("Injector " + queueNumber + " starting at "
				+ System.currentTimeMillis());

		currentDecreaseWait = System.currentTimeMillis() / 1000;

		TuplesBatch batch = new TuplesBatch();

		long accumulated_delay = 0;

		boolean done = false;
		boolean sleepThisRound = false;

		long before_ts = System.nanoTime();

		while (!done) {

			sleepThisRound = false;

			long nanoS = System.nanoTime();
			Tuple t = getNextTuple(deterministicTSs ? tupleCounter
					: (nanoS - referenceNanoSecond) / 1000, nanoS);
			tupleCounter++;

			batch.addTuple(t);
			queues.addBatch(batch, queueNumber);
			this.inputRate.increase(1);
			batch = new TuplesBatch();
			sleepThisRound = true;

			if (!sleepOnlyForBatches || sleepThisRound) {

				long after_ts = System.nanoTime();
				long sleep_us = this.periods.get(periodIndex).longValue()
						- (after_ts - before_ts) / 1000 - accumulated_delay;

				if (sleep_us > 0) {
					long a = System.nanoTime();
					try {
						Thread.sleep((int) sleep_us / 1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					long b = System.nanoTime();
					accumulated_delay = ((b - a) / 1000 - sleep_us);
				} else {
					accumulated_delay -= this.periods.get(periodIndex)
							.longValue();
				}

				before_ts = System.nanoTime();

			}

			if (System.currentTimeMillis() / 1000 - currentDecreaseWait >= this.waits
					.get(periodIndex)) {
				periodIndex++;
				if (periodIndex == this.periods.size()) {
					done = true;
				} else {
					currentDecreaseWait = System.currentTimeMillis() / 1000;
				}
			}

		}

		if (batch.size() > 0) {
			queues.addBatch(batch, queueNumber);
			this.inputRate.increase(1);
			batch = new TuplesBatch();
		}

		System.out.println("Injector " + this.queueNumber
				+ ": all tuples sent!");

	}

	void writeStats() {
		this.inputRate.writeStats();
	}

}
