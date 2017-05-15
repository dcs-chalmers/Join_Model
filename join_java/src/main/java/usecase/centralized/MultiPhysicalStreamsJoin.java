package usecase.centralized;

//import java.util.ArrayList;
//import java.util.LinkedList;
//import java.util.List;

import join.ProcessingThread;
import join.Queues;
import join.Tuple;
import join.WindowType;
import stat.CountStat;

class MultiPhysicalStreamsJoin implements ProcessingThread {

	private Queues inputQueues;
	private Queues outputQueue;
	private int processingThreadID;
	private int numberOfProcessingThreads;
	private long ws;
	private CountStat comparisons;
	private CountStat outputs;
	private CountStat alpha_and_beta_times;
	private CountStat beta_times;
	private boolean continueProcessing = true;

	private CircularRWindowArray rTuples;
	private CircularSWindowArray sTuples;

	private long startTS;
	private long rTuplesSize;
	private long sTuplesSize;
	private RTuple rTuple;
	private STuple sTuple;

	private WindowType windowType;

	private long before_comparison;
	private long before_output;

	private long readyTupleCounter;
	// I keep counters from each logical stream to purge tuples from the window
	private long readyTupleCounterR;
	private long readyTupleCounterS;

	MultiPhysicalStreamsJoin(Queues inputQueues, Queues outputQueue,
			int processingID, int numberOfProcessingThreads, long windowSize,
			String path, String id, WindowType windowType) {

		this.inputQueues = inputQueues;
		this.outputQueue = outputQueue;
		this.processingThreadID = processingID;
		this.numberOfProcessingThreads = numberOfProcessingThreads;
		this.ws = windowSize;

		this.windowType = windowType;

		rTuples = new CircularRWindowArray();
		sTuples = new CircularSWindowArray();

		if (KeepStats.keepStats) {
			comparisons = new CountStat(path
					+ System.getProperty("file.separator") + id + "_"
					+ processingThreadID + "_comparisons.csv", true);
			outputs = new CountStat(path + System.getProperty("file.separator")
					+ id + "_" + processingThreadID + "_outputRate.csv", true);
			alpha_and_beta_times = new CountStat(path
					+ System.getProperty("file.separator") + id + "_"
					+ processingThreadID + "_alpha_and_beta.csv", true);
			beta_times = new CountStat(path
					+ System.getProperty("file.separator") + id + "_"
					+ processingThreadID + "_beta.csv", true);
		}

		readyTupleCounter = 0;

		readyTupleCounterR = 0;
		readyTupleCounterS = 0;

	}

	private void processRTuple(long systemTs, long ts, int x, float y) {

		if (windowType == WindowType.TIMEBASED) {
			while (sTuples.size() > 0 && sTuples.getTS(0) < startTS)
				sTuples.discard();
		} else {
			// NOTICE THAT YOU PURGE YOUR OWN WINDOW FOR TUPLE-BASED WINDOWS!!!
			while (rTuples.size() > 0 && rTuples.getTS(0) < startTS)
				rTuples.discard();
		}

		sTuplesSize = sTuples.size();

		if (sTuplesSize > 0) {
			if (KeepStats.keepStats) {
				before_comparison = System.nanoTime();
			}
			for (int i = 0; i < sTuplesSize; i++) {

				if (x >= sTuples.getA(i) - 10 && x <= sTuples.getA(i) + 10
						&& y >= sTuples.getB(i) - 10
						&& y <= sTuples.getB(i) + 10) {
					if (KeepStats.keepStats) {
						before_output = System.nanoTime();
					}
					outputQueue.addTuple(new RSTuple(systemTs, ts, x, y,
							sTuples.getA(i), sTuples.getB(i), sTuples.getC(i),
							sTuples.getD(i)), processingThreadID);
					if (KeepStats.keepStats) {
						beta_times.increase(System.nanoTime() - before_output);
						outputs.increase(1);
					}

				}

			}
			if (KeepStats.keepStats)
				comparisons.increase(sTuplesSize);
			if (KeepStats.keepStats) {
				alpha_and_beta_times.increase(System.nanoTime()
						- before_comparison);
			}
		}

		if (windowType == WindowType.TIMEBASED) {
			if (readyTupleCounter % numberOfProcessingThreads == processingThreadID) {
				rTuples.add(ts, x, y);
			}
		} else {
			if (readyTupleCounterR % numberOfProcessingThreads == processingThreadID) {
				rTuples.add(ts, x, y);
			}
		}

	}

	private void processSTuple(long systemTs, long ts, int a, float b,
			double c, boolean d) {

		if (windowType == WindowType.TIMEBASED) {
			while (rTuples.size() > 0 && rTuples.getTS(0) < startTS)
				rTuples.discard();
		} else {
			// NOTICE THAT YOU PURGE YOUR OWN WINDOW FOR TUPLE-BASED WINDOWS!!!
			while (sTuples.size() > 0 && sTuples.getTS(0) < startTS)
				sTuples.discard();
		}

		rTuplesSize = rTuples.size();

		if (rTuplesSize > 0) {

			if (KeepStats.keepStats) {
				before_comparison = System.nanoTime();
			}
			for (int i = 0; i < rTuplesSize; i++) {

				if (rTuples.getX(i) >= a - 10 && rTuples.getX(i) <= a + 10
						&& rTuples.getY(i) >= b - 10
						&& rTuples.getY(i) <= b + 10) {

					if (KeepStats.keepStats) {
						before_output = System.nanoTime();
					}
					this.outputQueue.addTuple(
							new RSTuple(systemTs, ts, rTuples.getX(i), rTuples
									.getY(i), a, b, c, d), processingThreadID);

					if (KeepStats.keepStats) {
						beta_times.increase(System.nanoTime() - before_output);
						outputs.increase(1);
					}

				}

			}
			if (KeepStats.keepStats)
				comparisons.increase(rTuplesSize);
			if (KeepStats.keepStats) {
				alpha_and_beta_times.increase(System.nanoTime()
						- before_comparison);
			}
		}

		if (windowType == WindowType.TIMEBASED) {
			if (readyTupleCounter % numberOfProcessingThreads == processingThreadID) {
				sTuples.add(ts, a, b, c, d);
			}
		} else {
			if (readyTupleCounterS % numberOfProcessingThreads == processingThreadID) {
				sTuples.add(ts, a, b, c, d);
			}
		}

	}

	private void processTupleDeterministic() {

		Tuple toProc = inputQueues.getTuple(processingThreadID);

		if (toProc != null) {

			if (toProc.isFromR()) {
				rTuple = (RTuple) toProc;

				if (windowType.equals(WindowType.TIMEBASED)) {
					startTS = toProc.getTS() - ws;
					processRTuple(rTuple.systemInputTimestamp,
							rTuple.timestamp, rTuple.x, rTuple.y);
					readyTupleCounter++;
				} else {
					startTS = readyTupleCounterR - ws;
					processRTuple(rTuple.systemInputTimestamp,
							readyTupleCounterR, rTuple.x, rTuple.y);
					readyTupleCounterR++;
				}

			} else {
				sTuple = (STuple) toProc;

				if (windowType.equals(WindowType.TIMEBASED)) {
					startTS = toProc.getTS() - ws;
					processSTuple(sTuple.systemInputTimestamp,
							sTuple.timestamp, sTuple.a, sTuple.b, sTuple.c,
							sTuple.d);
					readyTupleCounter++;
				} else {
					startTS = readyTupleCounterS - ws;
					processSTuple(sTuple.systemInputTimestamp,
							readyTupleCounterS, sTuple.a, sTuple.b, sTuple.c,
							sTuple.d);
					readyTupleCounterS++;
				}

			}

		}

	}

	public void run() {

		while (continueProcessing) {

			processTupleDeterministic();
		}

	}

	public void writeStats() {
	}

	public void stop() {
		continueProcessing = false;
	}

	private class CircularRWindowArray {

		private static final int length = 250000;
		private int size = 0;
		private int addIndex = 0;
		private int readIndex = 0;
		private long[] _ts = new long[length];
		private int[] _x = new int[length];
		private float[] _y = new float[length];

		private void add(long ts, int x, float y) {
			if (size == length)
				throw new RuntimeException("CircularWindowArray exhausted!");
			_ts[addIndex] = ts;
			_x[addIndex] = x;
			_y[addIndex] = y;
			addIndex = (addIndex + 1) % length;
			size++;
		}

		private int getX(int i) {
			return _x[(readIndex + i) % length];
		}

		private float getY(int i) {
			return _y[(readIndex + i) % length];
		}

		private void discard() {
			readIndex = (readIndex + 1) % length;
			size--;
		}

		private long getTS(int i) {
			return _ts[(readIndex + i) % length];
		}

		private int size() {
			return size;
		}

	}

	private class CircularSWindowArray {

		private static final int length = 250000;
		private int size = 0;
		private int addIndex = 0;
		private int readIndex = 0;
		private long[] _ts = new long[length];
		private int[] _a = new int[length];
		private float[] _b = new float[length];
		private double[] _c = new double[length];
		private boolean[] _d = new boolean[length];

		private void add(long ts, int a, float b, double c, boolean d) {
			if (size == length)
				throw new RuntimeException("CircularWindowArray exhausted!");
			_ts[addIndex] = ts;
			_a[addIndex] = a;
			_b[addIndex] = b;
			_c[addIndex] = c;
			_d[addIndex] = d;
			addIndex = (addIndex + 1) % length;
			size++;
		}

		private int getA(int i) {
			return _a[(readIndex + i) % length];
		}

		private float getB(int i) {
			return _b[(readIndex + i) % length];
		}

		private double getC(int i) {
			return _c[(readIndex + i) % length];
		}

		private boolean getD(int i) {
			return _d[(readIndex + i) % length];
		}

		private void discard() {
			readIndex = (readIndex + 1) % length;
			size--;
		}

		private long getTS(int i) {
			return _ts[(readIndex + i) % length];
		}

		private int size() {
			return size;
		}

	}

}
