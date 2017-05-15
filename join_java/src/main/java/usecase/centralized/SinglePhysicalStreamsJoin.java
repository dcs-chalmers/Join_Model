package usecase.centralized;

import join.ProcessingThread;
import join.Queues;
import join.Tuple;
import join.WindowType;
import stat.CountStat;

class SinglePhysicalStreamsJoin implements ProcessingThread {

	private Queues inputQueues;
	private Queues outputQueue;
	private int processingThreadID;
	private long ws;
	private CountStat comparisons;
	private CountStat outputs;
	private CountStat alpha_and_beta_times;
	private CountStat beta_times;
	private boolean continueProcessing = true;

	private CircularRWindowArray rTuples;
	private CircularSWindowArray sTuples;

	private long readyTupleCounter;

	private long startTS;
	private long rTuplesSize;
	private long sTuplesSize;
	private RTuple rTuple;
	private STuple sTuple;

	private boolean takeInputsDeterministically;
	private WindowType windowType;

	private boolean simulateSaturationAfterThreshold = false;
	private long comparisonsThreshold;
	private long comparisonsInThisDeltaT;
	private long deltaT;
	private long prevTime = -1;

	private long before_comparison;
	private long after_comparison;
	private long before_output;

	void setSimulatedSaturation(long comparisonsThreshold, long deltaT) {
		this.simulateSaturationAfterThreshold = true;
		this.comparisonsThreshold = comparisonsThreshold;
		this.deltaT = deltaT;
	}

	SinglePhysicalStreamsJoin(Queues inputQueues, Queues outputQueue,
			long windowSize, String path, String id,
			boolean takeInputDeterministically, WindowType windowType) {

		this.inputQueues = inputQueues;
		this.outputQueue = outputQueue;
		this.processingThreadID = 0;
		this.ws = windowSize;

		this.takeInputsDeterministically = takeInputDeterministically;
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

	}

	private void processRTuple(long systemTs, long ts, int x, float y) {

		if (windowType == WindowType.TIMEBASED) {
			while (sTuples.size() > 0 && sTuples.getTS(0) < startTS)
				sTuples.discard();
		} else {
			// NOTICE THAT YOU PURGE YOUR OWN WINDOW FOR TUPLE-BASED WINDOWS!!!
			if (rTuples.size() == ws)
				rTuples.discard();
		}

		sTuplesSize = sTuples.size();

		if (sTuplesSize > 0) {

			// Here I am always measuring because of processing quota
			before_comparison = System.nanoTime();

			for (int i = 0; i < sTuplesSize; i++) {

				if (x >= sTuples.getA(i) - 10 && x <= sTuples.getA(i) + 10
						&& y >= sTuples.getB(i) - 10
						&& y <= sTuples.getB(i) + 10) {
					if (KeepStats.keepStats) {
						before_output = System.nanoTime();
					}
					outputQueue.addTuple(new RSTuple(systemTs, ts, x, y,
							sTuples.getA(i), sTuples.getB(i), sTuples.getC(i),
							sTuples.getD(i)), 0);
					if (KeepStats.keepStats) {
						beta_times.increase(System.nanoTime() - before_output);
						outputs.increase(1);
					}

				}

			}

			if (KeepStats.keepStats)
				comparisons.increase(sTuplesSize);

			after_comparison = System.nanoTime();
			if (KeepStats.keepStats) {
				alpha_and_beta_times.increase(after_comparison
						- before_comparison);
			}
			if (simulateSaturationAfterThreshold) {
				comparisonsInThisDeltaT += after_comparison - before_comparison;
				simulateSaturation();
			}
		}

		if (readyTupleCounter % 1 == processingThreadID) {
			rTuples.add(ts, x, y);
		}

	}

	private void simulateSaturation() {
		if (comparisonsInThisDeltaT >= comparisonsThreshold) {
			while (System.currentTimeMillis() / 1000 < prevTime + deltaT) {
			}
		}

		if (System.currentTimeMillis() / 1000 >= prevTime + deltaT) {
			prevTime = System.currentTimeMillis() / 1000;
			comparisonsInThisDeltaT = 0;
		}
	}

	private void processSTuple(long systemTs, long ts, int a, float b,
			double c, boolean d) {

		if (windowType == WindowType.TIMEBASED) {
			while (rTuples.size() > 0 && rTuples.getTS(0) < startTS)
				rTuples.discard();
		} else {
			// NOTICE THAT YOU PURGE YOUR OWN WINDOW FOR TUPLE-BASED WINDOWS!!!
			if (sTuples.size() == ws)
				sTuples.discard();
		}

		rTuplesSize = rTuples.size();

		if (rTuplesSize > 0) {

			before_comparison = System.nanoTime();

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
			after_comparison = System.nanoTime();
			if (KeepStats.keepStats) {
				alpha_and_beta_times.increase(after_comparison
						- before_comparison);
			}
			if (simulateSaturationAfterThreshold) {
				comparisonsInThisDeltaT += after_comparison - before_comparison;
				simulateSaturation();
			}

		}

		if (readyTupleCounter % 1 == processingThreadID) {
			sTuples.add(ts, a, b, c, d);
		}

	}

	private void processTupleDeterministic() {

		Tuple R = inputQueues.peekTuple(0);
		Tuple S = inputQueues.peekTuple(1);
		Tuple toProc = null;
		if (R != null && S != null) {
			if (R.getTS() <= S.getTS())
				toProc = inputQueues.getTuple(0);
			else
				toProc = inputQueues.getTuple(1);
		}

		if (toProc != null) {

			readyTupleCounter++;

			startTS = toProc.getTS() - ws;
			if (toProc.isFromR()) {
				rTuple = (RTuple) toProc;
				processRTuple(rTuple.systemInputTimestamp, rTuple.timestamp,
						rTuple.x, rTuple.y);
			} else {
				sTuple = (STuple) toProc;
				processSTuple(sTuple.systemInputTimestamp, sTuple.timestamp,
						sTuple.a, sTuple.b, sTuple.c, sTuple.d);
			}

		}

	}

	private void processTupleNonDeterministic() {

		Tuple toProc = null;
		for (int input = 0; input < inputQueues.getNumberOfQueues(); input++) {
			toProc = inputQueues.getTuple(input);

			if (toProc != null) {

				startTS = toProc.getTS() - ws;
				if (toProc.isFromR()) {
					rTuple = (RTuple) toProc;
					processRTuple(rTuple.systemInputTimestamp,
							rTuple.timestamp, rTuple.x, rTuple.y);
				} else {
					sTuple = (STuple) toProc;
					processSTuple(sTuple.systemInputTimestamp,
							sTuple.timestamp, sTuple.a, sTuple.b, sTuple.c,
							sTuple.d);
				}
				break;

			}
		}

	}

	public void run() {

		prevTime = System.currentTimeMillis() / 1000;
		while (continueProcessing) {

			if (takeInputsDeterministically)
				processTupleDeterministic();
			else
				processTupleNonDeterministic();

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
