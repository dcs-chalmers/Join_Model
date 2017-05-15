package usecase.centralized;

import join.Queues;
import join.Tuple;

class SInjector extends Injector {

	SInjector(Queues queues, int queueNumber, long referenceNanoSecond,
			String periodsFile, int numberOfGenerators,
			boolean sleepOnlyForBatches, boolean deterministicTSs,
			boolean logTuples, String logFile, String statFile) {
		super(queues, queueNumber, referenceNanoSecond, periodsFile,
				numberOfGenerators, sleepOnlyForBatches, deterministicTSs,
				logTuples, logFile, statFile);
	}

	@Override
	protected Tuple getNextTuple(long ts, long systemInputTS) {
		return new STuple(ts, systemInputTS, this.random.nextInt(200),
				(float) this.random.nextInt(200), 0.0, false);
	}

}
