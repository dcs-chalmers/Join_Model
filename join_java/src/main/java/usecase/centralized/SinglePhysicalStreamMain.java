package usecase.centralized;

import java.util.LinkedList;
import java.util.List;

import join.Queues;
import join.QueuesInput;
import join.QueuesOutput;
import join.WindowType;

public class SinglePhysicalStreamMain {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		if (args.length < 11) {
			System.out.println("Usage:\nMain " + "periodsFileR periodsFileS "
					+ "winSize experimentID "
					+ "statisticsFolder sleepForBatch "
					+ "takeInsDeterministic winType simulateSaturation"
					+ "deltaT comparisonsThreshold rIns sIns n");
			return;
		}

		System.out.println("periodsFile (R): " + args[0]);
		String periodsFileR = args[0];
		System.out.println("periodsFile (S): " + args[1]);
		String periodsFileS = args[1];
		System.out.println("winSize: " + args[2]);
		int ws = Integer.valueOf(args[2]);
		System.out.println("experimentID: " + args[3]);
		String id = args[3];
		System.out.println("statisticsFolder: " + args[4]);
		String path = args[4];
		System.out.println("sleepForBatch: " + args[5]);
		boolean sleepForBatch = Boolean.valueOf(args[5]);
		System.out.println("takeInsDeterministic: " + args[6]);
		boolean takeInsDeterministic = Boolean.valueOf(args[6]);
		System.out.println("winType: " + args[7]);
		String winType = args[7];
		WindowType type = null;
		if (winType.equals("time-based")) {
			type = WindowType.TIMEBASED;
		} else if (winType.equals("tuple-based")) {
			type = WindowType.TUPLEBASED;
		} else {
			throw new RuntimeException("Unkown window type!");
		}

		boolean simulateSaturation = Boolean.valueOf(args[8]);
		System.out.println("simulateSaturation: " + simulateSaturation);
		long deltaT = Long.valueOf(args[9]);
		System.out.println("deltaT: " + deltaT);
		long comparisonsThreshold = Long.valueOf(args[10]);
		System.out.println("comparisonsThreshold: " + comparisonsThreshold);

		Queues inQueues = new QueuesInput(2, true, path, id);
		Queues outQueues = new QueuesOutput(false, true, path, id);

		long referenceNanoSecond = System.nanoTime();
		int inputNumber = 0;

		List<Thread> injectorThreads = new LinkedList<Thread>();
		List<Injector> injectors = new LinkedList<Injector>();

		for (int i = 0; i < 1; i++) {
			Injector inj = new RInjector(inQueues, inputNumber,
					referenceNanoSecond, periodsFileR, 1, sleepForBatch, false,
					false, "", path + System.getProperty("file.separator") + id
							+ "_" + i + "_RInj_rate.csv");
			injectors.add(inj);
			injectorThreads.add(new Thread(inj));
			inputNumber++;
		}

		for (int i = 0; i < 1; i++) {
			Injector inj = new SInjector(inQueues, inputNumber,
					referenceNanoSecond, periodsFileS, 1, sleepForBatch, false,
					false, "", path + System.getProperty("file.separator") + id
							+ "_" + i + "_SInj_rate.csv");
			injectors.add(inj);
			injectorThreads.add(new Thread(inj));
			inputNumber++;
		}

		System.out.println("Activating threads...");

		SinglePhysicalStreamsJoin join = new SinglePhysicalStreamsJoin(
				inQueues, outQueues, ws, path, id, takeInsDeterministic, type);

		if (simulateSaturation)
			join.setSimulatedSaturation(comparisonsThreshold, deltaT);

		Thread joinThread = new Thread(join);
		// Then the processing threads
		joinThread.start();

		// Finally, the injectors...
		try {

			for (Thread t : injectorThreads)
				t.start();
			for (Thread t : injectorThreads)
				try {
					t.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

		} catch (OutOfMemoryError e) {
			e.printStackTrace();
		}

		System.out.println("Stopping threads");
		// Now let's stop the processing threads
		join.stop();

		System.out.println("Writing statistics");
		for (Injector inj : injectors) {
			inj.writeStats();
		}

		inQueues.writeStats();
		outQueues.writeStats();

		System.out.println("bye bye");

	}
}
