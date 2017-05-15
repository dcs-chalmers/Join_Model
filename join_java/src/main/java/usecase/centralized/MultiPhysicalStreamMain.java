package usecase.centralized;

import java.util.LinkedList;
import java.util.List;

//import join.Queues;
import join.QueuesInputSorting;
//import join.QueuesOutput;
import join.QueuesOutputSorting;
import join.WindowType;

public class MultiPhysicalStreamMain {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		if (args.length < 10) {
			System.out.println("Usage:\nMain " + "periodsFileR periodsFileS "
					+ "winSize experimentID "
					+ "statisticsFolder sleepForBatch "
					+ "winType simulateSaturation"
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
		System.out.println("winType: " + args[6]);
		String winType = args[6];
		WindowType type = null;
		if (winType.equals("time-based")) {
			type = WindowType.TIMEBASED;
		} else if (winType.equals("tuple-based")) {
			type = WindowType.TUPLEBASED;
		} else {
			throw new RuntimeException("Unkown window type!");
		}

		int rIns = Integer.valueOf(args[7]);
		System.out.println("rIns: " + rIns);
		int sIns = Integer.valueOf(args[8]);
		System.out.println("sIns: " + sIns);
		int n = Integer.valueOf(args[9]);
		System.out.println("n: " + n);

		QueuesInputSorting inQueues = new QueuesInputSorting(rIns + sIns, n,
				path, id);
		QueuesOutputSorting outQueues = new QueuesOutputSorting(n, true, path,
				id);

		long referenceNanoSecond = System.nanoTime();
		int inputNumber = 0;

		List<Thread> injectorThreads = new LinkedList<Thread>();
		List<Injector> injectors = new LinkedList<Injector>();

		Thread inQueuesThread = new Thread(inQueues);
		inQueuesThread.start();
		Thread outQueuesThread = new Thread(outQueues);
		outQueuesThread.start();

		for (int i = 0; i < rIns; i++) {
			Injector inj = new RInjector(inQueues, inputNumber,
					referenceNanoSecond, periodsFileR, rIns, sleepForBatch,
					false, false, "", path
							+ System.getProperty("file.separator") + id + "_"
							+ i + "_RInj_rate.csv");
			injectors.add(inj);
			injectorThreads.add(new Thread(inj));
			inputNumber++;
		}

		for (int i = 0; i < sIns; i++) {
			Injector inj = new SInjector(inQueues, inputNumber,
					referenceNanoSecond, periodsFileS, sIns, sleepForBatch,
					false, false, "", path
							+ System.getProperty("file.separator") + id + "_"
							+ i + "_SInj_rate.csv");
			injectors.add(inj);
			injectorThreads.add(new Thread(inj));
			inputNumber++;
		}

		List<MultiPhysicalStreamsJoin> pus = new LinkedList<MultiPhysicalStreamsJoin>();
		List<Thread> pusThreads = new LinkedList<Thread>();
		for (int i = 0; i < n; i++) {
			MultiPhysicalStreamsJoin join = new MultiPhysicalStreamsJoin(
					inQueues, outQueues, i, n, ws, path, id, type);
			pus.add(join);
			pusThreads.add(new Thread(join));
		}
		for (Thread t : pusThreads) {
			t.start();
		}

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

		for (MultiPhysicalStreamsJoin j : pus) {
			j.stop();
		}

		inQueues.stopRunning();
		outQueues.stopRunning();

		for (Injector inj : injectors) {
			inj.writeStats();
		}

		inQueues.writeStats();
		outQueues.writeStats();

	}
}
