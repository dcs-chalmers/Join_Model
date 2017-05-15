package stat;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map.Entry;
import java.util.TreeMap;

public class CountStat {

	private long count;
	private TreeMap<Long, Long> countStats;

	private PrintWriter out;
	private boolean immediateWrite;

	private long prevTs;

	public CountStat(String outputFile, boolean immediateWrite) {
		this.count = 0;
		this.countStats = new TreeMap<Long, Long>();
		this.immediateWrite = immediateWrite;

		FileWriter outFile;
		try {
			outFile = new FileWriter(outputFile, false);
			out = new PrintWriter(outFile);
		} catch (IOException e) {
			e.printStackTrace();
		}

		prevTs = System.currentTimeMillis() / 1000;

	}

	public void increase(long v) {

		long ts = System.currentTimeMillis() / 1000;

		while (prevTs < ts) {
			if (immediateWrite) {
				out.println(prevTs * 1000 + "," + count);
				out.flush();
			} else {
				this.countStats.put(prevTs * 1000, count);
			}
			count = 0;
			prevTs++;
		}
		count += v;
	}

	public void writeStats() {

		long ts = System.currentTimeMillis() / 1000;

		while (prevTs < ts) {
			if (immediateWrite) {
				out.println(prevTs * 1000 + "," + count);
				out.flush();
			} else {
				this.countStats.put(prevTs * 1000, count);
			}
			count = 0;
			prevTs++;
		}

		if (!immediateWrite) {
			try {
				for (Entry<Long, Long> stat : countStats.entrySet()) {

					long time = stat.getKey();
					long counter = stat.getValue();

					out.println(time + "," + counter);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		out.flush();
		out.close();

	}

}