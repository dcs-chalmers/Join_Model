package usecase.centralized;

import join.StreamID;
import join.Tuple;

class STuple implements Tuple {

	long timestamp;
	long systemInputTimestamp;
	int a;
	float b;
	double c;
	boolean d;

	STuple(long ts, long systemInputTimestamp, int a, float b, double c,
			boolean d) {
		timestamp = ts;
		this.systemInputTimestamp = systemInputTimestamp;
		this.a = a;
		this.b = b;
		this.c = c;
		this.d = d;
	}

	public long getTS() {
		return timestamp;
	}

	public long getSystemInputTS() {
		return systemInputTimestamp;
	}

	public int compareTo(Tuple o) {
		if (getTS() == o.getTS())
			return 0;
		return getTS() < o.getTS() ? -1 : 1;
	}

	public String toString() {
		return "[systs=" + systemInputTimestamp + ", ts=" + timestamp + ", a="
				+ a + ", b=" + b + ", c=" + c + ", d=" + d + "]";
	}

	public StreamID getStreamID() {
		return StreamID.S;
	}

	public boolean isFromR() {
		return false;
	}

	public Tuple getCopy() {
		return new STuple(timestamp, systemInputTimestamp, a, b, c, d);
	}

	public void setSystemInputTS(long systemTS) {
		this.systemInputTimestamp = systemTS;
	}

}
