package usecase.centralized;

import join.StreamID;
import join.Tuple;

class RTuple implements Tuple {

	long timestamp;
	long systemInputTimestamp;
	int x;
	float y;

	RTuple(long ts, long systemInputTimestamp, int x, float y) {
		timestamp = ts;
		this.systemInputTimestamp = systemInputTimestamp;
		this.x = x;
		this.y = y;
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

	@Override
	public String toString() {
		return "[systs=" + systemInputTimestamp + ", ts=" + timestamp + ", x="
				+ x + ", y=" + y + ", z=]";
	}

	public StreamID getStreamID() {
		return StreamID.R;
	}

	public boolean isFromR() {
		return true;
	}

	public Tuple getCopy() {
		return new RTuple(timestamp, systemInputTimestamp, x, y);
	}

	public void setSystemInputTS(long systemTS) {
		this.systemInputTimestamp = systemTS;
	}
}
