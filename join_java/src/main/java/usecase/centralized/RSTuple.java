package usecase.centralized;

import join.StreamID;
import join.Tuple;

class RSTuple implements Tuple {

	private long timestamp;
	private long systemInputTimestamp;
	private int x;
	private float y;
	private int a;
	private float b;
	private double c;
	private boolean d;

	RSTuple(long systemInputTimestamp, long timestamp, int x, float y, int a,
			float b, double c, boolean d) {

		this.systemInputTimestamp = systemInputTimestamp;
		this.timestamp = timestamp;
		this.x = x;
		this.y = y;
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

	@Override
	public String toString() {
		return "[ts=" + timestamp + ", x=" + x + ", y=" + y + ", a=" + a
				+ ", b=" + b + ", c=" + c + ", d=" + d;
	}

	public StreamID getStreamID() {
		return null;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + a;
		result = prime * result + Float.floatToIntBits(b);
		long temp;
		temp = Double.doubleToLongBits(c);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + (d ? 1231 : 1237);
		result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
		result = prime * result + x;
		result = prime * result + Float.floatToIntBits(y);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RSTuple other = (RSTuple) obj;
		if (a != other.a)
			return false;
		if (Float.floatToIntBits(b) != Float.floatToIntBits(other.b))
			return false;
		if (Double.doubleToLongBits(c) != Double.doubleToLongBits(other.c))
			return false;
		if (d != other.d)
			return false;
		if (timestamp != other.timestamp)
			return false;
		if (x != other.x)
			return false;
		if (Float.floatToIntBits(y) != Float.floatToIntBits(other.y))
			return false;
		return true;
	}

	public boolean isFromR() {
		return false;
	}

	public Tuple getCopy() {
		return null;
	}

	public void setSystemInputTS(long systemTS) {
		this.systemInputTimestamp = systemTS;
	}
}
