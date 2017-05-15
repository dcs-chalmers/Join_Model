package join;

import java.util.LinkedList;

public class TuplesBatch {

	private LinkedList<Tuple> tuples = new LinkedList<Tuple>();

	public void addTuple(Tuple tuple) {
		tuples.add(tuple);
	}

	public LinkedList<Tuple> getTuples() {
		return tuples;
	}

	public int size() {
		return tuples.size();
	}

}
