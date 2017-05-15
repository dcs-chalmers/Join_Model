package join;

public interface Queues {

	public void addTuple(Tuple tuple, int queue);

	public void addBatch(TuplesBatch batch, int queue);

	public Tuple getTuple(int queue);

	public Tuple peekTuple(int queue);

	public int getNumberOfQueues();

	public void writeStats();

}
