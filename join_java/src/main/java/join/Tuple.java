package join;

public interface Tuple extends Comparable<Tuple> {

    public long getTS();

    public long getSystemInputTS();

    public void setSystemInputTS(long systemTS);

    public StreamID getStreamID();

    public boolean isFromR();

    public Tuple getCopy();

}
