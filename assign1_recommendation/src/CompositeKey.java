import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKey implements WritableComparable<CompositeKey> {

    private Text first;
    private DoubleWritable second;

    public CompositeKey() {
        set(new Text(), new DoubleWritable());
    }

    public CompositeKey(String first, Double second) {
        set(new Text(first), new DoubleWritable(second));
    }

    public CompositeKey(Text first, DoubleWritable second) {
        set(first,second);
    }

    public void set(Text first, DoubleWritable second) {
        this.first = first;
        this.second = second;
    }

    public Text getFirst() {
        return first;
    }

    public DoubleWritable getSecond() {
        return second;
    }

    @Override
    public int compareTo(CompositeKey compositeKey) {
        return compositeKey.getSecond().get() >= second.get() ? 1 : 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof CompositeKey) {
            CompositeKey tp = (CompositeKey) o;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
    }


    @Override
    public String toString() {
        return first.toString();
    }

    public String returnValue() {
        return second.toString();
    }
}
