import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class DoubleArrayWritable extends ArrayWritable {
    public DoubleArrayWritable(Class<? extends Writable> valueClass, Writable[] values) {
        super(valueClass, values);
    }
}
