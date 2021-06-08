package CustomWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import javax.imageio.IIOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class TextPair implements WritableComparable<TextPair> {
    private Text first;
    private Text second;

    public TextPair(){
        this(new Text(),new Text());

    }
    public TextPair(String first,String second){
        this(new Text(first),new Text(second));
    }
    public TextPair(Text first,Text second){
        set(first, second);

    }
    public void set(Text first, Text second){
        this.first = first;
        this.second = second;
    }
    public Text getFirst(){
        return first;
    }
    public Text getSecond(){
        return second;
    }

    @Override
    public int compareTo(TextPair cp) {
        int cmp = first.compareTo(cp.first);
        if (cmp != 0){
            return cmp;
        }
        return second.compareTo(cp.second);
    }

    @Override
    public String toString() {
        return "TextPair{" +
                "first=" + first +
                ", second=" + second +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TextPair)) return false;
        TextPair textPair = (TextPair) o;
        return Objects.equals(getFirst(), textPair.getFirst()) && Objects.equals(getSecond(), textPair.getSecond());
    }

    @Override
    public int hashCode() {
        return first.hashCode()*163 + second.hashCode();
    }

    public static class Comparator extends WritableComparator{
        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
        public Comparator(){
            super(TextPair.class);
        }
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2){
            try {
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1,s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2,s2);
                int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
                if (cmp != 0){
                    return cmp;
                }
                return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1, b2, s2 + firstL2, l2 - firstL2);

            }catch (IOException e){
                throw new IllegalArgumentException(e);
            }
        }

    }
    static {
        WritableComparator.define(TextPair.class,new Comparator());
    }

}
