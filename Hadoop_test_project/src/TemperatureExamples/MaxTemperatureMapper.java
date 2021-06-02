import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BooleanWritable;
//import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Random;


public class MaxTemperatureMapper extends Mapper<Text,LongWritable,Text,IntWritable> {
    private final static int MISSING = 9999;
    @Override
    protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
        //逻辑代码
        String year = "1997";
        Random a = new Random();
        for (int x: new int[10]){
//            Boolean b = Boolean.TRUE;
            int num = a.nextInt(100);
            context.write(new Text(year),new IntWritable(num));
//            context.write();
        }


    }
}
