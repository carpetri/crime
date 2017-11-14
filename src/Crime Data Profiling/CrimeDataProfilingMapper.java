import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CrimeDataProfilingMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] Array = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
			for(int ColNum = 0; ColNum <= Array.length - 2; ColNum++) {			
				context.write(new Text(String.valueOf(ColNum)), new Text(Array[ColNum]));
			}
	}
}