import java.io.IOException;
//import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;



public class MyReducerClass extends Reducer<Text,IntWritable,Text,IntWritable> {

	int max_sum=0;
	Text max_occured_key = new Text("");
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int sum = 0;
		
		for (IntWritable val : values) {
			 sum += val.get();
		}
		
		if(sum > max_sum)
	    {
	        max_sum = sum;
	        max_occured_key.set(key);
	    }
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
	  context.write(max_occured_key, new IntWritable(max_sum));
	}
	
}
