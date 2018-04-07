package assignment1;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * This combiner is local reduce after map stage which is used to count photo taken in same locality 
 * @author taurus
 *
 */

public class Task1Combiner extends Reducer<Text, IntWritable, Text, IntWritable>{
	private IntWritable count  = new IntWritable();
	public void reduce(Text key, Iterable<IntWritable> values, 
			Context context
	) throws IOException, InterruptedException {
		int sum =0;
		for(IntWritable e: values){
			sum += e.get();
		}
		count.set(sum);
		context.write(key, count);
	
	}
}
