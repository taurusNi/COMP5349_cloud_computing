package assignment1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * This reducer does the same thing with Task1Combiner except the output is Text
 * See Task1Combiner
 * 
 * output:
 * place_name \t number
 * @author taurus
 *
 */

public class Task1Reducer extends Reducer<Text, IntWritable, Text, Text>{
	private Text valueOut = new Text();
	private Text keyOut = new Text();
	public void reduce(Text key, Iterable<IntWritable> values, 
			Context context
	) throws IOException, InterruptedException {
		int sum =0;
		for(IntWritable e: values){
			sum += e.get();
		}
		valueOut.set(String.valueOf(sum));//transfer int to String
		String [] temp = key.toString().split(",");
//		if(temp[0].contains("\\+")){
//			temp[0].replace("\\+"," ");
//		}
		keyOut.set(temp[0].trim());
//		context.write(key, valueOut);
		context.write(keyOut, valueOut);
	
	}
}