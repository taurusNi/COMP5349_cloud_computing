package assignment1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * This reducer is used to output 50 top locality 
 * output format:
 * place_name \t number
 * @author taurus
 *
 */
public class Task2Reducer extends Reducer<SortNumber, Text, Text, Text>{
	private int limit;
	private static int num=0;
	private Text valueOut = new Text();
	private String count = new String();
	//private int temp;
	public void setup(Context context){
		//it is used get the maximum output lines, default is 50
		count = context.getConfiguration().get("limit.number","50"); 
		limit = Integer.parseInt(count);
	}
	public void reduce(SortNumber key, Iterable<Text> values, 
			Context context
	) throws IOException, InterruptedException {
		for (Text text: values){
			if(num<limit){
			valueOut.set(key.getNum().toString());
			num++;
			context.write(text, valueOut);
			}
		}
	}
}