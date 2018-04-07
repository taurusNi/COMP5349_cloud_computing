package assignment1;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This mapper is used to read the output from the Task1Reducer
 * input format:
 * place_name \t number
 * 
 * output format:
 * key:  SortNumber (IntWritable number)
 * value: place_name
 * 
 * @author taurus
 *
 */

public class Task2Mapper extends Mapper<Object,Text, SortNumber, Text>{
	private SortNumber keyOut;
	private Text valueOut = new Text();
	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {
		
			String[] dataArray = value.toString().split("\t"); 
			int count  = Integer.parseInt(dataArray[1]);
			keyOut = new SortNumber(count);
//			keyOut.set(count);
			valueOut.set(dataArray[0]);
			context.write(keyOut,valueOut);
	}

}
