package assignment1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * This partitioner is used to make sure global counting of the same locality level
 * Default there are three reducers 
 * @author taurus
 *
 */

public class Task1Partitioner extends Partitioner<Text,IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numPartition) {
		String key1 =  key.toString();
		String capital = key1.substring(0,1);
		if(numPartition==0){
			return 0;
		}
		if(capital.compareTo("Q")<0){ //the capital of the localityName is before Q and some other numbers and signs
			return 0;
		}else if(capital.compareTo("Q")>=0&&capital.compareTo("Z")<=0){
			return 1;
		}else{
			return 2; // for those are not English letters and latter of letter Z
		}
	}
		
}
