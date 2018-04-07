package assignment1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
/**
 * This partitioner is used to make sure global counting of the same tag
 * Default there are six reducers 
 * 
 * @author taurus
 *
 */
public class Task3Partitioner extends Partitioner<Text,Text> {

	@Override
	public int getPartition(Text key, Text value, int numPartition) {
		String key1 =  key.toString();
		String capital = key1.substring(0,1);
		if(numPartition==0){
			return 0;
		}
		//Default is six reducers
		if(capital.compareTo("I")<0){ //the capital of the city is before Q and some other numbers and signs
			return 0;
		}else if(capital.compareTo("I")>=0&&capital.compareTo("P")<0){
			return 1;
		}else if(capital.compareTo("P")>=0&&capital.compareTo("U")<0){
			return 2; 
		}else if(capital.compareTo("U")>=0&&capital.compareTo("Y")<0){
			return 3; 
		}else if(capital.compareTo("Y")>=0&&capital.compareTo("Z")<0){
			return 4;
		}else{
			return 5;// for those are not English letters and latter of the letter Zå
		}
	}
		
}
