package assignment1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
/**
 * 
 * 
 * input format:
 * key: number of locality (SortNumber)
 * value: place_name = (tag1:tag1Number) (tag2:tag2Number)...(tag10:tag10Number)
 * 
 * output format:
 * place_name \t number \t (tag1:tag1Number) (tag2:tag2Number)...(tag10:tag10Number)
 * @author taurus
 *
 */
public class Task3LastReducer extends Reducer<SortNumber, Text, Text, Text>{
//	private ArrayList<TagsNumber> lists = new ArrayList<TagsNumber>();
	private int limit;
	private static int num=0;
	private Text keyOut = new Text();
	private Text valueOut = new Text();
	private String count = new String();
	public void setup(Context context){
		//it is used get the maximum output lines, default is 50
		count = context.getConfiguration().get("limit.number","50"); 
		limit = Integer.parseInt(count);
	}
	
	public void reduce(SortNumber key, Iterable<Text> values, 
			Context context
	) throws IOException, InterruptedException {
			for(Text text:values){
				if(num<50){
					num++;
					String [] temp = text.toString().split("=");
					String [] temp2 = temp[0].split(",");
//					keyOut.set(temp[0].trim());//it is the locality name
					keyOut.set(temp2[0].trim());//it is the locality name
					valueOut.set(key.getNum().toString()+"\t"+temp[1]);
					context.write(keyOut,valueOut);
					
				}
				
			}
		
		}
	
	}