package assignment1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * This combiner is local reducer which is used to count same tag
 * 
 * input format:
 * key: place_name
 * value: 1 ...1 tag1 tag2 ... tagn which is iterable
 * 
 * output format:
 * key: place_name
 * value: tag1-=tag1Number,tag2-=tag2Number....tagn-=tagnNumber
 * 
 * key: place_name
 * value: number of photos
 * @author taurus
 *
 */

public class Task3CombinerForTags extends Reducer<Text, Text, Text, Text> {
	Text result = new Text();
	public void reduce(Text key, Iterable<Text> values, 
			Context context
	) throws IOException, InterruptedException {
		int sum = 0;
		Map<String, Integer> tagsFrequency = new HashMap<String,Integer>();	
		for(Text text:values){
			if(text.toString().equals("1")){
				sum++;
			}
			else{
			String [] tags = text.toString().split(" ");
			for(String tag:tags){
				tag  = tag.trim();
				if(tag.length()>0){
					if(tagsFrequency.get(tag)!=null){
						//if the tag is already exit, add 1
						tagsFrequency.put(tag, tagsFrequency.get(tag)+1);
					}else{
						//if the tag is not exit, add new entry with number 1
						tagsFrequency.put(tag, 1);
					}
				}
				
			}
		}
		}
		//put all together
		StringBuffer strBuf = new StringBuffer();
		for (String tag: tagsFrequency.keySet()){//one tag of tags
			
				strBuf.append(tag + "-="+tagsFrequency.get(tag)+",");
		}
		result.set(strBuf.toString());
		context.write(key, result);
		result.set(String.valueOf(sum));
		context.write(key, result);

		
			}

}
