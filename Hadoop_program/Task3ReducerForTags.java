package assignment1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
/**
 * This reducer is used to extract top 10 tags of the one locality
 * 
 * input format:
 * key: place_name
 * value: number1 ...numbern tag1-=tag1Number,tag2-=tag2Number....tagn-=tagnNumber  which is iteraable
 * 
 * output format:
 * key: place_name
 * value: totalNumber \t tag1:tag1Number \t tag2:tag2Number ...\t tag10:tag10Number (if there are more than 10 tags,otherwise there are all tags 
 * of that locality)
 * @author taurus
 *
 */
public class Task3ReducerForTags extends Reducer<Text, Text, Text, Text>{
	private Text result = new Text();
	public void reduce(Text key, Iterable<Text> values, 
			Context context
	) throws IOException, InterruptedException {
		int sum=0;
		Map<String, Integer> tagsFrequency = new HashMap<String,Integer>();	
		for (Text text: values){
			if(text.toString().matches("\\d*")){
				if(!text.toString().equals("")){
				sum+=Integer.parseInt(text.toString());
				}
			}else{
			String [] temp  = text.toString().split(",");
			for(String temp2 :temp){
				temp2 = temp2.trim();
				String [] t  = temp2.split("-=");
				if(t.length==2){
				String tag = t[0];
				String temp_num = t[1]; 
				int num = Integer.parseInt(temp_num);
				//same thing see Task3CombinerForTags
				if (tagsFrequency.containsKey(tag)){
					tagsFrequency.put(tag, tagsFrequency.get(tag) +num);
				}else{
					tagsFrequency.put(tag, num);
					}
				}
			}
			}
		}
		ArrayList<TagsNumber> tagList = new ArrayList<TagsNumber>(); //store all things and number of tag in the list
		for(String tag: tagsFrequency.keySet()){
			tagList.add(new TagsNumber(tag,tagsFrequency.get(tag)));
		}
		
		Collections.sort(tagList, new Comparator<TagsNumber>() { //sort depending on the tags number descending
			@Override
			public int compare(TagsNumber o1, TagsNumber o2) {
				int cmp = o1.getTagNumber()-o2.getTagNumber();
				if(cmp!=0){
					return cmp*(-1);
				}
				return o1.getTagName().compareTo(o2.getTagName());
			}
		});
		
		StringBuffer strBuf = new StringBuffer();
		strBuf.append(sum+"\t");
		if(tagList.size()<=10){
			//in case the total tags are less or equal than 10
			for(TagsNumber e:tagList){
				strBuf.append(e.getTagName()+":"+e.getTagNumber()+"\t");
			}
			
		}else{
			for(int i=0;i<10;i++){
				strBuf.append(tagList.get(i).getTagName()+":"+tagList.get(i).getTagNumber()+"\t");
			}
		}
		result.set(strBuf.toString().trim());
		context.write(key, result);
	}
}
