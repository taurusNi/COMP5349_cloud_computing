package assignment1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
/**
 * This mapper is used for doing map-side join and to get all locality information
 * 
 * 
 * Then it reads locality information which is the output of Task3ReducerForTags
 * input format:
 * place_name \t number of photos \t tag1:tag1Number \t tag2:tag2Number ...\t tag10:tag10Number
 * 
 * Then it joins depending on the place_name
 * 
 * output format:
 * key: number of locality (SortNumber)
 * value: place_name = (tag1:tag1Number)  (tag2:tag2Number) ... (tag10:tag10Number)
 * @author taurus
 *
 */
public class Task3LastMapper extends Mapper<Object,Text,SortNumber,Text> {
	private ArrayList<TagsNumber> lists = new ArrayList<TagsNumber>();
	private SortNumber keyOut;
	private Text valueOut = new Text();
//	public void setup(Context context)
//			throws java.io.IOException, InterruptedException{
//		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());//There are two files cached
//		if (cacheFiles != null && cacheFiles.length > 0) {
//			String line;
//			String[] tokens;
//			 //read cached file
//			BufferedReader placeReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
//			try {
//				while ((line = placeReader.readLine()) != null) {
//					tokens = line.split("\t");
//					//tokens[0] place_name and tokens[1] number
//					lists.add(new TagsNumber(tokens[0],Integer.parseInt(tokens[1])));
//				}
//			} 
//			finally {
//				placeReader.close();
//			}
//		}
//		
//	}
	
	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {
		String[] dataArray = value.toString().split("\t"); 
//		if(dataArray.length<2){
//			return;
//		}
		
		String cityName = dataArray[0];
//		for(TagsNumber e:lists){
//			if(e.getTagName().equals(cityName)){// if it is the same locality
//				keyOut=new SortNumber(e.getTagNumber());
//				StringBuffer sb = new StringBuffer();
//				sb.append(e.getTagName()+"=");
//				for(int i=1;i<dataArray.length;i++){
//					sb.append(" ("+dataArray[i]+") ");
//				}
//				valueOut.set(sb.toString().trim());
//				context.write(keyOut, valueOut);
//			}
//		}
		keyOut=new SortNumber(Integer.parseInt(dataArray[1]));
		StringBuffer sb = new StringBuffer();
		sb.append(cityName+"=");
		for(int i=2;i<dataArray.length;i++){
			sb.append(" ("+dataArray[i]+") ");
		}
		valueOut.set(sb.toString().trim());
		context.write(keyOut, valueOut);

		
		
		
	}
	
}
