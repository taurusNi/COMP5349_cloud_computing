package assignment1;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This mapper is used for doing map-side join
 * It firstly reads the place information from the DistributedCache (small one)
 * input format:
 * key: place_id
 * value: place_name=place_type_id=place_url
 * 
 * Then it reads photo information
 * input format:
 * photo-id \t owner \t tags \t data-taken \t place-id \t accuracy
 * 
 * Then it joins depending on the place-id
 * 
 * output format:
 * key: place_name
 * value 1
 * 
 * 
 * @author taurus
 *
 */

public class Task1MapJoinMapper extends Mapper<Object,Text,Text,IntWritable>{
	private Hashtable <String, String> placeTable = new Hashtable<String, String>();
	private Text keyOut = new Text();
	private final static IntWritable ONE = new IntWritable(1);
	// get the distributed file and parse it
	public void setup(Context context)
		throws java.io.IOException, InterruptedException{
		//get cached file
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());//There is one file cached
		if (cacheFiles != null && cacheFiles.length > 0) {
			String line;
			String[] tokens;
            //read files
			BufferedReader placeReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
			try {
				while ((line = placeReader.readLine()) != null) {
					tokens = line.split("\t");
					String [] temp = tokens[1].split("="); //place_name, place_type_id, place_url
					String [] place_name = temp[0].split(","); //get each place component
					String [] placeURL = temp[2].split("/");
					String localityName = null;
					
					if(temp[1].equals("7")){
						StringBuffer cn = new StringBuffer();
						for(int i=placeURL.length-1;i>0;i--){
							cn.append(placeURL[i].trim()+", ");
						}
//						 for(String e:place_name){
//							 cn.append(e.trim()+", ");//remove space
//						 }
						 String tp = cn.toString().trim();
						 if(tp.contains("+")){
							 tp = tp.replace("+", " ");
						 }
						 localityName = tp.substring(0, tp.length()-1);
//						 cityName  = placeURL[placeURL.length-1].split("\\+"); //if the name contains space it is +
					}else{
						StringBuffer cn = new StringBuffer();
//						for(int i=1;i<place_name.length;i++){
//							cn.append(place_name[i].trim()+", ");
//						}
						for(int i=placeURL.length-2;i>0;i--){
							cn.append(placeURL[i].trim()+", ");
						}
						String tp = cn.toString().trim();
						if(tp.contains("+")){
							tp = tp.replace("+", " ");
						 }
						localityName = tp.substring(0, tp.length()-1);
//						 cityName  = placeURL[placeURL.length-2].split("\\+");
					}
//					StringBuffer sb = new StringBuffer();
//					for(String e:cityName){
//						sb.append(e+" ");
//					}
					//tokens[0] placeID and tokens[1] localityName
//					placeTable.put(tokens[0], sb.toString().trim());
					placeTable.put(tokens[0], localityName);
				}
			} 
			finally {
				placeReader.close();
			}
		}
	}
	
	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		//the input file is about the photo information
		String[] dataArray = value.toString().split("\t"); 
		if (dataArray.length < 6){ // a not complete record as the length should be six
			return; // ignore it
		}
		String placeId = dataArray[4];  //palce_id
		String placeName = placeTable.get(placeId); //localityName
		if (placeName !=null){// it is matched
			keyOut.set(placeName.trim()); //It is countryName, trim() is needed as there has space for place-name
			context.write(keyOut, ONE); 
		}
		
	}

}
