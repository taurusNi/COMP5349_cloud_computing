package assignment1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * This mapper is used for doing map-side join and to get valid tags
 * 
 * It firstly reads the place information from the DistributedCache (small one)
 * input format:
 * key: place_id
 * value: place_name=place_type_id=place_url
 * 
 * Then it reads photo information
 * input format:
 * photo-id \t owner \t tags \t data-taken \t place-id \t accuracy
 * 
 * Then it joins depending on the place-id and filters invalid tags
 * 
 * output format:
 * key place_name
 * value tag1 tag2.....tagN
 * 
 * key place_name
 * value 1
 * 
 * @author taurus
 *
 */
public class Task3MapperForTags extends Mapper<Object, Text, Text, Text>{
	private Hashtable <String, String> placeTable = new Hashtable<String, String>();
	private Text keyOut = new Text(), valueOut = new Text();
	
	public void setPlaceTable(Hashtable<String,String> place){
		placeTable = place;
	}
	
	// get the distributed file and parse it
	public void setup(Context context)
		throws java.io.IOException, InterruptedException{
		
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		if (cacheFiles != null && cacheFiles.length > 0) {
			String line;
			String[] tokens;
            //read cached file
			BufferedReader placeReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
			try {
				while ((line = placeReader.readLine()) != null) {
					tokens = line.split("\t");
					//tokens[0] placeID and tokens[1] place_name=place_type_id=place_url
					placeTable.put(tokens[0], tokens[1]); 
				}
			} 
			finally {
				placeReader.close();
			}
		}
	}
	
	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		//this is from other input file
		String[] dataArray = value.toString().split("\t"); //split the data into array
		if (dataArray.length < 5){ // a not complete record with all data
			return; // don't emit anything
		}
		
		
		String placeId = dataArray[4]; 
		String places = placeTable.get(placeId); //place_name=place_type_id=place_url
		if (places !=null){
			String [] placeComponent = places.split("=");//the length is three and 0 is for placeName and 1 is for type and 2 is for placeURL
			String placeName = placeComponent[0];
			String [] placeSeparate = placeName.split(","); //get separate place
			String [] placeURL = placeComponent[2].split("/");
			String localityName = null;
			
			//get the localityName which is like the same in Task1, which is used to join
//			if(placeComponent[1].equals("7")){	
//				StringBuffer cn = new StringBuffer();
//				 for(String e:placeSeparate){
//					 cn.append(e.trim()+", ");
//				 }
//				 String temp = cn.toString().trim();
//				 localityName = temp.substring(0, temp.length()-1);
//			}else if(placeComponent[1].equals("22")){
//				StringBuffer cn = new StringBuffer();
//				//start from the second components
//				for(int i=1;i<placeSeparate.length;i++){
//					cn.append(placeSeparate[i].trim()+", ");
//				}
//				String temp = cn.toString().trim();
//				localityName = temp.substring(0, temp.length()-1);
//			}
			
			if(placeComponent[1].equals("7")){
				StringBuffer cn = new StringBuffer();
				for(int i=placeURL.length-1;i>0;i--){
					cn.append(placeURL[i].trim()+", ");
				}
//				 for(String e:place_name){
//					 cn.append(e.trim()+", ");//remove space
//				 }
				 String tp = cn.toString().trim();
				 if(tp.contains("+")){
					 tp = tp.replace("+", " ");
				 }
				 localityName = tp.substring(0, tp.length()-1);
//				 cityName  = placeURL[placeURL.length-1].split("\\+"); //if the name contains space it is +
			}else{
				StringBuffer cn = new StringBuffer();
//				for(int i=1;i<place_name.length;i++){
//					cn.append(place_name[i].trim()+", ");
//				}
				for(int i=placeURL.length-2;i>0;i--){
					cn.append(placeURL[i].trim()+", ");
				}
				String tp = cn.toString().trim();
				if(tp.contains("+")){
					tp = tp.replace("+", " ");
				 }
				localityName = tp.substring(0, tp.length()-1);
//				 cityName  = placeURL[placeURL.length-2].split("\\+");
			}
			
			String [] tags = dataArray[2].split(" "); //get separate tag
			String contryName = null;
			ArrayList<String> tagList = new ArrayList<String>();//store tags in list used to filter
			for(String e:tags){
				tagList.add(e);
			}
			for(int i=0;i<placeSeparate.length;i++){ //change the format of the place
				placeSeparate[i] = placeSeparate[i].trim(); //to remove the space
				if(i==placeSeparate.length-1){//this is used to get the abbre of the country which the abbre is not provided
					StringBuffer sb3 = new StringBuffer();
					String [] tmp = placeSeparate[i].split(" ");
					for(String e:tmp){
						sb3.append(e.substring(0, 1));
					}
					contryName = sb3.toString().toLowerCase();
				}
				if(placeSeparate[i].contains(" ")){//remove the space in the place name
					placeSeparate[i] = placeSeparate[i].replace(" ", "");
				}
				
			}
			
			
			
			//start filtering
			Iterator<String> list = tagList.iterator();
			while(list.hasNext()){
				String tag = list.next();
				if(tag.matches("\\d*")){ //it just filters all numbers
					list.remove();
					continue;
				}
				if(tag.equals("")){
					list.remove();
					continue;
				}
				if(tag.equalsIgnoreCase(contryName)){//Filter the abbr of country like uk
					list.remove();
					continue;
				}
				for(String e:placeSeparate){//depending on the separate place name filter including the place itself 
					//its parent but could not be abbr
//					e = e.toLowerCase();
					if(e.equalsIgnoreCase(tag)){
						list.remove();
						break;
					}
				}
				
			}
//			String [] cityName = null;
			
//			StringBuffer sb = new StringBuffer();
//			for(String e:cityName){
//				sb.append(e+" ");
//			}
			//get the new valid tags
			StringBuffer sb2 = new StringBuffer();
			for(String e:tagList){
				sb2.append(e+" ");
			}
			keyOut.set(localityName.trim()); //all trim()
			valueOut.set(sb2.toString().trim());
			context.write(keyOut, valueOut);
			valueOut.set("1");
			context.write(keyOut, valueOut);
		}
		
	}
}
