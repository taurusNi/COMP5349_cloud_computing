package assignment1;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import org.apache.hadoop.io.Text;


public class MapGetPlaceName extends Mapper<Object,Text,Text,Text>{
	private Text placeId= new Text(), localityName = new Text();
	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {
				String[] dataArray = value.toString().split("\t"); 
				if (dataArray.length < 7){ // it is a not complete record as the length should be seven
					return; // ignore it
				}
				String placeTypeID = dataArray[5];  //placeTypeid should be 22 or 7
				if(placeTypeID.equals("7")||placeTypeID.equals("22")){
						String [] placeURL = dataArray[6].split("/");
						String [] cityName = null;
						if(placeTypeID.equals("7")){
							 cityName  = placeURL[placeURL.length-1].split("\\+"); //if the name contains space it is +
						}else{
							 cityName  = placeURL[placeURL.length-2].split("\\+");
						}
						StringBuffer sb = new StringBuffer();

						for(String e:cityName){
							sb.append(e+" ");
						}
						placeId.set(dataArray[0]);//store placeID used to join
						localityName.set(sb.toString().trim()); //store the localityName it have to be name as there are same name but different placeID e.g. Altstadt-Nord
						context.write(placeId, localityName);
				}	
			}

}
