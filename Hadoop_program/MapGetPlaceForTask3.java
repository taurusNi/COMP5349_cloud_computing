package assignment1;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import org.apache.hadoop.io.Text;

/**
 * 
 * This mapper is used to filter legal locality which type-id is 7 or 22
 * 
 * input format:
 * place_id \t woeid \t latitude \t longitude \t place_name \t place_type_id \t place_url
 * 
 * output format:
 * key: place_id
 * value: place_name=place_type_id=place_url
 * 
 * @author taurus
 *
 */

public class MapGetPlaceForTask3 extends Mapper<Object,Text,Text,Text>{
	private Text placeId= new Text(), localityName = new Text();
	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {
				//Store each elements of line of input file
				String[] dataArray = value.toString().split("\t"); 
				if (dataArray.length < 7){ // it is a not complete record as the length should be seven
					return; // ignore it
				}
				String placeTypeID = dataArray[5];  //palce_type_id
				//placeTypeid should be 22 or 7
				if(placeTypeID.equals("7")||placeTypeID.equals("22")){
						String [] temp = dataArray[6].split("/");
						if(placeTypeID.equals("7")&&temp.length==4){//remove invalid records
							String cityName = dataArray[4]; //place_name
							placeId.set(dataArray[0]);//store placeID used to join
							localityName.set(cityName + "="+ dataArray[5] +"="+dataArray[6]); //all things used for both task1 and task3 
							context.write(placeId, localityName);
						}else if(placeTypeID.equals("22")&&temp.length==5){
							String cityName = dataArray[4]; //place_name
							placeId.set(dataArray[0]);//store placeID used to join
							localityName.set(cityName + "="+ dataArray[5] +"="+dataArray[6]); //all things used for both task1 and task3 
							context.write(placeId, localityName);
						}
						
				}	
			}

}
