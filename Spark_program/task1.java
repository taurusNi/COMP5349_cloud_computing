package assignment2;

/**
 * measures
 * sample, FSC-A, SSC-A, CD48, Ly6G, CD117, SCA1, CD11b, CD150, CD11c, B220, Ly6C,...
 * 
 * experiments.csv
 * sample, date, experiment, day, subject, kind, instrument, researchers
 * 
 * 
 * It firstly reads valid records of experiments, puts all data into one partition and caches it for later join
 * Then it reads all valid records of measurements and aggregate by key and join with records stored previously
 * After joining, aggregate the records by key again and then sort the records
 * 
 * output
 * researcher \t numberOfMeasurements
 * @author taurus
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

public class task1 {
	
	public static void main(String[] args) {
		
		if(args.length<3){
			System.err.println("Usage: task1 <outputPath> <inputExperimentPath> <inputMeasurementsPath...>  ");
			System.exit(2);
			
		}
		String outputDataPath = args[0];
		String inputExperimentPath = args[1];
		String inputMeaPath1 = args[2];
		SparkConf conf = new SparkConf();
		conf.setAppName("Count number of measurement done by each researcher");
		//It is used to access a cluster
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaPairRDD<String, String> Sample_reasearcher = sc.textFile(inputExperimentPath).flatMapToPair(s->{
			String[] elements = s.split(",");
			ArrayList<Tuple2<String,String>> results =
					new ArrayList<Tuple2<String,String>>();
			if(elements.length==8){ //valid record
				String [] people = elements[7].split(";");
				for(String e:people){
					if(!elements[0].equals("sample")){
					results.add(new Tuple2<String,String>(elements[0],e.trim()));}
				}
			}
			return results.iterator();
		}).coalesce(1).cache();
//		Broadcast<List<Tuple2<String, String>>> bb = sc.broadcast(Sample_reasearcher.collect());
		sc.textFile(inputMeaPath1).filter(s->{
			String[] elements = s.split(",");
			//elements[1] FSC-A elements[2] SSC-A
			if(elements.length==17){
				if(elements[0].equals("sample")){
					return false;
				}else{
					return (Integer.parseInt(elements[1])>=1
							&&Integer.parseInt(elements[1])<=150000
							&&(Integer.parseInt(elements[2])>=1
							&&Integer.parseInt(elements[2])<=150000));
				}
			}else{
				return false;
			}
			

		}).mapToPair(s->{
			String[] elements = s.split(",");
			return new Tuple2<String, Integer>(elements[0],1);
		}).aggregateByKey(
				0,
				1,
				(r,v)->r+v,
				(r1,r2)-> r1+r2)
		 .join(Sample_reasearcher)
		 .mapToPair(s->{
					return new Tuple2<String,Integer>(s._2._2,s._2._1); //_2 is researcher _1 is number
				})
		 .reduceByKey((n1,n2)->n1+n2).mapToPair(s->{
					return new Tuple2<ResearchNum,Integer>(new ResearchNum(s._1,s._2),0);
				})
		 .sortByKey().map(s->{
					return s._1.getPeople_name()+"\t"+s._1.getNumber();
				}).saveAsTextFile(outputDataPath);
		sc.close();
		
		

		
		
		
	}

}
