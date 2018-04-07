package assignment2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import ml.MovieRatingCount;
import scala.Tuple2;
/**
 * measures
 * sample, FSC-A, SSC-A, CD48, Ly6G, CD117, SCA1, CD11b, CD150, CD11c, B220, Ly6C,...
 * 
 * It reads the output of task2 and transform the data into pair of (clusterID,centers) which is used to get newest centers
 * and pair of (clusterID, filterNum) which is used to get the number of point need to be removed
 * 
 * it reads all valid records of measurements and transforms records into pair of data with instance of class Point
 * and then remove the outliers depending on the filterNum and distance of each point to each cluter center which will 
 * get new data set and cache the data for later use.
 * 
 * using K-means to get newest centers
 * get each cluster with its ID, number and center.
 * 
 * output
 * clusterID \t number_of_measurements \t Ly6C \t CD11b \t SCA1
 * 
 * 
 * @author taurus
 *
 */
public class task3 {
	public static void main(String[] args) {
		if(args.length<4){
			System.err.println("Usage: K-means <outputPath>  "
					+ "<the number of clusters> [<the number of itreations>] <task2OutPutPath> <inputMeasurementsPath...> ");
			System.exit(2);
		}
		String outputDataPath = args[0];
		int k = Integer.parseInt(args[1]);
		String training_data=null;
		String task2Out=null;
		int iterations=0;
		if(args.length==4){
			training_data = args[3];
			task2Out = args[2];
			iterations = 10;
		}else{
			training_data = args[4];
			task2Out = args[3];
			iterations = Integer.parseInt(args[2]);
		}
		
		SparkConf conf = new SparkConf();
		conf.setAppName("outlier");
		//It is used to access a cluster
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaPairRDD<Integer,Point> tempresult2 =null;
		
//		Double [] distanceLimit = new Double[k];
//		for(int i=0;i<k;i++){
//			distanceLimit[i]=10.0;
//		}
		
		List<Tuple2<Integer,Point>> centers = sc.textFile(task2Out).mapToPair(s->{
			String [] elements = s.split("\t");
			int clusterID = Integer.parseInt(elements[0]);
//			int num = Integer.parseInt(elements[1]);
			double Ly6C = Double.parseDouble(elements[2]);
			double CD11b = Double.parseDouble(elements[3]);
			double SCA1 = Double.parseDouble(elements[4]);
			return new Tuple2<Integer,Point>(clusterID,new Point(Ly6C,CD11b,SCA1));
		}).collect();
		Point [] clusters = new Point[k];
		for(int m=0;m<k;m++){
			clusters[centers.get(m)._1-1] = centers.get(m)._2;
		}
		JavaPairRDD<Integer,Integer> number = sc.textFile(task2Out).mapToPair(s->{
			String [] elements = s.split("\t");
			int clusterID = Integer.parseInt(elements[0]);
			int num = Integer.parseInt(elements[1]);
//			double Ly6C = Double.parseDouble(elements[2]);
//			double CD11b = Double.parseDouble(elements[3]);
//			double SCA1 = Double.parseDouble(elements[4]);
			return new Tuple2<Integer,Integer>(clusterID,(int)( num*0.1));
		}).cache();
		JavaPairRDD<Integer,Point> newDataSet = sc.textFile(training_data).filter(s->{
			
			String[] elements = s.split(",");
			
			//elements[1] FSC-A elements[2] SSC-A
			if(elements.length==17){ //valid record length
				if(elements[0].equals("sample")){
					return false;
				}else{
					return (Integer.parseInt(elements[1])>=1
							&&Integer.parseInt(elements[1])<=150000
							&&Integer.parseInt(elements[2])>=1
							&&Integer.parseInt(elements[2])<=150000);
				}
			}else{
				return false;
			}
			
//sample[0], FSC-A[1], SSC-A[2], CD48[3], Ly6G[4], CD117[5], SCA1[6], CD11b[7], CD150[8], CD11c[9], B220[10], Ly6C[11]
		}).mapToPair(s->{//done
			String[] elements = s.split(",");
			double Ly6C = Double.parseDouble(elements[11]);
			double CD11b = Double.parseDouble(elements[7]);
			double SCA1 = Double.parseDouble(elements[6]);
			//public Point(double ly6c, double cD11b, double sCA1) {
			return new Tuple2<Integer,Point>(0,new Point(Ly6C,CD11b,SCA1)); //0 means no cluster
			
		}).mapToPair(s->{
			int place=0;//the first cluster
			double dis  = Point.distance(s._2, clusters[0]);
			for(int j=1;j<k;j++){//starting from second cluster
				if(dis>Point.distance(s._2, clusters[j])){//find minimum distance
					dis = Point.distance(s._2, clusters[j]);
					place=j;
					s._2.setDis(dis);
				}
			}
			return new Tuple2<Integer,Point>(place+1,s._2);//get each point belongs to each cluster
		}).aggregateByKey(
				new ArrayList<Point>(),
				1,
				(r,v)->{r.add(v);
						return r;},
				(r1,r2)->{r1.addAll(r2);
				return r1;}
				).join(number).flatMapToPair(s->{
					Collections.sort(s._2._1);//sort Point descending by distance with cluster
					ArrayList<Tuple2<Integer,Point>> temp = new ArrayList<Tuple2<Integer,Point>>();
					for(Point e:s._2._1.subList(s._2._2, s._2._1.size())){//done
						temp.add(new Tuple2<Integer,Point>(0,e));
					};
					return temp.iterator();
					
				}).cache();

//		//random get initial center points
		Point [] clusters2 = new Point[k];
		List<Tuple2<Integer,Point>> temp = newDataSet.takeSample(false, k);
		for(int i=0;i<k;i++){
			clusters2[i] = temp.get(i)._2;
		}
//		//fixed
//		Point [] clusters2 = new Point[k];
//		for(int l=0;l<k;l++){
//			clusters2[l]=new Point(l,l,l);
//		}
//		tempresult2= kmean(iterations,newDataSet,k,clusters2);
		for(int i=0;i<iterations;i++){
			 tempresult2 = newDataSet.mapToPair(s->{
				int place=0;//the first cluster
				double dis  = Point.distance(s._2, clusters2[0]);
				for(int j=1;j<k;j++){//starting from second cluster
					if(dis>Point.distance(s._2, clusters2[j])){//find minimum distance
						dis = Point.distance(s._2, clusters2[j]);
						place=j;
					}
				}
				return new Tuple2<Integer,Point>(place+1,s._2);//get each point belongs to each cluster
				
			}).aggregateByKey(
					new Tuple2<Point,Integer>(new Point(0,0,0),0),
					1,
					//done
					//public Point(double ly6c, double cD11b, double sCA1) {
					(r,v)-> new Tuple2<Point,Integer>(new Point(r._1.getLy6C()+v.getLy6C(),
							r._1.getCD11b()+v.getCD11b(),r._1.getSCA1()+v.getSCA1()),r._2+1),
					//done
					(r1,r2)->new Tuple2<Point,Integer>(new Point(r1._1.getLy6C()+r2._1.getLy6C(),
							r1._1.getCD11b()+r2._1.getCD11b(),r1._1.getSCA1()+r2._1.getSCA1()),
							r1._2+r2._2))
			.mapToPair(s->{
				//public Point(double ly6c, double cD11b, double sCA1) {
				//done
				return new Tuple2<Integer,Point>(s._1,new Point(s._2._1.getLy6C()/s._2._2,s._2._1.getCD11b()/s._2._2,
						s._2._1.getSCA1()/s._2()._2));
			}).cache();
//			result.saveAsTextFile(outputDataPath+i);
			List<Tuple2<Integer,Point>> newCenters = tempresult2.collect();
			double sum=0;
			for(int m=0;m<k;m++){
				sum += Point.distance(clusters2[newCenters.get(m)._1-1], newCenters.get(m)._2);//._1 gets the cluster 
			}
			if(sum<0.0001){//threshold for stop iterating
				break;
			}else{
				for(int m=0;m<k;m++){
					clusters2[newCenters.get(m)._1-1] = newCenters.get(m)._2;
				}
			}
		}
		newDataSet.mapToPair(s->{
			int place=0;//the first cluster
			double dis  = Point.distance(s._2, clusters2[0]);
			for(int j=1;j<k;j++){//starting from second cluster
				if(dis>Point.distance(s._2, clusters2[j])){//find minimum distance
					dis = Point.distance(s._2, clusters2[j]);
					place=j;
				}
			}
			return new Tuple2<Integer,Integer>(place+1,1);//get each point belongs to each cluster
			
		}).aggregateByKey(
				0,
				1,
				(r,v)->r+v,
				(r1,r2)->r1+r2)
//		.mapToPair(s->{
//			return new Tuple2<Integer,Integer>(s._1,s._2);
//		})
		.join(tempresult2).sortByKey().map(s->{
			return s._1+"\t"+s._2._1+"\t"+s._2._2.getLy6C()+"\t"+s._2._2.getCD11b()+"\t"+s._2._2.getSCA1();
		}).saveAsTextFile(outputDataPath);
		sc.close();
	}
	public static JavaPairRDD<Integer,Point> kmean(int iterations,JavaPairRDD<Integer,Point> newDataSet,int k,Point [] clusters2){
		JavaPairRDD<Integer,Point> tempresult2 =null;
		for(int i=0;i<iterations;i++){
			 tempresult2 = newDataSet.mapToPair(s->{
				int place=0;//the first cluster
				double dis  = Point.distance(s._2, clusters2[0]);
				for(int j=1;j<k;j++){//starting from second cluster
					if(dis>Point.distance(s._2, clusters2[j])){//find minimum distance  done
						dis = Point.distance(s._2, clusters2[j]);
						place=j;
					}
				}
				return new Tuple2<Integer,Point>(place+1,s._2);//get each point belongs to each cluster
				
			}).aggregateByKey(
					new Tuple2<Point,Integer>(new Point(0,0,0),0),
					1,
					//done
					//public Point(double ly6c, double cD11b, double sCA1) {
					(r,v)-> new Tuple2<Point,Integer>(new Point(r._1.getLy6C()+v.getLy6C(),
							r._1.getCD11b()+v.getCD11b(),r._1.getSCA1()+v.getSCA1()),r._2+1),
					//done
					(r1,r2)->new Tuple2<Point,Integer>(new Point(r1._1.getLy6C()+r2._1.getLy6C(),
							r1._1.getCD11b()+r2._1.getCD11b(),r1._1.getSCA1()+r2._1.getSCA1()),
							r1._2+r2._2))
			.mapToPair(s->{
				//public Point(double ly6c, double cD11b, double sCA1) {
				//done
				return new Tuple2<Integer,Point>(s._1,new Point(s._2._1.getLy6C()/s._2._2,s._2._1.getCD11b()/s._2._2,
						s._2._1.getSCA1()/s._2()._2));
			}).cache();
//			result.saveAsTextFile(outputDataPath+i);
			List<Tuple2<Integer,Point>> newCenters = tempresult2.collect();
			double sum=0;
			for(int m=0;m<k;m++){
				sum += Point.distance(clusters2[newCenters.get(m)._1-1], newCenters.get(m)._2);//._1 gets the cluster 
			}
			if(sum<0.0001){//threshold for stop iterating
				break;
			}else{
				for(int m=0;m<k;m++){
					clusters2[newCenters.get(m)._1-1] = newCenters.get(m)._2;
				}
			}
		}
		return tempresult2;
	}

}
