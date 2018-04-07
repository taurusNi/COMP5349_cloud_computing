package assignment2;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
/**
 * measures
 * sample, FSC-A, SSC-A, CD48, Ly6G, CD117, SCA1, CD11b, CD150, CD11c, B220, Ly6C,...
 * 
 * it reads all valid records of measurements and transforms records into pair of data with instance of class Point
 * and cache the data for later use.
 * using K-means to get newest centers
 * get each cluster with its ID, number and center.
 * 
 * output
 * clusterID \t number_of_measurements \t Ly6C \t CD11b \t SCA1
 * 
 * @author taurus
 *
 */
public class task2 {
	public static void main(String[] args) {
		if(args.length<3){
			System.err.println("Usage: K-means <outputPath>  "
					+ "<the number of clusters> [<the number of itreations>] <inputMeasurementsPath...> ");
			System.exit(2);
		}
		String outputDataPath = args[0];
		int k = Integer.parseInt(args[1]);
		int iterations =0;
		String training_data = null;
		if(args.length==3){
			training_data = args[2];
			iterations=10;
			
		}else{
			training_data = args[3];
			iterations=Integer.parseInt(args[2]);
		}
		
		SparkConf conf = new SparkConf();
		conf.setAppName("K-means");
		//It is used to access a cluster
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaPairRDD<Integer,Point> tempresult =null;
		JavaPairRDD<Integer,Point> points = sc.textFile(training_data).filter(s->{
			
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
		}).mapToPair(s->{//done2
			String[] elements = s.split(",");
			double Ly6C = Double.parseDouble(elements[11]);
			double CD11b = Double.parseDouble(elements[7]);
			double SCA1 = Double.parseDouble(elements[6]);
			//public Point(double ly6c, double cD11b, double sCA1) {
			return new Tuple2<Integer,Point>(0,new Point(Ly6C,CD11b,SCA1)); //0 means no cluster
			
		}).cache();
		//random get initial center points
		Point [] clusters = new Point[k];
		List<Tuple2<Integer,Point>> temp = points.takeSample(false, k);
		for(int i=0;i<k;i++){
			clusters[i] = temp.get(i)._2;
		}
		//fixed
//		Point [] clusters = new Point[k];
//		for(int l=0;l<k;l++){
//			double t = (double) l;
//			clusters[l]=new Point(t,t,t);
//		}
		
		//k-means
		for(int i=0;i<iterations;i++){
			 tempresult = points.mapToPair(s->{
				int place=0;//the first cluster
				double dis  = Point.distance(s._2, clusters[0]);
				for(int j=1;j<k;j++){//.done starting from second cluster
					if(dis>Point.distance(s._2, clusters[j])){//find minimum distance
						dis = Point.distance(s._2, clusters[j]);
						place=j;
					}
				}
				return new Tuple2<Integer,Point>(place+1,s._2);//get each point belongs to each cluster
				
			}).aggregateByKey(
					new Tuple2<Integer,Point>(0,new Point(0,0,0)),
					1,
					//done
					//public Point(double ly6c, double cD11b, double sCA1) {
					(r,v)-> new Tuple2<Integer,Point>(r._1+1,new Point(r._2.getLy6C()+v.getLy6C(),
							r._2.getCD11b()+v.getCD11b(),r._2.getSCA1()+v.getSCA1())),
					//done
					(r1,r2)->new Tuple2<Integer,Point>(r1._1+r2._1,new Point(r1._2.getLy6C()+r2._2.getLy6C(),
							r1._2.getCD11b()+r2._2.getCD11b(),r1._2.getSCA1()+r2._2.getSCA1())))
					//done
			.mapToPair(s->{
				//public Point(double ly6c, double cD11b, double sCA1) {
				//done2
				return new Tuple2<Integer,Point>(s._1,new Point(s._2._2.getLy6C()/s._2._1,s._2._2.getCD11b()/s._2._1,
						s._2._2.getSCA1()/s._2._1));
			}).cache();
//			result.saveAsTextFile(outputDataPath+i);
			List<Tuple2<Integer,Point>> newCenters = tempresult.collect();
			double sum=0;
			for(int m=0;m<k;m++){
				sum += Point.distance(clusters[newCenters.get(m)._1-1], newCenters.get(m)._2);//._1 gets the cluster 
			}
			if(sum<0.0001){//threshold for stop iterating
				break;
			}else{
				for(int m=0;m<k;m++){
					clusters[newCenters.get(m)._1-1] = newCenters.get(m)._2;
				}
			}
		}
		
		points.mapToPair(s->{
			int place=0;//the first cluster
			double dis  = Point.distance(s._2, clusters[0]);
			for(int j=1;j<k;j++){//starting from second cluster
				if(dis>Point.distance(s._2, clusters[j])){//find minimum distance
					dis = Point.distance(s._2, clusters[j]);
					place=j;
				}
			}
			return new Tuple2<Integer,Integer>(place+1,1);//get each point belongs to each cluster
			
		}).aggregateByKey(
				0,
				1,
				//done
				//public Point(double ly6c, double cD11b, double sCA1) {
				(r,v)->r+v,
				//done
				(r1,r2)->r1+r2)
		.join(tempresult).sortByKey().map(s->{
			return s._1+"\t"+s._2._1+"\t"+s._2._2.getLy6C()+"\t"+s._2._2.getCD11b()+"\t"+s._2._2.getSCA1();
		}).saveAsTextFile(outputDataPath);
		sc.close();
		
	}

}
