package assignment2;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * measures
 * sample, FSC-A, SSC-A, CD48, Ly6G, CD117, SCA1, CD11b, CD150, CD11c, B220, Ly6C,...
 * 
 * it reads all valid records of measurements and transforms records into pair of data with instance of class PointMulti
 * and cache the data for later use. The dimension number should be 0-13.
 * using K-means to get newest centers
 * get each cluster with its ID, number and center.
 * 
 * output
 * clusterID \t number_of_measurements \t Ly6C \t CD11b \t SCA1 \t something
 * @author taurus
 *
 */

public class task2Multiple {
	public static void main(String[] args) {
		
		if(args.length<4){
			System.err.println("Usage: K-means <outputPath>  "
					+ "<the number of clusters> [<the number of itreations>] <D=additional dimensions,.....>> "
					+ "<inputMeasurementsPath...> ");
			System.exit(2);
		}
//		int dnum = 0;
		String outputDataPath = args[0];
		int iterations=0;
		String training_data=null;
		String [] D = null;
		int k = Integer.parseInt(args[1]);
		if(args.length==4){
			training_data = args[3];
			iterations = 10;
			D= args[2].split("=");
		}else{
			training_data = args[4];
			iterations = Integer.parseInt(args[2]);
			D= args[3].split("=");
		}
		
		String [] d = D[1].split(",");
		SparkConf conf = new SparkConf();
		conf.setAppName("K-means");
		//It is used to access a cluster
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaPairRDD<Integer,PointMulti> tempresult =null;
		JavaPairRDD<Integer,PointMulti> points = sc.textFile(training_data).filter(s->{
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
			Double [] dimen = new Double[d.length];
//			double Ly6C = Double.parseDouble(elements[11]);
//			double CD11b = Double.parseDouble(elements[7]);
//			double SCA1 = Double.parseDouble(elements[6]);
			for(int l=0;l<d.length;l++){
				dimen[l] = Double.parseDouble(elements[Integer.parseInt(d[l])+3]);
			}
			//public Point(double ly6c, double cD11b, double sCA1) {
//			dnum  = dimen.size();
			return new Tuple2<Integer,PointMulti>(0,new PointMulti(dimen)); //0 means no cluster
			
		}).cache();
		
		//random get initial center points
		Double [] men  = new Double[d.length];
		for(int t=0;t<d.length;t++){
			men[t]=0.0;
		}
		PointMulti [] clusters = new PointMulti[k];
		List<Tuple2<Integer,PointMulti>> temp = points.takeSample(false, k);
		for(int i=0;i<k;i++){
			clusters[i] = temp.get(i)._2;
		}
		//fixed
//		PointMulti [] clusters = new PointMulti[k];
//		for(int i=0;i<k;i++){
//			Double [] dimen = new Double[d.length];
//			for(int j=0;j<d.length;j++){
//				dimen[j]=  (double) i;
//			}
//			clusters[i] = new PointMulti(dimen);
//		}
		
		//kmaens
		for(int i=0;i<iterations;i++){
			tempresult = points.mapToPair(s->{
				int place=0;//the first cluster
				double dis  = PointMulti.distance(s._2, clusters[0]);
				for(int j=1;j<k;j++){//starting from second cluster
					if(dis>PointMulti.distance(s._2, clusters[j])){//find minimum distance
						dis = PointMulti.distance(s._2, clusters[j]);
						place=j;
					}
				}
				return new Tuple2<Integer,PointMulti>(place+1,s._2);//get each point belongs to each cluster
				
			}).aggregateByKey(
					new Tuple2<Integer,PointMulti>(0,new PointMulti(men)),
					1,
					(r,v)->new Tuple2<Integer,PointMulti>(r._1+1,new PointMulti(update(r._2.getDimensions(),v.getDimensions()))), 
					(r1,r2)->new Tuple2<Integer,PointMulti>(r1._1+r2._1,new PointMulti(update(r1._2.getDimensions(),r2._2.getDimensions()))))
			.mapToPair(s->{
				for(int mm=0;mm<s._2._2.getDimensions().length;mm++){
					s._2._2.getDimensions()[mm] = s._2._2.getDimensions()[mm]/s._2._1;
				}
				return new Tuple2<Integer,PointMulti>(s._1,s._2._2);
			}).cache();
//			tempresult.cache();
			List<Tuple2<Integer,PointMulti>> newCenters = tempresult.collect();
			double sum=0;
			for(int m=0;m<k;m++){
				sum += PointMulti.distance(clusters[newCenters.get(m)._1-1], newCenters.get(m)._2);//._1 gets the cluster 
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
			double dis  = PointMulti.distance(s._2, clusters[0]);
			for(int j=1;j<k;j++){//starting from second cluster
				if(dis>PointMulti.distance(s._2, clusters[j])){//find minimum distance
					dis = PointMulti.distance(s._2, clusters[j]);
					place=j;
				}
			}
			return new Tuple2<Integer,Integer>(place+1,1);//get each point belongs to each cluster
			
		}).aggregateByKey(
				0,
				1,
				(r,v)->r+v,
				(r1,r2)->r1+r2)
		.join(tempresult).sortByKey().map(s->{
			StringBuffer sb = new StringBuffer();
			sb.append(s._1+"\t");
			sb.append(s._2._1+"\t");
			for(int i=0;i<s._2._2.getDimensions().length;i++){
				sb.append(s._2._2.getDimensions()[i]+"\t");
			}
			return sb.toString();
//			return s._1+"\t"+s._2._1+"\t"+s._2._2.getLy6C()+"\t"+s._2._2.getCD11b()+"\t"+s._2._2.getSCA1();
		}).saveAsTextFile(outputDataPath);
		sc.close();			
			
			
			
		}
	
	public static Double [] update(Double[]a,Double[]b){
		for(int m=0;m<b.length;m++){
			a[m]=a[m]+b[m];
		}
		return a;
	}
		
		
	}


