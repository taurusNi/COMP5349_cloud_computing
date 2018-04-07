package assignment2;

import java.io.Serializable;
import java.util.ArrayList;

public class PointMulti implements Serializable,Comparable{
	private Double [] dimensions;
	private double dis;
//	private int cluster;
	public PointMulti(Double [] a) {
		this.dimensions = a;

	}
	public double getDis() {
		return dis;
	}

	public void setDis(double dis) {
		this.dis = dis;
	}
	
	
	public Double [] getDimensions() {
		return dimensions;
	}
	public void setDimensions(Double [] dimensions) {
		this.dimensions = dimensions;
	}
	
	//done
//	public static double distance(Point a,Point b){
//		double dis = Math.sqrt(Math.pow
//				(a.CD11b-b.CD11b,2.0)+Math.pow
//				(a.Ly6C-b.Ly6C,2.0)+Math.pow
//				(a.SCA1-b.SCA1,2.0));
//		return dis;
//	}
	public static double distance(PointMulti a,PointMulti b){
		double sum=0;
		for(int i=0;i<a.dimensions.length;i++){
			sum+=Math.pow(a.dimensions[i]-b.dimensions[i], 2.0);
		}
		return Math.sqrt(sum);
		}
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for(Double e:dimensions){
			sb.append(e+"\t");
		}
		return sb.toString();
	}

	@Override
	public int compareTo(Object o) {
		PointMulti m2 = (PointMulti)o;
		if(this.dis - m2.dis>0){//done
			return -1;
		}
		if(this.dis - m2.dis<0){
			return 1;
		}
		if(this.dis - m2.dis==0){
			if(this.dimensions[0]-m2.dimensions[0]>0){
				return -1;
			}
			if(this.dimensions[0]-m2.dimensions[0]<0){
				return 1;
			}
			return 0;
		}
		return 0;
//		double dff = this.dis - m2.dis;
//		if(dff!=0){
//			return (int) (dff*(-1));
//		}
	}
	

}
