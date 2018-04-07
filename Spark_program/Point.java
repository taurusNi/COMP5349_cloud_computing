package assignment2;

import java.io.Serializable;

public class Point implements Serializable,Comparable{
	private double Ly6C; 
	private double CD11b;
	private double SCA1;
	private double dis;
//	private int cluster;
	public Point(double ly6c, double cD11b, double sCA1) {
		this.Ly6C = ly6c;
		this.CD11b = cD11b;
		this.SCA1 = sCA1;
	}
	


	public double getDis() {
		return dis;
	}

	public void setDis(double dis) {
		this.dis = dis;
	}
	
	public double getLy6C() {
		return Ly6C;
	}
	public void setLy6C(double ly6c) {
		Ly6C = ly6c;
	}
	public double getCD11b() {
		return CD11b;
	}
	public void setCD11b(double cD11b) {
		CD11b = cD11b;
	}
	public double getSCA1() {
		return SCA1;
	}
	public void setSCA1(double sCA1) {
		SCA1 = sCA1;
	}
	//done
	public static double distance(Point a,Point b){
		double dis = Math.sqrt(Math.pow
				(a.CD11b-b.CD11b,2.0)+Math.pow
				(a.Ly6C-b.Ly6C,2.0)+Math.pow
				(a.SCA1-b.SCA1,2.0));
		return dis;
	}
	@Override
	public String toString() {
		return "Point [Ly6C=" + Ly6C + ", CD11b=" + CD11b + ", SCA1=" + SCA1 + "]";
	}

	@Override
	public int compareTo(Object o) {
		Point m2 = (Point)o;
		if(this.dis - m2.dis>0){//done
			return -1;
		}
		if(this.dis - m2.dis<0){
			return 1;
		}
		if(this.dis - m2.dis==0){
			if(this.Ly6C- m2.Ly6C>0){
				return -1;
			}
			if(this.Ly6C- m2.Ly6C<0){
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
