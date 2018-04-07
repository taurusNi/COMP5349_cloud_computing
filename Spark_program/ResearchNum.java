package assignment2;

import java.io.Serializable;

public class ResearchNum implements Comparable, Serializable{
	
	private String people_name;
	private int number;
	
	public ResearchNum(String people_name, int number) {
		super();
		this.people_name = people_name;
		this.number = number;
	}
	public String getPeople_name() {
		return people_name;
	}
	public void setPeople_name(String people_name) {
		this.people_name = people_name;
	}
	public int getNumber() {
		return number;
	}
	public void setNumber(int number) {
		this.number = number;
	}
	@Override
	public String toString() {
		return people_name + "\t" +  number;
	}
	@Override
	public int compareTo(Object o) {
		ResearchNum m2 = (ResearchNum)o;
		int dff = this.number - m2.number;
		if(dff!=0){
			return dff*(-1);
		}
		return this.people_name.compareTo(m2.getPeople_name());
	}
	
	
	
	

}
