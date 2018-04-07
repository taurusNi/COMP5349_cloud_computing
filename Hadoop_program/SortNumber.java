package assignment1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * This class is used to be a specific key used to sort
 * 
 * @author taurus
 *
 */

public class SortNumber implements WritableComparable<SortNumber> {

	private IntWritable num;
	
	public SortNumber(){
		num = new IntWritable();
	}
	
	public SortNumber(int num){
		this.num=new IntWritable(num);
	}
	
	
	public IntWritable getNum() {
		return num;
	}


	public void setNum(IntWritable num) {
		this.num = num;
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		num.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		num.write(out);
	}

	@Override
	public int compareTo(SortNumber other) {
		int cmp = num.compareTo(other.num)*(-1); //descending order
		return cmp;
	}
	

}
