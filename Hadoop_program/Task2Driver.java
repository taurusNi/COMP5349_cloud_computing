package assignment1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task2Driver {
public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 1) {
			//use the second input arguments as the output path, the optional input is the limitNum (the maximum output lines)
			System.err.println("Usage: ReplicationJoinDriver <out> [,<limitNum>]  ");
			System.exit(2);
		}
		
		Job SortJob = new Job(conf,"Sort Limit");
		SortJob.setJarByClass(Task2Driver.class);
		SortJob.setNumReduceTasks(1);
		SortJob.setMapperClass(Task2Mapper.class);
		SortJob.setReducerClass(Task2Reducer.class);
		SortJob.setOutputKeyClass(SortNumber.class);
		SortJob.setOutputValueClass(Text.class);
		if(otherArgs[otherArgs.length-1].contains("Limit_num")){ //the last input
			String [] temp  = otherArgs[otherArgs.length-1].split("=");
			conf.set("limit.number",temp[1]);
		}
		for(int i=0;i<3;i++){
			//Default is 3 reducers of the Task2, so put three files into input path
			MultipleInputs.addInputPath(SortJob,new Path("/user/heni7690/assignment1/task1/part-r-0000"+i) , 
					TextInputFormat.class,Task2Mapper.class);
		}
		TextOutputFormat.setOutputPath(SortJob, new Path(otherArgs[0]));
		SortJob.waitForCompletion(true);
		// remove the temporary path
		System.exit(SortJob.waitForCompletion(true) ? 0 : 1);
	}
	
	
}
