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

public class Task1MapJoinDriver {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 3) {
			//use the second input arguments as the output path
			System.err.println("Usage: ReplicationJoinDriver <inPlace> <out> <inPhoto...>  ");
			System.exit(2);
		}
		Path tmpFilterOut = new Path("/user/heni7690/assignment1/tmpFilterOut");
		// the first job
		Job placeFilterJob = new Job(conf, "Place Filter");
		placeFilterJob.setJarByClass(MapGetPlaceNameDriver.class);
		placeFilterJob.setNumReduceTasks(0);
//		placeFilterJob.setMapperClass(MapGetPlaceName.class);
		placeFilterJob.setMapperClass(MapGetPlaceForTask3.class);
		placeFilterJob.setOutputKeyClass(Text.class);
		placeFilterJob.setOutputValueClass(Text.class);
		TextInputFormat.addInputPath(placeFilterJob, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(placeFilterJob, tmpFilterOut);
		placeFilterJob.waitForCompletion(true);
		
		// the second job
		Job joinJob = new Job(conf, "Replication Join");
		//store the file generated by job one in the distributedcache
		DistributedCache.addCacheFile(new Path("/user/heni7690/assignment1/tmpFilterOut/part-m-00000").toUri(),joinJob.getConfiguration());
		joinJob.setJarByClass(Task1MapJoinDriver.class);
		joinJob.setNumReduceTasks(3);
		joinJob.setMapperClass(Task1MapJoinMapper.class); 
		joinJob.setCombinerClass(Task1Combiner.class);
		joinJob.setReducerClass(Task1Reducer.class);
		joinJob.setOutputKeyClass(Text.class);
		joinJob.setPartitionerClass(Task1Partitioner.class);
		//as the number is IntWriteable
		joinJob.setOutputValueClass(IntWritable.class);
//		joinJob.setOutputValueClass(Text.class);
		for(int i=2;i<otherArgs.length;i++){
			//add multiple inputs from the third to the last arguments
			MultipleInputs.addInputPath(joinJob,new Path(otherArgs[i]) , 
					TextInputFormat.class,Task1MapJoinMapper.class);
		}
		TextOutputFormat.setOutputPath(joinJob, new Path(otherArgs[1]));
		joinJob.waitForCompletion(true);
		FileSystem.get(conf).delete(tmpFilterOut, true);// remove the temporary path
		System.exit(joinJob.waitForCompletion(true) ? 0 : 1);
	}
}
