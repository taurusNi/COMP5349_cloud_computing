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

import assignment1.MapGetPlaceNameDriver;
import assignment1.Task1MapJoinMapper;

public class Task3Driver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 3) {
			System.err.println("Usage: ReplicationJoinDriver <inPlace> <out> <inPhoto...>  ");
			System.exit(2);
		}
		
		Path tmpFilterOut = new Path("/user/heni7690/assignment1/tmpFilterOut");
		// the first job
		Job placeFilterJob = new Job(conf, "Place Filter");
		placeFilterJob.setJarByClass(MapGetPlaceNameDriver.class);
		placeFilterJob.setNumReduceTasks(0);
		placeFilterJob.setMapperClass(MapGetPlaceForTask3.class);
		placeFilterJob.setOutputKeyClass(Text.class);
		placeFilterJob.setOutputValueClass(Text.class);
		TextInputFormat.addInputPath(placeFilterJob, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(placeFilterJob, tmpFilterOut);
		placeFilterJob.waitForCompletion(true);
		
		//second Job
		Path tmpFilterOut5 = new Path("/user/heni7690/assignment1/tmpFilterOut5");
		Job tagJob = new Job(conf, "counting of tag number and photo number");
		DistributedCache.addCacheFile(new Path("/user/heni7690/assignment1/tmpFilterOut/part-m-00000").toUri(),tagJob.getConfiguration());
		tagJob.setJarByClass(Task3Driver.class);
		tagJob.setNumReduceTasks(6);
		tagJob.setMapperClass(Task3MapperForTags.class);
		tagJob.setCombinerClass(Task3CombinerForTags.class);
		tagJob.setPartitionerClass(Task3Partitioner.class);
		tagJob.setReducerClass(Task3ReducerForTags.class);
		tagJob.setOutputKeyClass(Text.class);
		tagJob.setOutputValueClass(Text.class);
		for(int i=2;i<otherArgs.length;i++){
			MultipleInputs.addInputPath(tagJob,new Path(otherArgs[i]) , 
					TextInputFormat.class,Task3MapperForTags.class);
		}
		TextOutputFormat.setOutputPath(tagJob, tmpFilterOut5);
		tagJob.waitForCompletion(true);
		
		//third job
		Job lastJob = new Job(conf,"Output top 50");
		lastJob.setJarByClass(Task3Driver.class);
		lastJob.setNumReduceTasks(1);
		lastJob.setMapperClass(Task3LastMapper.class);
		lastJob.setReducerClass(Task3LastReducer.class);
		lastJob.setOutputKeyClass(SortNumber.class);
		lastJob.setOutputValueClass(Text.class);
		for(int i=0;i<6;i++){
			MultipleInputs.addInputPath(lastJob,new Path("/user/heni7690/assignment1/tmpFilterOut5/part-r-0000"+i) , 
					TextInputFormat.class,Task3LastMapper.class);
		}
		TextOutputFormat.setOutputPath(lastJob, new Path(otherArgs[1]));
		lastJob.waitForCompletion(true);
		FileSystem.get(conf).delete(tmpFilterOut5, true);
		FileSystem.get(conf).delete(tmpFilterOut, true);
		System.exit(lastJob.waitForCompletion(true) ? 0 : 1);
	}

}
