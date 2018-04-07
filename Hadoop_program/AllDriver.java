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

public class AllDriver {

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
		
		// the second job
		Path tasktemp = new Path("/user/heni7690/assignment1/task1");
		Job joinJob = new Job(conf, "Replication Join");
		DistributedCache.addCacheFile(new Path("/user/heni7690/assignment1/tmpFilterOut/part-m-00000").toUri(),joinJob.getConfiguration());
		joinJob.setJarByClass(Task1MapJoinDriver.class);
		joinJob.setNumReduceTasks(3);
		joinJob.setMapperClass(Task1MapJoinMapper.class); 
		joinJob.setCombinerClass(Task1Combiner.class);
		joinJob.setReducerClass(Task1Reducer.class);
		joinJob.setOutputKeyClass(Text.class);
		joinJob.setPartitionerClass(Task1Partitioner.class);
		joinJob.setOutputValueClass(IntWritable.class);
		if(otherArgs[otherArgs.length-1].contains("Limit_num")){ //the last input
			for(int i=2;i<otherArgs.length-1;i++){ //the last input is ignored
				MultipleInputs.addInputPath(joinJob,new Path(otherArgs[i]) , 
						TextInputFormat.class,Task1MapJoinMapper.class);
			}
			String [] temp  = otherArgs[otherArgs.length-1].split("=");
			conf.set("limit.number",temp[1]);
		}else{
		for(int i=2;i<otherArgs.length;i++){
			MultipleInputs.addInputPath(joinJob,new Path(otherArgs[i]) , 
					TextInputFormat.class,Task1MapJoinMapper.class);
		}
		}
		TextOutputFormat.setOutputPath(joinJob, tasktemp);
		joinJob.waitForCompletion(true);
		
		//third job
		Path task2 =  new Path("/user/heni7690/assignment1/task2");
		Job SortJob = new Job(conf,"Sort Limit");
		SortJob.setJarByClass(Task2Driver.class);
		SortJob.setNumReduceTasks(1);
		SortJob.setMapperClass(Task2Mapper.class);
		SortJob.setReducerClass(Task2Reducer.class);
		SortJob.setOutputKeyClass(SortNumber.class);
		SortJob.setOutputValueClass(Text.class);
		for(int i=0;i<3;i++){
			MultipleInputs.addInputPath(SortJob,new Path("/user/heni7690/assignment1/task1/part-r-0000"+i) , 
					TextInputFormat.class,Task2Mapper.class);
		}
		TextOutputFormat.setOutputPath(SortJob, task2);
		SortJob.waitForCompletion(true);
//		FileSystem.get(conf).delete(tmpFilterOut, true);// remove the temporary path
//		FileSystem.get(conf).delete(task1, true);
		
		//fourth Job
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
		
		//fifth job
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
