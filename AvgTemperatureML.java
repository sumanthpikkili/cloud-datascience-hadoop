/*
Author Name: Sumanth Pikkili
UTA ID: 1001100941
CSE 6331 - 002
Assignment 4: Introduction to Data Science using Map-Reduce (Hadoop) and ML
 */

package cloudassignment4_ML;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

//Combiner Class
public class AvgTemperature {
	public static void main(String[] args) throws IOException {

		JobConf conf = new JobConf(AvgTemperature.class);
		conf.setJobName("Average Temperature");
		//Setting the argument to accept the number of mappers as user input
		conf.setNumMapTasks(Integer.parseInt(args[2]));
		//Setting the argument to accept the number of reducers as user input
		conf.setNumReduceTasks(Integer.parseInt(args[3]));

		double start_time = System.currentTimeMillis();
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		//Setting the Mapper Class
		conf.setMapperClass(AvgMapper.class);
		//Setting the Reducer Class
		conf.setReducerClass(AvgReducer.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		//Setting the argument to accept input path
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		//Setting the argument to accept the output path
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);

		double end_time = System.currentTimeMillis();
		double elapsed_time = end_time-start_time;
		System.out.println("The total time taken to run is: " + elapsed_time);

	}
}

