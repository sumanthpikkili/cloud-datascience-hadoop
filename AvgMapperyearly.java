/*

Author Name: Sumanth Pikkili
UTA ID: 1001100941
CSE 6331 - 002
Assignment 4: Introduction to Data Science using Map-Reduce (Hadoop) and ML

 */

package cloudassignment4;


import java.io.IOException;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.commons.lang.StringUtils;

//Mapper Class
public class AvgMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
					throws IOException {
		String[] line = value.toString().split(","); // Fetching a record and splitting the columns by ","
		String dataPart = line[2]; // Fetching Date
		String avgtemp = line[3]; // Fetching Temperature
		String precp = line[6]; //Fetching Precipitation
		String wind = line[13]; //Fetching Wind

		String year  = dataPart.substring(0,4);

		if(StringUtils.isNumeric(avgtemp)){
			//Sending the year as a key
			output.collect(new Text(year),new Text(avgtemp + "," + precp + "," + wind));
		}

	}
}
