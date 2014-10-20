package mapred;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/*
 * @Author Manish Sapkota
 * Cloud computing project 2 page ranking
 */
public class RankingMapper
extends MapReduceBase
implements Mapper<LongWritable, Text, FloatWritable, Text>
{
	// read each line and emit page rank as key and page node as value to sort
	public void map(LongWritable key, Text value, OutputCollector<FloatWritable, Text> output, Reporter reporter)
			throws IOException
	{
		String line = value.toString();
		if (!line.isEmpty())
		{
			String[] tokens = line.split(" ");

			String nodeid = tokens[0];
			String[] pgVal = nodeid.split(":");

			output.collect(new FloatWritable(Float.parseFloat(pgVal[1])), new Text(pgVal[0]));
		}
	}
}