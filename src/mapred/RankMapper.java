package mapred;

import java.io.IOException;
import job.RankJob;
import job.RankJob.GraphStat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/*
 * @Author Manish Sapkota
 * Cloud computing project 2 page ranking
 */
public class RankMapper
extends MapReduceBase
implements Mapper<LongWritable, Text, Text, Text>
{
	private static final double DAMPINGFAC = 1.0D;

	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException
	{
		String line = value.toString();
		if (!line.isEmpty())
		{
			String[] tokens = line.split(" ");
			String nodeid = tokens[0];
			String[] pgVal = nodeid.split(":");
			double PR = 1.0D;
			// emit the adjacency list
			if (pgVal.length > 1)
			{
				PR = Double.parseDouble(pgVal[1]);
				output.collect(new Text(pgVal[0]), new Text("links:" + line.substring(nodeid.length(), line.length())));
			}
			else
			{
				output.collect(new Text(nodeid), new Text("links:" + line.substring(nodeid.length(), line.length())));
			}
			
			//emit current page rank. will be used for convergence criteria calculation
			output.collect(new Text(pgVal[0]), new Text("prevPR:" + PR));

			// distribute the page rank values
			Double delta = Double.valueOf(PR * DAMPINGFAC / (tokens.length - 1));

			String link = "";
			int listCount = 0;
			
			// previous min and max
			double minOut = reporter.getCounter(RankJob.GraphStat.minOut).getValue() / RankJob.Multiplier;
			double maxOut = reporter.getCounter(RankJob.GraphStat.maxOut).getValue() / RankJob.Multiplier;
			for (int i = 1; i < tokens.length; i++)
			{
				link = tokens[i];
				// count the number of edges based on out degree
				if (!link.equals(""))
				{
					reporter.getCounter(RankJob.GraphStat.nEdges).increment(1L);
					listCount++;
					output.collect(new Text(link), new Text(delta.toString()));
				}
			}
			
			// change the min and max counter values
			if (minOut > listCount)
			{
				if (minOut > 0.0D) {
					reporter.getCounter(RankJob.GraphStat.minOut).increment((long)(-1.0D * minOut * RankJob.Multiplier));
				}
				reporter.getCounter(RankJob.GraphStat.minOut).increment((long)(listCount * RankJob.Multiplier));
			}
			else if (maxOut < listCount)
			{
				if (maxOut > 0.0D) {
					reporter.getCounter(RankJob.GraphStat.maxOut).increment((long)(-1.0D * maxOut * RankJob.Multiplier));
				}
				reporter.getCounter(RankJob.GraphStat.maxOut).increment((long)(listCount * RankJob.Multiplier));
			}
		}
	}
}