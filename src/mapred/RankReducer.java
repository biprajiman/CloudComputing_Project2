package mapred;

import java.io.IOException;
import java.util.Iterator;
import job.RankJob;
import job.RankJob.GraphStat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/*
 * @Author Manish Sapkota
 * Cloud computing project 2 page ranking
 */
public class RankReducer
extends MapReduceBase
implements Reducer<Text, Text, Text, Text>
{
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException
	{
		Double PR = Double.valueOf(0.0D);
		String val = "";
		String link = "";

		double residual = 1.0D;
		double prevpg = 0.0D;
		int listCount = 0;
		
		while (values.hasNext())
		{
			val = ((Text)values.next()).toString();
			if (!val.equals("")) {
				if (val.contains("links:"))// adjacency list identifier
				{
					link = val.substring(6, val.length());
				}
				else if (val.contains("prevPR"))// previous rank identifier
				{
					prevpg = Double.parseDouble(val.substring(7));
				}
				else
				{
					PR = Double.valueOf(PR.doubleValue() + Double.parseDouble(val));
					listCount++;
				}
			}
		}
		
		// calculate residual ie. previous page rank minu new page rank to get the convergence criteria
		residual = Math.abs(prevpg - PR.doubleValue());
		
		// count the number of nodes and sum up the residuals
		reporter.getCounter(RankJob.GraphStat.nNodes).increment(1L);
		reporter.getCounter(RankJob.GraphStat.RESIDUAL).increment((long)(residual * RankJob.Multiplier));

		output.collect(new Text(key + ":" + String.format("%.2f", PR)), new Text(link));
	}
}