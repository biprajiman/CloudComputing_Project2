package job;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import mapred.RankMapper;
import mapred.RankReducer;
import mapred.RankingMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/*
 * @Author Manish Sapkota
 * Cloud computing project 2 page ranking
 */
public class RankJob
{
	// Counters used to gather the graph statistics
	public static enum GraphStat
	{
		RESIDUAL, nNodes, nEdges, minOut, maxOut;
	}

	public static double Multiplier = 1000000.0D;
	public static double threshold = 0.005;// threshold for convergence

	// to delete the folders if exists
	public void DeleteOutputDirectory(String outputString, Configuration conf)
			throws IOException
	{
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(outputString))) {
			fs.delete(new Path(outputString), true);
		}
	}

	private void rankOrdering(String inputPath, String outputPath)
			throws IOException
	{
		JobConf conf = new JobConf(RankJob.class);

		conf.setOutputKeyClass(FloatWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path[] { new Path(inputPath) });
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setMapperClass(RankingMapper.class);
		
		// delete the output folder if it exists
		DeleteOutputDirectory(outputPath,conf);
		
		JobClient.runJob(conf);
	}

	public JobConf createJob(int iteration, String inputString, String outputString)
			throws IOException
	{
		JobConf conf = new JobConf();
		conf.setJarByClass(getClass());

		int last = iteration - 1;
		conf.setMapperClass(RankMapper.class);
		conf.setReducerClass(RankReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		if (iteration == 0) {
			FileInputFormat.setInputPaths(conf, new Path[] { new Path(inputString) });
		} else {
			FileInputFormat.setInputPaths(conf, new Path[] { new Path(outputString + last) });
		}
		
		// Delete output directory if it exists.
		DeleteOutputDirectory(outputString + iteration,conf);
		conf.set("mapred.output.dir", outputString + iteration);
		return conf;
	}

	public static void main(String[] args)
	{
		String inputString = args[0];
		String outputString = args[1];
		String rankedOut = args[2];
		RankJob job = new RankJob();
		JobClient client = new JobClient();
		int i = 0;
		long totalNodes = 0L;
		long totalEdges = 0L;

		double maxOut = 0.0D;
		double minOut = 0.0D;

		long startTime = 0L;
		ArrayList<Double> completionTime = new ArrayList();
		try
		{
			while(true)
			{
				JobConf conf = job.createJob(i, inputString, outputString);
				client.setConf(conf);

				startTime = System.currentTimeMillis();
				RunningJob runningJob = JobClient.runJob(conf);
				runningJob.waitForCompletion();

				// get the residual, max and min out degree counters
				double totalResidual = runningJob.getCounters()
						.findCounter(GraphStat.RESIDUAL).getValue() / Multiplier;
				maxOut = runningJob.getCounters()
						.findCounter(GraphStat.maxOut).getValue() / Multiplier;
				minOut = runningJob.getCounters()
						.findCounter(GraphStat.minOut).getValue() / Multiplier;

				// get total nodes and edges from the counters
				totalNodes = runningJob.getCounters().findCounter(GraphStat.nNodes).getValue();
				totalEdges = runningJob.getCounters().findCounter(GraphStat.nEdges).getValue();

				totalResidual /= totalNodes;

				completionTime.add(Double.valueOf((System.currentTimeMillis() - startTime) / 1000.0D));
				System.out.println("Job in " + i + "th Finished in " + (System.currentTimeMillis() - startTime) / 1000.0D + " seconds");

				if(totalResidual<threshold) break;// Criteria for convergence
				
				i++;
				
				//if(i>9)break; to run for 10 iterations
			} 
			
			System.out.println("Number of Nodes : " + totalNodes + "\n");
			System.out.println("Number of Edges : " + totalEdges + "\n");
			System.out.println("Minimum outdegree : " + minOut + "\n");
			System.out.println("Maximum outdegree : " + maxOut + "\n");
			System.out.println("Average outdegree : " + totalEdges / totalNodes + "\n");
			System.out.println("Iteration : " + i + "\n");

			// Map job to get the ranked output for top 10 pages
			
			job.rankOrdering((outputString + i), rankedOut);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
}