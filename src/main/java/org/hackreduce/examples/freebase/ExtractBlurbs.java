/*
 *  Copyright 2011. Thomas F. Morris
 *  Licensed under new BSD license
 *  http://www.opensource.org/licenses/bsd-license.php
 */
package org.hackreduce.examples.freebase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hackreduce.mappers.FreebaseTopicMapper;
import org.hackreduce.models.FreebaseTopicRecord;


/**
 * This MapReduce job will extract text blurbs for all topics which have them
 * and classify them by type.
 */
public class ExtractBlurbs extends Configured implements Tool {

	public enum Count {
		TOTAL_RECORDS,
	}

	public static class ExtractBlurbMapper extends FreebaseTopicMapper<Text, LongWritable> {

		// Just to save on object instantiation
		public static final LongWritable ONE_COUNT = new LongWritable(1);
		private static final int MIN_BLURB_LENGTH = 100;

		@Override
		protected void map(FreebaseTopicRecord record, Context context) throws IOException,
				InterruptedException {

			context.getCounter(Count.TOTAL_RECORDS).increment(1); 
			// Emit a record for each type the topic has if it has a text blurb
			String blurb = record.getBlurb();
			if(blurb != null && blurb.contains("Boston")/* blurb.length() > MIN_BLURB_LENGTH*/){
				for(String t : record.getFb_types()){
					context.write(new Text(t), new LongWritable(blurb.length()));
				}
			}
		}
	}


	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new ExtractBlurbs(), args);
		System.exit(result);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

        if (args.length != 2) {
        	System.err.println("Usage: " + getClass().getName() + " <input> <output>");
        	System.exit(2);
        }

        // Creating the MapReduce job (configuration) object
        Job job = new Job(conf);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

		job.setInputFormatClass(TextInputFormat.class);

        // Tell the job which Mapper to use
        job.setMapperClass(ExtractBlurbMapper.class);
        
        // No reducer, we just want to collect all the strings
        job.setNumReduceTasks(0);

		// This is what the Mapper will be outputting
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Setting the input folder of the job 
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// Preparing the output folder by first deleting it if it exists
        Path output = new Path(args[1]);
        FileSystem.get(conf).delete(output, true);
	    FileOutputFormat.setOutputPath(job, output);

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
