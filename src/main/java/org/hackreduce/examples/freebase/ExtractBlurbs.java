/*
 *  Copyright 2011. Thomas F. Morris
 *  Licensed under new BSD license
 *  http://www.opensource.org/licenses/bsd-license.php
 */
package org.hackreduce.examples.freebase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.hackreduce.mappers.FreebaseTopicMapper;
import org.hackreduce.mappers.ModelMapper;
import org.hackreduce.models.FreebaseTopicRecord;


/**
 * This MapReduce job will extract text blurbs for all topics which have them
 * and classify them by type.
 */
public class ExtractBlurbs extends org.hackreduce.examples.RecordCounter {

	public enum Count {
		TOTAL_RECORDS,
	}

	public static class RecordCounterMapper extends FreebaseTopicMapper<Text, LongWritable> {

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

	@Override
	public void configureJob(Job job) {
		job.setInputFormatClass(TextInputFormat.class);
		job.setNumReduceTasks(10);
	}

	@Override
	public Class<? extends ModelMapper<?, ?, ?, ?, ?>> getMapper() {
		return RecordCounterMapper.class;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new ExtractBlurbs(), args);
		System.exit(result);
	}

}
