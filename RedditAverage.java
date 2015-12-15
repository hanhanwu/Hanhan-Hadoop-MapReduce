import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RedditAverage extends Configured implements Tool {
	
	public static class RedditMapper
    extends Mapper<LongWritable, Text, Text, LongPairWritable>{
 
        @Override
        public void map(LongWritable key, Text value, Context context
                ) throws IOException, InterruptedException {
        	ObjectMapper json_mapper = new ObjectMapper();
        	 
        	String inputString = value.toString();
        	JsonNode data = json_mapper.readValue(inputString, JsonNode.class);
        	
        	Text outputKey = new Text(data.get("subreddit").textValue());
        	long commentNum = 1;
        	long score = data.get("score").longValue();
        	LongPairWritable outputValue = new LongPairWritable(commentNum,score);
        	context.write(outputKey, outputValue);
        }
    }
	
	public static class RedditCombiner
	extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
		
		@Override
        public void reduce(Text key, Iterable<LongPairWritable> values,
                Context context
                ) throws IOException, InterruptedException {
			long totalComments = 0;
			long totalScore = 0;
            for (LongPairWritable value : values) {
            	totalComments += value.get_0();
            	totalScore += value.get_1();
            }
            LongPairWritable outputTotalValue = new LongPairWritable(totalComments, totalScore);
            context.write(key, outputTotalValue);
        }
	}
	
	public static class RedditReducer
	extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();
		
		@Override
        public void reduce(Text key, Iterable<LongPairWritable> values,
                Context context
                ) throws IOException, InterruptedException {
			long totalComments = 0;
			long totalScore = 0;
			double averageScore = 0;
            for (LongPairWritable value : values) {
            	totalComments += value.get_0();
            	totalScore += value.get_1();
            }
            averageScore = totalScore/totalComments;
            result.set(averageScore);
            context.write(key, result);
        }
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
        System.exit(res);
	}
	
	@Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "reddit average");
        job.setJarByClass(RedditAverage.class);
 
        job.setInputFormatClass(TextInputFormat.class);
 
        job.setMapperClass(RedditMapper.class);
        job.setCombinerClass(RedditCombiner.class);
        job.setReducerClass(RedditReducer.class);
 
        job.setMapOutputValueClass(LongPairWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextInputFormat.addInputPath(job, new Path(args[1]));
        TextOutputFormat.setOutputPath(job, new Path(args[2]));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
