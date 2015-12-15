import java.io.IOException;
import java.util.Random;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class EulerEstimator extends Configured implements Tool {
	
	public static class EulerMapper
    extends Mapper<LongWritable, Text, Text, IntWritable>{
 
        @Override
        public void map(LongWritable key, Text value, Context context
                ) throws IOException, InterruptedException {
        	String fileNameComplete = ((FileSplit) context.getInputSplit()).getPath().getName();
        	int fileHash = fileNameComplete.hashCode();
        	long keyBytes = key.get();
        	long seed  = keyBytes + fileHash;
        	
        	String line = value.toString();
        	long iter = Long.parseLong(line);
        	long count = 0;
        	Random rand = new Random();
        	rand.setSeed(seed);
        	for(int i = 0; i < iter; i ++) {
        		double sum = 0.0;
        		while (sum < 1) {
        			sum += rand.nextDouble();
        			count ++;
        		}
        	}
        	
        	context.getCounter("Euler", "iterations").increment(iter);
        	context.getCounter("Euler", "count").increment(count);
        }
    }

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new EulerEstimator(), args);
        System.exit(res);
	}
	
	@Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "euler estimator");
        job.setJarByClass(EulerEstimator.class);
 
        job.setInputFormatClass(TextInputFormat.class);
 
        job.setMapperClass(EulerMapper.class);
 
        job.setOutputFormatClass(NullOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
