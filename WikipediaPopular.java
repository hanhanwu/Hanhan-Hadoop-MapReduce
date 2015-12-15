import java.io.IOException;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WikipediaPopular extends Configured implements Tool {
	
	public static class WikiMapper
    extends Mapper<LongWritable, Text, Text, LongWritable>{
 
        @Override
        public void map(LongWritable key, Text value, Context context
                ) throws IOException, InterruptedException {
            String fileNameComplete = ((FileSplit) context.getInputSplit()).getPath().getName();
            String fileName = fileNameComplete.substring(11, 22);
            
            String line = value.toString();
            if (line.startsWith("en")) {
            	String[] elems = line.split(" +");
            	String title = elems[1];
            	if (!title.equals("Main_Page") && !title.startsWith("Special:")) {
            		LongWritable count = new LongWritable(Long.parseLong(elems[2]));            		
            		context.write(new Text(fileName), count);
            	}
            }
        }
    }
	
	public static class MaxReducer
	extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();
		
		@Override
        public void reduce(Text key, Iterable<LongWritable> values,
                Context context
                ) throws IOException, InterruptedException {
            long max = 1;
            for (LongWritable val : values) {
                if (max < val.get()) max = val.get();
            }
            result.set(max);
            context.write(key, result);
        }
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
        System.exit(res);
	}
	
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "wiki popular");
        job.setJarByClass(WikipediaPopular.class);
 
        job.setInputFormatClass(TextInputFormat.class);
 
        job.setMapperClass(WikiMapper.class);
        job.setCombinerClass(MaxReducer.class);
        job.setReducerClass(MaxReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextInputFormat.addInputPath(job, new Path(args[1]));
        TextOutputFormat.setOutputPath(job, new Path(args[2]));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
