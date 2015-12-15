
import java.io.IOException;
import java.util.Stack;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
 
import com.google.common.base.Charsets;
 
public class MultiLineJSONInputFormat extends TextInputFormat {
 
    public class MultiLineRecordReader extends RecordReader<LongWritable, Text> {
        LineRecordReader linereader;
        LongWritable current_key;
        Text current_value;
 
        public MultiLineRecordReader(byte[] recordDelimiterBytes) {
            linereader = new LineRecordReader(recordDelimiterBytes);
        }
 
        @Override
        public void initialize(InputSplit genericSplit,
                TaskAttemptContext context) throws IOException {
            linereader.initialize(genericSplit, context);
        }
 
        @Override
        public boolean nextKeyValue() throws IOException {
        	
            boolean res = false;
            current_value = new Text("");
            Stack<String> stack = new Stack<String>();

        	while (linereader.nextKeyValue()) {
        		LongWritable temp_key = linereader.getCurrentKey();
                Text temp_value = linereader.getCurrentValue();                        
                if (temp_value.find("{") == 0) {
                	res = true;
                    current_key = temp_key;
                    //current_value = temp_value;
                    String str1 = current_value.toString().concat(temp_value.toString());
                	current_value = new Text(str1);
                	stack.push("{");
                }
                else {
                	String str2 = current_value.toString().concat(temp_value.toString());
                	current_value = new Text(str2);
                    if (temp_value.find("}") == (temp_value.getLength() - 1)) {
                    	if (!stack.empty()) {
                    		stack.pop();
                    	}
                    	if(stack.size() == 0) break;
                    }
                }
             }            
        	        	
            return res;
        }
 
        @Override
        public float getProgress() throws IOException {
            return linereader.getProgress();
        }
 
        @Override
        public LongWritable getCurrentKey() {
            return current_key;
        }
 
        @Override
        public Text getCurrentValue() {
            return current_value;
        }
 
        @Override
        public synchronized void close() throws IOException {
            linereader.close();
        }
    }
 
    // shouldn't have to change below here
 
    @Override
    public RecordReader<LongWritable, Text> 
    createRecordReader(InputSplit split,
            TaskAttemptContext context) {
        // same as TextInputFormat constructor, except return MultiLineRecordReader
        String delimiter = context.getConfiguration().get(
                "textinputformat.record.delimiter");
        byte[] recordDelimiterBytes = null;
        if (null != delimiter)
            recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
        return new MultiLineRecordReader(recordDelimiterBytes);
    }
 
    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        // let's not worry about where to split within a file
        return false;
    }
}
