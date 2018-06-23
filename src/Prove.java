import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

public class Prove extends Configured implements Tool {


	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
	
	
	public static class XMLMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.map(key, value, context);
			
			SAXBuilder saxBuilder = new SAXBuilder();
			Reader reader = new StringReader(value.toString());
			
			try {
				Document document = saxBuilder.build(reader);
				
				Element root = document.getRootElement();
				
				String author = root.getChild("author").getText();
				
				System.out.println(author);
				
				
			} catch (JDOMException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	
	/**
	* @param args the command line arguments
	*/
	public static void main(String[] args) {
		try {
		String[] myArgs = new GenericOptionsParser(args).getRemainingArgs();
		runJob(myArgs[0], myArgs[1]);
					
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}


	public static void runJob(String input, String output ) throws IOException, ClassNotFoundException, InterruptedException {
	
		Configuration conf = new Configuration();
	
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");
		conf.set("io.serializations",
				"org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
	
		Job job = new Job(conf, "jobName");
	
	
		FileInputFormat.setInputPaths(job, input);
		job.setJarByClass(Prove.class);
		job.setMapperClass(XMLMapper.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		Path outPath = new Path(output);
		FileOutputFormat.setOutputPath(job, outPath);
		// se esiste il path di out lo elimina che bello!
		FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
		if (dfs.exists(outPath)) {
			dfs.delete(outPath, true);
		}
	
		job.waitForCompletion(true);

	}

}
