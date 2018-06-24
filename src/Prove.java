import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;

public class Prove {

	
	
	public static class XMLCategoryRebuiltMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		private Pattern pattern = Pattern.compile("\\[Categoria:(.*?)\\]");
		private Matcher matcher;
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			SAXBuilder saxBuilder = new SAXBuilder();
			Reader reader = new StringReader(value.toString());
			
			try {
				Document document = saxBuilder.build(reader);
				
				Element root = document.getRootElement();
				Element title = root.getChild("title");
				String pageTitle = title.getTextTrim();
				String textContent = root.getChild("revision").getChild("text").getText();
				
				matcher = pattern.matcher(textContent);
				
				while(matcher.find()) {
					String category = cleanString(matcher.group(1));
					context.write(new Text(pageTitle), new Text(category));
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
		
		
		private String cleanString(String string) {
			int index = string.indexOf('|');
			if(index != -1)
				return string.substring(0, index);
			else
				return string;
		}
	}
	
	
	
	
	public static class XMLCategoryRebuiltReducer extends Reducer<Text, Text, Text, Text> {
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			context.write(new Text("Page Title"), new Text("Category Title"));
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			for(Text value : values)
				context.write(new Text(key), new Text(value));
			
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
		
		Job job = configJob("CategoryRebuiltJob", conf, input, output);
	
		job.waitForCompletion(true);

	}
	
	private static Job configJob(String title, Configuration conf, String input, String output) throws IOException {
		Job job = Job.getInstance(conf, title);
		
		job.setJarByClass(Prove.class);
		job.setJar("CategoryRebuiltMapReduce.jar");
		
		job.setMapperClass(XMLCategoryRebuiltMapper.class);
		job.setReducerClass(XMLCategoryRebuiltReducer.class);
		
		job.setInputFormatClass(XmlInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
//		job.setSortComparatorClass(TimeComparator.class);
		
		String inputFile = input;
		FileInputFormat.setInputPaths(job, new Path(inputFile));
		
		Path outPath = new Path(output);
		FileOutputFormat.setOutputPath(job, outPath);
		
		// if the output folder already exists, delete it so that hadoop don't broke the balls
		FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
		if (dfs.exists(outPath))
			dfs.delete(outPath, true);
		
		return job;
	}

}
