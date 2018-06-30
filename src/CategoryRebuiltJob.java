import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
//import java.io.Reader;
//import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

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
//import org.jdom2.Document;
//import org.jdom2.Element;
//import org.jdom2.JDOMException;
//import org.jdom2.input.SAXBuilder;
import org.w3c.dom.Node;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

public class CategoryRebuiltJob {

	public static class XMLCategoryRebuiltMapper extends Mapper<LongWritable, Text, LongWritable, PageWritable> {

		private Pattern pattern = Pattern.compile("\\[Categoria:(.*?)\\]");
		private Matcher matcher;

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


			try {
//				SAXBuilder saxBuilder = new SAXBuilder();
//				Reader reader = new StringReader(value.toString());
//				
//				Document document = saxBuilder.build(reader);
//
//				Element root = document.getRootElement();
//				Element title = root.getChild("title");
//				String pageTitle = title.getTextTrim();
//				String textContent = root.getChild("revision").getChild("text").getText();
//				String pageId = root.getChild("id").getText();
				
				InputStream is = new ByteArrayInputStream(value.toString().getBytes("UTF-8"));

				DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();

				DocumentBuilder documentBuilder = dbFactory.newDocumentBuilder();
				Document document = documentBuilder.parse(is);
				document.getDocumentElement().normalize();

				String textContent = "";
				String pageTitle = "";
				String pageId = "";

				Node root = document.getElementsByTagName("page").item(0);

				if (root.getNodeType() == Node.ELEMENT_NODE) {
					Element element = (Element) root;
					pageId = element.getElementsByTagName("id").item(0).getTextContent();
					textContent = element.getElementsByTagName("text").item(0).getTextContent();
					pageTitle = element.getElementsByTagName("title").item(0).getTextContent();
				}

				List<String> categories = new ArrayList<>();
				// categories stores the matched page's categories, if any, write the page with
				// a '-'placeholder.

				matcher = pattern.matcher(textContent);
				while (matcher.find()) {
					String category = cleanString(matcher.group(1));
					categories.add(category);
				}

				if (categories.isEmpty())
					categories.add("-");

				for (String cat : categories) {
					context.write(new LongWritable(Long.valueOf(pageId)), 
							new PageWritable(pageTitle, pageId, cat, PageWritable.UNDEFINED_STRING, PageWritable.UNDEFINED_STRING, PageWritable.UNDEFINED_STRING));
				}
			} catch (ParserConfigurationException | SAXException e) {
				e.printStackTrace();
			}

		}

		private String cleanString(String string) {
			int index = string.indexOf('|');
			if (index != -1)
				return string.substring(0, index);
			else
				return string;
		}
	}

	public static class XMLCategoryRebuiltReducer extends Reducer<LongWritable, PageWritable, Text, Text> {

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			context.write(new Text("PageId"), new Text("PageTitle" + "\t" + "CategoryTitle"));
		}

		@Override
		protected void reduce(LongWritable pageID, Iterable<PageWritable> values, Context context)
				throws IOException, InterruptedException {

			for (PageWritable value : values)
				context.write(new Text(pageID.toString()), new Text(value.getTitle() + "\t" + value.getCategory()));

		}
	}

	 /**
	 * @param args
	 * the command line arguments
	 * @throws Exception
	 */
	 public static void main(String[] args) throws Exception {
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

	public static void runJob(String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
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

		 job.setJarByClass(CategoryRebuiltJob.class);

		job.setMapperClass(XMLCategoryRebuiltMapper.class);
		job.setReducerClass(XMLCategoryRebuiltReducer.class);

		job.setInputFormatClass(XmlInputFormat.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(PageWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		String inputFile = input;
		FileInputFormat.setInputPaths(job, new Path(inputFile));

		Path outPath = new Path(output);
		FileOutputFormat.setOutputPath(job, outPath);

		// if the output folder already exists, delete it so that hadoop don't broke the
		// balls
		FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
		if (dfs.exists(outPath))
			dfs.delete(outPath, true);

		return job;
	}
}
