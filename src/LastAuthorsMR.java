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
//import org.jdom2.input.SAXBuilder;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;


public class LastAuthorsMR {

	public static class XMLLastAuthorsRebuiltMapper extends Mapper<LongWritable, Text, LongWritable, PageWritable> {

		private Pattern pattern = Pattern.compile("\\[Categoria:(.*?)\\]");
		private Matcher matcher;

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			try {
//				SAXBuilder saxBuilder = new SAXBuilder();
//				Reader reader = new StringReader(value.toString());
//				Document document = saxBuilder.build(reader);
//
//				Element root = document.getRootElement();

//				Element id = root.getChild("id");
//				String pageId = id.getText();
//
//				Element title = root.getChild("title");
//				String pageTitle = title.getText();
//
//				Element author = root.getChild("revision").getChild("contributor");
//
//				String authorId = PageWritable.UNDEFINED_STRING;
//				String authorName = PageWritable.UNDEFINED_STRING;
//				String authorIp = PageWritable.UNDEFINED_STRING;
//
//				Element idElement = author.getChild("id");
//				Element usernameElement = author.getChild("username");
//				Element ipElement = author.getChild("ip");
//
//				if (idElement != null && usernameElement != null) {
//					authorId = idElement.getText();
//					authorName = usernameElement.getText();
//				} else if (ipElement != null) {
//					authorIp = ipElement.getText();
//
//				}
//
//				String textContent = root.getChild("revision").getChild("text").getText();

				
				
				InputStream is = new ByteArrayInputStream(value.toString().getBytes("UTF-8"));

				DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();

				DocumentBuilder documentBuilder = dbFactory.newDocumentBuilder();
				Document document = documentBuilder.parse(is);
				document.getDocumentElement().normalize();
				
				Node root = document.getElementsByTagName("page").item(0);
				
				String pageId = PageWritable.UNDEFINED_STRING;
				String pageTitle = PageWritable.UNDEFINED_STRING;
				String authorId = PageWritable.UNDEFINED_STRING;
				String authorName = PageWritable.UNDEFINED_STRING;
				String authorIp = PageWritable.UNDEFINED_STRING;
				String textContent = PageWritable.UNDEFINED_STRING;
				
				if (root.getNodeType() == Node.ELEMENT_NODE) {
					Element element = (Element) root;
					pageId = element.getElementsByTagName("id").item(0).getTextContent();
					pageTitle = element.getElementsByTagName("title").item(0).getTextContent();
					
					Element contributor = (Element) element.getElementsByTagName("revision").item(0);
					NodeList ipListNode = contributor.getElementsByTagName("ip");
					NodeList usernameNodeList =  contributor.getElementsByTagName("username");
					NodeList authorIdNodeList = contributor.getElementsByTagName("id");
					if(ipListNode.getLength() > 0) {
						authorIp = ipListNode.item(0).getTextContent();
					} else if(usernameNodeList.getLength() > 0 && authorIdNodeList.getLength() > 0) {
						authorName = usernameNodeList.item(0).getTextContent();
						authorId = authorIdNodeList.item(0).getTextContent();
					}
					
					textContent = element.getElementsByTagName("text").item(0).getTextContent();
				}
				
				matcher = pattern.matcher(textContent);

				List<String> categories = new ArrayList<>();
				// categories stores the matched page's categories, if any, write the page with
				// a '-'placeholder.

				while (matcher.find()) {
					String category = cleanString(matcher.group(1));
					categories.add(category);
				}

				if (categories.isEmpty())
					categories.add(PageWritable.UNDEFINED_STRING);

				for (String cat : categories) {
					context.write(new LongWritable(Long.valueOf(pageId)),
							new PageWritable(pageTitle, pageId, cat, authorName, authorId, authorIp));
				}
			} catch (SAXException | ParserConfigurationException e) {
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

	public static class XMLLastAuthorRebuiltReducer extends Reducer<LongWritable, PageWritable, Text, Text> {

		@Override
		protected void setup(Reducer<LongWritable, PageWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(new Text("PageID"), new Text("PageTitle" + "\t" + "CategoryTitle" + "\t" + "AuthorID"
					+ "\t" + "AuthorName" + "\t" + "AuthorIP" + "\t"));
		}

		@Override
		protected void reduce(LongWritable key, Iterable<PageWritable> value, Context context) throws IOException, InterruptedException {
			for (PageWritable pg : value)
				context.write(new Text(key.toString()), new Text(pg.toString()));
		}
	}

	/**
	 * @param args
	 *            the command line arguments
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

		Job jobLastAuthors = Job.getInstance(conf, "la");

		jobLastAuthors.setInputFormatClass(XmlInputFormat.class);

		jobLastAuthors.setMapperClass(XMLLastAuthorsRebuiltMapper.class);
		jobLastAuthors.setReducerClass(XMLLastAuthorRebuiltReducer.class);

		jobLastAuthors.setMapOutputKeyClass(LongWritable.class);
		jobLastAuthors.setMapOutputValueClass(PageWritable.class);

		jobLastAuthors.setOutputKeyClass(Text.class);
		jobLastAuthors.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(jobLastAuthors, new Path(input));

		Path outPath = new Path(output);
		FileOutputFormat.setOutputPath(jobLastAuthors, outPath);

		// if the output folder already exists, delete it so that hadoop don't broke the
		// balls
		FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
		if (dfs.exists(outPath))
			dfs.delete(outPath, true);

		jobLastAuthors.waitForCompletion(true);

	}

}
