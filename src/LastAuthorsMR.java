import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
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

public class LastAuthorsMR
{

	public static class XMLLastAuthorsRebuiltMapper extends Mapper<LongWritable, Text, Text, PageWritable>
	{

		private Pattern pattern = Pattern.compile("\\[Categoria:(.*?)\\]");
		private Matcher matcher;

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{

			SAXBuilder saxBuilder = new SAXBuilder();
			Reader reader = new StringReader(value.toString());

			try
			{
				Document document = saxBuilder.build(reader);

				Element root = document.getRootElement();

				Element id = root.getChild("id");
				String pageId = id.getTextTrim();

				Element title = root.getChild("title");
				String pageTitle = title.getTextTrim();

				Element author = root.getChild("revision").getChild("contributor");

				String authorId = "";
				String authorName = "";
				String authorIp = "";

				if (author.getChildren().contains("id") && author.getChildren().contains("username"))
				{
					authorId = author.getChild("id").getTextTrim();
					authorName = author.getChild("username").getTextTrim();
				} else if (author.getChildren().contains("ip"))
					authorIp = author.getChild("ip").getTextTrim();

				String textContent = root.getChild("revision").getChild("text").getText();

				matcher = pattern.matcher(textContent);

				while (matcher.find())
				{
					String category = cleanString(matcher.group(1));

					context.write(new Text(pageId), new PageWritable(pageTitle, pageId, category, authorName, authorId, authorIp));
				}
			} catch (Exception e)
			{
				e.printStackTrace();
			}

		}

		private String cleanString(String string)
		{
			int index = string.indexOf('|');
			if (index != -1)
				return string.substring(0, index);
			else
				return string;
		}
	}

	public static class XMLLastAuthorRebuiltReducer extends Reducer<Text, PageWritable, Text, Text>
	{

		@Override
		protected void setup(Reducer<Text, PageWritable, Text, Text>.Context context) throws IOException, InterruptedException
		{
			context.write(new Text("Page ID"), new Text("Page Title" + "\t" + "Category Title" + "\t" + "Author ID" + "\t" + "Author Name" + "\t" + "Author IP" + "\t"));
		}

		@Override
		protected void reduce(Text key, Iterable<PageWritable> value, Reducer<Text, PageWritable, Text, Text>.Context context) throws IOException, InterruptedException
		{
			for (PageWritable pg : value)
				context.write(new Text(key), new Text(pg.toString()));
		}
	}

	public static void main(String[] args)
	{
		
		Configuration conf = new Configuration();
		try
		{
			String [] myArgs = new GenericOptionsParser(args).getRemainingArgs();
			Job jobLastAuthors = Job.getInstance(conf, "la");
			
			jobLastAuthors.setInputFormatClass(XmlInputFormat.class);
			
			jobLastAuthors.setMapOutputKeyClass(Text.class);
			jobLastAuthors.setMapOutputValueClass(PageWritable.class);
		
			jobLastAuthors.setOutputKeyClass(Text.class);
			jobLastAuthors.setOutputValueClass(Text.class);

			FileInputFormat.setInputPaths(jobLastAuthors, new Path(myArgs[0]));
			FileOutputFormat.setOutputPath(jobLastAuthors, new Path(myArgs[1]));
			
			jobLastAuthors.waitForCompletion(true);
			
			System.exit(0);
			
		} catch (IOException | ClassNotFoundException | InterruptedException e)
		{
			e.printStackTrace();
		}
		
		
	}

}
