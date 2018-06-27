import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringJoiner;

import org.apache.hadoop.io.Writable;

public class PageWritable implements Writable {
	
	public static final String UNDEFINED_STRING = "-";

	private String title;
	private String id;
	private String category;
	private String authorName;
	private String authorId;
	private String authorIP;

	public PageWritable() {
	}

	public PageWritable(String title, String id, String category, String authorName, String authorId, String authorIP) {
		super();
		this.title = title;
		this.id = id;
		this.category = category;
		this.authorName = authorName;
		this.authorId = authorId;
		this.authorIP = authorIP;
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		title = dataInput.readUTF();
		id = dataInput.readUTF();
		category = dataInput.readUTF();
		authorId = dataInput.readUTF();
		authorName = dataInput.readUTF();
		authorIP = dataInput.readUTF();
	}

	public String getAuthorIP() {
		return authorIP;
	}

	public void setAuthorIP(String authorIP) {
		this.authorIP = authorIP;
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeUTF(title);
		dataOutput.writeUTF(id);
		dataOutput.writeUTF(category);
		dataOutput.writeUTF(authorId);
		dataOutput.writeUTF(authorName);
		dataOutput.writeUTF(authorIP);
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public String getAuthorName() {
		return authorName;
	}

	public void setAuthorName(String authorName) {
		this.authorName = authorName;
	}

	public String getAuthorId() {
		return authorId;
	}

	public void setAuthorId(String authorId) {
		this.authorId = authorId;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public boolean valueIsNotGiven(String value) {
		return value.equals("-");
	}

	@Override
	public String toString() {
		StringJoiner sj = new StringJoiner("\t");
		sj.add(title).add(category).add(authorId).add(authorName).add(authorIP);
		return sj.toString();
	}

}
