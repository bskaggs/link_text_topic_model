package com.github.bskaggs.phoenix;

import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.github.bskaggs.phoenix.io.TextArrayWritable;


public class CountSpecificDabClasses extends Configured implements Tool {

	public final static Pattern languageStripper = Pattern.compile("\\A[^:]*:");
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		Path working = new Path(args[0]);
		Path dabListPath = new Path(working, "sorted_dabs_list");
		Path inLinkPath = new Path(working, "sorted_simple_in_links/part-r-00000");
		Reader dabReader = new MapFile.Reader(fs, dabListPath.toString(), conf);
		Reader inLinkReader = new MapFile.Reader(fs, inLinkPath.toString(), conf);
		
		TextArrayWritable array = (TextArrayWritable) dabReader.get(new Text(args[1]), new TextArrayWritable());
		for (Text article : array.array) {
			Text links = (Text) inLinkReader.get(article, new Text());
			int count = 0;
			if (links != null) {
				count = links.toString().split("\\t").length;
			}
			String shortTitle = languageStripper.matcher(article.toString()).replaceAll("");
			shortTitle = shortTitle.replace("\\", "\\\\").replace("\"", "\\\"");
			System.out.println("\"" + shortTitle + "\"\t" + count);
		}
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new CountSpecificDabClasses(), args));
	}	
}
