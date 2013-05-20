package com.github.bskaggs.phoenix;

import gnu.trove.iterator.TObjectLongIterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.github.bskaggs.phoenix.io.TextLongMapWritable;


public class CountSpecificTextClasses extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		Path working = new Path(args[0]);
		Path textIndexPath = new Path(working, "sorted_link_text_index/part-r-00000");
		Reader textIndexReader = new MapFile.Reader(fs, textIndexPath.toString(), conf);
		
		TextLongMapWritable map = (TextLongMapWritable) textIndexReader.get(new Text(args[1]), new TextLongMapWritable());
		TObjectLongIterator<Text> it = map.iterator();
		while (it.hasNext()) {
			it.advance();
			System.out.println(it.key() + "\t" + it.value());
		}
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new CountSpecificTextClasses(), args));
	}	
}
