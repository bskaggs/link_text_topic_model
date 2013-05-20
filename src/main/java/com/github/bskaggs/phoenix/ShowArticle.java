package com.github.bskaggs.phoenix;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ShowArticle extends Configured implements Tool {
	
	private Reader textIndexReader;

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		Path working = new Path(args[0]);
		Path textIndexPath = new Path(working, "sorted_text/part-r-00000");
		textIndexReader = new MapFile.Reader(fs, textIndexPath.toString(), conf);
		if (args.length > 1) {
			for (int i = 1; i < args.length; i++) {
				dump(args[i]);
			}
		} else {
			Scanner in = new Scanner(System.in);
			while(in.hasNextLine()) {
				dump(in.nextLine());
			}
		}
		return 0;
	}
	
	private void dump(String articleName) throws IOException { 
		System.out.println("=====" + articleName + "=====");
		System.out.println(textIndexReader.get(new Text(articleName), new Text()));
		System.out.println("\n\n\n");
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new ShowArticle(), args));
	}	
}
