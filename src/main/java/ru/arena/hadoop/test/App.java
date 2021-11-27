package ru.arena.hadoop.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.util.Progressable;

/**
 * Hello world!
 *
 */
public class App {
	public static void createFolderOnHDFS(Configuration conf, String dirName) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);
		System.out.println("Подключились к:  " + fileSystem.getUri());
		Path path = new Path(dirName);
		if (fileSystem.exists(path)) {
			System.out.println("Dir " + dirName + " already exists");
		} else {
			boolean rez = fileSystem.mkdirs(path);
			System.out.println(rez);
		}
		fileSystem.close();
	}

	public static void writeFileToHDFS(Configuration conf, String dirName, String fileName) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);
		Path file = new Path(dirName + "/" + fileName);
		if (fileSystem.exists(file)) 
			fileSystem.delete(file, true);
		OutputStream os = fileSystem.create(file, new Progressable() {

			public void progress() {
				System.out.print(".");
			}
		});
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
		br.write("Hello World");
		br.close();
		fileSystem.close();
		System.out.println();
	}


	public static void readFileFromHDFS(Configuration conf, String dirName, String fileName) throws IOException {
		System.out.print("Read test file:");
		FileSystem fileSystem = FileSystem.get(conf);
		Path hdfsReadPath = new Path(dirName + "/" + fileName);
		FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
		String line = null;
		while ((line = bufferedReader.readLine()) != null) {
			System.out.println(line);
		}
		bufferedReader.close();
		inputStream.close();
		fileSystem.close();
		System.out.println();
	}

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path("conf/core-site.xml"));
		conf.addResource(new Path("conf/hdfs-site.xml"));
//		conf.set("fs.defaultFS", "hdfs://176.118.164.173:8020");
//		conf.set("fs.default.name", "hdfs://176.118.164.173:8020");
//		conf.set("fs.defaultFS", "hdfs://192.168.2.2:8020");
//		conf.set("fs.default.name", "hdfs://192.168.2.2:8020");
		String dirName = "/tmp/testdir";
		// Values of hosthdfs:port can be found in the core-site.xml in the
		// fs.default.name
		createFolderOnHDFS(conf, dirName);
		writeFileToHDFS(conf, dirName, "test.txt");
		readFileFromHDFS(conf, dirName, "test.txt");
	}
}
