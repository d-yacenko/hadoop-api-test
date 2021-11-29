package ru.arena.hadoop.test;
import org.apache.commons.io.IOUtils;
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
import java.util.Date;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;

/**
 * Hello hadoop!
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
	
	public static void writeFileToKrbHFDS() throws IOException {
		System.out.println("Connect to ADH...");
        // set kerberos host and realm
        System.setProperty("java.security.krb5.realm", "ARENA.RU");
        System.setProperty("java.security.krb5.kdc", "master-0.arena.ru");

        Configuration conf = new Configuration();
		conf.addResource(new Path("conf/core-site.xml"));
		conf.addResource(new Path("conf/hdfs-site.xml"));

        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hadoop.security.authorization", "true");

        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());

        // hack for running locally with fake DNS records
        // set this to true if overriding the host name in /etc/hosts
        conf.set("dfs.client.use.datanode.hostname", "true");

        // server principal
        // the kerberos principle that the namenode is using
        conf.set("dfs.namenode.kerberos.principal.pattern", "hdfs-namenode/*@ARENA.RU");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab("team0@ARENA.RU", "conf/team0.keytab");

        FileSystem fs = FileSystem.get(conf);
        System.out.print("Подключились к:  ");
        System.out.println(fs.getUri());
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("./"), true);
        while(files.hasNext()) {
            LocatedFileStatus file = files.next();
            System.out.println(IOUtils.toString(fs.open(file.getPath())));
        }
        Path file = new Path("/user/team0/success");
		if (fs.exists(file)) 
			fs.delete(file, true);
		OutputStream os = fs.create(file, new Progressable() {
			public void progress() {
				System.out.print(".");
			}
		});
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
		br.write(""+new Date(System.currentTimeMillis()));
		br.close();
		fs.close();
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
		String dirName = "/tmp/testdir"; 
		// Values of hosthdfs:port can be found in the core-site.xml in the
		// fs.default.name
		 
		// sample work with SIMPLE authentication 
		// createFolderOnHDFS(conf, dirName);
		// writeFileToHDFS(conf, dirName, "test.txt");
		// readFileFromHDFS(conf, dirName, "test.txt");
		
		// sample work with kerberos
		writeFileToKrbHFDS();
		try {
			TimeUnit.SECONDS.sleep(10);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
