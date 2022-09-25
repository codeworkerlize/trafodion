
import java.io.IOException;

import java.io.OutputStream;

import java.net.URI;

import java.net.URISyntaxException;



import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.ftp.FTPFileSystem;

import org.apache.hadoop.io.IOUtils;



public class FTPtoHDFS

{
	
public static void main(String[] args) throws IOException, URISyntaxException  
{

String src="test1.txt";

Configuration conf = new Configuration();

FTPFileSystem ftpfs = new FTPFileSystem();

ftpfs.setConf(conf);

ftpfs.initialize(new URI("ftp://username:password@host"), conf); 

FSDataInputStream fsdin = ftpfs.open(new Path(src), 1000);

FileSystem fileSystem=FileSystem.get(conf);

OutputStream outputStream=fileSystem.create(new Path(args[0]));

IOUtils.copyBytes(fsdin, outputStream, conf, true);

ftpfs.close();

}

}


