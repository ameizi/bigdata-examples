package com.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.util.Arrays;

public class HdfsTest {

    private FileSystem fs;

    @Before
    public void init() throws Exception {
        // 构造一个配置参数对象，设置一个参数：我们要访问的hdfs的URI；
        // 从而FileSystem.get()方法就知道应该是去构造一个访问hdfs文件系统的客户端，以及hdfs的访问地址；
        // new Configuration()的时候，构造函数就会去加载jar包中的hdfs-default.xml，
        // 然后再加载classpath下的hdfs-site.xml；
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hdp20-01:9000");
        /**
         * 参数优先级： 1、客户端代码中设置的值 2、classpath下用户自定义配置文件 3、然后是服务器的默认配置
         */
        conf.set("dfs.replication", "1");
        // 获取一个hdfs的访问客户端，根据参数，这个实例应该是DistributedFileSystem的实例；
        // fs = FileSystem.get(conf)；
        // 如果这样去获取，那conf里就可以不要配"fs.defaultFS"参数，而且，这个客户端的身份标识已经是root；
        fs = FileSystem.get(new URI("hdfs://hdp20-01:9000"), conf, "amz");
    }

    @Test
    public void testAddFileToHdfs() throws Exception {
        // 要上传的文件所在的本地路径
        Path src = new Path("D:/hdfs/files/HTTP_20130313143750.txt");
        // 要上传到hdfs的目标路径
        Path dst = new Path("/aaa");
        fs.copyFromLocalFile(src, dst);
        fs.close();
    }

    @Test
    public void testDownloadFileToLocal() throws IllegalArgumentException, IOException {
        // 参数1：HDFS中的源文件路径  参数2：本地存储目标路径
        fs.copyToLocalFile(new Path("/aaa"), new Path("d:/"));
        fs.close();
    }

    @Test
    public void testMkdirAndDeleteAndRename() throws IllegalArgumentException, IOException {
        // 创建目录
        fs.mkdirs(new Path("/a1/b1/c1"));
        // 删除文件夹 ，如果是非空文件夹，参数2必须给值true
        fs.delete(new Path("/aaa"), true);
        // 重命名文件或文件夹
        fs.rename(new Path("/a1"), new Path("/a2"));
    }

    @Test
    public void testListFiles() throws FileNotFoundException, IllegalArgumentException, IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            // 获取一个文件的元信息对象
            LocatedFileStatus fileStatus = listFiles.next();
            // 文件名
            System.out.println(fileStatus.getPath().getName());
            // 文件的块大小
            System.out.println(fileStatus.getBlockSize());
            // 文件访问权限信息
            System.out.println(fileStatus.getPermission());
            // 文件长度
            System.out.println(fileStatus.getLen());
            // 文件块位置信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation bl : blockLocations) {
                System.out.println("block-length:" + bl.getLength() + "--" + "block-offset:" + bl.getOffset());
                // 文件块所在的datanode节点
                String[] hosts = bl.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("--------------分割线--------------");
        }
    }

    @Test
    public void testListAll() throws IllegalArgumentException, IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        String flag = "d--             ";
        for (FileStatus fstatus : listStatus) {
            if (fstatus.isFile()) {
                flag = "f--         ";
            }
            System.out.println(flag + fstatus.getPath().getName());
        }
    }

    @Test
    public void testRandomAccess() throws IllegalArgumentException, IOException {
        //先获取一个文件的输入流----针对hdfs上的
        FSDataInputStream in = fs.open(new Path("/install.log.syslog"));
        //可以将流的起始偏移量进行自定义
        in.seek(22);
        //再构造一个文件的输出流----针对本地的
        FileOutputStream out = new FileOutputStream(new File("ins.txt"));
        //19L表示读取的长度
        IOUtils.copyBytes(in, out, 19L, true);
    }

    @Test
    public void testCat() throws IllegalArgumentException, IOException {
        FSDataInputStream in = fs.open(new Path("/wordcount/in/words.txt"));
        IOUtils.copyBytes(in, System.out, 1024);
    }

    @Test
    public void testUpLoadToHdfs() throws IllegalArgumentException, IOException {
        FileInputStream in = new FileInputStream("D:/hdfs/wordcount/article.txt");
        FSDataOutputStream out = fs.create(new Path("/sequence.txt"), true);
        //再将输入流中数据传输到输出流
        IOUtils.copyBytes(in, out, 4096);
    }

    @Test
    public void testCat2() throws IllegalArgumentException, IOException {
        Path path = new Path("/jdk-7u79-linux-x64.tar.gz");
        FSDataInputStream in = fs.open(path);
        // 获取文件信息
        FileStatus fileStatus = fs.getFileStatus(path);
        //获取这个文件的所有block的信息
        BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0L, fileStatus.getLen());
        System.out.println(Arrays.toString(fileBlockLocations));
        FileOutputStream out = new FileOutputStream(new File("D:/block1"));
        in.seek(fileBlockLocations[1].getOffset());
        IOUtils.copyBytes(in, out, fileBlockLocations[1].getLength(), true);
    }
}
