package com.m6d.hiveudf;

import com.jointhegrid.hive_test.HiveTestService;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

public class RankTest extends HiveTestService {

  public RankTest() throws IOException {
    super();
  }

  public void testRank() throws Exception {
    Path p = new Path(this.ROOT_DIR, "rankfile");

    FSDataOutputStream o = this.getFileSystem().create(p);
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(o));
    bw.write("twelve\t12\n");
    bw.write("twelve\t1\n");
    bw.write("eleven\t11\n");
    bw.write("eleven\t10\n");
    bw.close();

    String jarFile;
    jarFile = Rank.class.getProtectionDomain().getCodeSource().getLocation().getFile();
    client.execute("add jar " + jarFile);
    client.execute("create temporary function rank as 'com.m6d.hiveudf.Rank'");
    client.execute("create table  ranktest  (str string, countVal int) row format delimited fields terminated by '09' lines terminated by '10'");
    client.execute("load data local inpath '" + p.toString() + "' into table ranktest");

    client.execute("select a.*  from (select str, countVal, rank(str) rankStr from ranktest distribute by str sort by str, countVal) a order by a.rankStr desc");
    List<String> expected = Arrays.asList("eleven\t10\t2", "twelve\t1\t2", "eleven\t11\t1", "twelve\t12\t1");
    assertEquals(expected, client.fetchAll());

    client.execute("select str, countVal, rank(str) rankStr from ranktest distribute by str sort by str, countVal");
    expected = Arrays.asList("eleven\t10\t2", "eleven\t11\t1", "twelve\t1\t2", "twelve\t12\t1");
    assertEquals(expected, client.fetchAll());
    client.execute("drop table ranktest");
  }
}
