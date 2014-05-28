package io.teknek.cassandra.simple;

import junit.framework.Assert;

import org.junit.Test;

public class HelloCassandraTest extends AbstractFarsandra {
  
  @Test
  public void helloCassandra() throws Exception {
    FramedConnWrapper conn= new FramedConnWrapper("127.0.0.1", 9160);
    conn.open();
    Assert.assertEquals("Test Cluster", conn.getClient().describe_cluster_name());
    conn.close();
  }

}
