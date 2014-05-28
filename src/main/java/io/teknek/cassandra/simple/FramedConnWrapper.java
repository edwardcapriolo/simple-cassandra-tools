package io.teknek.cassandra.simple;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;

public class FramedConnWrapper {
  private TTransport transport;

  private TProtocol proto;

  private TSocket socket;
  
  private Cassandra.Client client;

  public FramedConnWrapper(String host, int port) {
    socket = new TSocket(host, port);
    transport = new TFramedTransport(socket);
    proto = new TBinaryProtocol(transport);
  }

  public void open() throws Exception {
    transport.open();
    client = new Cassandra.Client(proto);
  }

  public void close() throws Exception {
    transport.close();
    socket.close();
  }

  public Cassandra.Client getClient() {
    return client;
  }
}

