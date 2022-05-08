package common.rpc;

import master.rpc.Master;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import region.rpc.Region;

/**
 * 连接 Thrift 服务器。
 */
public class ThriftClient {

  public static Master.Client getForMaster(String host, int port)
      throws TTransportException {
    TTransport transport = new TSocket(host, port);
    transport.open();
    TProtocol protocol = new TBinaryProtocol(transport);
    return new Master.Client(protocol);
  }

  public static Region.Client getForRegionServer(String host, int port)
      throws TTransportException {
    TTransport transport = new TSocket(host, port);
    transport.open();
    TProtocol protocol = new TBinaryProtocol(transport);
    return new Region.Client(protocol);
  }

  public static void main(String[] args) throws TException {
    var client = ThriftClient.getForMaster("127.0.0.1", 5099);
    client.getRegionsOfTable("test", false, false);
    client.finishCopyTable("test", 0);
  }
}
