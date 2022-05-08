package common.rpc;

import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import master.rpc.Master;
import master.rpc.Master.Processor;
import master.rpc.cacheTable;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

@Slf4j
public class ThriftServer {

  private final TServer server;
  private final int port;

  public ThriftServer(TProcessor processor, int port)
      throws TTransportException {
    this.port = port;
    TServerSocket serverTransport = new TServerSocket(port);
    server = new TThreadPoolServer(
        new TThreadPoolServer
            .Args(serverTransport)
            .processor(processor)
            .protocolFactory(new TBinaryProtocol.Factory()));
  }

  public int getPort() {
    return port;
  }

  public void startServer() {
    server.serve();
  }

  /**
   * Only for testing.
   *
   * @param args
   * @throws TTransportException
   */
  public static void main(String[] args)
      throws TTransportException, InterruptedException {
    // 样例实现
    Master.Iface handler = new Master.Iface() {
      @Override
      public List<cacheTable> getRegionsOfTable(String tableName,
          boolean isCreate, boolean isDrop) throws TException {
        System.out.println("getRegionsOfTable()");
        return new ArrayList<cacheTable>();
      }

      @Override
      public void finishCopyTable(String tableName, int uid)
          throws TException {
        System.out.println("finishCopyTable()");
      }
    };

    Processor<Master.Iface> processor = new Master.Processor<>(handler);

    ThriftServer server = new ThriftServer(processor, 5099);
    server.startServer();
    Thread.sleep(1000000);
  }
}
