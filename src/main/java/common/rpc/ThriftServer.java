package common.rpc;

import lombok.extern.slf4j.Slf4j;
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

  /**
   * 创建 Thrift 服务。
   * @param processor
   * @param port 端口
   * @throws TTransportException
   */
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

  /**
   * 启动 Thrift 服务。
   */
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

  }
}
