package common.meta;

public class ClientInfoFactory {

  public static ClientInfo from(String str) {
    String[] list = str.split(",", -1);
    if (list.length != 3 || list[0].length() == 0 || list[1].length() == 0) {
      throw new IllegalArgumentException("Bad format");
    }
    String ip = list[0];
    int rpcPort = Integer.parseInt(list[1]);
    int socketPort = (list[2].length() > 0) ? Integer.parseInt(list[2]) : 0;
    return ClientInfoFactory.from(ip, rpcPort, socketPort);
  }

  public static ClientInfo from(String ip, int rpcPort) {
    return new ClientInfo(ip, rpcPort, 0, 0);
  }

  public static ClientInfo from(String ip, int rpcPort, int socketPort) {
    return new ClientInfo(ip, rpcPort, socketPort, 0);
  }

  public static String toString(ClientInfo clientInfo) {
    return clientInfo.ip + "," + clientInfo.rpcPort + ","
        + clientInfo.socketPort;
  }
}
