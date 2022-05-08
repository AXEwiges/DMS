package common.meta;

import config.config;
import lombok.Data;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author AXEwiges
 * @version 5.8
 * */
@Data
class logLoad implements Serializable {
    @Serial
    private static final long serialVersionUID = 7510582097494459L;
    public List<String> Log;
    public String tableName;
    public Integer checkPoint;

    public logLoad(List<String> strings, String name, Integer integer) {
        Log = strings;
        tableName = name;
        checkPoint = integer;
    }
}

/**
 * @author AXEwiges
 * @version 5.8
 * */
//@Data
public class DMSLog {
    /**
    * 主要Log，存储全部的语句日志，key为表名，value为日志列表
    * */
    public ConcurrentHashMap<String, List<String>> mainLog;
    /**
     * 缓存Log，同步中的接收者的临时语句日志，key为表名，value为日志列表
     * */
    public ConcurrentHashMap<String, List<String>> bufferLog;
    /**
     * 临时Log，用于前述结构销毁和重建中的交换变量，key为表名，value为日志列表
     * */
    public ConcurrentHashMap<String, List<String>> tempLog;
    /**
     * 各表的传输时刻检查点，用于同步检验
     * */
    public ConcurrentHashMap<String, Integer> checkPoints;
    /**
     * 同步线程，不断监听，获取Log
     * */
    public syncRecvThread monitorThread;
    /**
     * 必要的config数据
     * */
    public config _LC;
    public DMSLog(config _C){
        _LC = _C;

        mainLog = new ConcurrentHashMap<>();
        bufferLog = new ConcurrentHashMap<>();
        tempLog = new ConcurrentHashMap<>();
        checkPoints = new ConcurrentHashMap<>();
        monitorThread = new syncRecvThread();

        monitorThread.start();
    }
    /**
     * 用于向日志中某个表项添加一条
     * @param tableName 表名
     * @param statement 语句
     * */
    public void add(String tableName, String statement){
        mainLog.get(tableName).add(statement);
        Integer integer = checkPoints.get(tableName) + 1;
        checkPoints.put(tableName, integer);
    }

    class syncSendThread extends Thread {
        public static logLoad payload;
        private final Socket socket;
        public syncSendThread(logLoad payload, Socket socket) {
            syncSendThread.payload = payload;
            this.socket = socket;
        }

        @Override
        public void run() {
            System.out.println("Start Send to: " + socket.toString());
            try{
                ObjectOutputStream sendOut = new ObjectOutputStream(socket.getOutputStream());
                sendOut.writeObject(payload);
                sendOut.flush();
                System.out.println("Send Successfully");
                System.out.println(payload.toString());
            } catch (Exception ignored) {
                try {
                    socket.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    class syncRecvThread extends Thread {
        public ServerSocket server;
        public syncRecvThread() {
            try {
                this.server = new ServerSocket(_LC.network.port);
                System.out.println("Start Receive Sync Message");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            System.out.println("Start Recv Process, Listening: " + server.toString());
            while(true){
                Socket recv = null;
                try{
                    recv = server.accept();
                    ObjectInputStream recvInputStream = new ObjectInputStream(recv.getInputStream());
                    if(recvInputStream.readObject() != null){
                        System.out.println(recvInputStream.readObject().toString());
                    }
                    if(recvInputStream.readObject() instanceof logLoad log){
                        mainLog.put(log.tableName, log.Log);
                        checkPoints.put(log.tableName, log.checkPoint);
                        System.out.println("Received Successfully");
                        for(Map.Entry<String, List<String>> m : mainLog.entrySet()){
                            for(String s : m.getValue()){
                                System.out.println(s);
                            }
                        }
                    }
                } catch (Exception ignored) {
                    try {
                        if(recv != null)
                            recv.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    /**
     * 用于向目标服务器传输一个表的日志进行同步
     * @param ip ip地址
     * @param port 端口
     * @param tableName 表名
     * */
    public synchronized void transfer(String ip, String port, String tableName) {
        logLoad payload = new logLoad(mainLog.get(tableName), tableName, checkPoints.get(tableName));
        System.out.println("Start Wait");
        try {
            syncSendThread sender = new syncSendThread(payload, new Socket(ip, Integer.parseInt(port)));
            sender.start();
        } catch (Exception ignored) {

        }
    }
    /**
     * 用于接收日志，成功将通知master
     * */
    public static synchronized void receive(){

    }
}
