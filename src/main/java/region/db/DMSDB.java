package region.db;

public class DMSDB {
    public static DMSDB DBDIR;
    public String storageSpace = "E:\\SQL\\DMS\\src\\main\\java\\region\\db\\DBFiles\\";

    public DMSDB(String root){
        DBDIR = new DMSDB();
        DBDIR.storageSpace = root;
    }

    public DMSDB(){

    }

    public static void changeDIR(String root){
        DBDIR.storageSpace = root;
    }
}
