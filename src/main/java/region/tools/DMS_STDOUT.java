package region.tools;

public class DMS_STDOUT {
    public int status; //0 failed, 1 success, 2 exception
    public String info; //exception message
    public static final String[] type = {"[Failed]", "[Successful]", "[Exception]"};

    public DMS_STDOUT(int status, String msg) {
        this.status = status;
        this.info = msg;
    }

    public String data() {
        return type[status] + " " + info;
    }
}
