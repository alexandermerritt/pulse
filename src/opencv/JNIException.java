import java.lang.RuntimeException;
// must match jthrow_type
public class JNIException extends RuntimeException {
    public static final int PROTOBUF = 1;
    public static final int MEMC_NOTFOUND = 2;
    public static final int OPENCV = 3;
    public static final int NORECOVER = 4;

    public static final String PROTOBUF_S = "PROTOBUF";
    public static final String MEMC_NOTFOUND_S = "MEMC_NOTFOUND";
    public static final String OPENCV_S = "OPENCV";
    public static final String NORECOVER_S = "NORECOVER";

    public int type;
    public String msg;

    JNIException(int _type, String _msg) {
        super(_msg);
        type = _type;
        msg = _msg;
    }

    public String e2s() {
        String ret;
        switch (type) {
            case PROTOBUF: ret = PROTOBUF_S; break;
            case MEMC_NOTFOUND: ret = MEMC_NOTFOUND_S; break;
            case OPENCV: ret = OPENCV_S; break;
            case NORECOVER: ret = NORECOVER_S; break;
            default: return "UNKNOWN: " + msg;
        }
        return (ret + ": " + msg);
    }
}
