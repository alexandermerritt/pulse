import java.io.*;
import java.lang.*;
import java.util.*;
import java.lang.management.ManagementFactory;

public class Logger {
    public PrintWriter out;
    public String logPath;

    public String uniqName() {
        return ManagementFactory.getRuntimeMXBean().getName();
    }
    Logger(String name) {
        logPath = new String("/tmp/" + name
                + "_" + uniqName()
                + "_" + Integer.toString(Math.abs(new Random().nextInt()))
                + "_.log");
    }
    public boolean error() { return (out != null && out.checkError()); }
    public boolean open() {
        try {
            out = new PrintWriter(
                    new BufferedWriter(
                        new FileWriter(logPath)));
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    // static methods to avoid having to write null checks everywhere

    // global toggle
    private static boolean enable = true;
    public static void enable() { enable = true; }
    public static void disable() { enable = false; }

    public static void println(Logger l, String s) {
        if (enable && (l != null) && !l.error())
            l.out.println(s);
    }

    public static void close(Logger l) {
        if (l != null)
            l.out.close();
    }
}

