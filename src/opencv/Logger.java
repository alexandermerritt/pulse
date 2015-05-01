import java.io.*;
import java.lang.*;
import java.util.*;
import java.lang.management.ManagementFactory;

public class Logger {
    public PrintWriter out;
    public String logPath;
    public String prefix;

    public String uniqName() {
        return ManagementFactory.getRuntimeMXBean().getName();
    }
    Logger(String name) {
        logPath = new String("/tmp/" + name
                + "_" + uniqName()
                + "_" + Integer.toString(Math.abs(new Random().nextInt()))
                + "_.log");
        prefix = name;
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
    private boolean enabled = false; // XXX keep off
    public void enable() { enabled = true; }
    public void disable() { enabled = false; }

    // anything that goes to the log also goes to stdout
    public static void println(Logger l, String s) {
        if ((l != null) && l.enabled && !l.error()) {
            l.out.println(l.prefix + " " + s);
            System.out.println(l.prefix + " " + s);
        }
    }

    public static void close(Logger l) {
        if (l != null)
            l.out.close();
    }
}

