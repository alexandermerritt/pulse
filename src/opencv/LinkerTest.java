import java.io.IOException;
import java.io.Serializable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.lang.reflect.Field;
import java.lang.RuntimeException;
import java.util.*;

class LinkerTest {
    static String servers;

    private static class Worker extends Thread {
        public void run() {
            // setup and connect to object store testing
            JNILinker jni = new JNILinker(servers);
            long visits = 16384;

            HashSet<String> seen, next;
            seen = new HashSet<String>();
            next = new HashSet<String>();
            next.add(new String("100000772955143706751"));

            HashSet<String> neighbors = new HashSet<String>();
            while (next.size() > 0) {
                neighbors.clear();
                Iterator<String> iter = next.iterator();
                String v = iter.next();
                next.remove(v);
                try {
                    jni.neighbors(v, neighbors);
                    System.out.println(v + " has "
                            + neighbors.size() + " links");
                }
                catch (JNIException e) {
                    System.out.println("exception: " + e.e2s());
                    switch (e.type) {
                        case JNIException.PROTOBUF:
                        case JNIException.OPENCV:
                        case JNIException.MEMC_NOTFOUND:
                            break;
                        case JNIException.NORECOVER:
                        default:
                            throw e;
                    }
                    continue;
                }
                for (String item : neighbors)
                    if (seen.add(item))
                        next.add(item);
                if (--visits == 0)
                    break;
            }

            HashSet<String> images = new HashSet<String>();
            HashSet<String> keys   = new HashSet<String>();

            for (String v : seen) {
                try {
                    jni.imagesOf(v, keys);
                    System.out.println(v + " has "
                            + keys.size() + " images");
                }
                catch (JNIException e) {
                    System.out.println("exception: " + e.e2s());
                    switch (e.type) {
                        case JNIException.PROTOBUF:
                        case JNIException.OPENCV:
                        case JNIException.MEMC_NOTFOUND:
                            break;
                        case JNIException.NORECOVER:
                        default:
                            throw e;
                    }
                    continue;
                }
                for (String img : keys)
                    images.add(img);
            }

            for (String key : images) {
                try {
                    System.out.println("feature search: " + key);
                    jni.feature(key);
                }
                catch (JNIException e) {
                    System.out.println("exception: " + e.e2s());
                    switch (e.type) {
                        case JNIException.PROTOBUF:
                        case JNIException.OPENCV:
                        case JNIException.MEMC_NOTFOUND:
                            break;
                        case JNIException.NORECOVER:
                        default:
                            throw e;
                    }
                    continue;
                }
            }
        }
    }

    public static void main(String[]  args) {
        servers = "--SERVER=10.0.0.1:11211"
            + " --SERVER=10.0.0.2:11211"
            + " --SERVER=10.0.0.3:11211"
            + " --SERVER=10.0.0.4:11211"
            + " --SERVER=10.0.0.5:11211"
            + " --SERVER=10.0.0.6:11211"
            + " --SERVER=10.0.0.7:11211";

        LinkedList threads = new LinkedList<Thread>();
        for (int i = 0; i < 3; i++) {
            Worker t = new Worker();
            t.start();
            threads.add(t);
        }

        Iterator<Thread> iter = threads.iterator();
        while (iter.hasNext()) {
            try {
                iter.next().join();
            } catch (InterruptedException e) {
            }
        }

//        // imagesOf test
//        vertex = "";
//        HashSet<String> image_keys = new HashSet<String>();
//        for (String v : neighbors) {
//            if (0 != jni.imagesOf(v, image_keys)) {
//                System.out.println("Error: jni.imagesOf()");
//                continue;
//            }
//            System.out.println(v + " has " + image_keys.size()
//                    + " images");
//
//            if (image_keys.size() > 1)
//                vertex = v;
//            else
//                continue;
//
//            // feature detection test
//            for (String image_key : image_keys) {
//                System.out.println("computing features for " + image_key);
//                if (0 != jni.feature(image_key)) {
//                    System.out.println("Error: jni.feature()");
//                    continue;
//                }
//            }
//
//            System.out.println("calling match");
//            if (0 != jni.match(image_keys)) {
//                System.out.println("Error: jni.match()");
//                continue;
//            }
//            System.out.println("image reduced to "
//                    + image_keys.size());
//
//            StringBuffer montage_key = new StringBuffer();
//            System.out.println("calling montage");
//            if (0 != jni.montage(image_keys, montage_key)) {
//                System.out.println("Error: jni.montage()");
//                continue;
//            }
//            System.out.println("montage key: " + montage_key);
    }
}
