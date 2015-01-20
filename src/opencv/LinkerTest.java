import java.io.IOException;
import java.util.HashSet;

class LinkerTest {

    public static void main(String[]  args) {

        // setup and connect to object store testing
        JNILinker jni = new JNILinker();
        if (0 != jni.connect("--SERVER=10.0.0.1:11211 "
                    + "--SERVER=10.0.0.2:11211")) {
            System.out.println("Error: jni.connect()");
            return;
        }

        // neighbors test
        String vertex = "100000772955143706751";
        HashSet<String> others = new HashSet<String>();
        if (0 != jni.neighbors(vertex, others)) {
            System.out.println("Error: jni.neighbors()");
            return;
        }
        System.out.println(others.size() + " neighbors");

        // imagesOf test
        vertex = "";
        HashSet<String> image_keys = new HashSet<String>();
        for (String v : others) {
            if (0 != jni.imagesOf(v, image_keys)) {
                System.out.println("Error: jni.imagesOf()");
                return;
            }
            System.out.println(v + " has " + image_keys.size()
                    + " images");
            if (image_keys.size() > 0) {
                vertex = v;
                break;
            }
        }
        if (vertex.length() < 1) {
            System.out.println("No neighbors have any images.");
            return;
        }

        // feature detection test TODO
        for (String image_key : image_keys) {
            System.out.println("computing features for " + image_key);
            if (0 != jni.feature(image_key)) {
                System.out.println("Error: jni.feature()");
                return;
            }
            break; // just do one
        }
    }
}
