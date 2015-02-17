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
                continue;
            }
            System.out.println(v + " has " + image_keys.size()
                    + " images");

            if (image_keys.size() > 1)
                vertex = v;
            else
                continue;

            // feature detection test
            for (String image_key : image_keys) {
                System.out.println("computing features for " + image_key);
                if (0 != jni.feature(image_key)) {
                    System.out.println("Error: jni.feature()");
                    continue;
                }
            }

            System.out.println("calling match");
            if (0 != jni.match(image_keys)) {
                System.out.println("Error: jni.match()");
                continue;
            }
            System.out.println("image reduced to "
                    + image_keys.size());

            StringBuffer montage_key = new StringBuffer();
            System.out.println("calling montage");
            if (0 != jni.montage(image_keys, montage_key)) {
                System.out.println("Error: jni.montage()");
                continue;
            }
            System.out.println("montage key: " + montage_key);
        }
    }
}
