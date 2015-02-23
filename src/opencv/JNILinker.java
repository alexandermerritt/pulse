import java.io.Serializable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.lang.reflect.Field;
import java.lang.RuntimeException;

// How to use:
//   1. javah -jni JNILinker (produces header)
//   2. implement funcs in your cc file
//   3. build shared object from cc
//   4. load shared object at runtime like below
// If prototypes in this class change, header requires regeneration,
// and cc file must be matched + rebuilt.

// XXX serializable required for Storm to contain instance of this
// class in any bolt/spout.
public class JNILinker implements Serializable {

    // XXX in order to find this library, it must be in the resource/
    // directory of the jar. In order to find the libraries it depends
    // on, such as cuda, you must specify
    //      java.library.path
    // in the storm.yaml configuration file.
    static {
        // update system path (needed for local topology)
        String path = System.getProperty("java.library.path");
        path += ":.";
        System.setProperty("java.library.path", path);

        // force re-read of system path
        // http://blog.cedarsoft.com/2010/11/setting-java-library-path-programmatically/
        try {
            Field syspath = ClassLoader.class.getDeclaredField("sys_paths");
            syspath.setAccessible(true);
            syspath.set(null, null);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException("Error: no sys_path: "
                    + e.toString());
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Error updating sys_path: "
                    + e.toString());
        }

        System.loadLibrary("jnilinker");
    }

    // Open persistent connection to memcached
    public native int connect(String servers);

    // Query object store for given key, returns set of keys
    // representing neighbor vertices.
    public native int neighbors(String vertex, HashSet<String> others);

    // Query object store for metadata of given vertex and return a
    // list of keys representing the images associated with it.
    public native int imagesOf(String vertex, HashSet<String> keys);

    // Compute features of image. Store back into object store. Uses
    // the GPGPU. Return value <0 is error, else a count of number of
    // features found.
    public native int feature(String image_key);

    public native int match(HashSet<String> image_keys);

    public native int montage(HashSet<String> image_keys,
            StringBuffer montage_key);

    public native int writeImage(String key, String path);
}
