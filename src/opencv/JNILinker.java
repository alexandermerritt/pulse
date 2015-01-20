import java.io.Serializable;
import java.io.IOException;
import java.util.HashSet;

// How to use:
//   1. javah -jni JNILinker (produces header)
//   2. implement funcs in your cc file
//   3. build shared object from cc
//   4. load shared object at runtime like below
// If prototypes in this class change, header requires regeneration,
// and cc file must be matched + rebuilt.



// XXX serializable required for Storm to contain instance of this
// class in any bolt/spout. Not sure of implications... need to
// store/save shared library info and restore?
public class JNILinker implements Serializable {

    // NOTE: shared library should be in same dir as process when
    // FIRST instance of this class is instanstiated. For Storm, we
    // find ourselves in the resources/ dir somewhere in /tmp/ as
    // provided by the user-submitted jar file.
    static {
        String dir = System.getProperty("user.dir");
        String lib = dir + "/libjnilinker.so";
        System.load(lib);
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
    // the GPGPU.
    public native int feature(String image_key);
}
