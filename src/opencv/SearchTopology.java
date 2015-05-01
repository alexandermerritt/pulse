import backtype.storm.*;
import backtype.storm.coordination.*;
import backtype.storm.coordination.CoordinatedBolt.*;
import backtype.storm.drpc.*;
import backtype.storm.spout.*;
import backtype.storm.task.*;
import backtype.storm.topology.*;
import backtype.storm.topology.base.*;
import backtype.storm.tuple.*;
import java.io.*;
import java.lang.*;
import java.util.*;
import storm.trident.*;
import storm.trident.operation.*;
import storm.trident.tuple.TridentTuple;
import backtype.storm.utils.DRPCClient;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.ShellProcess;

import java.lang.management.ManagementFactory;

// Inner classes are static as they are considered top-level and not
// internal to SearchTopology:
// http://docs.oracle.com/javase/tutorial/java/javaOO/nested.html

// Constructors should not be declared to return 'void' else the Java
// compiler doesn't recognize it as the constructor.

// JNILinker objects should only be instantiated as close to being
// 'online' as possible, i.e., the prepare() or activate() methods.
// This ensures the bolts and processes are on the respective nodes
// that Storm places them on, and the resources/ directory is
// available.

// Each bolt/spout class in java is just a proxy for the C++ class
// equivalent
public class SearchTopology {

    // ---------------------------------------------------------------
    // GLOBAL CONFIGURATION KNOBS
    // ---------------------------------------------------------------

    // spout - how long between releasing new tuples
    static final int delayEach = 1000;
    // spout - how many items to send
    static final int maxCount  = 256;

    // reqmgr - true if we throttle requests to be one at a time. new
    // ones released when prior completes round-trip
    static boolean reqSerialize = false;

    // imagesof - limit how many unique images we see for a request,
    // then forward to feature + montage
    static final int maxImgsPer = 48;

    // topology - how many neighbor bolts to create
    static int numNeighbor = 3;

    // parallelism hints given to storm for each bolt
    public static class SearchConfig {
        // cluster information (hardware)
        public static int numNodes      = 7;
        public static int perNodeGPUs   = 3;
        public static int perNodeCPUs   = 12;
        public static int numGPUs       = (numNodes * perNodeGPUs);
        // global storm configuration
        public static int numWorkers            = numNodes;
        public static int maxTaskParallelism    = 256;
        // how many threads to use for each bolt
        public static class threads {
            public static final int spout       = 1;
            public static final int reqMgr      = 1;
            public static final int neighbor    = numNodes*3;
            public static final int uniq        = numNodes*3;
            public static final int imagesOf    = numNodes*3;
            public static final int feature     = numGPUs;
            public static final int montage     = numNodes*3;
        }
        // how many instances of each bolt object. make 1:1
        public static class tasks {
            public static final int spout       = threads.spout;
            public static final int reqMgr      = threads.reqMgr;
            public static final int neighbor    = threads.neighbor;
            public static final int uniq        = threads.uniq;
            public static final int imagesOf    = threads.imagesOf;
            public static final int feature     = threads.feature;
            public static final int montage     = threads.montage;
        }
    }

    public static void printConfigKnobs() {
        StringBuffer sb = new StringBuffer();

        sb.append("\n");
        sb.append("TOPOCONFIG delayEach " + delayEach + "\n");
        sb.append("TOPOCONFIG maxCount " + maxCount + "\n");
        sb.append("TOPOCONFIG reqSerialize " + reqSerialize + "\n");
        sb.append("TOPOCONFIG maxImgsPer " + maxImgsPer + "\n");
        sb.append("TOPOCONFIG numNeighbor " + numNeighbor + "\n");

        sb.append("TOPOCONFIG numWorkers "
                + SearchConfig.numWorkers + "\n");
        sb.append("TOPOCONFIG maxTaskParallelism "
                + SearchConfig.maxTaskParallelism + "\n");
        sb.append("TOPOCONFIG spout threads "
                + SearchConfig.threads.spout + "\n");
        sb.append("TOPOCONFIG reqMgr threads "
                + SearchConfig.threads.reqMgr + "\n");
        sb.append("TOPOCONFIG neighbor threads "
                + SearchConfig.threads.neighbor + "\n");
        sb.append("TOPOCONFIG uniq threads "
                + SearchConfig.threads.uniq + "\n");
        sb.append("TOPOCONFIG imagesOf threads "
                + SearchConfig.threads.imagesOf + "\n");
        sb.append("TOPOCONFIG feature threads "
                + SearchConfig.threads.feature + "\n");
        sb.append("TOPOCONFIG montage threads "
                + SearchConfig.threads.montage + "\n");

        System.out.println(sb.toString());
    }

    // ---------------------------------------------------------------
    // Common code
    // ---------------------------------------------------------------

    // 0: request ID 1: action 2: action-specific 3: etc
    public static class Labels {
        // TODO vertex count -> request tuple count
        // vertices may be converted to images or something else later
        public static class Stream {
            public static final String vertices = "stream_vertices";
            public static final String counts = "stream_count";
            public static final String images = "stream_images";
            public static final String montage = "stream_montage";
            public static final String completed = "stream_completed";
        }
        // Fields
        public static final String reqID = "field_reqID";
        public static final String vertex = "field_vertex";
        public static final String count = "field_count";
        public static final String image = "field_image";
    }

    // XXX instead of maintaining as array, covnert all vertices to
    // 0-X values and just give us the max...
    public static String[] vertices;

    public static Object lock = new Object();
    public static String resourcesDir;
    public static final String confName = new String("pulse.conf");

    public static void setResourceDir(Map conf) {
        synchronized(lock) {
            if (null != resourcesDir)
                return;
            resourcesDir = new String();
            resourcesDir += conf.get("storm.local.dir");
            resourcesDir += "/supervisor/stormdist";
            resourcesDir += "/" + conf.get("storm.id");
            resourcesDir += "/resources";
        }
    }

    public static String getResourcePath(String filename) {
        if (null == resourcesDir)
            throw new RuntimeException("Error: resourcesDir not yet initialized");
        String s = resourcesDir + "/" + filename;
        return s;
    }

    public static String readMemcInfo(String confPath)
        throws IOException {

        String memc = new String();
        BufferedReader in = new BufferedReader(new FileReader(confPath));
        while (in.ready()) {
            String line = in.readLine();
            String[] tokens = line.split(" ");
            if (tokens[0].equals("memc"))
                if (tokens[1].equals("serv"))
                    memc = line.substring(line.indexOf("--SERVER"));
        }
        if (memc.length() == 0)
            throw new IllegalArgumentException(
                    "No memc entry in conf");

        return memc;
    }

    public static void readVertexList(String confPath)
        throws IOException {

        String idsPath = new String();
        BufferedReader in = new BufferedReader(new FileReader(confPath));
        while (in.ready()) {
            String line = in.readLine();
            String[] tokens = line.split(" ");
            if (tokens[0].equals("graph"))
                if (tokens[1].equals("idsfile"))
                    idsPath = tokens[2];
        }
        if (idsPath.length() == 0)
            throw new IllegalArgumentException(
                    "No graph idsfile entry in conf");

        // open the file and suck in all values
        idsPath = getResourcePath(idsPath);
        in = new BufferedReader(new FileReader(idsPath));
        ArrayDeque<String> entries = new ArrayDeque<String>();
        while (in.ready())
            entries.add(in.readLine());
        vertices = entries.toArray(new String[0]);
        System.out.println(Integer.toString(vertices.length)
                + " vertices");
    }

    public static final String TopologyName = new String("search");

    // ---------------------------------------------------------------
    // Regular Bolts
    // ---------------------------------------------------------------

    // simplify my life
    public static abstract class SimpleSpout implements IRichSpout {
        Logger log;
        SpoutOutputCollector c;
        // initialize below fields in subclass' constructor
        Fields outputFields;
        String name, memcInfo;
        boolean active = false;

        @Override
        public void declareOutputFields(OutputFieldsDeclarer d) {
            d.declare(outputFields);
        }

        @Override
        public void open(Map conf, TopologyContext context,
                SpoutOutputCollector collector) {
            c = collector;
            log = new Logger(name);
            log.open();

            setResourceDir(conf);

            Set<String> keys = conf.keySet();
            for (String key : keys) {
                Logger.println(log, "CONF " + key + " "
                        + conf.get(key));
            }

            String confPath = getResourcePath(confName);
            try {
                memcInfo = readMemcInfo(confPath);
            } catch (IOException e) {
                System.err.println("Error opening conf file");
            }
        }

        @Override public void close() {
            Logger.close(log);
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            Logger.println(log, "getComponentConfig()");
            return null;
        }

        @Override
        public void fail(Object msgId) {
            Logger.println(log, "fail()");
        }
        @Override
        public void ack(Object msgId) {
            Logger.println(log, "ack()");
        }
        @Override
        public void deactivate() {
            Logger.println(log, "deactivate()");
            active = false;
        }
        @Override
        public void activate() {
            Logger.println(log, "activate()");
            active = true;
        }
    }

    // simplify my life
    public static abstract class SimpleBolt implements IRichBolt {
        Logger log;
        OutputCollector c;
        // initialize below fields in subclass' constructor
        Fields outputFields;
        String name, memcInfo;

        @Override
        public Map<String, Object> getComponentConfiguration() {
            Logger.println(log, "getComponentConfig()");
            return null;
        }

        @Override
        public void prepare(Map conf,
                TopologyContext context, OutputCollector collector) {
            c = collector;
            log = new Logger(name);
            log.open();

            setResourceDir(conf);

            String confPath = getResourcePath(confName);
            try {
                memcInfo = readMemcInfo(confPath);
            } catch (IOException e) {
                System.err.println("Error opening conf file");
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer d) {
            Logger.println(log, "declareOutputFields()");
            d.declare(outputFields);
        }

        @Override public void cleanup() {
            Logger.println(log, "cleanup()");
        }
    }

    // ---------------------------------------------------------------
    // Spout - Generator
    // ---------------------------------------------------------------

    public static class Generator extends SimpleSpout {
        private Random rand;
        private int count, startID, vertexIdx;

        Generator() {
            name = "GENERATOR";
            count = 0;
            startID = 0;
            vertexIdx = 0;
        }

        @Override // ignore superclass
        public void declareOutputFields(OutputFieldsDeclarer d) {
            Fields fields = new Fields(Labels.reqID, Labels.vertex);
            d.declareStream(Labels.Stream.vertices, fields);
            Logger.println(log, Labels.Stream.vertices
                    + Labels.reqID + " " + Labels.vertex);
        }

        private Date last;

        @Override
        public void nextTuple() {

            if (!active)
                return;

            if (count > maxCount)
                return;

            if (delayEach > 0) {
                if (null != last) {
                    Date now = new Date();
                    long t0 = last.getTime(), t1 = now.getTime();
                    if ((t1 - t0) < delayEach)
                        return;
                    last = now;
                } else {
                    last = new Date();
                }
            }

            String reqID = Integer.toString(startID++);
            String vertex = new String(vertices[vertexIdx]);
            vertexIdx = (vertexIdx + 1) % vertices.length;

            Values values = new Values(reqID, vertex);
            c.emit(Labels.Stream.vertices, values);
            Logger.println(log, "emit " + reqID + " " + vertex);

            count++;
        }

        public void open(Map conf, TopologyContext context,
                SpoutOutputCollector collector) {
            super.open(conf, context, collector);
            rand = new Random();

            String confPath = getResourcePath(confName);
            try {
                readVertexList(confPath);
            } catch (IOException e) {
                System.err.println("Error reading vertex list: " + e);
                throw new RuntimeException("vertex list: " + e);
            }
            Logger.println(log, "started");

            printConfigKnobs();
        }
    }

    // ---------------------------------------------------------------
    // Bolt - RequestManager
    // ---------------------------------------------------------------

    // tracks requests in workflow
    public static class RequestManager extends SimpleBolt {

        class TrackingInfo {
            // end-to-end timing
            Date sent, recv;
            // progress, info, etc. throughout processing
            // TODO
            String montage_key;
        }

        // indexed by request ID
        HashMap<String,TrackingInfo> requests;
        LinkedList<Values> _nextIDs;
        List<Values> nextIDs;

        boolean firstSent;

        RequestManager() {
            name = "REQMGR";
            requests = new HashMap<String,TrackingInfo>();
            _nextIDs = new LinkedList<Values>();
            nextIDs = Collections.synchronizedList(_nextIDs);
            firstSent = false;
        }

        private TrackingInfo tracking(String reqID) {
            TrackingInfo info = requests.get(reqID);
            if (null == info) {
                info = new TrackingInfo();
                info.sent = new Date();
                Logger.println(log, "start at "
                        + info.sent.getTime() + " for " + reqID);
                info.recv = null;
                info.montage_key = null;
                requests.put(reqID, info);
            }
            return info;
        }

        public void prepare(Map conf,
                TopologyContext context, OutputCollector collector) {
            super.prepare(conf, context, collector);
            log.enable(); // REQMGR on, to record latencies
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer d) {
            Fields fields = new Fields(Labels.reqID, Labels.vertex);
            d.declareStream(Labels.Stream.vertices, fields);
        }
        private boolean streamIsCompleted(Tuple tuple) {
            String stream = tuple.getSourceStreamId();
            return (stream.equals(Labels.Stream.completed));
        }
        private boolean streamIsVertex(Tuple tuple) {
            String stream = tuple.getSourceStreamId();
            return (stream.equals(Labels.Stream.vertices));
        }

        private void _handleVertex(Values values) {
            String reqID = (String)values.get(0);
            if (reqID == "")
                throw new RuntimeException("reqID is empty");
            // vertex stream comes from Spout, should always be new
            // requests, not existing ones
            if (requests.containsKey(reqID)) {
                throw new RuntimeException("invalid reqID: "
                        + reqID + " already inprogress");
            }
            tracking(reqID); // add it
            Logger.println(log, "emitting reqID " + reqID
                    + " value " + (String)values.get(1));
            c.emit(Labels.Stream.vertices, values);
        }

        private void handleVertex(Tuple tuple) {
            String reqID = tuple.getString(0);
            String vertex = tuple.getString(1);
            _handleVertex(new Values(reqID, vertex));
        }

        private void handleVertex2(Tuple tuple) {
            if (!firstSent) {
                handleVertex(tuple);
                firstSent = true;
            } else {
                String reqID = tuple.getString(0);
                String vertex = tuple.getString(1);
                synchronized(nextIDs) {
                    nextIDs.add(new Values(reqID, vertex));
                }
            }
        }

        private void handleCompleted(Tuple tuple) {
            String reqID = tuple.getString(0);
            String image = tuple.getString(1);
            // completed stream comes from montage bolt, should have
            // added an entry from when the spout told us about it
            if (!requests.containsKey(reqID))
                throw new RuntimeException("invalid completion: "
                        + reqID + " not associated");
            TrackingInfo info = tracking(reqID);
            info.recv = new Date();
            long lapsed = (info.recv.getTime() - info.sent.getTime());
            String note = "finish at " + info.recv.getTime()
                + " for " + reqID + " lapsed " + lapsed + " ms";
            Logger.println(log, note);
            System.out.println(note);
            requests.remove(reqID);

            if (reqSerialize) {
                Values nextValues;
                synchronized(nextIDs) {
                    if (nextIDs.size() > 0)
                        nextValues = nextIDs.remove(0);
                    else
                        throw new RuntimeException(
                                "REQMGR didn't get new IDs in time"
                                + " to emit the next");
                }
                _handleVertex(nextValues);
            }
        }

        @Override
        public void execute(Tuple tuple) {
            String reqID = tuple.getString(0);
            if (streamIsCompleted(tuple)) {
                handleCompleted(tuple);
            } else if (streamIsVertex(tuple)) {
                if (reqSerialize)
                    handleVertex2(tuple);
                else
                    handleVertex(tuple);
            }
            else throw new RuntimeException("unknown stream");
            c.ack(tuple);
        }
    }

    // ---------------------------------------------------------------
    // Bolt - Neighbors
    // ---------------------------------------------------------------

    public static class Neighbors extends SimpleBolt {
        private JNILinker jni;
        private Random rand;

        Neighbors() { 
            name = "NEIGHBORS";
        }

        public void prepare(Map conf,
                TopologyContext context, OutputCollector collector) {
            super.prepare(conf, context, collector);
            jni = new JNILinker(memcInfo);
        }

        @Override // ignore superclass implementation
        public void declareOutputFields(OutputFieldsDeclarer d) {
            Fields fields = new Fields(Labels.reqID, Labels.vertex);
            d.declareStream(Labels.Stream.vertices, fields);
            fields = new Fields(Labels.reqID, Labels.count);
            d.declareStream(Labels.Stream.counts, fields);
        }

        private boolean streamIsVertex(Tuple tuple) {
            String stream = tuple.getSourceStreamId();
            return (stream.equals(Labels.Stream.vertices));
        }

        @Override
        public void execute(Tuple tuple) {
            String reqID = tuple.getString(0);
            Logger.println(log, "execute: " + reqID);
            Values values;

            if (!streamIsVertex(tuple))
                throw new RuntimeException("neighbors got wrong stream");

            String vertex = tuple.getString(1);
            Logger.println(log, "querying neighbors for " + vertex);
            HashSet<String> others = new HashSet<String>();
            try {
                jni.neighbors(vertex, others);
            }
            catch (JNIException e) {
                System.out.println("exception: " + e.e2s());
                Logger.println(log, "exception: " + e.e2s());
                switch (e.type) {
                    case JNIException.PROTOBUF:
                    case JNIException.OPENCV:
                    case JNIException.MEMC_NOTFOUND:
                        c.ack(tuple);
                        return;
                    case JNIException.NORECOVER:
                    default:
                        throw e;
                }
            }

            int count = others.size();

            values = new Values(reqID, Integer.toString(count));
            c.emit(Labels.Stream.counts, values);
            Logger.println(log, "emit count ("
                    + reqID + ", " + Integer.toString(count) + ")");

            for (String v : others) {
                values = new Values(reqID, v);
                c.emit(Labels.Stream.vertices, values);
            }

            c.ack(tuple);
        }
    }

    // ---------------------------------------------------------------
    // Bolt - Uniquer
    // ---------------------------------------------------------------

    // use with .fieldsGrouping() to see all tuples from same request
    public static class Uniquer extends SimpleBolt {
        class TrackingInfo {
            long vertexRecv, vertexWait;
            long countRecv, countWait;
            HashSet<String> values;
            public boolean done() {
                boolean nonZero = (vertexRecv > 0) && (vertexWait > 0)
                    && (countRecv > 0) && (countWait > 0);
                return (nonZero && (vertexRecv >= vertexWait)
                        && (countRecv >= countWait));
            }
        }

        // hashed by request ID
        HashMap<String, TrackingInfo> keys;
        String endingBoltID;

        private Uniquer() { }
        Uniquer(String endingBoltID) {
            name = "UNIQ";
            keys = new HashMap<String, TrackingInfo>();
            this.endingBoltID = endingBoltID;
        }

        @Override // ignore superclass implementation
        public void declareOutputFields(OutputFieldsDeclarer d) {
            Fields fields = new Fields(Labels.reqID, Labels.vertex);
            d.declareStream(Labels.Stream.vertices, fields);
            fields = new Fields(Labels.reqID, Labels.count);
            d.declareStream(Labels.Stream.counts, fields);
        }

        public void prepare(Map conf,
                TopologyContext context, OutputCollector collector) {
            super.prepare(conf,context,collector);
        }

        private TrackingInfo tracking(String reqID) {
            TrackingInfo info = keys.get(reqID);
            if (info == null) {
                Logger.println(log, "New tracking info for " + reqID);
                info = new TrackingInfo();
                info.values = new HashSet<String>();
                info.countWait = 1;
                keys.put(reqID, info);
            }
            return info;
        }

        private boolean streamIsCount(Tuple tuple) {
            String stream = tuple.getSourceStreamId();
            return (stream.equals(Labels.Stream.counts));
        }

        private boolean streamIsVertex(Tuple tuple) {
            String stream = tuple.getSourceStreamId();
            return (stream.equals(Labels.Stream.vertices));
        }

        private void handleCount(Tuple tuple) {
            String reqID = tuple.getString(0);
            long count = Long.decode(tuple.getString(1)).longValue();
            TrackingInfo info = keys.get(reqID);
            info.countRecv++;
            Logger.println(log, reqID + " countRecv " + info.countRecv);
            String origin = tuple.getSourceComponent();
            Logger.println(log, "origin: '" + origin + "'"
                    + " endingBoltID: '" + endingBoltID + "'");
            if (!origin.equals(endingBoltID)) {
                info.countWait += count;
                Logger.println(log, reqID
                        + " endingBoltID: "
                        + endingBoltID + " this: " + origin
                        + " countWait " + info.countWait);
            }
            info.vertexWait += count;
            Logger.println(log,
                    reqID + " vertexWait " + info.vertexWait);
        }

        private void handleVertex(Tuple tuple) {
            String reqID = tuple.getString(0);
            TrackingInfo info = keys.get(reqID);
            String vertex = tuple.getString(1);
            info.vertexRecv++;
            Logger.println(log, reqID + " vertexRecv " + info.vertexRecv);
            if (info.values.add(vertex)) {
                Values val = new Values(reqID, vertex);
                // should go to FeatureBolts
                c.emit(Labels.Stream.vertices, val);
                Logger.println(log, "emit: " + vertex);
            }
            if (info.done()) {
                Logger.println(log, "Got all for " + reqID);
                // should go to MontageBolt
                String count = Long.toString(info.vertexRecv);
                Values values = new Values(reqID, count);
                c.emit(Labels.Stream.counts, values);
                keys.remove(reqID);
            }
        }

        @Override
        public void execute(Tuple tuple) {
            String reqID = tuple.getString(0);
            TrackingInfo info = tracking(reqID);
            if (streamIsCount(tuple))
                handleCount(tuple);
            else if (streamIsVertex(tuple))
                handleVertex(tuple);
            else throw new RuntimeException("unknown stream");
            c.ack(tuple);
        }
    }

    // ---------------------------------------------------------------
    // Bolt - ImagesOf
    // If a vertex has no images, we emit '0' as the count... may be a
    // bug if feature cannot handle this.
    // ---------------------------------------------------------------

    // accepts vertex and count stream
    // emits count -> montage bolt (not feature)
    // emits images -> feature bolt
    public static class ImagesOf extends SimpleBolt {
        private JNILinker jni;
        class TrackingInfo {
            long vertexRecv, vertexWait;
            long countRecv, imagesSent;
            HashSet<String> imageIDs;
            boolean completed;
            public boolean done() {
                // ignore countrecv/vertexwait... since we may exceed
                // maxImgsPer before obtaining one anyway... XXX
                boolean nonZero = (vertexRecv > 0) && (imagesSent > 0);
                boolean reachedLimit =
                    ((vertexWait > 0) && (vertexRecv >= vertexWait))
                    || (imagesSent >= maxImgsPer);
                return (nonZero && reachedLimit);
            }
            public void markEmitted() {
                completed = true;
            }
            // if we receive more images than maxImgsPer, then we
            // emit it to the next stage, but we may still be
            // receiving images from prior stages... so we ignore
            // those instead of pretending they are new requests
            public boolean wasEmitted() {
                return completed;
            }
        }
        private TrackingInfo tracking(String reqID) {
            TrackingInfo info = keys.get(reqID);
            if (info == null) {
                Logger.println(log, "New tracking info for " + reqID);
                info = new TrackingInfo();
                keys.put(reqID, info);
                info.imageIDs = new HashSet<String>();
                info.completed = false;
                info.vertexRecv = info.vertexWait = 0;
                info.countRecv = info.imagesSent = 0;
            }
            return info;
        }


        // hashed by request ID
        HashMap<String, TrackingInfo> keys;
        String endingBoltID;

        ImagesOf() {
            name = "IMAGESOF";
            keys = new HashMap<String, TrackingInfo>();
        }
        public void prepare(Map conf,
                TopologyContext context, OutputCollector collector) {
            super.prepare(conf, context, collector);
            jni = new JNILinker(memcInfo);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer d) {
            Fields fields = new Fields(Labels.reqID, Labels.count);
            d.declareStream(Labels.Stream.counts, fields);
            fields = new Fields(Labels.reqID, Labels.image);
            d.declareStream(Labels.Stream.images, fields);
        }

        private void checkDone(TrackingInfo info, Tuple tuple) {
            String reqID = tuple.getString(0);
            if (info.done() && !info.wasEmitted()) {
                Logger.println(log, "got all for " + reqID);
                // should go to MontageBolt
                String sent = Long.toString(info.imagesSent);
                Values values = new Values(reqID, sent);
                Logger.println(log, "emitting completed reqID "
                        + reqID + ":"
                        + " vertexRecv " + info.vertexRecv
                        + " vertexWait " + info.vertexWait
                        + " countRecv " + info.countRecv
                        + " imagesSent " + info.imagesSent
                        );
                c.emit(Labels.Stream.counts, values);
                // instead of removing, we keep it around but mark it
                // was completed.. this is b/c we may decide to send
                // "completed" before receiving all images
                //keys.remove(reqID);
                info.markEmitted();
            }
            if (info.wasEmitted()) {
                Logger.println(log, "got req for "
                        + "completed request " + reqID
                        + " - ignoring");
            }
        }

        private void handleCount(Tuple tuple) {
            String reqID = tuple.getString(0);
            long count = Long.decode(tuple.getString(1)).longValue();
            TrackingInfo info = keys.get(reqID);
            info.countRecv++;
            if (info.countRecv > 1)
                throw new RuntimeException("imagesof seen >1 count");
            Logger.println(log, reqID + " countRecv " + info.countRecv);
            info.vertexWait += count;
            Logger.println(log, reqID + " vertexWait " +
                    info.vertexWait);

            // we may get the count message only after having received
            // all vertex tuples, so need to check here, too
            checkDone(info, tuple);
        }

        private void handleVertex(Tuple tuple) {
            String reqID = tuple.getString(0);
            TrackingInfo info = keys.get(reqID);
            if (info.wasEmitted()) {
                Logger.println(log,
                        "ignoring new vertex for reqID " + reqID);
                return;
            }
            String vertex = tuple.getString(1);
            info.vertexRecv++;
            Logger.println(log, reqID
                    + " vertexRecv " + info.vertexRecv);
            HashSet<String> imageKeys = new HashSet<String>();
            try {
                jni.imagesOf(vertex, imageKeys);
            }
            catch (JNIException e) {
                System.out.println("exception: " + e.e2s());
                Logger.println(log, "exception: " + e.e2s());
                switch (e.type) {
                    case JNIException.PROTOBUF:
                    case JNIException.OPENCV:
                    case JNIException.MEMC_NOTFOUND:
                        checkDone(info, tuple);
                        return;
                    case JNIException.NORECOVER:
                    default:
                        throw e;
                }
            }

            for (String imageID : imageKeys) {
                // filter out duplicates for this request ID
                if (info.imageIDs.add(vertex)) {
                    c.emit(Labels.Stream.images,
                            new Values(reqID, imageID));
                    info.imagesSent++;
                    checkDone(info, tuple);
                }
            }

            // check if we got the final vertex message (only if we
            // might receive the count msg prior to the last vertex)
            checkDone(info, tuple);
        }

        private boolean streamIsCount(Tuple tuple) {
            String stream = tuple.getSourceStreamId();
            return (stream.equals(Labels.Stream.counts));
        }

        private boolean streamIsVertex(Tuple tuple) {
            String stream = tuple.getSourceStreamId();
            return (stream.equals(Labels.Stream.vertices));
        }

        @Override
        public void execute(Tuple tuple) {
            String reqID = tuple.getString(0);
            TrackingInfo info = tracking(reqID);
            if (streamIsCount(tuple))
                handleCount(tuple);
            else if (streamIsVertex(tuple))
                handleVertex(tuple);
            else throw new RuntimeException("unknown stream");
            c.ack(tuple);
        }
    }

    // ---------------------------------------------------------------
    // Bolt - Feature
    // ---------------------------------------------------------------

    public static class Feature extends SimpleBolt {
        private JNILinker jni;

        Feature() {
            name = "FEATURE";
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer d) {
            Fields fields = new Fields(Labels.reqID, Labels.image);
            d.declareStream(Labels.Stream.images, fields);
        }

        public void prepare(Map conf,
                TopologyContext context, OutputCollector collector) {
            super.prepare(conf, context, collector);
            jni = new JNILinker(memcInfo);
        }

        @Override
        public void execute(Tuple tuple) {
            String reqID = tuple.getString(0);
            String imageID = tuple.getString(1);
            Logger.println(log, "execute reqID " + reqID
                    + " imageID " + imageID);
            try {
                jni.feature(imageID);
            }
            catch (JNIException e) {
                String msg = e.e2s();
                System.out.println("exception: " + msg);
                Logger.println(log, "exception: " + msg);
                switch (e.type) {
                    case JNIException.OPENCV:
                        // XXX can happen if
                        // - nvidia-smi -c EXCLUSIVE_* and
                        // - too many bolts assigned to node
                        if (msg.contains("CUDA"))
                            throw e;
                    case JNIException.PROTOBUF:
                    case JNIException.MEMC_NOTFOUND:
                        // ignore, but keep going - this means montage
                        // or other bolts should not expect images to
                        // all have features
                        break;
                    case JNIException.NORECOVER:
                    default:
                        throw e;
                }
            }
            Logger.println(log, "emit reqID " + reqID
                    + " imageID " + imageID);
            Values values = new Values(reqID, imageID);
            c.emit(Labels.Stream.images, values);
            c.ack(tuple);
        }
    }

    // ---------------------------------------------------------------
    // Bolt - Montage
    // ---------------------------------------------------------------

    // receives counts from imagesOf
    // receives imageIDs from featurebolt
    public static class Montage extends SimpleBolt {
        private JNILinker jni;
        class TrackingInfo {
            long imageRecv, imageWait;
            long countRecv;
            boolean emitted;
            HashSet<String> imageIDs;
            public boolean done() {
                boolean nonZero = (imageRecv > 0) && (imageWait > 0)
                    && (countRecv > 0); // must consider countRecv
                return (nonZero && (imageRecv >= imageWait));
            }
            public boolean wasEmitted() { return emitted; }
            public void setEmitted() { emitted = true; }
        }
        private TrackingInfo tracking(String reqID) {
            TrackingInfo info = keys.get(reqID);
            if (info == null) {
                Logger.println(log, "New tracking info for " + reqID);
                info = new TrackingInfo();
                info.imageIDs = new HashSet<String>();
                info.emitted = false;
                keys.put(reqID, info);
            }
            return info;
        }

        // hashed by request ID
        HashMap<String, TrackingInfo> keys;
        String endingBoltID;

        Montage() {
            name = "MONTAGE";
            keys = new HashMap<String, TrackingInfo>();
        }
        public void prepare(Map conf,
                TopologyContext context, OutputCollector collector) {
            super.prepare(conf, context, collector);
            jni = new JNILinker(memcInfo);
        }

        @Override // spits out montage key (which is an image)
        public void declareOutputFields(OutputFieldsDeclarer d) {
            Fields fields = new Fields(Labels.reqID, Labels.image);
            d.declareStream(Labels.Stream.completed, fields);
        }
        private boolean streamIsCount(Tuple tuple) {
            String stream = tuple.getSourceStreamId();
            return (stream.equals(Labels.Stream.counts));
        }
        private boolean streamIsImage(Tuple tuple) {
            String stream = tuple.getSourceStreamId();
            return (stream.equals(Labels.Stream.images));
        }
        private void handleCount(Tuple tuple) {
            String reqID = tuple.getString(0);
            long count = Long.decode(tuple.getString(1)).longValue();
            TrackingInfo info = keys.get(reqID);
            info.countRecv++;
            if (info.countRecv > 1)
                throw new RuntimeException("montage seen >1 count");
            Logger.println(log, reqID + " countRecv " + info.countRecv);
            info.imageWait += count;
            Logger.println(log, reqID + " imageWait " +
                    info.imageWait);
        }
        private void handleImage(Tuple tuple) {
            String reqID = tuple.getString(0);
            TrackingInfo info = keys.get(reqID);
            if (info.wasEmitted()) {
                Logger.println(log, "got tuple for previously"
                        + " emitted request " + reqID 
                        + " - ignoring");
                return;
            }
            String imageID = tuple.getString(1);
            info.imageRecv++;
            Logger.println(log, reqID + " imageRecv " +
                    info.imageRecv);

            info.imageIDs.add(imageID);
            Logger.println(log, reqID + " set has "
                    + Integer.toString(info.imageIDs.size())
                    + " images");

            if (info.done() && !info.wasEmitted()) {
                Logger.println(log, "Got all "
                        + Integer.toString(info.imageIDs.size())
                        + " images (uniq'd set) for " + reqID);
                StringBuffer montage_key = new StringBuffer();
                try {
                    jni.montage(info.imageIDs, montage_key);
                }
                catch (JNIException e) {
                    System.out.println("exception: " + e.e2s());
                    Logger.println(log, "exception: " + e.e2s());
                    switch (e.type) {
                        case JNIException.PROTOBUF:
                        case JNIException.OPENCV:
                        case JNIException.MEMC_NOTFOUND:
                            // XXX hack: just pick some existing image...
                            montage_key.append(imageID);
                            break;
                        case JNIException.NORECOVER:
                        default:
                            throw e;
                    }
                }
                // XXX add a failed status (e.g. as field), if needed?
                Values v = new Values(reqID, montage_key.toString());
                c.emit(Labels.Stream.completed, v);
                info.setEmitted();

                // don't remove... hold onto it b/c we may get
                // remaining image IDs for something we already
                // completed...
                //keys.remove(reqID);
                info.imageIDs.clear();
            }
        }

        @Override
        public void execute(Tuple tuple) {
            String reqID = tuple.getString(0);
            TrackingInfo info = tracking(reqID);
            if (streamIsCount(tuple))
                handleCount(tuple);
            else if (streamIsImage(tuple))
                handleImage(tuple);
            else throw new RuntimeException("unknown stream");
            c.ack(tuple);
        }
    }

    // ---------------------------------------------------------------
    // Topologies
    // ---------------------------------------------------------------

    public static void doRegular(String[] args, Config config)
        throws Exception {
        // TODO make a diagram for what this workflow looks like

        String neighBase = "neighbor", name = "";
        Fields sortByID = new Fields(Labels.reqID);

        TopologyBuilder builder = new TopologyBuilder();
        SpoutDeclarer gen = builder.setSpout("gen", new Generator(),
                SearchConfig.threads.spout);
        gen.setNumTasks(SearchConfig.tasks.spout);

        BoltDeclarer reqMgr = builder.setBolt("mgr",
                new RequestManager(), SearchConfig.threads.reqMgr);
        reqMgr.setNumTasks(SearchConfig.tasks.reqMgr);
        reqMgr.fieldsGrouping("gen", Labels.Stream.vertices, sortByID);

        name = neighBase + (numNeighbor - 1);
        BoltDeclarer uniq = builder.setBolt("uniq",
                new Uniquer(name), SearchConfig.threads.uniq);
        uniq.setNumTasks(SearchConfig.tasks.uniq);

        name = neighBase + 0;
        BoltDeclarer neigh = builder.setBolt(name, new Neighbors(),
                SearchConfig.threads.neighbor);
        neigh.setNumTasks(SearchConfig.tasks.neighbor);
        neigh.shuffleGrouping("mgr", Labels.Stream.vertices);
        uniq.fieldsGrouping(name, Labels.Stream.counts, sortByID);
        uniq.fieldsGrouping(name, Labels.Stream.vertices, sortByID);
        for (int n = 1; n < numNeighbor; n++) {
            String prior = neighBase + (n - 1);
            name = neighBase + n;
            neigh = builder.setBolt(name, new Neighbors(),
                    SearchConfig.threads.neighbor);
            neigh.fieldsGrouping(prior, Labels.Stream.vertices, sortByID);
            neigh.setNumTasks(SearchConfig.tasks.neighbor);
            uniq.fieldsGrouping(name, Labels.Stream.counts, sortByID);
            uniq.fieldsGrouping(name, Labels.Stream.vertices, sortByID);
        }

        BoltDeclarer imagesof = builder.setBolt("imagesOf",
                new ImagesOf(), SearchConfig.threads.imagesOf);
        imagesof.setNumTasks(SearchConfig.tasks.imagesOf);
        imagesof.fieldsGrouping("uniq", Labels.Stream.vertices, sortByID);
        imagesof.fieldsGrouping("uniq", Labels.Stream.counts, sortByID);

        BoltDeclarer feature = builder.setBolt("feature", new Feature(),
                SearchConfig.threads.feature);
        feature.setNumTasks(SearchConfig.tasks.feature);
        feature.shuffleGrouping("imagesOf", Labels.Stream.images);

        BoltDeclarer montage = builder.setBolt("montage", new Montage(),
                SearchConfig.threads.montage);
        montage.setNumTasks(SearchConfig.tasks.montage);
        montage.fieldsGrouping("feature",
                Labels.Stream.images, sortByID);
        montage.fieldsGrouping("imagesOf",
                Labels.Stream.counts, sortByID);

        reqMgr.fieldsGrouping("montage",
                Labels.Stream.completed, sortByID);

        StormTopology t = builder.createTopology();
        if (args.length > 0) {
            config.setNumWorkers(SearchConfig.numWorkers);
            config.setMaxTaskParallelism(SearchConfig.maxTaskParallelism);
            StormSubmitter.submitTopology(args[0], config, t);
        } else {
            config.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("search", config, t);
            Thread.sleep(60 * 1000); // ms
            cluster.shutdown();
        }
    }

    // ---------------------------------------------------------------
    // main
    // ---------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.setDebug(false); // how much is dumped into the log files
        long q = (1<<17);
        config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, q);
        config.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, q);
        //config.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
        //config.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
        doRegular(args, config);
    }
}

