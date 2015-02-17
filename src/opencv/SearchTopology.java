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

// Each bolt/spout class in java is just a proxy for the C++ class
// equivalent
public class SearchTopology {

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

    public static final String _confPath = new String("pulse.conf");

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

            try {
                memcInfo = readMemcInfo(_confPath);
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
        }
        @Override
        public void activate() {
            Logger.println(log, "activate()");
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
        public void prepare(Map stormConf,
                TopologyContext context, OutputCollector collector) {
            c = collector;
            log = new Logger(name);
            log.open();
            Logger.println(log, "prepare()");

            try {
                memcInfo = readMemcInfo(_confPath);
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
        private int count;

        Generator() {
            name = "gen-spout";
            count = 0;
        }

        @Override // ignore superclass
        public void declareOutputFields(OutputFieldsDeclarer d) {
            Fields fields = new Fields(Labels.reqID, Labels.vertex);
            d.declareStream(Labels.Stream.vertices, fields);
            Logger.println(log, Labels.Stream.vertices
                    + Labels.reqID + " " + Labels.vertex);
        }

        // TODO use a thread
        @Override
        public void nextTuple() {

            // XXX remove barrier once code works
            if (count > 0)
                return;

            String reqID = Long.toString(Math.abs(rand.nextLong()));

            //int idx = Math.abs(rand.nextInt()) % vertices.length;
            //String vertex = new String(vertices[idx]);
            String vertex = new String("106587101791064367906");

            Values values = new Values(reqID, vertex);
            c.emit(Labels.Stream.vertices, values);
            Logger.println(log, "emit " + reqID + " " + vertex);

            // TODO allow customization of the rate

            count++;
        }

        public void open(Map conf, TopologyContext context,
                SpoutOutputCollector collector) {
            super.open(conf, context, collector);
            rand = new Random();

            String confPath = new String(_confPath);
            try {
                readVertexList(confPath);
            } catch (IOException e) {
                System.err.println("Error reading vertex list: " + e);
                throw new RuntimeException("vertex list: " + e);
            }
        }
    }

    // ---------------------------------------------------------------
    // Bolt - RequestManager
    // ---------------------------------------------------------------

    public static class RequestManager extends SimpleBolt {
        HashSet<String> reqSent;
        JNILinker jni;

        // TODO add time information,etc.
        RequestManager() {
            name = "request-manager-bolt";
            reqSent = new HashSet<String>();
            jni = new JNILinker();
        }

        public void prepare(Map stormConf,
                TopologyContext context, OutputCollector collector) {
            super.prepare(stormConf, context, collector);
            if (0 != jni.connect(memcInfo)) {
                throw new RuntimeException(
                        "Error: jni.connect(" + memcInfo + ")");
            }
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

        private void handleVertex(Tuple tuple) {
            String reqID = tuple.getString(0);
            String vertex = tuple.getString(1);
            if (!reqSent.add(reqID)) {
                throw new RuntimeException("invalid reqID: "
                        + reqID + " already inprogress");
            }
            Values values = new Values(reqID, vertex);
            c.emit(Labels.Stream.vertices, values);
        }

        private void handleCompleted(Tuple tuple) {
            String reqID = tuple.getString(0);
            String image = tuple.getString(1);
            if (!reqSent.remove(reqID))
                throw new RuntimeException("invalid completion: "
                        + reqID + " not associated");
            System.out.println("Completed " +reqID + image);
            String path = new String("/tmp/") + image;
            jni.writeImage(image, path);
        }

        @Override
        public void execute(Tuple tuple) {
            String reqID = tuple.getString(0);
            if (streamIsCompleted(tuple))
                handleCompleted(tuple);
            else if (streamIsVertex(tuple))
                handleVertex(tuple);
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
            name = "neighbors-bolt";
            jni = new JNILinker();
        }

        public void prepare(Map stormConf,
                TopologyContext context, OutputCollector collector) {
            super.prepare(stormConf, context, collector);
            if (0 != jni.connect(memcInfo)) {
                throw new RuntimeException(
                        "Error: jni.connect(" + memcInfo + ")");
            }
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
            int ret = jni.neighbors(vertex, others);
            if (ret != 0)
                throw new RuntimeException(
                        "Error jni.neighbors");

            int count = others.size();

            values = new Values(reqID, Integer.toString(count));
            c.emit(Labels.Stream.counts, values);
            Logger.println(log, "emit count ("
                    + reqID + ", " + Integer.toString(10) + ")");

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
            name = "uniquer-bolt";
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
    // ---------------------------------------------------------------

    // accepts vertex and count stream
    // emits count -> montage bolt (not feature)
    // emits images -> feature bolt
    public static class ImagesOf extends SimpleBolt {
        private JNILinker jni;
        class TrackingInfo {
            final int maxPerRequest = 50;
            long vertexRecv, vertexWait;
            long countRecv, imagesSent;
            HashSet<String> imageIDs;
            public boolean done() {
                boolean nonZero = (vertexRecv > 0) && (vertexWait > 0)
                    && (countRecv > 0);
                boolean reachedLimit = (vertexRecv >= vertexWait)
                    || (vertexRecv >= maxPerRequest);
                return (nonZero && reachedLimit);
            }
        }
        private TrackingInfo tracking(String reqID) {
            TrackingInfo info = keys.get(reqID);
            if (info == null) {
                Logger.println(log, "New tracking info for " + reqID);
                info = new TrackingInfo();
                keys.put(reqID, info);
                info.imageIDs = new HashSet<String>();
            }
            return info;
        }


        // hashed by request ID
        HashMap<String, TrackingInfo> keys;
        String endingBoltID;

        ImagesOf() {
            jni = new JNILinker();
            name = "images-of-bolt";
            keys = new HashMap<String, TrackingInfo>();
        }
        public void prepare(Map stormConf,
                TopologyContext context, OutputCollector collector) {
            super.prepare(stormConf, context, collector);
            if (0 != jni.connect(memcInfo)) {
                throw new RuntimeException(
                        "Error: jni.connect(" + memcInfo + ")");
            }
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
            String vertex = tuple.getString(1);
            if (info.done()) {
                Logger.println(log, "Got all for " + reqID);
                // should go to MontageBolt
                String sent = Long.toString(info.imagesSent);
                Values values = new Values(reqID, sent);
                c.emit(Labels.Stream.counts, values);
                keys.remove(reqID);
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
            String vertex = tuple.getString(1);
            info.vertexRecv++;
            Logger.println(log, reqID + " vertexRecv " +
                    info.vertexRecv);

            HashSet<String> imageKeys = new HashSet<String>();
            if (0 != jni.imagesOf(vertex, imageKeys))
                throw new RuntimeException("jni imagesof " + vertex);

            for (String imageID : imageKeys) {
                // filter out duplicates for this request ID
                if (info.imageIDs.add(vertex)) {
                    c.emit(Labels.Stream.images,
                            new Values(reqID, imageID));
                    info.imagesSent++;
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
            jni = new JNILinker();
            name = "feature-bolt";
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer d) {
            Fields fields = new Fields(Labels.reqID, Labels.image);
            d.declareStream(Labels.Stream.images, fields);
        }

        public void prepare(Map stormConf,
                TopologyContext context, OutputCollector collector) {
            super.prepare(stormConf, context, collector);
            if (0 != jni.connect(memcInfo)) {
                throw new RuntimeException(
                        "Error: jni.connect(" + memcInfo + ")");
            }
        }

        @Override
        public void execute(Tuple tuple) {
            String reqID = tuple.getString(0);
            String imageID = tuple.getString(1);
            if (0 != jni.feature(imageID))
                throw new RuntimeException("Error jni.feature");
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
            HashSet<String> imageIDs;
            public boolean done() {
                boolean nonZero = (imageRecv > 0) && (imageWait > 0)
                    && (countRecv > 0);
                return (nonZero && (imageRecv >= imageWait));
            }
        }
        private TrackingInfo tracking(String reqID) {
            TrackingInfo info = keys.get(reqID);
            if (info == null) {
                Logger.println(log, "New tracking info for " + reqID);
                info = new TrackingInfo();
                info.imageIDs = new HashSet<String>();
                keys.put(reqID, info);
            }
            return info;
        }

        // hashed by request ID
        HashMap<String, TrackingInfo> keys;
        String endingBoltID;

        Montage() {
            jni = new JNILinker();
            name = "montage-bolt";
            keys = new HashMap<String, TrackingInfo>();
        }
        public void prepare(Map stormConf,
                TopologyContext context, OutputCollector collector) {
            super.prepare(stormConf, context, collector);
            if (0 != jni.connect(memcInfo)) {
                throw new RuntimeException(
                        "Error: jni.connect(" + memcInfo + ")");
            }
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
            String imageID = tuple.getString(1);
            info.imageRecv++;
            Logger.println(log, reqID + " imageRecv " +
                    info.imageRecv);

            info.imageIDs.add(imageID);
            Logger.println(log, reqID + " set has "
                    + Integer.toString(info.imageIDs.size())
                    + " images");

            if (info.done()) {
                Logger.println(log, "Got all "
                        + Integer.toString(info.imageIDs.size())
                        + " images for " + reqID);
                StringBuffer montage_key = new StringBuffer();
                if (0 != jni.montage(info.imageIDs, montage_key))
                    throw new RuntimeException("jni montage");
                Values v = new Values(reqID, montage_key.toString());
                c.emit(Labels.Stream.completed, v);
                keys.remove(reqID);
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

    public static void doRegular(String[] args, Config stormConf)
        throws Exception {
        System.out.println("Using regular Storm topology");

        int numNeighbor = 1;
        String neighBase = "neighbor", name = "";
        Fields sortByID = new Fields(Labels.reqID);

        TopologyBuilder builder = new TopologyBuilder();
        SpoutDeclarer gen = builder.setSpout("gen", new Generator(), 1);

        BoltDeclarer reqMgr = builder.setBolt("mgr",
                new RequestManager(), 1);
        reqMgr.shuffleGrouping("gen", Labels.Stream.vertices);

        name = neighBase + (numNeighbor - 1);
        BoltDeclarer uniq = builder.setBolt("uniq",
                new Uniquer(name), 1);

        name = neighBase + 0;
        builder.setBolt(name, new Neighbors(), 1)
            .shuffleGrouping("mgr", Labels.Stream.vertices);
        uniq.fieldsGrouping(name, Labels.Stream.counts, sortByID);
        uniq.fieldsGrouping(name, Labels.Stream.vertices, sortByID);
        for (int n = 1; n < numNeighbor; n++) {
            String prior = neighBase + (n - 1);
            name = neighBase + n;
            builder.setBolt(name, new Neighbors(), 1)
                .fieldsGrouping(prior, Labels.Stream.vertices, sortByID);
            uniq.fieldsGrouping(name, Labels.Stream.counts, sortByID);
            uniq.fieldsGrouping(name, Labels.Stream.vertices, sortByID);
        }

        BoltDeclarer imagesof = builder.setBolt("imagesOf",
                new ImagesOf(), 1);
        imagesof.fieldsGrouping("uniq",
                Labels.Stream.vertices, sortByID);
        imagesof.fieldsGrouping("uniq",
                Labels.Stream.counts, sortByID);

        BoltDeclarer feature = builder.setBolt("feature",
                new Feature(), 1);
        feature.shuffleGrouping("imagesOf", Labels.Stream.images);

        BoltDeclarer montage = builder.setBolt("montage",
                new Montage(), 1);
        montage.fieldsGrouping("feature",
                Labels.Stream.images, sortByID);
        montage.fieldsGrouping("imagesOf",
                Labels.Stream.counts, sortByID);

        reqMgr.fieldsGrouping("montage",
                Labels.Stream.completed, sortByID);

        StormTopology t = builder.createTopology();
        if (args.length > 1) {
            stormConf.setNumWorkers(1);
            StormSubmitter.submitTopology(args[0], stormConf, t);
        } else {
            stormConf.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("search", stormConf, t);
            Thread.sleep(60 * 1000); // ms
            cluster.shutdown();
        }
    }

    // ---------------------------------------------------------------
    // main
    // ---------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Config stormConf = new Config();
        stormConf.setDebug(true);

        stormConf.setMaxTaskParallelism(8);

        doRegular(args, stormConf);
    }
}

