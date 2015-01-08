import backtype.storm.*;
import backtype.storm.coordination.*;
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

// Inner classes are static as they are considered top-level and not internal to
// SearchTopology:
// http://docs.oracle.com/javase/tutorial/java/javaOO/nested.html

// Constructors should not be declared to return 'void' else the Java compiler
// doesn't recognize it as the constructor.

// Each bolt/spout class in java is just a proxy for the C++ class equivalent
public class SearchTopology {

    // -------------------------------------------------------------------------
    // Common code
    // -------------------------------------------------------------------------

    // 0: request ID 1: action 2: action-specific 3: etc
    public static class Labels {
        // TODO vertex count -> request tuple count
        // vertices may be converted to images or something else later
        public static class Stream {
            public static final String vertices = "stream_vertices";
            public static final String counts = "stream_vertexCount";
        }
        // Fields
        public static final String reqID = "field_reqID";
        public static final String vertex = "field_vertex";
        public static final String vertexCount = "field_vertexCount";
    }

    // XXX instead of maintaining as array, covnert all vertices to 0-X values
    // and just give us the max...
    public static String[] vertices;

    public static void readVertexList(String confPath) throws IOException {
        String idsPath = new String();
        BufferedReader in = new BufferedReader(new FileReader(confPath));
        while (in.ready()) {
            String line = in.readLine();
            String[] tokens = line.split(" ");
            if (0 == tokens[0].compareTo("graph"))
                if (0 == tokens[1].compareTo("idsfile"))
                    idsPath = tokens[2];
        }
        if (idsPath.length() == 0)
            throw new IllegalArgumentException("No graph idsfile entry in conf");

        // open the file and suck in all values
        in = new BufferedReader(new FileReader(idsPath));
        ArrayDeque<String> entries = new ArrayDeque<String>();
        while (in.ready())
            entries.add(in.readLine());
        vertices = entries.toArray(new String[0]);
        System.out.println(Integer.toString(vertices.length) + " vertices");
    }

    public static final String TopologyName = new String("search");

    // -------------------------------------------------------------------------
    // Regular Bolts
    // -------------------------------------------------------------------------

    // simplify my life
    public static abstract class SimpleSpout implements IRichSpout {
        Logger log;
        SpoutOutputCollector c;
        // initialize below fields in subclass' constructor
        Fields outputFields;
        String name;

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
        String name;

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

    public static class Generator extends SimpleSpout {
        private Random rand;
        private int count;

        Generator() {
            name = "generator-spout";
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
            if (count > 0)
                return;
            String reqID = Long.toString(Math.abs(rand.nextLong()));
            String vertex = Long.toString(Math.abs(rand.nextLong()));
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
        }
    }

    public static class Spitter extends SimpleBolt {
        private Random rand;

        Spitter() { 
            name = "spitter-bolt";
        }

        @Override // ignore superclass implementation
        public void declareOutputFields(OutputFieldsDeclarer d) {
            Fields fields =
                new Fields(Labels.reqID, Labels.vertex);
            d.declareStream(Labels.Stream.vertices, fields);

            fields =
                new Fields(Labels.reqID, Labels.vertexCount);
            d.declareStream(Labels.Stream.counts, fields);
        }

        private boolean streamIsVertex(Tuple tuple) {
            String stream = tuple.getSourceStreamId();
            return (stream.compareTo(Labels.Stream.vertices) == 0);
        }

        @Override
        public void execute(Tuple tuple) {
            String reqID = tuple.getString(0);
            Logger.println(log, "execute: " + reqID);
            Values values;

            if (!streamIsVertex(tuple))
                throw new RuntimeException("spitter got wrong stream");

            int count;
            for (count = 0; count < 10; count++) {
                String vertex = Integer.toString(count);
                values = new Values(reqID, vertex);
                // does this go to both the next spitter, and the unique bolt?
                c.emit(Labels.Stream.vertices, values);
                Logger.println(log, "emit vertices ("
                        + reqID + ", " + vertex + ")");
            }

            values = new Values(reqID, Integer.toString(count));
            c.emit(Labels.Stream.counts, values);
            Logger.println(log, "emit count ("
                    + reqID + ", " + Integer.toString(count) + ")");

            c.ack(tuple);
        }
    }

    // use with .fieldsGrouping() to see all tuples from same request
    public static class Uniquer extends SimpleBolt {
        class TrackingInfo {
            long received, count;
            HashSet<String> values; // vertices
            public boolean complete() {
                return ((count > 0) && (received >= count));
            }
        }

        // hashed by request ID
        HashMap<String, TrackingInfo> keys;

        Uniquer() {
            name = "uniquer-bolt";
            keys = new HashMap<String, TrackingInfo>();
        }

        @Override // ignore superclass implementation
        public void declareOutputFields(OutputFieldsDeclarer d) {
            // same as Spitter, as we only unique the entries
            Fields fields =
                new Fields(Labels.reqID, Labels.vertex);
            d.declareStream(Labels.Stream.vertices, fields);

            fields =
                new Fields(Labels.reqID, Labels.vertexCount);
            d.declareStream(Labels.Stream.counts, fields);
        }

        private void handleCount(Tuple tuple) {
            String reqID = tuple.getString(0);
            String count = tuple.getString(1);
            TrackingInfo info = keys.get(reqID);
            info.count += Long.decode(count).longValue();
            Logger.println(log, "recv count ("
                    + reqID + ", " + count + ")"
                    + " total " + Long.toString(info.count)
                    + " uniq " + Integer.toString(info.values.size()));
        }

        private void handleVertex(Tuple tuple) {
            String reqID = tuple.getString(0);
            TrackingInfo info = keys.get(reqID);
            String vertex = tuple.getString(1);
            info.values.add(vertex);
            info.received++;
            Logger.println(log, "recv vertex ("
                    + reqID + ", " + vertex + ")");
        }

        private void addTracking(String reqID) {
            TrackingInfo info = keys.get(reqID);
            if (info != null)
                return;
            info = new TrackingInfo();
            info.values = new HashSet<String>();
            keys.put(reqID, info);
        }

        private boolean streamIsCount(Tuple tuple) {
            String stream = tuple.getSourceStreamId();
            return (stream.compareTo(Labels.Stream.counts) == 0);
        }

        private boolean streamIsVertex(Tuple tuple) {
            String stream = tuple.getSourceStreamId();
            return (stream.compareTo(Labels.Stream.vertices) == 0);
        }

        @Override
        public void execute(Tuple tuple) {
            String reqID = tuple.getString(0); // first for all streams
            addTracking(reqID);
            if (streamIsCount(tuple))
                handleCount(tuple);
            else if (streamIsVertex(tuple))
                handleVertex(tuple);
            else throw new RuntimeException("unknown stream");
            c.ack(tuple);
        }
    }

    // -----------------------------------------------------

    public static class GraphSpout extends BaseRichSpout {
        //Logger log;
        @Override
        public void open(Map conf,
                TopologyContext context,
                SpoutOutputCollector collector) {
            //log = new Logger("GraphSpout");
            //log.open();
        }
        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
        @Override
        public void nextTuple() {
            // collector.emit(String streamId, tuples)
            // collector.emit(int taskId, tuples)
            // collector.emit(String streamId, int taskId, tuples)
            //collector.emit(new Values("tuple", 0));
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(Labels.reqID, Labels.vertex));
        }
    }

    public static class BFSUnix extends ShellBolt implements IRichBolt {
        public BFSUnix() {
            super("/bin/sh", "run", "--bolt=bfs");
        }
        @Override
        public void execute(Tuple tuple) {
            String msg = new String();
            msg += "Got a tuple, sending to Unix";
            System.out.println(msg);
            super.execute(tuple);
        }
        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(Labels.reqID, Labels.vertex));
        }
    }

    // -------------------------------------------------------------------------
    // DRPC Bolts
    // -------------------------------------------------------------------------

    // remove duplicate vertices
    public static class PartialUniquer extends BaseBatchBolt {
        BatchOutputCollector _collector;
        PrintWriter out;
        int inCount = 0;
        Set<String> _vertices = new HashSet<String>();
        Object _id;
        @Override
        public void prepare(Map conf, TopologyContext context,
                BatchOutputCollector c, Object id) {
            _collector = c;
            _id = id;

            String hostVal = ManagementFactory.getRuntimeMXBean().getName();
            String logPath = new String("/tmp/uniqbolt-" + hostVal);
            try {
                out = new PrintWriter(new BufferedWriter(new FileWriter(logPath)));
            } catch (IOException e) {
                System.out.println("Error: PartialUniquer could not"
                        + " open log file: " + logPath
                        + ": " + e);
                return;
            }
            String text = new String("PartialUniquer prepare() invoked");
            out.println(text);
        }
        @Override
        public void execute(Tuple tuple) {
            inCount++;
            _vertices.add(tuple.getString(1));
        }
        @Override
        public void finishBatch() {
            String text = new String();
            text += "finishBatch(): uniq " + Integer.toString(inCount)
                + " -> " + Integer.toString(_vertices.size());
            out.println(text);
            for (String vertex : _vertices)
                _collector.emit(new Values(_id, vertex));
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(Labels.reqID, Labels.vertex));
        }
    }

    public static class Repeater extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            Object id = tuple.getValue(0);
            String vertex = tuple.getString(1);
            for (int i = 0; i < 10; i++)
                collector.emit(new Values(id, vertex));
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(Labels.reqID, Labels.vertex));
        }
    }

    // connect unix bolts with other java bolts..
    // some stupid bug in storm where batch bolts cannot follow unix bolts in a
    // drpc workflow
    public static class UnixGlue extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            Object id = tuple.getValue(0);
            String vertex = tuple.getString(1);
            collector.emit(new Values(id, vertex));
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(Labels.reqID, Labels.vertex));
        }
    }

    // for each vertex, emit the IDs of images associated with it
    public static class ImageList extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            Object id = tuple.getValue(0);
            String vertex = tuple.getString(1);
            // XXX
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(Labels.reqID, "image"));
        }
    }

    // compute HOG operator; emit images that have objects in them;
    // store the hog feature set in the object store
    public static class Hog extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            Object id = tuple.getValue(0);
            String vertex = tuple.getString(1);
            // XXX
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(Labels.reqID, "result"));
        }
    }

    // convert all keys into one tuple
    // b/c a shellbolt cannot be a batch for some reason
    public static class PreMontage extends BaseBatchBolt {
        BatchOutputCollector _collector;
        List<String> keys = new LinkedList<String>();
        Object _id;
        @Override
        public void prepare(Map conf, TopologyContext context,
                BatchOutputCollector c, Object id) {
            _collector = c;
            _id = id;
        }
        @Override
        public void execute(Tuple tuple) {
            keys.add(tuple.getString(1));
        }
        @Override
        public void finishBatch() {
            String result = new String();
            for (String key : keys) // XXX huge tuple...
                result += key + ",";
            _collector.emit(new Values(_id, result));
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(Labels.reqID, "all-keys"));
        }
    }

    // give the huge tuple to unix process to merge images together
    public static class MontageUnix extends ShellBolt implements IRichBolt {
        public MontageUnix() {
            super("/bin/sh", "run", "--bolt=montage");
        }
        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(Labels.reqID, "montage-key"));
        }
    }

    // -------------------------------------------------------------------------
    // Topologies
    // -------------------------------------------------------------------------

    public static void doRegular(String[] args, Config stormConf)
        throws Exception {
        System.out.println("Using regular Storm topology");

        TopologyBuilder builder = new TopologyBuilder();

        SpoutDeclarer gen = builder.setSpout("gen", new Generator(), 1);
        BoltDeclarer uniq = builder.setBolt("uniq0", new Uniquer(), 1);

        int numSpit = 2;
        Fields sortByID = new Fields(Labels.reqID);
        String name = "spit0";
        builder.setBolt(name, new Spitter(), 1)
            .shuffleGrouping("gen", Labels.Stream.vertices);
        uniq.fieldsGrouping(name, Labels.Stream.counts, sortByID);
        uniq.fieldsGrouping(name, Labels.Stream.vertices, sortByID);
        for (int s = 1; s < numSpit; s++) {
            String prior = "spit" + Integer.toString(s-1);
            name = "spit" + Integer.toString(s);
            builder.setBolt(name, new Spitter(), 1)
                .fieldsGrouping(prior, Labels.Stream.vertices, sortByID);
            // XXX does this work? all tuples belonging to same reqID must
            // arrive at same instance of uniquer
            uniq.fieldsGrouping(name, Labels.Stream.counts, sortByID);
            uniq.fieldsGrouping(name, Labels.Stream.vertices, sortByID);
        }

        StormTopology t = builder.createTopology();
        if (args.length > 1) {
            stormConf.setNumWorkers(1);
            StormSubmitter.submitTopology(args[0], stormConf, t);
        } else {
            stormConf.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("search", stormConf, t);
            Thread.sleep(5 * 1000); // ms
            cluster.shutdown();
        }
    }

    public static void doDRPC(Config stormConf) {
        System.out.println("Using linearDRPC");
        LinearDRPCTopologyBuilder builder =
            new LinearDRPCTopologyBuilder("search");

        // XXX Ordering is important.. ? Weird bugs
        // Unix bolt -> !batch bolt -> any bolt

        builder.addBolt(new BFSUnix(), 1).shuffleGrouping();
        //builder.addBolt(new UnixGlue(), 1).shuffleGrouping();
        builder.addBolt(new PartialUniquer(), 1)
            .shuffleGrouping();
            //.fieldsGrouping(new Fields(Labels.reqID, "vertex"));
        //builder.addBolt(new ImageList(), 4).shuffleGrouping();
        //builder.addBolt(new Hog(), 1).shuffleGrouping();

        builder.addBolt(new PreMontage(), 1).shuffleGrouping();
        builder.addBolt(new MontageUnix(), 1).shuffleGrouping();

        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("search", stormConf,
                builder.createLocalTopology(drpc));

        // XXX add some tags to guide the filter

        // TODO instead of only vertex, send src-vertex and vertex, so the
        // BFS walking doesn't emit the prior vertex for walking again, or
        // each walk we put them through a uniquer?

        Random rand = new Random(0);
        for (int i = 0; i < 1; i++) {
            //int idx = rand.nextInt(vertices.length);
            int idx = 0;
            String vertex = vertices[idx];
            System.out.println("execute: " + vertex + " "
                    + "output: " + drpc.execute("search", vertex));
        }

        cluster.shutdown();
        drpc.shutdown();
    }

    public static void doTrident(Config stormConf) {
        System.out.println("Using Trident");

        TridentTopology trident = new TridentTopology();
        LocalDRPC drpc = new LocalDRPC();
        StormTopology storm;

        // XXX https://issues.apache.org/jira/browse/STORM-151
        // It seems shellbolt doesn't grok with Trident... need to implement
        // Function which requires another prototype for execute() that
        // shellbolt doesn't implement. Also, most examples online that use
        // Trident are already out-of-date..

        Stream s = trident.newDRPCStream("search", drpc)
            ; //.each(new Fields("args"), new Spit(), new Fields("word"));
        storm = trident.build();

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("search", stormConf, storm);

        String input = new String("hello");
        System.out.println("execute: " + input + " "
                + "output: " + drpc.execute("search", input));

        // for remote RPC connections
        // DRPCClient client = new DRPCClient("localhost", 3772, 9000);

        cluster.shutdown();
        drpc.shutdown();
    }

    // -------------------------------------------------------------------------
    // main
    // -------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        if (args.length < 1)
            throw new IllegalArgumentException("Specify path to conf");

        String s = new String("abc");

        String confPath = args[0];
        //readVertexList(confPath);

        Config stormConf = new Config();
        stormConf.setDebug(true);

        stormConf.setMaxTaskParallelism(8);

        doRegular(args, stormConf);
        //doDRPC(stormConf);
        //doTrident(stormConf);
    }
}

