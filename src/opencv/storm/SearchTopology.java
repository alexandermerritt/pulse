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
            if (tokens[0].equals("graph"))
                if (tokens[1].equals("idsfile"))
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

    // TODO
    //public static class ReqMgr extends SimpleBolt { }

    public static class Neighbors extends SimpleBolt {
        private Random rand;

        Neighbors() { 
            name = "neighbors-bolt";
        }

        @Override // ignore superclass implementation
        public void declareOutputFields(OutputFieldsDeclarer d) {
            Fields fields = new Fields(Labels.reqID, Labels.vertex);
            d.declareStream(Labels.Stream.vertices, fields);
            fields = new Fields(Labels.reqID, Labels.vertexCount);
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

            values = new Values(reqID, Integer.toString(10));
            c.emit(Labels.Stream.counts, values);
            Logger.println(log, "emit count ("
                    + reqID + ", " + Integer.toString(10) + ")");

            int count;
            for (count = 0; count < 10; count++) {
                String vertex = Integer.toString(count);
                values = new Values(reqID, vertex);
                // does this go to both the next neighbors, and the unique bolt?
                c.emit(Labels.Stream.vertices, values);
            }

            c.ack(tuple);
        }
    }

    // use with .fieldsGrouping() to see all tuples from same request
    public static class Uniquer extends SimpleBolt {
        class TrackingInfo {
            long vertexRecv, vertexWait;
            long countRecv, countWait;
            HashSet<String> values; // TODO consider bloomfilter instead
            public boolean done() {
                boolean nonZero = (vertexRecv > 0) && (vertexWait > 0)
                    && (countRecv > 0) && (countWait > 0);
                return (nonZero && (vertexRecv >= vertexWait)
                        && (countRecv >= countWait));
            }
        }

        // hashed by request ID
        HashMap<String, TrackingInfo> keys;
        String lastID;

        private Uniquer() { }
        Uniquer(String lastID) {
            name = "uniquer-bolt";
            keys = new HashMap<String, TrackingInfo>();
            this.lastID = lastID;
        }

        @Override // ignore superclass implementation
        public void declareOutputFields(OutputFieldsDeclarer d) {
            Fields fields = new Fields(Labels.reqID, Labels.vertex);
            d.declareStream(Labels.Stream.vertices, fields);
            fields = new Fields(Labels.reqID, Labels.vertexCount);
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
            if (!origin.equals(lastID)) {
                info.countWait += count;
                Logger.println(log, reqID
                        + " lastID: " + lastID + " this: " + origin
                        + " countWait " + info.countWait);
            }
            info.vertexWait += count;
            Logger.println(log, reqID + " vertexWait " + info.vertexWait);
        }

        private void handleVertex(Tuple tuple) {
            String reqID = tuple.getString(0);
            TrackingInfo info = keys.get(reqID);
            String vertex = tuple.getString(1);
            info.vertexRecv++;
            Logger.println(log, reqID + " vertexRecv " + info.vertexRecv);
            if (info.values.add(vertex)) {
                Values val = new Values(reqID, vertex);
                c.emit(Labels.Stream.vertices, val);
                Logger.println(log, "emit: " + vertex);
            }
            if (info.done()) {
                Logger.println(log, "Got all for " + reqID);
                keys.remove(reqID);
            }
        }

        // TODO figure out when request is completed

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

    // -------------------------------------------------------------------------
    // Topologies
    // -------------------------------------------------------------------------

    public static void doRegular(String[] args, Config stormConf)
        throws Exception {
        System.out.println("Using regular Storm topology");

        int numNeighbor = 2;
        String neighBase = "neighbor", name = "";
        Fields sortByID = new Fields(Labels.reqID);

        TopologyBuilder builder = new TopologyBuilder();
        SpoutDeclarer gen = builder.setSpout("gen", new Generator(), 1);

        name = neighBase + (numNeighbor - 1);
        BoltDeclarer uniq = builder.setBolt("uniq0", new Uniquer(name), 1);

        name = neighBase + 0;
        builder.setBolt(name, new Neighbors(), 1)
            .shuffleGrouping("gen", Labels.Stream.vertices);
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
    }
}

