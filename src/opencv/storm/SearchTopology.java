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

    public static class Long2 {
        public static long toLong(String val) {
            return Long.decode(val).longValue();
        }
    }

    public static class Logger {
        public PrintWriter out;
        public String logPath;

        public String uniqName() {
            return ManagementFactory.getRuntimeMXBean().getName();
        }
        Logger(String name) {
            logPath = new String("/tmp/" + name + "_" + uniqName() + "_.log");
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
        public static void println(Logger l, String s) {
            if ((l != null) && !l.error())
                l.out.println(s);
        }

        public static void close(Logger l) {
            if (l != null)
                l.out.close();
        }
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
        private int count;
        private Random rand;
        private boolean submittedOne;

        Generator() {
            outputFields = new Fields("id", "vertex");
            name = "generator-spout";
            submittedOne = false;
        }

        // TODO use a thread
        @Override
        public void nextTuple() {
            if (submittedOne) {
                try { Thread.sleep(1 * 1000); }
                catch (InterruptedException e) { }
                return;
            }

            String id = new String(
                    Long.toString(
                        Math.abs(rand.nextLong())));
            Logger.println(log, "nextTuple " + id);
            c.emit(new Values(id, "vertex-" + id));
            submittedOne = true;
        }

        public void open(Map conf, TopologyContext context,
                SpoutOutputCollector collector) {
            super.open(conf, context, collector);
            rand = new Random();
            Logger.println(log, "Generator:open()");
        }
    }

    public static class Spitter extends SimpleBolt {
        private Random rand;

        Spitter() { 
            outputFields = new Fields("id", "vertex-or-count", "null-or-count");
            name = "spitter-bolt";
        }

        public void prepare(Map stormConf,
                TopologyContext context,
                OutputCollector collector) {
            super.prepare(stormConf, context, collector);
        }

        // TODO count per unique request ID
        @Override
        public void execute(Tuple tuple) {
            int count = 0;
            for (int r = 0; r < 10; r++) {
            for (int i = 0; i < 100; i++) {
                String val = new String(tuple.getString(1)
                        + "_haxx-" + Integer.toString(i));
                c.emit(new Values(tuple.getString(0), val, "n/a"));
                Logger.println(log, tuple.getString(1) + " execute "
                        + tuple.getString(1) + " emit " + val );
                count++;
            }
            }
            c.emit(new Values(tuple.getString(0),
                        Uniquer.fieldExpected, Integer.toString(count)));
            c.ack(tuple);
        }
    }

    // use with .fieldsGrouping() to see all tuples from same request
    public static class Uniquer extends SimpleBolt {
        private HashSet<String> keys;
        private long needed, received;
        private String id;
        public static final String fieldExpected = "__uniquer_expected";
        Uniquer() {
            outputFields = new Fields("id", "count");
            name = "uniquer-bolt";
            keys = new HashSet();
            needed = received = 0;
        }
        @Override
        public void execute(Tuple tuple) {
            if (0 == tuple.getString(1).compareTo(fieldExpected)) {
                needed = Long2.toLong(tuple.getString(2));
                Logger.println(log, "Expecting: " + tuple.getString(2));
            } else {
                received++;
                keys.add(tuple.getString(1));
                Logger.println(log, "execute " + tuple.getString(1));
                id = new String(tuple.getString(0));
            }
            if ((needed > 0) && (received == needed)) {
                Logger.println(log, "Got all expected tuples; emitting "
                        + Integer.toString(keys.size()));
                c.emit(new Values(id, Integer.toString(keys.size())));
            }
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
            declarer.declare(new Fields("id", "vertex"));
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
            declarer.declare(new Fields("id", "vertex"));
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
            declarer.declare(new Fields("id", "vertex"));
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
            declarer.declare(new Fields("id", "vertex"));
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
            declarer.declare(new Fields("id", "vertex"));
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
            declarer.declare(new Fields("id", "image"));
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
            declarer.declare(new Fields("id", "result"));
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
            declarer.declare(new Fields("id", "all-keys"));
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
            declarer.declare(new Fields("id", "montage-key"));
        }
    }

    // -------------------------------------------------------------------------
    // Topologies
    // -------------------------------------------------------------------------

    public static void doRegular(String[] args, Config stormConf)
        throws Exception {
        System.out.println("Using regular Storm topology");

        TopologyBuilder builder = new TopologyBuilder();
//        builder.setSpout("spout", new GraphSpout(), 1);
//        builder.setBolt("user", new UserBolt(), 1)
//            .shuffleGrouping("spout");
//        builder.setBolt("feature", new FeatureBolt(), 1)
//            .shuffleGrouping("user", "toFeature");
//        builder.setBolt("montage", new MontageBolt(), 1)
//            .fieldsGrouping("feature", new Fields("requestID"));
//
//        builder.setBolt("reqstat", new ReqStatBolt(), 1)
//            .fieldsGrouping("user", "toStats", new Fields("requestID"));

        builder.setSpout("generator", new Generator(), 1);
        builder.setBolt("spitter1", new Spitter(), 1)
            .shuffleGrouping("generator");
        builder.setBolt("uniquer1", new KeyedFairBolt(new Uniquer()), 1)
            .fieldsGrouping("spitter1", new Fields("id"));

        if (args.length > 1) {
            stormConf.setNumWorkers(1);
            StormSubmitter.submitTopology(args[0], stormConf, builder.createTopology());
        } else {
            stormConf.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("search", stormConf, builder.createTopology());
            Thread.sleep(20 * 1000); // ms
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
            //.fieldsGrouping(new Fields("id", "vertex"));
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
        readVertexList(confPath);

        Config stormConf = new Config();
        stormConf.setDebug(true);

        stormConf.setMaxTaskParallelism(8);

        doRegular(args, stormConf);
        //doDRPC(stormConf);
        //doTrident(stormConf);
    }
}

