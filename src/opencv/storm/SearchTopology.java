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

// Each bolt/spout class in java is just a proxy for the C++ class equivalent
public class SearchTopology {

    // walk the graph, emit the neighbor vertices
    public static class BFS extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            Object id = tuple.getValue(0);
            String vertex = tuple.getString(1);
            // XXX walk the graph
            for (int i = 0; i < 2; i++) {
                collector.emit(new Values(id, vertex + "+0"));
                collector.emit(new Values(id, vertex + "+1"));
                collector.emit(new Values(id, vertex + "+2"));
                collector.emit(new Values(id, vertex + "+3"));
                collector.emit(new Values(id, vertex + "+4"));
            }
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "vertex"));
        }
    }

    // remove duplicate vertices
    public static class PartialUniquer extends BaseBatchBolt {
        BatchOutputCollector _collector;
        Set<String> _vertices = new HashSet<String>();
        Object _id;
        @Override
        public void prepare(Map conf, TopologyContext context,
                BatchOutputCollector c, Object id) {
            _collector = c;
            _id = id;
        }
        @Override
        public void execute(Tuple tuple) {
            _vertices.add(tuple.getString(1));
        }
        @Override
        public void finishBatch() {
            for (String vertex : _vertices)
                _collector.emit(new Values(_id, vertex));
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "vertex"));
        }
    }

    // don't forward vertices that don't match some filter
    public static class Filter extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            Object id = tuple.getValue(0);
            String vertex = tuple.getString(1);
            if (vertex.contains("+0"))
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

    // smush the images together into one, return the key of the image in the
    // object store
    public static class Montage extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            Object id = tuple.getValue(0);
            String vertex = tuple.getString(1);
            collector.emit(new Values(id, "montagekeynigga"));
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "result"));
        }
    }

    public static LinearDRPCTopologyBuilder construct() {
        LinearDRPCTopologyBuilder builder =
            new LinearDRPCTopologyBuilder("search");

        builder.addBolt(new BFS(), 2);
        builder.addBolt(new BFS(), 2).shuffleGrouping();
        //builder.addBolt(new BFS(), 4).shuffleGrouping();
        //builder.addBolt(new BFS(), 4).shuffleGrouping();
        //builder.addBolt(new BFS(), 4).shuffleGrouping();
        builder.addBolt(new PartialUniquer(), 4).fieldsGrouping(new Fields("id", "vertex"));
        builder.addBolt(new Filter(), 3).shuffleGrouping();
        //builder.addBolt(new ImageList(), 4).shuffleGrouping();
        //builder.addBolt(new Hog(), 1).shuffleGrouping();

        return builder;
    }

    public static void main(String[] args) throws Exception {
        LinearDRPCTopologyBuilder builder = construct();

        Config conf = new Config();
        conf.setDebug(true);

        conf.setMaxTaskParallelism(1);
        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("search-drpc", conf,
                builder.createLocalTopology(drpc));

        // XXX add some tags to guide the filter
        String vertex = new String("1337");
        System.out.println("output: " + drpc.execute("search", vertex));
        // XXX have some loop that generates queries

        cluster.shutdown();
        drpc.shutdown();
    }
}

