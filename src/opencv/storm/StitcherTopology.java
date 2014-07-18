/**
 * StitcherTopology.java
 */

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.spout.ShellSpout;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.ShellBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.coordination.IBatchBolt;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.lang.Integer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

// Helpful Notes.
//
// - ShellBolt constructor basically acts as a call to system(). See
// StitcherTopology.h for the command line flags to use.
//
// - Each bolt/spout class in java is just a proxy for the C++ class equivalent

public class StitcherTopology {

    public static class GraphSpout
            extends ShellSpout implements IRichSpout
        {
            public GraphSpout()
            {
                super("stormstitcher", "--spout");
            }

            @Override
            public Map<String, Object> getComponentConfiguration()
            {
                return null;
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer)
            {
                // nodeID == graph node
                declarer.declare(new Fields("requestID", "userID"));
            }
        }

    public static class UserBolt
            extends ShellBolt implements IRichBolt
        {
            public UserBolt()
            {
                super("stormstitcher", "--bolt=user");
            }

            @Override
            public Map<String, Object> getComponentConfiguration()
            {
                return null;
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer)
            {
                declarer.declareStream("toFeature", // to FeatureBolt
                        new Fields("requestID", "imageID"));
                declarer.declareStream("toStats", // to ReqStatBolt
                        new Fields("requestID", "userID", "numImages"));
            }
        }

    public static class FeatureBolt
            extends ShellBolt implements IRichBolt
        {
            public FeatureBolt()
            {
                super("stormstitcher", "--bolt=feature");
            }

            @Override
            public Map<String, Object> getComponentConfiguration()
            {
                return null;
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer)
            {
                declarer.declare(new Fields("requestID", "imageID"));
            }
        }

    public static class ReqStatBolt
            extends ShellBolt implements IRichBolt
        {
            public ReqStatBolt()
            {
                super("stormstitcher", "--bolt=reqstat");
            }

            @Override
            public Map<String, Object> getComponentConfiguration()
            {
                return null;
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer)
            {
                // emits nothing
            }
        }

    public static class MontageBolt
            extends ShellBolt implements IRichBolt
        {
            public MontageBolt()
            {
                super("stormstitcher", "--bolt=montage");
            }

            @Override
            public Map<String, Object> getComponentConfiguration()
            {
                return null;
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer)
            {
                // emits nothing
            }
        }

    public static void main(String[] args)
        throws Exception
    {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new GraphSpout(), 1);
        builder.setBolt("user", new UserBolt(), 1)
            .shuffleGrouping("spout");
        builder.setBolt("feature", new FeatureBolt(), 1)
            .shuffleGrouping("user", "toFeature");
        builder.setBolt("montage", new MontageBolt(), 1)
            .fieldsGrouping("feature", new Fields("requestID"));

        builder.setBolt("reqstat", new ReqStatBolt(), 1)
            .fieldsGrouping("user", "toStats", new Fields("requestID"));

        Config conf = new Config();
        conf.setDebug(true);

        if(args!=null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(4);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("stitcher", conf, builder.createTopology());
            Thread.sleep(30 * 1000); // ms
            cluster.shutdown();
        }
    }
}

