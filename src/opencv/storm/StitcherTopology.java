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
import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.IllegalArgumentException;
import java.io.FileNotFoundException;
import java.io.IOException;

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
                super("/bin/sh", "run", "--spout");
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
                super("/bin/sh", "run", "--bolt=user");
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
                super("/bin/sh", "run", "--bolt=feature");
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
                super("/bin/sh", "run", "--bolt=reqstat");
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
                super("/bin/sh", "run", "--bolt=montage");
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

    public static class StitcherConfig
    {
        public StitcherConfig() { }

        public int spoutTasks;
        public int featureTasks;
        public int userTasks;
        public int montageTasks;
        public int reqstatTasks;

        public int maxTaskParallel;
        public int numWorkers;
        public int localSleep;

        private void parseLine(String line)
            throws IllegalArgumentException
        {
            String[] cols = line.split(" ");

            if (line.charAt(0) == '#')
                return;
            if (!cols[0].equals("storm"))
                return;
            if (cols.length != 3)
                throw new IllegalArgumentException(
                        "Invalid columns: " + line);

            if (cols[1].equals("spout"))
                spoutTasks = Integer.parseInt(cols[2]);
            else if (cols[1].equals("feature"))
                featureTasks = Integer.parseInt(cols[2]);
            else if (cols[1].equals("user"))
                userTasks = Integer.parseInt(cols[2]);
            else if (cols[1].equals("montage"))
                montageTasks = Integer.parseInt(cols[2]);
            else if (cols[1].equals("reqstat"))
                reqstatTasks = Integer.parseInt(cols[2]);
            else if (cols[1].equals("maxparallel"))
                maxTaskParallel = Integer.parseInt(cols[2]);
            else if (cols[1].equals("localsleep"))
                localSleep = Integer.parseInt(cols[2]);
            else if (cols[1].equals("workers"))
                numWorkers = Integer.parseInt(cols[2]);
            else
                throw new IllegalArgumentException(
                        "Invalid identifier for storm: " + cols[1]);
        }

        public void readConfig(String path)
            throws IllegalArgumentException, FileNotFoundException, IOException
        {
            BufferedReader r;
            String line;

            r = new BufferedReader(new FileReader(path));
            line = r.readLine();
            while (null != line) {
                parseLine(line);
                line = r.readLine();
            }

            r.close();
        }

    }

    public static void main(String[] args)
        throws Exception
    {
        if (args.length < 1)
            throw new IllegalArgumentException("Specify path to config file");

        StitcherConfig sc = new StitcherConfig();
        sc.readConfig(args[0]);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new GraphSpout(), sc.spoutTasks);
        builder.setBolt("user", new UserBolt(), sc.userTasks)
            .shuffleGrouping("spout");
        builder.setBolt("feature", new FeatureBolt(), sc.featureTasks)
            .shuffleGrouping("user", "toFeature");
        builder.setBolt("montage", new MontageBolt(), sc.montageTasks)
            .fieldsGrouping("feature", new Fields("requestID"));

        builder.setBolt("reqstat", new ReqStatBolt(), sc.reqstatTasks)
            .fieldsGrouping("user", "toStats", new Fields("requestID"));

        Config conf = new Config();
        conf.setDebug(true);

        if (args.length > 1) {
            conf.setNumWorkers(sc.numWorkers);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(sc.maxTaskParallel);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("stitcher", conf, builder.createTopology());
            Thread.sleep(sc.localSleep * 1000); // ms
            cluster.shutdown();
        }
    }
}

