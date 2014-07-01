import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.ShellBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.lang.Integer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class StitcherTopology {

    public static class RequestSpout
            extends BaseRichSpout
        {
            private SpoutOutputCollector _collector;
            private int _msgId;

            public RequestSpout()
            {
            }

            @Override
            public void fail(Object msgId)
            {
            }

            @Override
            public void ack(Object msgId)
            {
            }

            @Override
            public void nextTuple()
            {
                _collector.emit(new Values("hello from spout"));
                try {
                    Thread.sleep(100); // ms
                } catch (InterruptedException e) {
                    // shut up compiler
                }
            }

            @Override
            public void open(Map conf, TopologyContext context,
                    SpoutOutputCollector collector)
            {
                _collector = collector;
            }

//            @Override
//            public Map<String, Object> getComponentConfiguration()
//            {
//                System.out.println("getComponentConfiguration");
//                return null;
//            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer)
            {
                declarer.declare(new Fields("word"));
            }

        }

    public static class FeatureBolt
            extends ShellBolt implements IRichBolt
        {
            private OutputCollector _collector;

            public FeatureBolt()
            {
                super("feature_bolt");
            }

            @Override
            public void prepare(Map stormConf,
                    TopologyContext context,
                    OutputCollector collector)
            {
                _collector = collector;
            }

            @Override
            public void execute(Tuple input)
            {
                String text = input.getString(0);
                System.out.println("Got: " + text);
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer)
            {
                declarer.declare(new Fields("word"));
            }

            @Override
            public Map<String, Object> getComponentConfiguration()
            {
                return null; // ???
            }
        }

    public static void main(String[] args)
        throws Exception
    {
        TopologyBuilder builder = new TopologyBuilder();
        // [spout] -> x
        builder.setSpout("spout", new RequestSpout(), 1);
        // [spout] -> [feature] -> x
        builder.setBolt("feature", new FeatureBolt(), 1)
            .shuffleGrouping("spout");

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

