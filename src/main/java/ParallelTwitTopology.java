import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class ParallelTwitTopology extends ConfigurableTopology {

    public static void main(String[] args) {
        ConfigurableTopology.start(new ParallelTwitTopology(), args);
    }

    @Override
    protected int run(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("tweets", new TwitSpout());
        builder.setBolt("hashTags", new GetHashTagBolt())
                .shuffleGrouping("tweets");
        builder.setBolt("counts", new LossyCountingBolt(), 4)
                .setNumTasks(8).fieldsGrouping("hashTags", new Fields("hashTag"));
        builder.setBolt("log", new LogBolt(args[1]))
                .globalGrouping("counts");
        conf.setDebug(true);

        String topologyName = "Parallel Count";

        if (args != null && args.length > 0) {
            topologyName = args[0];
        }

        return submit(topologyName, conf, builder);

    }
}
