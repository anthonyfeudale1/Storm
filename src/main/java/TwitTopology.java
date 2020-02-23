import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;

public class TwitTopology extends ConfigurableTopology {

    public static void main(String[] args) {
        ConfigurableTopology.start(new TwitTopology(), args);
    }

    @Override
    protected int run(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("tweets", new TwitSpout());
        builder.setBolt("hashTags", new GetHashTagBolt())
                .shuffleGrouping("tweets");
        builder.setBolt("counts", new CountingBolt())
                .globalGrouping("hashTags");
        builder.setBolt("log", new LogBolt(args[1]))
                .globalGrouping("counts");
        conf.setDebug(true);

        String topologyName = "Count";

        if (args != null && args.length > 0) {
            topologyName = args[0];
        }

        return submit(topologyName, conf, builder);

    }
}
