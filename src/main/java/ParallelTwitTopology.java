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
        conf.setDebug(true);
        conf.setNumWorkers(4);
        builder.setSpout("tweets", new TwitSpout());
        builder.setBolt("hashTags", new GetHashTagBolt())
                .shuffleGrouping("tweets");
        builder.setBolt("counts",
                new LossyCountingBolt(Double.parseDouble(args[2]), Double.parseDouble(args[3])), 4)
                .fieldsGrouping("hashTags", new Fields("hashTag"));
        builder.setBolt("log", new LogBolt(args[1]))
                .globalGrouping("counts");


        return submit(args[0], conf, builder);

    }
}
