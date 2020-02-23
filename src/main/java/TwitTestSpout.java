import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.*;

import java.util.Map;
import java.util.Random;

public class TwitTestSpout extends BaseRichSpout {
    SpoutOutputCollector collector;
    int inner_count = 1;
    int outer_count = 1;
    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }



    @Override
    public void nextTuple() {
        Utils.sleep(1);

        String mock = "{\"entities\": {" +
                "    \"hashtags\": [ {" +
                "      \"indices\": [" +
                "        32," +
                "        38" +
                "      ],\n" +
                "      \"text\": \"" + Integer.toString(outer_count) +  "\"" +
                "    }" +
                "    ]" +
                "}}";
        String text = new JSONObject().put("text", Integer.toString(outer_count)).toString();
        String hashTags = new JSONObject().put("hashtags", new JSONArray().put(0, text).toString()).toString();
        String entities = new JSONObject().put("entities", hashTags).toString();
        System.out.println(entities);
        try {
            Status newStatus = TwitterObjectFactory.createStatus(mock);

        collector.emit(new Values(newStatus));
        if (outer_count == inner_count && outer_count == 120) {
            inner_count = 1;
            outer_count = 1;
        }
        else if (inner_count == outer_count) {
            inner_count = 1;
            outer_count += 1;
        }
        else {
            inner_count += 1;
        }
        } catch (TwitterException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweets"));
    }
}
