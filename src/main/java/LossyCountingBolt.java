import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LossyCountingBolt extends BaseRichBolt {
    OutputCollector _collector;
    private ConcurrentHashMap<String, hashTagEntry> counts = new ConcurrentHashMap<String, hashTagEntry>();
    long startTime;

    double epsilon = 0.002;
    int width = (int) Math.ceil(1 / epsilon);
    double threshold;
    int currentBucket = 1;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        int b_current = 0;
        String hashTag = tuple.getStringByField("hashTag");
        if (counts.containsKey(hashTag)) {
            counts.put(hashTag, new hashTagEntry()counts.get(hashTag) + 1);
        } else {
            counts.put(hashTag, new hashTagEntry(hashTag, 1, b_current-1));
        }
        long currentTime = System.currentTimeMillis();
        if (currentTime >= startTime + 10000) {
            if (counts.size() > 0) {
                List<Map.Entry<String, Integer>> sortedCounts = sortByValue(counts);
                if (sortedCounts.size() > 100) {
                    sortedCounts = sortedCounts.subList(0, 100);
                }
                StringBuilder topTags = new StringBuilder("");
                for (Map.Entry<String, Integer> entry : sortedCounts) {
                    topTags.append("#").append(entry.getKey());
                }
                _collector.emit(tuple, new Values(topTags, currentTime));
                _collector.ack(tuple);
            }
            counts = new ConcurrentHashMap<String, Integer>();

            startTime = currentTime;
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("topTags", "time"));
    }

    public static List<Map.Entry<String, Integer>> sortByValue(ConcurrentHashMap<String, Integer> hm) {
        // Create a list from elements of HashMap
        List<Map.Entry<String, Integer>> list =
                new LinkedList<Map.Entry<String, Integer>>(hm.entrySet());

        // Sort the list
        list.sort(new Comparator<Map.Entry<String, Integer>>() {
            public int compare(Map.Entry<String, Integer> o1,
                               Map.Entry<String, Integer> o2) {
                return (o1.getValue()).compareTo(o2.getValue());
            }
        });


        return list;
    }

}

class hashTagEntry {
    public String hashTag;
    public int frequency;
    public double delta;

    public hashTagEntry(String _hashTag, int _frequency, double _delta) {
        hashTag = _hashTag;
        frequency = _frequency;
        delta = _delta;
    }
}
