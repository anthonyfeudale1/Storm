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

    double epsilon;
    int width;
    double s; //threshold
    int b_current = 1;
    int numberOfElements = 0;


    public LossyCountingBolt(double _epsilon, double _s) {
        epsilon = _epsilon;
        s = _s;
        width = (int) Math.ceil(1 / epsilon);
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        startTime = System.currentTimeMillis();
    }

    @Override
    public void execute(Tuple tuple) {


        String currentHashTag = tuple.getStringByField("hashTag");
        if (counts.containsKey(currentHashTag)) {
            hashTagEntry currentHashTagEntry = counts.get(currentHashTag);
            hashTagEntry temp = new hashTagEntry(currentHashTagEntry.hashTag, currentHashTagEntry.frequency + 1, currentHashTagEntry.delta);
            counts.put(currentHashTag, temp);
        } else {
            counts.put(currentHashTag, new hashTagEntry(currentHashTag, 1, b_current - 1));
        }
        numberOfElements += 1;

        long currentTime = System.currentTimeMillis();
        if (currentTime >= startTime + 10000) {
            if (counts.size() > 0) {
                ConcurrentHashMap<String, hashTagEntry> thresholdCounts = new ConcurrentHashMap<String, hashTagEntry>();

                for (String hashTag : counts.keySet()) {
                    boolean meetsThreshold = checkMeetsThreshold(hashTag);
                    if (meetsThreshold) {
                        thresholdCounts.put(hashTag, counts.get(hashTag));
                    }
                }
                if (!thresholdCounts.isEmpty()) {
                    List<Map.Entry<String, hashTagEntry>> sortedCounts = sortByValue(thresholdCounts);
                    if (sortedCounts.size() > 100) {
                        sortedCounts = sortedCounts.subList(0, 100);
                    }
                    StringBuilder topTags = new StringBuilder("");
                    for (Map.Entry<String, hashTagEntry> entry : sortedCounts) {
                        topTags.append("#").append(entry.getKey());
                    }
                    _collector.emit(tuple, new Values(topTags, currentTime));
                    _collector.ack(tuple);
                }
            }
            reset(currentTime);
        }
        else if (numberOfElements % width == 0) {
            prune();
            b_current += 1;

        }

    }

    private void reset(long currentTime) {
        counts = new ConcurrentHashMap<String, hashTagEntry>();
        startTime = currentTime;
        int b_current = 1;
        int numberOfElements = 0;
    }

    private void prune() {
        for (String hashTag : counts.keySet()) {
            hashTagEntry he = counts.get(hashTag);
            if (he.frequency + he.delta <= b_current)
                counts.remove(hashTag);
        }
    }

    private boolean checkMeetsThreshold(String hashTag) {
        if (s == -1.0)
            return true;
        double temp = (s - epsilon) * numberOfElements;
        return counts.get(hashTag).frequency >= temp;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("topTags", "time"));
    }

    public static List<Map.Entry<String, hashTagEntry>> sortByValue(ConcurrentHashMap<String, hashTagEntry> hm) {
        // Create a list from elements of HashMap
        List<Map.Entry<String, hashTagEntry>> list =
                new LinkedList<Map.Entry<String, hashTagEntry>>(hm.entrySet());

        // Sort the list
        list.sort(new Comparator<Map.Entry<String, hashTagEntry>>() {
            public int compare(Map.Entry<String, hashTagEntry> o1,
                               Map.Entry<String, hashTagEntry> o2) {
                return o2.getValue().frequency - o1.getValue().frequency;
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
