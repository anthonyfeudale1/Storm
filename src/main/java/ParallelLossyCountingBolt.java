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

public class ParallelLossyCountingBolt extends BaseRichBolt {
    OutputCollector _collector;
    private ConcurrentHashMap<String, HashTagEntry> counts = new ConcurrentHashMap<String, HashTagEntry>();
    static long startTime;

    double epsilon;
    int width;
    double s; //threshold
    int b_current = 1;
    int numberOfElements = 0;


    public ParallelLossyCountingBolt(double _epsilon, double _s) {
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

        numberOfElements += 1;
        String currentHashTag = tuple.getStringByField("hashTag");
        if (counts.containsKey(currentHashTag)) {
            HashTagEntry currentHashTagEntry = counts.get(currentHashTag);
            HashTagEntry temp = new HashTagEntry(currentHashTagEntry.hashTag, currentHashTagEntry.frequency + 1, currentHashTagEntry.delta);
            counts.put(currentHashTag, temp);
        } else {
            counts.put(currentHashTag, new HashTagEntry(currentHashTag, 1, b_current - 1));
        }

        long currentTime = System.currentTimeMillis();
        if (currentTime >= startTime + 10000) {
            if (counts.size() > 0) {
                ConcurrentHashMap<String, HashTagEntry> thresholdCounts = new ConcurrentHashMap<String, HashTagEntry>();

                for (String hashTag : counts.keySet()) {
                    boolean meetsThreshold = checkMeetsThreshold(hashTag);
                    if (meetsThreshold) {
                        thresholdCounts.put(hashTag, counts.get(hashTag));
                    }
                }
                if (!thresholdCounts.isEmpty()) {
                    List<Map.Entry<String, HashTagEntry>> sortedCounts = sortByValue(thresholdCounts);
                    if (sortedCounts.size() > 100) {
                        sortedCounts = sortedCounts.subList(0, 100);
                    }
                    StringBuilder topTags = new StringBuilder("");
                    for (Map.Entry<String, HashTagEntry> entry : sortedCounts) {
                        topTags.append(entry.getKey()).append(",").append(entry.getValue().frequency).append(" ");
                    }
                    _collector.emit(tuple, new Values(topTags.toString().trim()));
                    _collector.ack(tuple);
                }
            }
            startTime = currentTime;
        }
        else if (numberOfElements % width == 0) {
            prune();
            b_current += 1;

        }

    }

    private void prune() {
        for (String hashTag : counts.keySet()) {
            HashTagEntry he = counts.get(hashTag);
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
        declarer.declare(new Fields("topTags"));
    }

    public static List<Map.Entry<String, HashTagEntry>> sortByValue(ConcurrentHashMap<String, HashTagEntry> hm) {
        // Create a list from elements of HashMap
        List<Map.Entry<String, HashTagEntry>> list =
                new LinkedList<Map.Entry<String, HashTagEntry>>(hm.entrySet());

        // Sort the list
        list.sort(new Comparator<Map.Entry<String, HashTagEntry>>() {
            public int compare(Map.Entry<String, HashTagEntry> o1,
                               Map.Entry<String, HashTagEntry> o2) {
                return o2.getValue().frequency - o1.getValue().frequency;
            }
        });
        return list;
    }

}
