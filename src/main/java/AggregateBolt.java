import javafx.util.Pair;
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

public class AggregateBolt extends BaseRichBolt {
    OutputCollector _collector;

    static long startTime;
    ConcurrentHashMap<String, LinkedList<Pair<String, Integer>>> topFromSource = new ConcurrentHashMap<String, LinkedList<Pair<String, Integer>>>();


    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        startTime = System.currentTimeMillis();
    }

    @Override
    public void execute(Tuple tuple) {


        String topTagsString = tuple.getStringByField("topTags");
        LinkedList<Pair<String, Integer>> topTags = processTopTags(topTagsString);
        topFromSource.put(tuple.getSourceComponent(), topTags);



        long currentTime = System.currentTimeMillis();
        if (currentTime >= startTime + 10000) {
            startTime = currentTime;
            ConcurrentHashMap<String, Integer> counts = new ConcurrentHashMap<String, Integer>();
            for (String key : topFromSource.keySet()) {
                LinkedList<Pair<String, Integer>> keyTopTags = topFromSource.get(key);
                for(Pair<String, Integer> currentHashTag : keyTopTags) {
                    if (counts.containsKey(currentHashTag.getKey())) {
                        int newFreq = counts.get(currentHashTag.getKey()) + currentHashTag.getValue();
                        counts.put(currentHashTag.getKey(), newFreq );
                    } else {
                        counts.put(currentHashTag.getKey(), currentHashTag.getValue() );
                    }
                }
            }

            if (! counts.isEmpty()) {
                List<Map.Entry<String, Integer>> sortedCounts = sortByValue(counts);

                if (counts.size() > 100) {
                    sortedCounts = sortedCounts.subList(0, 100);
                    ConcurrentHashMap<String, Integer> newCounts = new ConcurrentHashMap<String, Integer>();
                    for (Map.Entry<String, Integer>entry: sortedCounts) {
                        newCounts.put(entry.getKey(), entry.getValue());
                    }
                    counts = newCounts;
                }

                    StringBuilder topAggregateTags = new StringBuilder("");
                    for (Map.Entry<String, Integer> entry : sortedCounts) {
                        topAggregateTags.append("#").append(entry.getKey());
                    }
                    _collector.emit(tuple, new Values(topAggregateTags, currentTime));
                    _collector.ack(tuple);

            }
        }

    }

    private LinkedList<Pair<String, Integer>> processTopTags(String topTagsString) {
        String[] hashTags = topTagsString.split(" ");

        LinkedList<Pair<String, Integer>> list =  new LinkedList<Pair<String, Integer>>();
        for (String hashTag : hashTags) {
            String[] temp = hashTag.split(",");
            list.add(new Pair<String, Integer>(temp[0], Integer.parseInt(temp[1])));
        }
        return list;
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
                return o2.getValue() - o1.getValue();
            }
        });
        return list;
    }

}
