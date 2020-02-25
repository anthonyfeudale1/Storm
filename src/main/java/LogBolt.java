import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class LogBolt extends BaseRichBolt {
    OutputCollector _collector;
    String outputPath;
    static long startTime;

    public LogBolt(String _outputPath) {
        this.outputPath = _outputPath;
        startTime = System.currentTimeMillis();
    }
    public LogBolt(boolean isParallel) {

    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String newTuple = tuple.getValueByField("time") + ": " +  tuple.getValueByField("topTags");
        System.out.println(newTuple);
        try (FileWriter fileWriter = new FileWriter(outputPath, true)) {
            BufferedWriter bw = new BufferedWriter(fileWriter);
            bw.write(newTuple);
            bw.newLine();
            bw.flush();
            _collector.emit(tuple, new Values(newTuple));
            _collector.ack(tuple);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("log"));
    }
}
