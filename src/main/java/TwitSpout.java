import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitSpout extends BaseRichSpout {
    SpoutOutputCollector collector;
    TwitterStream twitterStream;
    String consumerKey = "ZRDPGnLzr2o0G4XcnWamWgB0E";
    String consumerSecret = "MS3TJLR2SaRJVpgDIDX44VCjAhiiO4Tict5Y4UwbAwSuaw2sEe";
    String accessToken = "855445550644285440-ovquJy6Bc5WmWXgMe5URcnDD3lsF3R4";
    String accessTokenSecret = "MVQ4OiYH6pnScFvai7lnjxJjnWVANAsPdEJdHKEpqqMvV";

    LinkedBlockingQueue<Status> queue = null;

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        queue = new LinkedBlockingQueue<Status>(1000);
        twitterStream = new TwitterStreamFactory( new ConfigurationBuilder().setJSONStoreEnabled(true).build()) .getInstance();
        StatusListener listener = new StatusListener(){
            @Override
            public void onException(Exception e) { }

            @Override
            public void onStatus(Status status) {
                queue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) { }

            @Override
            public void onTrackLimitationNotice(int i) { }

            @Override
            public void onScrubGeo(long l, long l1) { }

            @Override
            public void onStallWarning(StallWarning stallWarning) { }
        };

        twitterStream.addListener(listener);
        twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
        AccessToken token = new AccessToken(accessToken, accessTokenSecret);
        twitterStream.setOAuthAccessToken(token);
        twitterStream.sample("en");
    }

    @Override
    public void nextTuple() {
        Status ret = queue.poll();
        if (ret == null) {
            Utils.sleep(50);
        } else {
            collector.emit(new Values(ret));
        }
    }

    public void close() {
        twitterStream.shutdown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweets"));
    }
}
