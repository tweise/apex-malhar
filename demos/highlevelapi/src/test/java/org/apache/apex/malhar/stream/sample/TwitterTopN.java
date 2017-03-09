package org.apache.apex.malhar.stream.sample;

import java.net.URI;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.datatorrent.api.*;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.snapshot.AppDataSnapshotServerMap;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;
import org.apache.apex.malhar.lib.window.*;
import org.apache.apex.malhar.lib.window.accumulation.SumLong;
import org.apache.apex.malhar.lib.window.accumulation.TopNByKey;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedKeyedStorage;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedStorage;
import org.apache.apex.malhar.lib.window.impl.KeyedWindowedOperatorImpl;
import org.apache.apex.malhar.lib.window.impl.WindowedOperatorImpl;
import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.apex.malhar.stream.api.operator.FunctionOperator;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.lib.util.KeyValPair;
import org.joda.time.Duration;
import org.junit.Test;

public class TwitterTopN implements StreamingApplication
{
  public static final String SCHEMA = "TwitterTopNSchema.json";
  public static final URI uri = URI.create("ws://localhost:8890/pubsub");
  public static final String topic = "twitter";

  private static class ExtractHashtags implements Function.FlatMapFunction<String, Tuple.TimestampedTuple<KeyValPair<String, Long>>>
  {
    @Override
    public Iterable<Tuple.TimestampedTuple<KeyValPair<String, Long>>> f(String input)
    {
      List<Tuple.TimestampedTuple<KeyValPair<String, Long>>> result = new LinkedList<>();
      String[] splited = input.split(" ", 2);

      long timestamp = Long.parseLong(splited[0]);
      Matcher m = Pattern.compile("#\\S+").matcher(splited[1]);
      while (m.find()) {
        Tuple.TimestampedTuple<KeyValPair<String, Long>> entry = new Tuple.TimestampedTuple<>(timestamp, new KeyValPair<>(m.group().substring(1), 1L));
        result.add(entry);
      }

      return result;
    }
  }

  private static class TupleToMap implements Function.MapFunction<Tuple.WindowedTuple<List<KeyValPair<String, Long>>>, List<Map<String, Object>>>
  {
    @Override
    public List<Map<String, Object>> f(Tuple.WindowedTuple<List<KeyValPair<String, Long>>> input)
    {
      List<Map<String, Object>> result = new ArrayList<>();
      Long timestamp = input.getTimestamp();
      for (KeyValPair<String, Long> kv : input.getValue()) {
        Map<String, Object> row = new HashMap<>();
        row.put("hashtag", kv.getKey());
        row.put("count", kv.getValue());
        row.put("timestamp", timestamp);
        result.add(row);
      }
      return result;
    }
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    LineByLineFileInputOperator fileInput = new LineByLineFileInputOperator();
    fileInput.setDirectory("/home/shunxin/Desktop/apache/apex-malhar/demos/highlevelapi/src/test/resources/data/sampleTweets.txt");

    FunctionOperator.FlatMapFunctionOperator<String, Tuple.TimestampedTuple<KeyValPair<String, Long>>> fmOp =
            new FunctionOperator.FlatMapFunctionOperator<>(new ExtractHashtags());

    KeyedWindowedOperatorImpl<String, Long, MutableLong, Long> countBykeyOp = new KeyedWindowedOperatorImpl<>();
    countBykeyOp.setAccumulation(new SumLong());
    countBykeyOp.setDataStorage(new InMemoryWindowedKeyedStorage<String, MutableLong>());
    countBykeyOp.setWindowOption(new WindowOption.TimeWindows(Duration.standardSeconds(5)));
    countBykeyOp.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    countBykeyOp.setTriggerOption(TriggerOption.AtWatermark().withEarlyFiringsAtEvery(1).accumulatingFiredPanes());

    TopNByKey<String, Long> topNByKey = new TopNByKey<>();
    topNByKey.setN(3);
    WindowedOperatorImpl<KeyValPair<String, Long>, Map<String, Long>, List<KeyValPair<String, Long>>> topN = new WindowedOperatorImpl<>();
    topN.setAccumulation(topNByKey);
    topN.setDataStorage(new InMemoryWindowedStorage<Map<String, Long>>());
    topN.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    topN.setTriggerOption(TriggerOption.AtWatermark().withEarlyFiringsAtEvery(5).accumulatingFiredPanes());

    FunctionOperator.MapFunctionOperator<Tuple.WindowedTuple<List<KeyValPair<String, Long>>>, List<Map<String, Object>>> mapOp =
            new FunctionOperator.MapFunctionOperator<>(new TupleToMap());

    AppDataSnapshotServerMap snapshotServerMap = new AppDataSnapshotServerMap();
    String JSON = SchemaUtils.jarResourceFileToString(SCHEMA);
    snapshotServerMap.setSnapshotSchemaJSON(JSON);

    PubSubWebSocketAppDataQuery wsQuery = new PubSubWebSocketAppDataQuery();
    wsQuery.enableEmbeddedMode();
    snapshotServerMap.setEmbeddableQueryInfoProvider(wsQuery);

    PubSubWebSocketAppDataResult wsResult = new PubSubWebSocketAppDataResult();
    wsResult.setUri(uri);
    wsResult.setTopic(topic);

    ConsoleOutputOperator console = new ConsoleOutputOperator();

    dag.addOperator("fileInput", fileInput);
    dag.addOperator("extractHashtags", fmOp);
    dag.addOperator("countByKey", countBykeyOp);
    dag.addOperator("topN", topN);
    dag.addOperator("TupleToMap", mapOp);
    dag.addOperator("snapshotServer", snapshotServerMap);
    dag.addOperator("QueryResult", wsResult);
    dag.addOperator("con", console);

    dag.addStream("rawTweets", fileInput.output, fmOp.input);
    dag.addStream("hashtags", fmOp.output, countBykeyOp.input);
    dag.addStream("countingResult", countBykeyOp.output, topN.input);
    dag.addStream("topNResult", topN.output, mapOp.input);
    dag.addStream("resultMap", mapOp.output, snapshotServerMap.input);
    dag.addStream("finalResult", snapshotServerMap.queryResult, wsResult.input, console.input);


  }

  @Test
  public void TwitterTopNTest() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    lma.prepareDAG(new TwitterTopN(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.run(60000);
  }
}
