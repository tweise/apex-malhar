package org.apache.apex.malhar.stream.sample;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;
import org.apache.apex.malhar.lib.window.*;
import org.apache.apex.malhar.lib.window.accumulation.TopN;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedStorage;
import org.apache.apex.malhar.lib.window.impl.WindowedOperatorImpl;
import org.apache.apex.malhar.lib.window.sample.pi.Application;
import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.CompositeStreamTransform;
import org.apache.apex.malhar.stream.api.WindowedStream;
import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.apex.malhar.stream.sample.complete.AutoComplete;
import org.apache.apex.malhar.stream.sample.complete.TopWikipediaSessions;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Throwables;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.KeyValPair;
import org.joda.time.Duration;
import org.junit.Test;

import javax.annotation.Nullable;

import static org.apache.apex.malhar.stream.api.Option.Options.name;

public class TwitterTopN
{
  private static class ExtractHashtags implements Function.FlatMapFunction<String, KeyValPair<String, Long>>
  {
    @Override
    public Iterable<KeyValPair<String, Long>> f(String input)
    {
      List<KeyValPair<String, Long>> result = new LinkedList<>();
      String [] splited = input.split(" ", 2);

      long timestamp = Long.parseLong(splited[0]);
      Matcher m = Pattern.compile("#\\S+").matcher(splited[1]);
      while (m.find()) {
        KeyValPair<String, Long> entry = new KeyValPair<>(m.group().substring(1), timestamp);
        result.add(entry);
      }

      return result;
    }
  }

  public static class TimestampExtractor implements com.google.common.base.Function<KeyValPair<String, Long>, Long>
  {

    @Override
    public Long apply(@Nullable KeyValPair<String, Long> input) {
      return input.getValue();
    }
  }

  @Test
  public void TwitterTopNTest()
  {
    WindowOption windowOption = new WindowOption.TimeWindows(Duration.standardSeconds(5));

    ApexStream<KeyValPair<String, Long>> tags = StreamFactory.fromFolder("/home/shunxin/Desktop/apache/apex-malhar/demos/highlevelapi/src/test/resources/data/sampleTweets.txt", name("tweetSampler"))
            .flatMap(new ExtractHashtags());

    tags.window(windowOption, new TriggerOption().accumulatingFiredPanes().withEarlyFiringsAtEvery(1))
            .accumulate(new TopN<KeyValPair<String, Long>>())
            .with("timestampExtractor", new TimestampExtractor())
            .print(name("console"))
            .runEmbedded(false, 60000, new Callable<Boolean>()
            {
              @Override
              public Boolean call() throws Exception
              {
                return false;
              }
            });
    /*LineByLineFileInputOperator fileInput = new LineByLineFileInputOperator();
    fileInput.setDirectory("/home/shunxin/Desktop/apache/apex-malhar/demos/highlevelapi/src/test/resources/data/sampleTweets.txt");

    WindowedOperatorImpl<String, > windowedOperator = new WindowedOperatorImpl<>();
    Accumulation<MutablePair<Double, Double>, MutablePair<MutableLong, MutableLong>, Double> piAccumulation = new Application.PiAccumulation();

    windowedOperator.setAccumulation(piAccumulation);
    windowedOperator.setDataStorage(new InMemoryWindowedStorage<MutablePair<MutableLong, MutableLong>>());
    windowedOperator.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    windowedOperator.setWindowOption(new WindowOption.TimeWindows(Duration.standardSeconds(10)));
    windowedOperator.setTriggerOption(TriggerOption.AtWatermark().withEarlyFiringsAtEvery(Duration.millis(1000)).accumulatingFiredPanes());*/
  }
}
