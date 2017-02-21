package org.apache.apex.malhar.stream.sample;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
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

  public static class TempWrapper
  {
    private KeyValPair<String, Long> value;
    private Long timestamp;

    public TempWrapper()
    {

    }

    public TempWrapper(KeyValPair<String, Long> value, Long timestamp)
    {
      this.value = value;
      this.timestamp = timestamp;
    }

    @Override
    public String toString()
    {
      return this.value + "  -  " + this.timestamp;
    }

    public Long getTimestamp()
    {
      return timestamp;
    }

    public void setTimestamp(Long timestamp)
    {
      this.timestamp = timestamp;
    }

    public KeyValPair<String, Long> getValue()
    {
      return value;
    }

    public void setValue(KeyValPair<String, Long> value)
    {
      this.value = value;
    }
  }

  public static class TimestampExtractor implements com.google.common.base.Function<TempWrapper, Long>
  {
    @Override
    public Long apply(@Nullable TempWrapper input)
    {
      return input.getTimestamp();
    }
  }

  public static class Comp implements Comparator<TempWrapper>
  {
    @Override
    public int compare(TempWrapper o1, TempWrapper o2)
    {
      return Long.compare(o1.getValue().getValue(), o2.getValue().getValue());
    }
  }

  @Test
  public void TwitterTopNTest()
  {
    WindowOption windowOption = new WindowOption.TimeWindows(Duration.standardSeconds(5));
    TopN<TempWrapper> topN = new TopN<>();
    topN.setN(5);
    topN.setComparator(new Comp());

    ApexStream<KeyValPair<String, Long>> tags = StreamFactory.fromFolder("/home/shunxin/Desktop/apache/apex-malhar/demos/highlevelapi/src/test/resources/data/sampleTweets.txt", name("tweetSampler"))
            .flatMap(new ExtractHashtags());

    tags.window(windowOption, new TriggerOption().accumulatingFiredPanes().withEarlyFiringsAtEvery(1))
            .countByKey(new Function.ToKeyValue<KeyValPair<String,Long>, String, Long>()
            {
              @Override
              public Tuple<KeyValPair<String, Long>> f(KeyValPair<String, Long> input)
              {
                return new Tuple.TimestampedTuple<>(input.getValue(), new KeyValPair<>(input.getKey(), 1L));
              }
            })
            .map(new Function.MapFunction<Tuple.WindowedTuple<KeyValPair<String,Long>>, TempWrapper>()
            {
              @Override
              public TempWrapper f(Tuple.WindowedTuple<KeyValPair<String, Long>> input)
              {
                return new TempWrapper(input.getValue(), input.getTimestamp());
              }
            })
            .window(windowOption, new TriggerOption().accumulatingFiredPanes().withEarlyFiringsAtEvery(1))
            .accumulate(topN)
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

  }
}
