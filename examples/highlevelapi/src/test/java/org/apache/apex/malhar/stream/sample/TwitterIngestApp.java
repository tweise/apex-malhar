package org.apache.apex.malhar.stream.sample;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.Callable;

import org.joda.time.Duration;
import org.junit.Test;

import org.apache.apex.malhar.lib.window.WindowOption;

import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.apex.malhar.stream.sample.complete.TwitterAutoComplete;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.contrib.twitter.TwitterSampleInput;

import static org.apache.apex.malhar.stream.api.Option.Options.name;

public class TwitterIngestApp
{
  public static class Collector extends BaseOperator
  {
    private String path = "/home/shunxin/Desktop/apache/apex-malhar/demos/highlevelapi/src/test/resources/data/sampleTweets.txt";
    private PrintWriter pw;

    @Override
    public void setup(Context.OperatorContext context)
    {
      super.setup(context);
      try {
        pw = new PrintWriter(new FileWriter(path));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void teardown()
    {
      pw.close();
      super.teardown();
    }


    public final transient DefaultInputPort<String> input = new DefaultInputPort<String>()
    {
      @Override
      public void process(String tuple)
      {
        pw.write(tuple + "\n");
      }
    };
  }


  static class ASCIIFilter implements Function.FilterFunction<String>
  {
    @Override
    public boolean f(String input)
    {
      return TwitterAutoComplete.StringUtils.isAscii(input);
    }
  }

  @Test
  public void twitterIngest() throws Exception
  {
    TwitterSampleInput input = new TwitterSampleInput();
    Collector collector = new Collector();

    input.setConsumerKey("9fi6uTaLrW91suW5alIl6bfUN");
    input.setConsumerSecret("TpsvVZzh5MLhDZuwiCWxwv9LyHJGamtZHnojlgdNa4mph7RHIK");
    input.setAccessToken("90819718-0djLN49fDwGdvlukLCC1bXwCYJ5snZbcV3pfIzvBJ");
    input.setAccessTokenSecret("3btrakD4YHRUdYeaDtrTtVgLXjzefsbxFx78dzmS6LOV3");

    WindowOption windowOption = new WindowOption.TimeWindows(Duration.standardMinutes(1));

    ApexStream<Object> tags = StreamFactory.fromInput(input, input.text, name("tweetSampler"))
        .filter(new ASCIIFilter())
        .filter(new Function.FilterFunction<String>()
        {
          @Override
          public boolean f(String input)
          {
            return !input.contains("\n");
          }
        })
        .map(new Function.MapFunction<String, String>()
        {
          @Override
          public String f(String input)
          {
            return System.currentTimeMillis() + " " + input;
          }
        }).print()
        .endWith(collector, collector.input);

    tags.runEmbedded(false, 60000, new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        return false;
      }
    });

  }
}
