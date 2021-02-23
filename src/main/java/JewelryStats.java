import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JewelryStats {
  public static class JewelryStatsMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
    
    public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
      String line = value.toString();
      String fields[] = line.split(",");
      String productId = fields[0];
      String price = fields[2];
      float rating = Float.parseFloat(fields[6]);
      Float priceInFl = null;
      if(!price.equals("unknown")){
        priceInFl = Float.parseFloat(price);
      }
      MapWritable valMap = new MapWritable();
      valMap.put(new Text("price"), new FloatWritable(priceInFl));
      valMap.put(new Text("rating"), new FloatWritable(rating));
      ctx.write(new Text(productId), valMap);
    }
  }

  public static class JewelryStatsReducer extends Reducer<Text, MapWritable, Text, MapWritable> {
    public void reduce(Text key, Iterable<MapWritable> values, Context ctx) throws IOException, InterruptedException{
      float sum = 0;
      int length = 0;
      float ratingSum = 0;
      int ratingLen = 0;

      for (MapWritable val : values) {
        FloatWritable priceW = (FloatWritable)val.get("price");
        Float price = priceW.get();
        if(price != null){
          sum += price;
          length++;
        }

        FloatWritable ratingW = (FloatWritable)val.get("rating");
        float rating = ratingW.get();
        ratingSum += rating;
        ratingLen++;
      }
      float averagePrice = sum / length;
      float averageRating = ratingSum / ratingLen;

      MapWritable valMap = new MapWritable();
      valMap.put(new Text("average-price"), new FloatWritable(averagePrice));
      valMap.put(new Text("average-rating"), new FloatWritable(averageRating));

      ctx.write(key, valMap);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "JewelryStats");

    job.setJarByClass(JewelryStats.class);
    job.setMapperClass(JewelryStatsMapper.class);
    job.setReducerClass(JewelryStatsReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}