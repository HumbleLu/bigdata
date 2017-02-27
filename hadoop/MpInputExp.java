import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MpInputExp
{
  public static class Map1 extends Mapper<LongWritable,Text,Text,IntWritable>
  {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
        {
                // get the student number
                String stNum = value.toString().split(";")[1];

                // get score
                int score = Integer.parseInt(value.toString().split(";")[2]);
                con.write(new Text(stNum), new IntWritable(score));
        }
  }
  public static class Map2 extends Mapper<LongWritable,Text,Text,IntWritable>
  {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
        {       
                // "shave" the input value
                String line = value.toString().replaceAll("}", "");

                if(line.contains(",")){
                        // get the student number
                        String stNum = line.split(",")[1].split(":")[1];

                        // get score
                        int score = Integer.parseInt(line.split(",")[2].split(":")[1]);
                        con.write(new Text(stNum), new IntWritable(score));
                }
        }
  }
  public static class Map3 extends Mapper<LongWritable,Text,Text,IntWritable>
  {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
        {
                // "shave" the input value
                String line=value.toString().replaceAll("[]\\[]", "");


                if(line.contains(";")){
                  // get the student number
                  String stNum = line.split(";")[1].split("=>")[1];

                  // get score
                  int score = Integer.parseInt(line.split(";")[2].split("=>")[1]);
                  con.write(new Text(stNum), new IntWritable(score));
                }
        }
  }
  public static class Red extends Reducer<Text,IntWritable,Text,IntWritable>
  {
       public void reduce(Text stNum, Iterable<IntWritable> scores, Context con)
        throws IOException , InterruptedException
        {
                int numerator = 0;
                int denominator = 0;
                for (IntWritable v : scores){
                    numerator += v.get();
                    denominator ++;
                }
                int avg = numerator/denominator;
                con.write(stNum, new IntWritable(avg));
        }
   }
  public static void main(String[] args) throws Exception
  {
        Configuration conf=new Configuration();
        String[] files=new GenericOptionsParser(conf,args).getRemainingArgs();
        Path inPath1=new Path(files[0]);
        Path inPath2=new Path(files[1]);
        Path inPath3=new Path(files[2]);
        Path outPath=new Path(files[3]);
        FileSystem hdfs = outPath.getFileSystem(conf);
        if (hdfs.exists(outPath)){
          hdfs.delete(outPath, true);
        };

        Job exampleJob = new Job(conf,"example");
        exampleJob.setJarByClass(MpInputExp.class);
        exampleJob.setMapperClass(Map1.class);
        exampleJob.setMapperClass(Map2.class);
        exampleJob.setMapperClass(Map3.class);
        exampleJob.setReducerClass(Red.class);
        exampleJob.setOutputKeyClass(Text.class);
        exampleJob.setOutputValueClass(IntWritable.class);

        MultipleInputs.addInputPath(exampleJob, inPath1, TextInputFormat.class, Map1.class);
        MultipleInputs.addInputPath(exampleJob, inPath2, TextInputFormat.class, Map2.class);
        MultipleInputs.addInputPath(exampleJob, inPath3, TextInputFormat.class, Map2.class);
        
        FileOutputFormat.setOutputPath(exampleJob, outPath);
        System.exit(exampleJob.waitForCompletion(true) ? 0:1);
  }
}