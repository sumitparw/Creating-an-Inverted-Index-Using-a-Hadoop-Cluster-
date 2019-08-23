import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class InvertedIndex{
        public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>
        {
                private Text w_k = new Text();
                public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
                {
                   String l = value.toString();
                   

                   //Handling \t tab characters as per the assignment guidelines 
                   String map_arr[] = l.split("\t", 2);
                   Text document_Id = new Text(map_arr[0]);
                   String lowerLine=map_arr[1].replaceAll("[^A-Za-z]+", " ").toLowerCase();

                   StringTokenizer stri_Token = new StringTokenizer(lowerLine);
                   while(stri_Token.hasMoreTokens())
                   {
                    w_k.set(stri_Token.nextToken());
                    context.write(w_k, document_Id);
                   }
                }
        }
        public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>
        {
                public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
                {
                         HashMap<String, Integer> hsh_Map = new HashMap();
                         for(Text val: values)
                         {
                                String v = val.toString();
                                if(! hsh_Map.containsKey(v)){
                                         hsh_Map.put(v,  new Integer(1));
                                }
                                 else{
                                        int old_v = hsh_Map.get(v);
                                        ++old_v;
                                        hsh_Map.put(v, new Integer(old_v));
                                 }
                         }
                         StringBuilder sbuilder = new StringBuilder("");
                         for(String ctr: hsh_Map.keySet())
                         {
                                 sbuilder.append(ctr+":"+hsh_Map.get(ctr)+" ");
                         }
                         Text output_V = new Text(sbuilder.toString());
                         context.write(key, output_V);
                }
        }
        public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException
        {
                if(args.length < 2)
                {
                        System.out.println("Insufficient Arguments");
                }else{
                        Configuration conf = new Configuration();
                        Job job = Job.getInstance(conf, "inverted index");
                        job.setJarByClass(InvertedIndex.class);
                        job.setMapperClass(InvertedIndexMapper.class);
                        job.setReducerClass(InvertedIndexReducer.class);
                        job.setMapOutputKeyClass(Text.class);
                        job.setMapOutputValueClass(Text.class);
                        job.setOutputKeyClass(Text.class);
                        job.setOutputValueClass(Text.class);
                        FileInputFormat.addInputPath(job, new Path(args[0]));
                        FileOutputFormat.setOutputPath(job, new Path(args[1]));
                        System.exit(job.waitForCompletion(true)? 0 : 1);
                }
        }
}
