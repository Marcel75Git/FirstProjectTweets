import CleanUpText.FilterSpanishLanguage;
import CleanUpText.FilterTweetsNotUse;
import CleanUpText.MapperLowerCase;
import CleanUpText.MyTweetsDelete;
import TopN.TopNMapper;
import TopN.TopNReducer;
import TrendingTopic.MapperTrendingTopic;
import TrendingTopic.ReducerTrendingTopic;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class FinalMain extends Configured implements Tool {
    //private Configuration conf;

    public int run(String[] args1) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = getConf();
        String[] args = new GenericOptionsParser(conf, args1).getRemainingArgs();
        FileSystem.get(conf).delete(new Path("src/main/resources/uno"), true);
        FileSystem.get(conf).delete(new Path("src/main/resources/two"), true);
        FileSystem.get(conf).delete(new Path("src/main/resources/output"), true);

        Job job1 = Job.getInstance(conf);
        job1.setJobName("Trending topic");
        job1.setJarByClass(FinalMain.class);

        job1.setMapperClass(MapperTrendingTopic.class);
        job1.setCombinerClass(ReducerTrendingTopic.class);
        job1.setReducerClass(ReducerTrendingTopic.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job1, new Path("src/main/resources/uno"));
        FileOutputFormat.setOutputPath(job1, new Path("src/main/resources/two"));

        ControlledJob cJob1 = new ControlledJob(conf);
        cJob1.setJob(job1);

        //**** creation second job

        Job job2 = Job.getInstance(conf);
        job2.setJobName("Job2_CleanUp");
        //job2.setJarByClass(FinalMain.class);

        job2.setJarByClass(FinalMain.class);
        Configuration lowerCaseMapper = new Configuration(false);
        ChainMapper.addMapper(job2, MapperLowerCase.class, IntWritable.class, Text.class, IntWritable.class, Text.class, lowerCaseMapper);

        Configuration myNewTweets = new Configuration(false);
        ChainMapper.addMapper(job2, MyTweetsDelete.class, IntWritable.class, Text.class, IntWritable.class, Text.class, myNewTweets);

        Configuration filterSpanish = new Configuration(false);
        ChainMapper.addMapper(job2, FilterSpanishLanguage.class, IntWritable.class, Text.class, IntWritable.class, Text.class, filterSpanish);

        Configuration filterNotUse = new Configuration(false);
        ChainMapper.addMapper(job2, FilterTweetsNotUse.class, IntWritable.class, Text.class, NullWritable.class, Text.class, filterNotUse);

        /* job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
               job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(IntWritable.class);
                job2.setInputFormatClass(KeyValueTextInputFormat.class);
                job2.setOutputFormatClass(TextOutputFormat.class);
                FileInputFormat.addInputPath(job2, new Path("./inter/part*"));
                FileOutputFormat.setOutputPath(job2, new Path(args[1]));*/

                job2.setOutputKeyClass(NullWritable.class);
                job2.setOutputValueClass(Text.class);

                job2.setInputFormatClass(TextInputFormat.class);
                job2.setOutputFormatClass(TextOutputFormat.class);

                FileInputFormat.addInputPath(job2, new Path(args[0]));
                FileOutputFormat.setOutputPath(job2, new Path("src/main/resources/uno"));

        ControlledJob cJob2 = new ControlledJob(conf);
        cJob2.setJob(job2);

        JobControl jobctrl = new JobControl("JobCtrl");
        jobctrl.addJob(cJob1);
        jobctrl.addJob(cJob2);
        cJob1.addDependingJob(cJob2);

        //**** creation third job

        Job job3 = Job.getInstance(conf);
        job3.setJobName("Job2_CleanUp");
        job3.setJarByClass(FinalMain.class);

        job3.setMapperClass(TopNMapper.class);
        job3.setReducerClass(TopNReducer.class);
        job3.setNumReduceTasks(1);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path("src/main/resources/two"));
        // la sortie du programme
        FileOutputFormat.setOutputPath(job3, new Path(args[1]));

        ControlledJob cJob3 = new ControlledJob(conf);
        cJob3.setJob(job3);

        jobctrl.addJob(cJob2);
        jobctrl.addJob(cJob3);
        cJob3.addDependingJob(cJob1);

        Thread jobRunnerThread = new Thread(new JobRunner(jobctrl));
        jobRunnerThread.start();
        while (!jobctrl.allFinished()) {
            System.out.println("Still running...");
            Thread.sleep(5000);
        }
        System.out.println("done");
        jobctrl.stop();

        // Cleaning intermediate data.. can be ignored.


        return(0);
    }


    class JobRunner implements Runnable {
        private JobControl control;

        public JobRunner(JobControl _control) {
            this.control = _control;
        }

        public void run() {
            this.control.run();
        }
    }


    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run(new FinalMain(), args);
        System.exit(exitCode);
    }

}
