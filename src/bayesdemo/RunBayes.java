package bayesdemo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class RunBayes {
	static String inputTrainingDataFilePath=null;
	static String outputTrainingDataDirtoryPath=null;
	static String outputTrainingDataFilePath=null;
	static String inputTestDataFilePath=null;
	static String outputTestDataResultPath=null;
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		 if (otherArgs.length != 4) {
		      System.err.println("Usage: RunBayes <datafilepath> <out> <testfilepath> <out>");
		      System.exit(2);
		}
		 inputTrainingDataFilePath=args[0];
		 outputTrainingDataDirtoryPath=args[1];
		 if(outputTrainingDataDirtoryPath.endsWith("/")==false)
			 outputTrainingDataFilePath=outputTrainingDataDirtoryPath+"/part-r-00000";
		 else
			 outputTrainingDataFilePath=outputTrainingDataDirtoryPath+"part-r-00000";
		 inputTestDataFilePath=args[2];
		 outputTestDataResultPath=args[3];
		 
		/*Job trainingJob = new Job(conf, "training bayes classifier");
		trainingJob.setJarByClass(Bayes.class);
		trainingJob.setMapperClass(Bayes.TrainingBayesMapper.class);
		trainingJob.setCombinerClass(Bayes.TrainingBayesReducer.class);
		trainingJob.setReducerClass(Bayes.TrainingBayesReducer.class);
		trainingJob.setOutputKeyClass(Text.class);
		trainingJob.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(trainingJob, new Path(inputTrainingDataFilePath));
		FileOutputFormat.setOutputPath(trainingJob, new Path(outputTrainingDataDirtoryPath));
		trainingJob.waitForCompletion(true) ;*/
		
		Bayes.test2(outputTrainingDataFilePath);
		conf = new Configuration();
		Job testJob = new Job(conf, "test bayes classifier");
		testJob.setJarByClass(Bayes.class);
		testJob.setMapperClass(Bayes.TestBayesMapper.class);
		testJob.setCombinerClass(Bayes.TestBayesReducer.class);
		testJob.setReducerClass(Bayes.TestBayesReducer.class);
		testJob.setOutputKeyClass(Text.class);
		testJob.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(testJob, new Path(inputTestDataFilePath));
		FileOutputFormat.setOutputPath(testJob, new Path(outputTestDataResultPath));
		testJob.waitForCompletion(true) ;
	}

}
