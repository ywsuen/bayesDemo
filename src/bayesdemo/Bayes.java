package bayesdemo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public final class Bayes {
	private final static IntWritable one = new IntWritable(1);
	private static final String[] CLASSIFICATION={">50K","<=50K"};

	private final static String[] ATTRIBUTE_TITLE={
		"age","workclass","education","education-num",
		"marital-status","occupation","relationship","race","sex",
		"capital","hours-per-week","native-country"};
	private static final String[] ATTRIBUTE={
		"age: continuous, 17, 90, 1",
		"workclass: Private, Self-emp-not-inc, Self-emp-inc, Federal-gov, Local-gov, State-gov, Without-pay, Never-worked",
		//"fnlwgt: continuous",
		"education: Bachelors, Some-college, 11th, HS-grad, Prof-school, Assoc-acdm, Assoc-voc, 9th, 7th-8th, 12th, Masters, 1st-4th, 10th, Doctorate, 5th-6th, Preschool",
		"education-num: continuous, 1, 16, 1",
		"marital-status: Married-civ-spouse, Divorced, Never-married, Separated, Widowed, Married-spouse-absent, Married-AF-spouse",
		"occupation: Tech-support, Craft-repair, Other-service, Sales, Exec-managerial, Prof-specialty, Handlers-cleaners, Machine-op-inspct, Adm-clerical, Farming-fishing, Transport-moving, Priv-house-serv, Protective-serv, Armed-Forces",
		"relationship: Wife, Own-child, Husband, Not-in-family, Other-relative, Unmarried",
		"race: White, Asian-Pac-Islander, Amer-Indian-Eskimo, Other, Black",
		"sex: Female, Male",
		"capital: continuous, -50000, 50000, 1",
		//"capital-gain: continuous",
		//"capital-loss: continuous",
		"hours-per-week: continuous, 0, 100, 1",
		"native-country: United-States, Cambodia, England, Puerto-Rico, Canada, Germany, Outlying-US(Guam-USVI-etc), India, Japan, Greece, South, China, Cuba, Iran, Honduras, Philippines, Italy, Poland, Jamaica, Vietnam, Mexico, Portugal, Ireland, France, Dominican-Republic, Laos, Ecuador, Taiwan, Haiti, Columbia, Hungary, Guatemala, Nicaragua, Scotland, Thailand, Yugoslavia, El-Salvador, Trinadad&Tobago, Peru, Hong, Holand-Netherlands"
		};
	private static final String[] ATTRIBUTE_DELIMITER={": ",", "};
	private static final String[] FITTING_DATA_DELIMITER={"#","\t"};
	private static final String[] TEST_DATA_DELIMITER={", "};
	private static final String MISSING_DATE_SIGN="?";
	private final static String FILE_DELIMITER=", ";
	
	private final static TreeMap<String, Double> attributeMap= new TreeMap<>();
	private final static Double[] classificationArray=new Double[CLASSIFICATION.length];
	
	public static class TrainingBayesMapper extends Mapper<Object,	 Text, Text, IntWritable>{
		
		
	    private Text word = new Text();
	    
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			String[] data=dataProcessing(dataSplit(value.toString()));
			
			String exampleValue =exempleTpye(value.toString());
			if(exampleValue.isEmpty()||exampleValue.compareTo(MISSING_DATE_SIGN)==0)
				return;
			word.set(createKey(exampleValue));
			context.write(word, one);
			for(int i=0;i<data.length;i++){
				word.set(createKey(exampleValue,ATTRIBUTE_TITLE[i],data[i]));
				context.write(word, one);
			}
		}
	}
	
	public static class TrainingBayesReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			 int sum = 0;
			 for (IntWritable val : values) {
				 sum += val.get();
		      }
			 result.set(sum);
			 context.write(key, result);
			 System.out.println(key.toString()+" "+result.toString());
		}
	}
	
	public static void test2(String reduredFilePath) throws IOException{
        String line=null;
        
        Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(URI.create(reduredFilePath),conf);
		InputStream in=fs.open(new Path(reduredFilePath));
		
        BufferedReader fittingDateBr = new BufferedReader(new InputStreamReader(in));
        TreeMap<String, Integer> tempMap=new TreeMap<>();
        while((line=fittingDateBr.readLine())!=null){
        	String[] str=line.split(FITTING_DATA_DELIMITER[1]);
        	tempMap.put(str[0], Integer.valueOf(str[1]));
        }
        fittingDateBr.close();
        
        Integer sumExample=0;
        for(int i=0;i<CLASSIFICATION.length;i++){
        	Integer temp=tempMap.get(createKey(CLASSIFICATION[i]));
        	if(temp==null){
        		System.out.println("missing data");
        		return;
        	}
        	sumExample+=temp;
        }
        for(int i=0;i<CLASSIFICATION.length;i++){
        	Integer temp=tempMap.get(createKey(CLASSIFICATION[i]));
        	classificationArray[i]=(double)temp/sumExample;
        }
        

        for(int i=0;i<ATTRIBUTE.length;i++){//每行
        	String[] str=ATTRIBUTE[i].split(ATTRIBUTE_DELIMITER[0]);
        	String attributeTitle = str[0];
        	String[] attributeValue=str[1].split(ATTRIBUTE_DELIMITER[1]);
        	for(int j=0; j<CLASSIFICATION.length;j++){//每个类别
        		Integer[] tempMapvalueArray;
        		String[] tempMapkeyArray;
        		Double sumTempMapValue=0.0;
        		if(attributeValue[0].compareTo("continuous")==0){
        			int begin=Integer.valueOf(attributeValue[1]);
        			int end=Integer.valueOf(attributeValue[2]);
        			int step=Integer.valueOf(attributeValue[3]);
        			int size=(end-begin)/step+1;
        			tempMapvalueArray=new Integer[size];
        			tempMapkeyArray=new String[size];
        			for(int z=begin,k=0;z<=end;k++,z+=step){
        				tempMapkeyArray[k] = createKey(CLASSIFICATION[j], attributeTitle, new Integer(z).toString());
        				tempMapvalueArray[k]=tempMap.get(tempMapkeyArray[k]);
		        		if(tempMapvalueArray[k]==null){
		        			tempMapvalueArray[k]=1;
		        		}
		        		sumTempMapValue+=tempMapvalueArray[k];
		        		
        			}
        			
        			
        		}
        		else{
        			tempMapvalueArray=new Integer[attributeValue.length];
            		tempMapkeyArray=new String[attributeValue.length];
		        	for(int k=0; k<attributeValue.length; k++){
		        		tempMapkeyArray[k] = createKey(CLASSIFICATION[j],attributeTitle,attributeValue[k]);
		        		tempMapvalueArray[k]=tempMap.get(tempMapkeyArray[k]);
		        		if(tempMapvalueArray[k]==null){
		        			tempMapvalueArray[k]=1;
		        		}
		        		sumTempMapValue+=tempMapvalueArray[k];
		        	}
        		}
	        	for(int k=0; k<tempMapkeyArray.length; k++){
	        		Double p= Double.valueOf(tempMapvalueArray[k])/sumTempMapValue;
	        		attributeMap.put(tempMapkeyArray[k], p);
	        	} 
        	}
        }
        
        
     
	}
	
	public static class TestBayesMapper extends Mapper<Object, Text, Text, IntWritable>{
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String line=value.toString();
			
        	String[] dataArray=dataProcessing(dataSplit(line));
        	if(dataArray==null){
        		System.err.println("find a null dataArray in mapper, line="+line);
        		return;
        	}
        	context.write(new Text("sum"), one);

        	String type=null;
        	Double maxPossibility=0.0;
        	for(int i=0; i<CLASSIFICATION.length;i++){
        		Double possibility=1.0;
        		for(int j=0; j<ATTRIBUTE_TITLE.length;j++){
        			if(dataArray[j].compareTo(MISSING_DATE_SIGN)==0)
        				continue;
        			Double f=attributeMap.get(createKey(CLASSIFICATION[i], ATTRIBUTE_TITLE[j], dataArray[j]));
        		
        			if(f==null){
        				System.err.println("can't find "+createKey(CLASSIFICATION[i], ATTRIBUTE_TITLE[j], dataArray[j]));
        				continue;
        			}
        			possibility*=f;
        		}
        		possibility*=classificationArray[i];
	        		
	        		if(possibility>maxPossibility){
	        			type=CLASSIFICATION[i];
	        			maxPossibility=possibility;
	        		}
	        	}
	        	
	        	if(exempleTpye(line).compareTo(type)==0){
	        		context.write(new Text("right"), one);
	        	}
		}
	}
	
	public static class TestBayesReducer extends Reducer<Text, IntWritable, IntWritable, IntWritable>{
		static int sum=0;
		static int right=0;
		@Override
		protected void reduce(Text arg0, Iterable<IntWritable> arg1,
				Reducer<Text, IntWritable, IntWritable, IntWritable>.Context arg2)
				throws IOException, InterruptedException {
			for(IntWritable i:arg1){
					if(arg0.compareTo(new Text("right"))==0){
						right+=i.get();
					}
					else if(arg0.compareTo(new Text("sum"))==0){
						sum+=i.get();
					}
					else{
						System.err.println("missing data in reducer, value="+i.get());
					}
			}
			System.out.println((float)right/sum);
		}
	}
	
	private static String[] dataSplit(String line){
		String[] s=line.split(TEST_DATA_DELIMITER[0]);
		
		return s;
	}
	
	private static String createKey(String classification, String attribute, String value){
		return classification+FITTING_DATA_DELIMITER[0]+attribute+FITTING_DATA_DELIMITER[0]+value;
	}
	private static String createKey(String key){
		return createKey(key, key, key);
	}
	
	private static String exempleTpye(String line) {
		String[] s=line.split(TEST_DATA_DELIMITER[0]);
		return s[s.length-1];
	}
	
	private static String[] dataProcessing(String[] args){
		if(args.length!=15)
			return null;
		String[] data=new String[ATTRIBUTE.length];
		for(int i=0;i<args.length-1;i++){
			if(i==0||i==1){
				data[i]=args[i];
			}
			else if(i==2){
				continue;
			}
			else if(3<=i&&i<=9){
				data[i-1]=args[i];
			}
			else if(i==10){
				if(args[10].compareTo(MISSING_DATE_SIGN)==0||args[11].compareTo(MISSING_DATE_SIGN)==0){
					data[9]=MISSING_DATE_SIGN;
				}
				else{
					Integer tempi=(Integer.valueOf(args[10])-Integer.valueOf(args[11]))/1*1;
					data[9]=tempi.toString();
				}
				
			}
			else if(i==12){
				if(args[12].compareTo(MISSING_DATE_SIGN)==0){
					data[i-2]=MISSING_DATE_SIGN;
				}
				else{
					Integer hours_per_week=Integer.valueOf(args[i])/1*1;
					data[i-2]=hours_per_week.toString();
				}
				
			}
			else if(i==13){
				data[i-2]=args[i];
			}
		}
		
		return data;
	}
}
