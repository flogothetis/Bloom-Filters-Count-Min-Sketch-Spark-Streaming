import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;


public class Count_min_Sketch {
	private static final Pattern SPACE = Pattern.compile(" ");



	public static void main(String[] args) throws Exception {

		//**********************Create the Spark Streaming context
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		int NumberofHashFunctions = 15;
		int NumberOfCounters= 22;
		String input_file = "ip_adresses_file.txt";
		String test_file= "ip_adresses_file.txt";



		// Create the context with a 1 second batch size
		SparkConf sparkConf = new SparkConf().setAppName("CountMinSketch").setMaster("local[*]");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
		ssc.checkpoint(".");
		//use it for Testing purposes
		SparkSession spark = SparkSession
				.builder()
				.appName("JavaWordCount")
				.getOrCreate();



		@SuppressWarnings("unchecked")

		// Initial state RDD input to mapWithState
		JavaPairRDD<String, String> initialRDD=InitSketch(NumberOfCounters*NumberofHashFunctions,ssc);


		//Load the training data

		JavaDStream<String> TrainingStream = LoadData(input_file,ssc);
		if( TrainingStream ==null) {System.out.println("The training file does not exist");return ;}
		//Load the test data 
		JavaDStream<String> QueriesStream = LoadData(test_file,ssc);
		if( QueriesStream ==null) return ;

		//Train the Count-Min Sketch
		//Consider Count-Min Sketch as 1d Bloom FIlter
		//Call hash functions and produce <key,value> pairs, where key= index and value= "1" ( single appearance of that IP in stream)
		JavaPairDStream<String, String> train_1=MapToPairTraining(TrainingStream,NumberOfCounters,NumberofHashFunctions);

		//Remove duplicates merge the common key to a single <key,value> pair
		JavaPairDStream<String, String> train_3=train_1.reduceByKey((i1,i2)-> String.valueOf(Long.parseLong(i1)+Long.valueOf(i2)),15);




		/*********************** UPDATE STATE *********************/
		Function2<List<String>, Optional<String>, Optional<String>> updateFunc =
				new Function2<List<String>, Optional<String>, Optional<String>>() {

			@Override
			public Optional<String> call(List<String> values, Optional<String> state) 
			{
				//Update the Count Min Sketch 

				Long newSum = Long.valueOf(state.or("0"));


				for(String i : values)
				{
					newSum += Long.valueOf(i);
				}
				return Optional.of(String.valueOf(newSum));
			}

		};


		//Update the state of the Sketch
		JavaPairDStream<String, String> stateDstream =train_3.updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext().defaultParallelism()), initialRDD);
		
		//Cache the current state as it will be used repeatedly
		stateDstream.cache();
		stateDstream.print(100000);

		///**************************** Queries ****************************//
		//Remove duplicate Queries
		JavaPairDStream<String, String> queury_1 = QueriesStream.mapToPair(s->new Tuple2<>(s,"1")).reduceByKey((i1,i2)->("1"));

		//Call Hash Functions and make  <key,value> pairs.
		//Keys will be the indices on the Count-Min Sketch 
		//Values will be the IPs that their sum should be checked . 
		JavaPairDStream<String,String> query_2= MapToPairQueury(queury_1,NumberOfCounters,NumberofHashFunctions);

		/* Join the current state of the stream with the query stream. The result will be comprised of the indices that are existed both
		 *  in the query stream and in Count-Min Sketch (State of the Stream). Specifically, the returned key-value pairs will be formed 
		 *  as <Index in Count-Min Sketch, [IP, Count]>. Then, we keep only the <IP,count>. This procedure is
		 * necessary to get the minimum counter (using reduceBykey --> keep only the minimum value with the same key). 
		 * 
		 */

		//

		JavaPairDStream<String, String> query_answer=query_2.join(stateDstream).mapToPair(f->new Tuple2<String,String>(f._2._1,f._2._2))
				.reduceByKey((i1,i2)-> {
					Long x= Long.parseLong(i1);
					Long y =Long.parseLong(i2);
					if(x.compareTo(y)<0) return i1;
					else return i2;
				});



		query_answer.print(100);

		ssc.start();
		ssc.awaitTermination();
	}


	//Initialize the Count Min Sketch with zeros
	static JavaPairRDD<String, String> InitSketch(int m,JavaStreamingContext ssc)
	{

		List<Tuple2<String, String>> tuples = new LinkedList<>();
		for (int i =0; i<m;i++)
		{
			tuples.add(new Tuple2<>(String.valueOf(i),"0"));

		}


		JavaPairRDD<String, String> initialRDD = ssc.sparkContext().parallelizePairs(tuples);
		return initialRDD;


	}


	static JavaPairDStream <String,String> MapToPairTraining ( JavaDStream<String> Stream , int counters ,int hash )
	{
		if ( hash <1 || counters <1) return null;

		JavaPairDStream<String, String> t2 = null ;
		final int counters1 =counters ;

		for (int i=0 ;i<hash ;i++)
		{
			final int x =i;
			JavaPairDStream<String, String> t1;
			//For each Hash Function produce a <key,value> => <Index in Sketch,1>

			t1= Stream.mapToPair(s -> new Tuple2<>(String.valueOf(BigInteger.valueOf((Long.parseLong(s)% counters1) + counters1*x) ), "1"));


			//Unite all the Mappers
			if(t2!=null)
				t2=t2.union(t1);
			else
				t2=t1;
		}
		return t2;

	}


	static JavaPairDStream <String,String> MapToPairQueury( JavaPairDStream<String,String> Stream , int counters ,int hash  )
	{

		if ( hash <1 || counters <1) return null;

		JavaPairDStream<String, String> t2 = null ;
		final int counters1 =counters ;

		for (int i=0 ;i<hash ;i++)
		{
			final int x =i;
			JavaPairDStream<String, String> t1;
			//For each Hash Function produce a <key,value> => <Index in Sketch, IP>


			t1= Stream.mapToPair(s -> new Tuple2<>(String.valueOf(BigInteger.valueOf((Long.parseLong(s._1)%(counters1))+counters1*x)),s._1));

			//Unite all the Mappers
			if(t2!=null)
				t2=t2.union(t1);
			else
				t2=t1;
		}


		return t2;

	}







	/********************* Load training Data************************
	 * @throws FileNotFoundException *******/
	static JavaDStream<String>  LoadData (String file,JavaStreamingContext ssc) throws FileNotFoundException 
	{
		Queue<JavaRDD<String>> rddQueue = new LinkedList<>();
		File files = new File(file); 
		if(!files.exists() || !files.canRead() || !files.isFile()) return null;


		BufferedReader br = new BufferedReader(new FileReader(files));
		String st; 

		try {
			while ((st = br.readLine()) != null) 
			{
				List<String> list = new ArrayList<>();
				list.add(st);

				rddQueue.add(ssc.sparkContext().parallelize(list));

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}




		JavaDStream<String> TrainingStream = ssc.queueStream(rddQueue);
		return TrainingStream;


	}



}