import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.regex.Pattern;

import scala.Tuple1;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.sql.SparkSession;
/*******************************8
 * 
 * 
 * @author Fragkoulis Logothetis 
 * Streaming Bloom Filters using Pair RDDs.
 *
 */




public class BloomFilter {
	private static final Pattern SPACE = Pattern.compile(" ");



	public static void main(String[] args) throws Exception {

		//Stop printing warnings on the stdout.
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		int NumberofHashFunctions = 15;
		int SizeOfBloomFilter= 22;
		String input_file = "ip_adresses_file.txt";
		String test_file= "ip_adresses_file.txt";


		if (NumberofHashFunctions > SizeOfBloomFilter )
		{
			System.out.println(" Number of Hash Functions are more than the size of the Bloom");
			System.out.println("Error");
			return ;
		}


		//Start a Spark Context
		SparkConf sparkConf = new SparkConf().setAppName("BloomFilter").setMaster("local[*]");
		//Set the sampling period 
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
		//Use to keep the current state
		ssc.checkpoint(".");
		@SuppressWarnings("unchecked")

		//Load the training data

		JavaDStream<String> TrainingStream = LoadData(input_file,ssc);
		if( TrainingStream ==null) {System.out.println("The training file does not exist");return ;}
		//Load the test data 
		JavaDStream<String> QueriesStream = LoadData(test_file,ssc);
		if( QueriesStream ==null) return ;

		//Train the Bloom Filter
		//Call (Map) hash functions and produce <key,value> pairs, where key= index and value= "1" 
		JavaPairDStream<String, String> train_1=MapToPairTraining(TrainingStream,NumberofHashFunctions, SizeOfBloomFilter);

		// Remove the duplicates 
		JavaPairDStream<String, String> train_2=train_1.reduceByKey((i1,i2)-> ("1"));
		train_2.print(100);




		//********Keep the state update about the new "1" in Bloom Filter

		Function2<List<String>, Optional<String>, Optional<String>> updateFunc =
				new Function2<List<String>, Optional<String>, Optional<String>>() {

			@Override
			public Optional<String> call(List<String> values, Optional<String> state) {
				// If the index exists, reset the value of that key to '1' 

				return Optional.of("1");
			}
		};


		//Update the state
		JavaPairDStream<String, String> stateDstream =train_2.updateStateByKey(updateFunc);
		stateDstream.cache();
		//***************************************************************

		//**************************** Queuries ****************************//
		//Remove duplicate (IP's)
		JavaPairDStream<String, String> queury_1 = QueriesStream.mapToPair(s->new Tuple2<>(s,"1")).reduceByKey((i1,i2)->("1"));

		//Call Hash Functions and produce  <key,value> pairs.
		//Keys will be the indices (pointers) to Bloom Filter's positions that should be checked if is equal to "1"
		//Values will be the IPs that should be checked if their existed in Bloom Filter. 
		JavaPairDStream<String,String> query_2= MapToPairQueury(queury_1,NumberofHashFunctions,SizeOfBloomFilter);

		/* Join the current state of the stream with the query stream. The result will be comprised of the indices that are existed both
		 *  in the query stream and in current state. Then, we swap the values with the keys and we set new values to '1'. This swapping is
		 * necessary to count the number of appearances of the i-th IP after the join operation. If that number is equal to the number of hash
		 *  functions, that IP appeared in the stream. These technique is accurate even if the hash functions point to the same Bloom Filter's position.  
		 */

		//
		JavaPairDStream<String, String> x1=query_2.join(stateDstream).mapToPair(f->new Tuple2<String,String>(f._2._1,"1"))
				.reduceByKey((i1,i2)-> String.valueOf(Integer.parseInt(i1)+Integer.parseInt(i2))).filter(f-> {
					if(f._2.equals(String.valueOf(NumberofHashFunctions))) return true;
					else return false;

				});

		//Keep only the IP address
		JavaDStream<Object> final_ips= x1.map(s->(new Tuple1<String>(s._1 )));
		final_ips.print(100);




		ssc.start();
		ssc.awaitTermination();
	}





	static JavaPairDStream <String,String> MapToPairTraining ( JavaDStream<String> Stream , int NumberofHashFunctions ,int SizeOfBloomFilter  )
	{

		JavaPairDStream<String, String> t2 = null ;
		int y= (int)((double)SizeOfBloomFilter/(double)NumberofHashFunctions);
		if(y-1<=0) y= 2;
		final int divider=y;


		for (int i=0 ;i<NumberofHashFunctions ;i++)
		{
			final int x =i;
			JavaPairDStream<String, String> t1;
			//For each Hash Function produce a <key,value> => <Index of Bloom Filter,1>

			t1= Stream.mapToPair(s -> new Tuple2<>(String.valueOf(BigInteger.valueOf(Long.parseLong(s)% (divider-1)+ divider*x) ), "1"));


			//Unite all the Mappers
			if(t2!=null)
				t2=t2.union(t1);
			else
				t2=t1;
		}
		return t2;

	}


	static JavaPairDStream <String,String> MapToPairQueury( JavaPairDStream<String,String> Stream , int NumberofHashFunctions,int SizeOfBloomFilter )
	{

		JavaPairDStream<String, String> t2 = null ;

		int y= (int)((double)SizeOfBloomFilter/(double)NumberofHashFunctions);
		if(y-1<=0) y= 2;
		final int divider=y;
		for (int i=0 ;i<NumberofHashFunctions ;i++)
		{
			final int x =i;
			JavaPairDStream<String, String> t1;
			//For each Hash Function produce a <key,value> => <Index of Bloom Filter,IP>
			t1= Stream.mapToPair(s -> new Tuple2<>(String.valueOf(BigInteger.valueOf(Long.parseLong(s._1)%(divider-1)+divider*x)),s._1));

			//Unite all the Mappers
			if(t2!=null)
				t2=t2.union(t1);
			else
				t2=t1;
		}
		return t2;

	}







	/********************* Load Training Data************************
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
