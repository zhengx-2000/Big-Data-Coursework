package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.studentfunctions.*;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleFiltered;

/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {

	
	public static void main(String[] args) {
		
		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
		
		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[2]"; // default is local mode with two executors
		
		String sparkSessionName = "BigDataAE"; // give the session a name
		
		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
	
		
		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries
		
		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		//if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v2.jl.fix.json";

		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);
		
		// Close the spark session
		spark.close();
		
		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {
			
			// We have set of output rankings, lets write to disk
			
			// Create a new folder 
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) outDirectory.mkdir();
			
			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}
		
		
	}
	
	
	
	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		
		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article
		
		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle
		
		//----------------------------------------------------------------
		// Your Spark Topology should be defined here
		//----------------------------------------------------------------
		// Step 1: Data preprocessing
		// Removed news with no title
		// Processed news title and content (first 5) into words
		// Generated multiple copies of news with different query and query term
		// Step 2.1: int currentDocumentLength (stored inside the dataset)
		// Use cache iff the computer has memory space larger than 5.1GB (for full dataset)
		Dataset<NewsArticleFiltered> newsFiltered = news.flatMap(new NewsFilterFlatMap(queries.collectAsList()),
				Encoders.bean(NewsArticleFiltered.class)).cache();

		// Step 1: Data preprocessing
		// Creating a map from query to their number of terms
		Map<String, Integer> queryToTermNumberMap = new HashMap<String, Integer>();
		List<Query> queriesList = queries.collectAsList();
		for (Query query : queriesList){
			queryToTermNumberMap.put(query.getOriginalQuery(), query.getQueryTerms().size());
		}
		// Broadcast queryToTermNumberMap
		Broadcast<Map<String, Integer>> queryToTermNumberMapBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queryToTermNumberMap);

		// Step 2: Calculate DPH
		// Step 2.1: int currentDocumentLength
		// The length of the document (in terms)
		// NewsLengthFlatMap Merged into NewsFilterFlatMap

		// Step 2.2: long totalDocsInCorpus
		// The total number of documents in the corpus
		long totalDocsInCorpus = news.count();
		// Broadcast totalDocsInCorpus
		Broadcast<Long> totalDocsInCorpusBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(totalDocsInCorpus);

		// Step 2.3: double averageDocumentLengthInCorpus
		// The average document length in the corpus (in terms)
		NewsArticleFiltered newsLengthSumed = newsFiltered.reduce(new NewsLengthReducer());
		int totalDocumentLengthInCorpus = newsLengthSumed.getCurrentDocumentLength();
		double averageDocumentLengthInCorpus = (double)totalDocumentLengthInCorpus / (double)totalDocsInCorpus;
		// Broadcast averageDocumentLengthInCorpus
		Broadcast<Double> averageDocumentLengthInCorpusBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(averageDocumentLengthInCorpus);

		// Step 2.4: short termFrequencyInCurrentDocument
		// Term Frequency (count) of the term in the document
		// Stored inside the dataset
		Dataset<NewsArticleFiltered> newsTermCounted = newsFiltered.flatMap(new CountTermsFlatMap(),
				Encoders.bean(NewsArticleFiltered.class)).cache();

		// Add keys for term here
		KeyValueGroupedDataset<String, NewsArticleFiltered> newsTermKeyAdded = newsTermCounted.groupByKey(
				new NewsTermCountedToTermKey(), Encoders.STRING());

		// Step 2.5: int totalTermFrequencyInCorpus
		// The sum of term frequencies for the term across all documents
		Dataset<Tuple2<String, NewsArticleFiltered>> newsTermSumed = newsTermKeyAdded.reduceGroups(new CorpusTermsReducer()).cache();
		List<Tuple2<String, NewsArticleFiltered>> newsTermSumedList = newsTermSumed.collectAsList();
		Map<String, Short> totalTermFrequencyInCorpusMap = new HashMap<String, Short>();
		for (Tuple2<String, NewsArticleFiltered> tuple : newsTermSumedList) totalTermFrequencyInCorpusMap.put(
				tuple._1, tuple._2.getTermFrequencyInCurrentDocument());
		// Broadcast totalTermFrequencyInCorpus
		Broadcast<Map<String, Short>> totalTermFrequencyInCorpusBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(totalTermFrequencyInCorpusMap);

		// Step 2.6: List<Double> DPHScoreList, double DPHScorePerTerm
		// Calculate the DPH for each item (regarding query term)
		// short termFrequencyInCurrentDocument,
		// int totalTermFrequencyInCorpus,
		// int currentDocumentLength,
		// double averageDocumentLengthInCorpus,
		// long totalDocsInCorpus
		KeyValueGroupedDataset<String, NewsArticleFiltered> newsDPHCalculated = newsTermKeyAdded.mapValues(
				new DPHScoreMap(totalDocsInCorpusBroadcast.value(), averageDocumentLengthInCorpusBroadcast.value(),
						totalTermFrequencyInCorpusBroadcast.value()),
				Encoders.bean(NewsArticleFiltered.class));


		// Step 3: double DPHScoreAverage
		// Calculate average DPH Score among query terms
		// Change key into Tuple2<Query, document(id)> here
		Dataset<NewsArticleFiltered> newsWithoutKey = newsDPHCalculated.flatMapGroups(new KeyDeleteFlatMapGroups(),
				Encoders.bean(NewsArticleFiltered.class)).cache();
		KeyValueGroupedDataset<Tuple2<Query, String>, NewsArticleFiltered> newsQueryKeyAdded = newsWithoutKey.groupByKey(
				new NewsFilteredToQueryTupleKey(), Encoders.tuple(Encoders.bean(Query.class), Encoders.STRING()));
		Dataset<Tuple2<Query, NewsArticleFiltered>> newsArticleFilteredAveraged = newsQueryKeyAdded.mapGroups(
				new DPHScoreMapGroups(),
				Encoders.tuple(Encoders.bean(Query.class), Encoders.bean(NewsArticleFiltered.class))).cache();

		// STEP 4: Output data
		// Transformed dataset into list since the sort method for Tuple2<> is hard to apply.
		List<Tuple2<Query, NewsArticleFiltered>> resultList = newsArticleFilteredAveraged.collectAsList();
		List<DocumentRanking> documentRankingList = new ArrayList<DocumentRanking>();
		// Loop through the entire list to find data belongs to specific query
		for (Query q: queriesList) {
			Iterator<Tuple2<Query, NewsArticleFiltered>> resultIterator = resultList.iterator();
			List<RankedResult> queryList = new ArrayList<RankedResult>();
			while (resultIterator.hasNext()) {
				Tuple2<Query, NewsArticleFiltered> testTuple = resultIterator.next();
				if (testTuple._1.getQueryTerms().equals(q.getQueryTerms())) {
					// Data structure transformation
					RankedResult rankedResult = new RankedResult(testTuple._2.getId(),
							testTuple._2.getArticle(),
							testTuple._2.getDPHScoreAverage());
					queryList.add(rankedResult);
				}
			}
			// Sort and Filter
			Collections.sort(queryList);
			Collections.reverse(queryList);
			List<RankedResult> finalResult = new ArrayList<RankedResult>();
			for (RankedResult r: queryList) {
				boolean accept = true;
				if (finalResult.isEmpty()) {
					finalResult.add(r);
				}
				for (RankedResult f : finalResult) {
					if(TextDistanceCalculator.similarity(f.getArticle().getTitle(), r.getArticle().getTitle()) < 0.5) {
						accept = false;
						break;
					}
				}
				if (accept) {
					finalResult.add(r);
				}
				if (finalResult.size() >= 10) {
					break;
				}
			}
			for (RankedResult test : finalResult) {
				System.out.println(test.getScore());
			}
			documentRankingList.add(new DocumentRanking(q, finalResult));
		}

		return documentRankingList; // replace this with the list of DocumentRanking output by your topology
	}


}
