package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import uk.ac.gla.dcs.bigdata.studentfunctions.*;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleFiltered;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleFilteredWithTerm;

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
		// TODO: Step 1: 数据预处理 & 文章长度 int currentDocumentLength
		Dataset<NewsArticleFiltered> newsFiltered = news.flatMap(new NewsFilterFlatMap(queries.collectAsList()), Encoders.bean(NewsArticleFiltered.class));

		// TODO: I need a map from query to their number of terms
		Map<String, Integer> queryToTermNumberMap = new HashMap<String, Integer>();
		List<Query> queriesList = queries.collectAsList();
		for (Query query : queriesList){
			queryToTermNumberMap.put(query.getOriginalQuery(), query.getQueryTerms().size());
		}
		// TODO: Broadcast queryToTermNumberMap
		Broadcast<Map<String, Integer>> queryToTermNumberMapBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queryToTermNumberMap);

		// TODO: Step 2: DPH计算
		// TODO: 文章长度 int currentDocumentLength
		// The length of the document (in terms)
		// NewsLengthFlatMap Merged into NewsFilterFlatMap

		// TODO: 数据集中文章数量 long totalDocsInCorpus
		// The total number of documents in the corpus
		long totalDocsInCorpus = newsFiltered.count();
		System.out.println("Valid News: " + totalDocsInCorpus);
		// TODO: Broadcast totalDocsInCorpus
		Broadcast<Long> totalDocsInCorpusBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(totalDocsInCorpus);

		// TODO: 数据集中平均文章长度 double averageDocumentLengthInCorpus
		// The average document length in the corpus (in terms)
		NewsArticleFiltered newsLengthSumed = newsFiltered.reduce(new NewsLengthReducer());
		int totalDocumentLengthInCorpus = newsLengthSumed.getCurrentDocumentLength();
		double averageDocumentLengthInCorpus = (double)totalDocumentLengthInCorpus / (double)totalDocsInCorpus;
		//System.out.println("Average document length: " + averageDocumentLengthInCorpus);
		// TODO: Broadcast averageDocumentLengthInCorpus
		Broadcast<Double> averageDocumentLengthInCorpusBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(averageDocumentLengthInCorpus);

		// TODO: 单次查询中文章中关键词数量 short termFrequencyInCurrentDocument
		// Term Frequency (count) of the term in the document
		Dataset<NewsArticleFiltered> newsTermCounted = newsFiltered.flatMap(new CountTermsFlatMap(), Encoders.bean(NewsArticleFiltered.class));
		System.out.println("newsTermCounted: " + newsTermCounted.count());
		/*List<NewsArticleFiltered> testList1 = newsTermCounted.collectAsList();
		for (int i = 0; i < testList1.size(); i++) {
			System.out.println("QueryTerm: " + testList1.get(i).getQuery().getQueryTerms());
		}*/

		// TODO: Add keys for term here
		KeyValueGroupedDataset<String, NewsArticleFiltered> newsTermKeyAdded = newsTermCounted.groupByKey(new NewsTermCountedToTermKey(), Encoders.STRING());

		// TODO: 单次查询中数据集中关键词数量 int totalTermFrequencyInCorpus
		// The sum of term frequencies for the term across all documents
		Dataset<Tuple2<String, NewsArticleFiltered>> newsTermSumed = newsTermKeyAdded.reduceGroups(new CorpusTermsReducer());
		List<Tuple2<String, NewsArticleFiltered>> newsTermSumedList = newsTermSumed.collectAsList();
		Map<String, Short> totalTermFrequencyInCorpusMap = new HashMap<String, Short>();
		for (Tuple2<String, NewsArticleFiltered> tuple : newsTermSumedList) totalTermFrequencyInCorpusMap.put(tuple._1, tuple._2.getTermFrequencyInCurrentDocument());
		// TODO: Broadcast totalTermFrequencyInCorpus
		Broadcast<Map<String, Short>> totalTermFrequencyInCorpusBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(totalTermFrequencyInCorpusMap);

		// TODO: 计算单个单词的DPH List<Double> DPHScoreList, double DPHScorePerTerm
		// short termFrequencyInCurrentDocument,
		// int totalTermFrequencyInCorpus,
		// int currentDocumentLength,
		// double averageDocumentLengthInCorpus,
		// long totalDocsInCorpus
		KeyValueGroupedDataset<String, NewsArticleFiltered> newsDPHCalculated = newsTermKeyAdded.mapValues(
				new DPHScoreMap(totalDocsInCorpusBroadcast.value(), averageDocumentLengthInCorpusBroadcast.value(),
						totalTermFrequencyInCorpusBroadcast.value()),
				Encoders.bean(NewsArticleFiltered.class));


		// TODO: Step 3: 单次查询DPH均分计算 double DPHScoreAverage
		// TODO: We need to change key for Tuple2<Query, document(id)> here
		Dataset<NewsArticleFiltered> newsWithoutKey = newsDPHCalculated.flatMapGroups(new KeyDeleteFlatMap(), Encoders.bean(NewsArticleFiltered.class));

		KeyValueGroupedDataset<Tuple2<Query, String>, NewsArticleFiltered> newsQueryKeyAdded = newsWithoutKey.groupByKey(new NewsFilteredToQueryTupleKey(), Encoders.tuple(Encoders.bean(Query.class), Encoders.STRING()));

		Dataset<Tuple2<Tuple2<Query, String>, NewsArticleFiltered>> newsArticleFilteredAveraged = newsQueryKeyAdded.reduceGroups(new DPHScoreReduceGroups(queryToTermNumberMapBroadcast.value()));
		Dataset<Tuple2<Query, NewsArticleFiltered>> newsArticleFilteredAveraged1 = newsQueryKeyAdded.mapGroups(new DPHScoreMapGroups(), Encoders.tuple(Encoders.bean(Query.class), Encoders.bean(NewsArticleFiltered.class)));
		List<Tuple2<Query, NewsArticleFiltered>> testList = newsArticleFilteredAveraged1.collectAsList();
		Iterator<Tuple2<Query, NewsArticleFiltered>> testIterator = testList.iterator();
		while (testIterator.hasNext()) {
			Tuple2<Query, NewsArticleFiltered> testTuple = testIterator.next();
			System.out.println("Query: " + testTuple._1.getQueryTerms() + " DPH: " + testTuple._2.getDPHScoreAverage());
		}
		//System.out.println("newsArticleFilteredAveraged: " + newsArticleFilteredAveraged.count());
		//List<Tuple2<Tuple2<Query, String>, NewsArticleFiltered>> testList = newsArticleFilteredAveraged.collectAsList();
		/*for (int j = 0; j < testList.size(); j++) {
			System.out.println("DPHScore: " + testList.get(j).getDPHScoreAverage());
		}*/

		// TODO: STEP 4: 排序输出单次query的results
		//Dataset<NewsArticleFiltered> queryResult = newsArticleFilteredAveraged.orderBy(col("DPHScoreAverage").desc_nulls_last());
		//rankedQueryResult = queryResult.flatMap(new NewsArticleFilteredToRankedResultFlatMap(), Encoders.bean(RankedResult.class));
		/*List<NewsArticleFiltered> testList = result.collectAsList();
		for (int j = 0; j < testList.size(); j++) {
			System.out.println("DPHScoreAverage: " + testList.get(j).getDPHScoreAverage());
		}*/



		// TODO: Step 5: 合并多次查询
		//Dataset<DocumentRanking> queriesResult = rankedQueryResult.map(new RankedResultToDocumentRankingMap(), Encoders.bean(DocumentRanking.class))
		//List<DocumentRanking> queriesList = queriesResult.collectAsList();
		// return List<DocumentRanking>
		// DocumentRanking: Query query, List<RankedResult> results
		// RankedResult: String docid, NewsArticle article, double score
		return null; // replace this with the list of DocumentRanking output by your topology
	}


}
