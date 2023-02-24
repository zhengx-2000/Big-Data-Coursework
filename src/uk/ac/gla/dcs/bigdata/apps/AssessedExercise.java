package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
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
		// TODO: Step 1: 数据预处理
		Dataset<NewsArticleFiltered> newsFiltered = news.flatMap(new NewsFilterFlatMap(), Encoders.bean(NewsArticleFiltered.class));

		List<Query>queryList = queries.collectAsList();

		// TODO: Step 2: DPH计算
		// TODO: 文章长度 int currentDocumentLength
		// The length of the document (in terms)
		Dataset<NewsArticleFiltered> newsLengthCounted = newsFiltered.flatMap(new NewsLengthFlatMap(), Encoders.bean(NewsArticleFiltered.class));


		// TODO: 数据集中文章数量 long totalDocsInCorpus
		// The total number of documents in the corpus
		long totalDocsInCorpus = newsLengthCounted.count();

		System.out.println("Valid News: " + totalDocsInCorpus);
		//System.out.println("Total News: " + news.count());

		// TODO: 数据集中平均文章长度 double averageDocumentLengthInCorpus
		// The average document length in the corpus (in terms)


		// 进入单次查询（与关键词相关）
		for (int i = 0; i < queries.count(); i++) {
			// Data structure definition
			Dataset<NewsArticleFiltered> newsDPHCalculated = null;

			List<String> queryTerms = queryList.get(i).getQueryTerms();
			System.out.println(queryTerms);

			// 分关键词查询
			for (String queryTerm : queryTerms) {
				// TODO: 单词查询中文章中关键词数量 short termFrequencyInCurrentDocumentMap
				// Term Frequency (count) of the term in the document
				Broadcast<String> termBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queryTerm);
				Dataset<NewsArticleFiltered> newsTermCounted = newsLengthCounted.flatMap(new CountTermsFlatMap(termBroadcast.value()), Encoders.bean(NewsArticleFiltered.class));
				System.out.println("Valid news now: " + newsTermCounted.count());
				// Dataset<Short> newsTermCounted = newsFiltered.map(new CountTerms(queryTerm), Encoders.SHORT());

				// TODO: 单次查询中数据集中关键词数量 int totalTermFrequencyInCorpusMap
				// The sum of term frequencies for the term across all documents
				NewsArticleFiltered newsTermSumed = newsTermCounted.reduce(new CorpusTermsReducer());
				int totalTermFrequencyInCorpusMap = newsTermSumed.getNumTerms();
				System.out.println("Term: " + queryTerm + " Sum: " + totalTermFrequencyInCorpusMap);

				// TODO: 计算单个单词的DPH double DPHScorePerTerm
				// short termFrequencyInCurrentDocument,
				// int totalTermFrequencyInCorpus,
				// int currentDocumentLength,
				// double averageDocumentLengthInCorpus,
				// long totalDocsInCorpus
				newsDPHCalculated = newsTermCounted.map(new DPHScoreTermMap(), Encoders.bean(NewsArticleFiltered.class));

			}
			// TODO: Step 3: 单次查询DPH均分计算 double DPHScorePerQuery
			if (newsDPHCalculated != null) {Dataset<NewsArticleFiltered> newsArticleFilteredAveraged = newsDPHCalculated.map(new DPHScoreQueryMap(), Encoders.bean(NewsArticleFiltered.class));}
			else throw new NullPointerException("newsDPHCalculated is undefined!");

			// TODO: STEP 4: 排序输出单次query的results


		}

		// TODO: Step 5: 合并多次查询


		// return List<DocumentRanking>
		// DocumentRanking: Query query, List<RankedResult> results
		// RankedResult: String docid, NewsArticle article, double score
		return null; // replace this with the list of DocumentRanking output by your topology
	}


}
