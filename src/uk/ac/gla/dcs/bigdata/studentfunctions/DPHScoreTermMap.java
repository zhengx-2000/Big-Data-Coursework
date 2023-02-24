package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.terrier.matching.models.DPH;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleFiltered;

import java.util.List;

/**
 * A mapping function to calculate DPH score list for each query.
 * @author Xiao Zheng
 */
public class DPHScoreTermMap implements MapFunction<NewsArticleFiltered, NewsArticleFiltered> {
    private long totalDocsInCorpus;
    private double averageDocumentLengthInCorpus;
    private int totalTermFrequencyInCorpus;
    private Dataset<NewsArticleFiltered> newsArticle;

    public DPHScoreTermMap(long totalDocsInCorpus, double averageDocumentLengthInCorpus, int totalTermFrequencyInCorpus, Dataset<NewsArticleFiltered> newsArticle){
        this.totalDocsInCorpus = totalDocsInCorpus;
        this.averageDocumentLengthInCorpus = averageDocumentLengthInCorpus;
        this.totalTermFrequencyInCorpus = totalTermFrequencyInCorpus;
        this.newsArticle = newsArticle;
    }

    public void setTotalDocsInCorpus(long totalDocsInCorpus) {
        this.totalDocsInCorpus = totalDocsInCorpus;
    }

    public void setAverageDocumentLengthInCorpus(double averageDocumentLengthInCorpus) {
        this.averageDocumentLengthInCorpus = averageDocumentLengthInCorpus;
    }

    public void setTotalTermFrequencyInCorpus(int totalTermFrequencyInCorpus) {
        this.totalTermFrequencyInCorpus = totalTermFrequencyInCorpus;
    }

    public void setNewsArticle(Dataset<NewsArticleFiltered> newsArticle) {
        this.newsArticle = newsArticle;
    }

    public long getTotalDocsInCorpus() {
        return totalDocsInCorpus;
    }

    public double getAverageDocumentLengthInCorpus() {
        return averageDocumentLengthInCorpus;
    }

    public int getTotalTermFrequencyInCorpus() {
        return totalTermFrequencyInCorpus;
    }

    public Dataset<NewsArticleFiltered> getNewsArticle() {
        return newsArticle;
    }

    @Override
    public NewsArticleFiltered call(NewsArticleFiltered news) throws Exception {
        // short termFrequencyInCurrentDocument,
        // int totalTermFrequencyInCorpus,
        // int currentDocumentLength,
        // double averageDocumentLengthInCorpus,
        // long totalDocsInCorpus
        short termFrequencyInCurrentDocument = news.getTermFrequencyInCurrentDocument();
        int currentDocumentLength = news.getCurrentDocumentLength();
        double DPHScore = DPHScorer.getDPHScore(termFrequencyInCurrentDocument, totalTermFrequencyInCorpus, currentDocumentLength, averageDocumentLengthInCorpus, totalDocsInCorpus);
        List<Double> DPHList = news.getDPHScoreList();
        DPHList.add(DPHScore);
        news.setDPHScoreList(DPHList);
        return news;
    }
}
