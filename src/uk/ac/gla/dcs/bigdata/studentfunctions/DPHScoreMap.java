package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleFiltered;

import java.util.Map;

/**
 * A mapping function to calculate the DPH score given the 5 components.
 * long totalDocsInCorpus, double averageDocumentLengthInCorpus are from the outside
 * short termFrequencyInCurrentDocument, int currentDocumentLength are from the news structure inside
 * int totalTermFrequencyInCorpus is attained through Map< String, Short> totalTermFrequencyInCorpus
 * @author Xiao Zheng
 */
public class DPHScoreMap implements MapFunction<NewsArticleFiltered, NewsArticleFiltered> {

    private static final long serialVersionUID = 5344673463262863810L;

    private long totalDocsInCorpus;
    private double averageDocumentLengthInCorpus;
    private Map<String, Short> totalTermFrequencyInCorpus;


    public DPHScoreMap(long totalDocsInCorpus, double averageDocumentLengthInCorpus, Map<String, Short> totalTermFrequencyInCorpus){
        this.totalDocsInCorpus = totalDocsInCorpus;
        this.averageDocumentLengthInCorpus = averageDocumentLengthInCorpus;
        this.totalTermFrequencyInCorpus = totalTermFrequencyInCorpus;
    }

    public void setTotalDocsInCorpus(long totalDocsInCorpus) {
        this.totalDocsInCorpus = totalDocsInCorpus;
    }

    public void setAverageDocumentLengthInCorpus(double averageDocumentLengthInCorpus) {
        this.averageDocumentLengthInCorpus = averageDocumentLengthInCorpus;
    }

    public void setTotalTermFrequencyInCorpus(Map<String, Short> totalTermFrequencyInCorpus) {
        this.totalTermFrequencyInCorpus = totalTermFrequencyInCorpus;
    }

    public long getTotalDocsInCorpus() {
        return totalDocsInCorpus;
    }

    public double getAverageDocumentLengthInCorpus() {
        return averageDocumentLengthInCorpus;
    }

    public Map<String, Short> getTotalTermFrequencyInCorpus() {
        return totalTermFrequencyInCorpus;
    }

    @Override
    public NewsArticleFiltered call(NewsArticleFiltered news) throws Exception {
        short termFrequencyInCurrentDocument = news.getTermFrequencyInCurrentDocument();
        int currentDocumentLength = news.getCurrentDocumentLength();
        double DPHScore;
        if (termFrequencyInCurrentDocument == 0) DPHScore = 0;
        else {
            DPHScore = DPHScorer.getDPHScore(termFrequencyInCurrentDocument,
                    totalTermFrequencyInCorpus.get(news.getTerm()), currentDocumentLength,
                    averageDocumentLengthInCorpus, totalDocsInCorpus);
        }
        news.setDPHScore(DPHScore);
        return news;
    }
}
