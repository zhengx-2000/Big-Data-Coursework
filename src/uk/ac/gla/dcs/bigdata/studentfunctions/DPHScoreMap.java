package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleFiltered;

import java.util.Map;

/**
 * A mapping function to calculate the DPH score given the 5 components.
 * long totalDocsInCorpus, double averageDocumentLengthInCorpus are from the outside
 * short termFrequencyInCurrentDocument, int currentDocumentLength are from the news structure inside
 * int totalTermFrequencyInCorpus is attained through Map< String, Short> totalTermFrequencyInCorpus
 * @author Xiao Zheng
 * @author Andi Zhang
 */
public class DPHScoreMap implements MapFunction<NewsArticleFiltered, NewsArticleFiltered> {

    private static final long serialVersionUID = 5344673463262863810L;
    
    Broadcast<Map<String, Short>> totalTermFrequencyInCorpusBroadcast;

    private long totalDocsInCorpus;
    private double averageDocumentLengthInCorpus;



    public DPHScoreMap(long totalDocsInCorpus, double averageDocumentLengthInCorpus, Broadcast<Map<String, Short>> totalTermFrequencyInCorpusBroadcast){
        this.totalDocsInCorpus = totalDocsInCorpus;
        this.averageDocumentLengthInCorpus = averageDocumentLengthInCorpus;
        this.totalTermFrequencyInCorpusBroadcast = totalTermFrequencyInCorpusBroadcast;
    }



    @Override
    public NewsArticleFiltered call(NewsArticleFiltered news) throws Exception {
        Map<String, Short> totalTermFrequencyInCorpus = totalTermFrequencyInCorpusBroadcast.value();
    	
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
