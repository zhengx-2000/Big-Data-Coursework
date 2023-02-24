package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.ReduceFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleFiltered;

/**
 * A reducer to count the total number of terms in the dataset.
 * @author Xiao Zheng
 */
public class CorpusTermsReducer implements ReduceFunction<NewsArticleFiltered> {

    private static final long serialVersionUID = 162707192224829079L;

    public CorpusTermsReducer() {}

    @Override
    public NewsArticleFiltered call(NewsArticleFiltered t1, NewsArticleFiltered t2) throws Exception {
        NewsArticleFiltered result = new NewsArticleFiltered();
        result.setTermFrequencyInCurrentDocument((short) (t1.getTermFrequencyInCurrentDocument() + t2.getTermFrequencyInCurrentDocument()));
        return result;
    }
}
