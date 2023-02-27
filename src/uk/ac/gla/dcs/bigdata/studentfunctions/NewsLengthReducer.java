package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.ReduceFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleFiltered;

/**
 * A reducer function to calculate the total number of words
 * @author Xiao Zheng
 */
public class NewsLengthReducer implements ReduceFunction<NewsArticleFiltered> {

    private static final long serialVersionUID = 1775657003510349423L;

    /**
     * Default constructor
     */
    public NewsLengthReducer(){}

    @Override
    public NewsArticleFiltered call(NewsArticleFiltered t1, NewsArticleFiltered t2) throws Exception {
        NewsArticleFiltered result = new NewsArticleFiltered();
        int l1 = t1.getCurrentDocumentLength();
        int l2 = t2.getCurrentDocumentLength();
        if (l1 + l2 > 0){
            result.setCurrentDocumentLength(l1 + l2);
        }
        return result;
    }
}
