package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.ReduceFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleFiltered;

/**
 * A reducer to calculate
 */
public class DPHScoreReducer implements ReduceFunction<NewsArticleFiltered> {
    @Override
    public NewsArticleFiltered call(NewsArticleFiltered newsArticleFiltered, NewsArticleFiltered t1) throws Exception {
        return null;
    }
}
