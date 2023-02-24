package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleFiltered;

/**
 * A mapping function to calculate DPH score list for each query.
 * @author Xiao Zheng
 * TODO: Needed to be implemented
 */
public class DPHScoreTermMap implements MapFunction<NewsArticleFiltered, NewsArticleFiltered> {
    @Override
    public NewsArticleFiltered call(NewsArticleFiltered newsArticleFiltered) throws Exception {
        return null;
    }
}
