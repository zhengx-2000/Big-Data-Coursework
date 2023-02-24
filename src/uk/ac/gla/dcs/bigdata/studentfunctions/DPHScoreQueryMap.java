package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleFiltered;

/**
 * A mapping function to calculate the average DPH score.
 * @author Xiao Zheng
 * TODO: This function is not implemented
 */
public class DPHScoreQueryMap implements MapFunction<NewsArticleFiltered, NewsArticleFiltered> {
    @Override
    public NewsArticleFiltered call(NewsArticleFiltered newsArticleFiltered) throws Exception {
        return null;
    }
}
