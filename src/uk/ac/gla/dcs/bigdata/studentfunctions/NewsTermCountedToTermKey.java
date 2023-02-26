package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleFiltered;

/**
 * A mapping function to extract keys
 * @author Xiao Zheng
 */
public class NewsTermCountedToTermKey implements MapFunction<NewsArticleFiltered, String> {
    private static final long serialVersionUID = 2710784100956936250L;

    @Override
    public String call(NewsArticleFiltered newsArticleFiltered) throws Exception {
        return newsArticleFiltered.getTerm();
    }
}
