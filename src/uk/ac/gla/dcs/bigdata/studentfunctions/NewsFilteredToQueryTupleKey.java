package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleFiltered;

/**
 * A key mapping function to set the key based on query and document ID.
 * @author Xiao Zheng
 */
public class NewsFilteredToQueryTupleKey implements MapFunction<NewsArticleFiltered, Tuple2<Query, String>> {
    private static final long serialVersionUID = 7582614491553580983L;

    @Override
    public Tuple2<Query, String> call(NewsArticleFiltered news) throws Exception {
        return new Tuple2<Query, String>(news.getQuery(), news.getId());
    }
}
