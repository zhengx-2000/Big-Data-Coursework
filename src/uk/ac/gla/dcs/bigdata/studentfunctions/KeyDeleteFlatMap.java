package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleFiltered;

import java.util.Iterator;

/**
 * A simple flat map helping function to delete key for a keyValuedGroupedDataset
 * @author Xiao Zheng
 */
public class KeyDeleteFlatMap implements FlatMapGroupsFunction<String, NewsArticleFiltered, NewsArticleFiltered> {

    private static final long serialVersionUID = 7969302770643517626L;

    @Override
    public Iterator<NewsArticleFiltered> call(String key, Iterator<NewsArticleFiltered> news) throws Exception {
        return news;
    }
}
