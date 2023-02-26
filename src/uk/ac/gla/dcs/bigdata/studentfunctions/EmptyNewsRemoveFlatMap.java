package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleFiltered;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class EmptyNewsRemoveFlatMap implements FlatMapGroupsFunction<String, NewsArticleFiltered, NewsArticleFiltered> {
    private static final long serialVersionUID = 3557274246848237685L;

    @Override
    public Iterator<NewsArticleFiltered> call(String s, Iterator<NewsArticleFiltered> newsList) throws Exception {
        List<NewsArticleFiltered> result = new ArrayList<NewsArticleFiltered>();
        if (newsList.hasNext()) {
            NewsArticleFiltered news = newsList.next();
            if (!news.getId().equals("Empty")) {
                result.add(news);
            }
        }
        return result.iterator();
    }
}
