package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapGroupsFunction;
import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleFiltered;

import java.util.Iterator;

public class DPHScoreMapGroups implements MapGroupsFunction<Tuple2<Query, String>, NewsArticleFiltered, Tuple2<Query, NewsArticleFiltered>> {
    @Override
    public Tuple2<Query, NewsArticleFiltered> call(Tuple2<Query, String> key, Iterator<NewsArticleFiltered> iterator) throws Exception {
        double averageDPH = 0.0;
        NewsArticleFiltered result = new NewsArticleFiltered();
        int count = 0;
        while (iterator.hasNext()) {
            NewsArticleFiltered news = iterator.next();
            if (result.getId() == null) {
                result.setId(news.getId());
                result.setQuery(news.getQuery());
                result.setArticle(news.getArticle());
            }
            averageDPH += news.getDPHScore();
            count++;
        }

        averageDPH /= count;
        result.setDPHScoreAverage(averageDPH);

        return new Tuple2<Query, NewsArticleFiltered>(key._1, result);
    }
}
