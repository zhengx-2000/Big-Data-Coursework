package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.ReduceFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleFiltered;

import java.util.Map;

/**
 * A reduce function to calculate the average DPH of the document for a query
 * @author Xiao Zheng
 */
public class DPHScoreReduceGroups implements ReduceFunction<NewsArticleFiltered> {

    private static final long serialVersionUID = 2070157499410910019L;

    private Map<String, Integer> queryToTermNumberMap;

    public DPHScoreReduceGroups(Map<String, Integer> queryToTermNumberMap) {
        this.queryToTermNumberMap = queryToTermNumberMap;
    }

    @Override
    public NewsArticleFiltered call(NewsArticleFiltered t1, NewsArticleFiltered t2) throws Exception {
        int termNumber = queryToTermNumberMap.get(t1.getQuery().getOriginalQuery());
        NewsArticleFiltered result = new NewsArticleFiltered();
        result.setId(t1.getId());
        result.setQuery(t1.getQuery());
        result.setArticle(t1.getArticle());
        result.setDPHScoreAverage(t1.getDPHScore() / termNumber + t2.getDPHScore() / termNumber);
        return result;
    }
}
