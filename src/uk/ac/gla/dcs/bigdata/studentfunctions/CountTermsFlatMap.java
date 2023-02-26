package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.FlatMapFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleFiltered;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A mapping function to calculate the number of specific terms in a news article
 * Also remove news with no keywords
 * @author Xiao Zheng
 */
public class CountTermsFlatMap implements FlatMapFunction<NewsArticleFiltered, NewsArticleFiltered> {

    private static final long serialVersionUID = 1726581915634753139L;

    /**
     * Default constructor
     */
    public CountTermsFlatMap() {}

    @Override
    public Iterator<NewsArticleFiltered> call(NewsArticleFiltered news) throws Exception {
        short numTerms = 0;
        String term = news.getTerm();
        for (String word : news.getTitleFiltered()) {
            if (term.equals(word)) numTerms ++;
        }
        for (String word : news.getContentsFiltered()) {
            if (term.equals(word)) numTerms ++;
        }
        if (numTerms != 0) {
            List<NewsArticleFiltered> result = new ArrayList<NewsArticleFiltered>(1);
            news.setTermFrequencyInCurrentDocument(numTerms);
            result.add(news);
            return result.iterator();
        }
        else {
            List<NewsArticleFiltered> result = new ArrayList<NewsArticleFiltered>(0);
            return result.iterator();
        }

    }
}
