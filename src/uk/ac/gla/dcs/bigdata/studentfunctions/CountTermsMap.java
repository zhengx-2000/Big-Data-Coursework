package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleFiltered;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A mapping function to calculate the number of specific terms in a news article
 * @author Xiao Zheng
 */
public class CountTermsMap implements MapFunction<NewsArticleFiltered, NewsArticleFiltered> {

    private static final long serialVersionUID = 1726581915634753139L;

    List<String> termsList;
    Map<String, Integer> numTerms = new HashMap<String, Integer>();

    /**
     * Default constructor
     */
    public CountTermsMap(List<String> termsList) {
        this.termsList = termsList;
    }

    public void setTermsList(List<String> termsList) {
        this.termsList = termsList;
    }

    public void setNumTerms(Map<String, Integer> numTerms) {
        this.numTerms = numTerms;
    }

    public List<String> getTermsList() {
        return termsList;
    }

    public Map<String, Integer> getNumTerms() {
        return numTerms;
    }

    @Override
    public NewsArticleFiltered call(NewsArticleFiltered news) throws Exception {
        for (String keyword : getTermsList()) {
            int count = 0;
            for (String word : news.getTitleFiltered()) {
                if (keyword.equals(word)) count ++;
            }
            for (String word : news.getContentsFiltered()) {
                if (keyword.equals(word)) count ++;
            }
            numTerms.put(keyword, count);
        }
        news.setNumTerms(numTerms);
        return news;
    }
}
