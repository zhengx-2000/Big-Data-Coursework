package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.FlatMapFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleFiltered;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A mapping function to calculate the number of specific terms in a news article
 * All remove articles with no keywords, since their DPH will be set to 0 by algorithm.
 * @author Xiao Zheng
 */
public class CountTermsFlatMap implements FlatMapFunction<NewsArticleFiltered, NewsArticleFiltered> {

    private static final long serialVersionUID = 1726581915634753139L;

    private String term;

    /**
     * Default constructor
     */
    public CountTermsFlatMap(String term) {
        this.term = term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public String getTerm() {
        return term;
    }

    @Override
    public Iterator<NewsArticleFiltered> call(NewsArticleFiltered news) throws Exception {
        short numTerms = 0;
        List<NewsArticleFiltered> newsList = new ArrayList<NewsArticleFiltered>();
        for (String word : news.getTitleFiltered()) {
            if (term.equals(word)) numTerms ++;
        }
        for (String word : news.getContentsFiltered()) {
            if (term.equals(word)) numTerms ++;
        }
        if (numTerms != 0) {
            news.setTermFrequencyInCurrentDocument(numTerms);
            newsList.add(news);
        }
        return newsList.iterator();
    }
}
