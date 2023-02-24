package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleFiltered;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This function is currently deprecated.
 * @author Xiao Zheng
 */
public class CountTerms implements MapFunction<NewsArticleFiltered, Short> {
    private static final long serialVersionUID = 1726581915634753139L;

    private String term;
    private short count;

    public String getTerm() {
        return term;
    }

    public short getCount() {
        return count;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public void setCount(short count) {
        this.count = count;
    }

    /**
     * Default constructor
     */
    public CountTerms(String term) {
        this.term = term;
    }

    @Override
    public Short call(NewsArticleFiltered news) throws Exception {
        setCount((short) 0);
        for (String word : news.getTitleFiltered()) {
            if (term.equals(word)) count ++;
        }
        for (String word : news.getContentsFiltered()) {
            if (term.equals(word)) count ++;
        }

        return count;
    }
}
