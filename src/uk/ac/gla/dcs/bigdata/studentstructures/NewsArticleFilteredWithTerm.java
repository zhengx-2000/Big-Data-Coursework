package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

import java.io.Serializable;

public class NewsArticleFilteredWithTerm implements Serializable {

    private static final long serialVersionUID = 7175250500529292580L;

    private Query query;
    private String term;
    private NewsArticleFiltered newsArticleFiltered;

    public NewsArticleFilteredWithTerm(){}

    public NewsArticleFilteredWithTerm(Query query, String term, NewsArticleFiltered newsArticleFiltered) {
        this.query = query;
        this.term = term;
        this.newsArticleFiltered = newsArticleFiltered;
    }

    public Query getQuery() {
        return query;
    }

    public String getTerm() {
        return term;
    }

    public NewsArticleFiltered getNewsArticleFiltered() {
        return newsArticleFiltered;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public void setNewsArticleFiltered(NewsArticleFiltered newsArticleFiltered) {
        this.newsArticleFiltered = newsArticleFiltered;
    }
}
