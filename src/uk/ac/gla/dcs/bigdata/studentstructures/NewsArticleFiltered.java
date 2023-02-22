package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * A structure to store the news after filtering.
 * @author Xiao Zheng
 */
public class NewsArticleFiltered implements Serializable {

    private static final long serialVersionUID = 5106114149354544161L;

    String id; // unique article identifier
    List<String> titleFiltered; // article title after filtered
    List<String> contentsFiltered; // the contents of the article body after filtered
    Map<String, Integer> numTerms; // Term Frequency (count) of the term in the document
    int newsLength; // The length of the document (in terms)
    NewsArticle article; // raw data structure

    public NewsArticleFiltered() {};

    public NewsArticleFiltered(String id, List<String> title, List<String> contentsFiltered, Map<String, Integer> numTerms, int newsLength, NewsArticle article) {
        super();
        this.id = id;
        this.titleFiltered = title;
        this.contentsFiltered = contentsFiltered;
        this.numTerms = numTerms;
        this.newsLength = newsLength;
        this.article = article;
    }

    public String getId() {
        return id;
    }

    public List<String> getTitleFiltered() {
        return titleFiltered;
    }

    public List<String> getContentsFiltered() {
        return contentsFiltered;
    }

    public Map<String, Integer> getNumTerms() {
        return numTerms;
    }

    public int getNewsLength() {
        return newsLength;
    }

    public NewsArticle getArticle() {
        return article;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setTitle(List<String> title) {
        this.titleFiltered = title;
    }

    public void setContentsFiltered(List<String> contentsFiltered) {
        this.contentsFiltered = contentsFiltered;
    }

    public void setNumTerms(Map<String, Integer> numTerms) {
        this.numTerms = numTerms;
    }

    public void setNewsLength(int newsLength) {
        this.newsLength = newsLength;
    }

    public void setArticle(NewsArticle article) {
        this.article = article;
    }
}
