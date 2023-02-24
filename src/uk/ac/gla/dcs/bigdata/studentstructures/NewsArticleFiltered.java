package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

import java.io.Serializable;
import java.util.List;

/**
 * A structure to store the news after filtering.
 * @author Xiao Zheng
 */
public class NewsArticleFiltered implements Serializable {

    private static final long serialVersionUID = 5106114149354544161L;

    private String id; // unique article identifier
    private List<String> titleFiltered; // article title after filtered
    private List<String> contentsFiltered; // the contents of the article body after filtered
    private short numTerms; // Term Frequency (count) of the term in the document
    private int newsLength; // The length of the document (in terms)
    private List<Double> DPHScoreList; // The DPH Score for each query
    private double DPHScoreAverage; // Averaged DPH Score
    private NewsArticle article; // raw data structure

    /**
     * Empty Constructor
     */
    public NewsArticleFiltered() {};

    public NewsArticleFiltered(String id, List<String> title, List<String> contentsFiltered, short numTerms, int newsLength, List<Double> DPHScoreList, double DPHScoreAverage, NewsArticle article) {
        super();
        this.id = id;
        this.titleFiltered = title;
        this.contentsFiltered = contentsFiltered;
        this.numTerms = numTerms;
        this.newsLength = newsLength;
        this.DPHScoreList = DPHScoreList;
        this.DPHScoreAverage = DPHScoreAverage;
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

    public short getNumTerms() {
        return numTerms;
    }

    public int getNewsLength() {
        return newsLength;
    }

    public List<Double> getDPHScoreList() {
        return DPHScoreList;
    }

    public double getDPHScoreAverage() {
        return DPHScoreAverage;
    }

    public NewsArticle getArticle() {
        return article;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setTitleFiltered(List<String> title) {
        this.titleFiltered = title;
    }

    public void setContentsFiltered(List<String> contentsFiltered) {
        this.contentsFiltered = contentsFiltered;
    }

    public void setNumTerms(short numTerms) {
        this.numTerms = numTerms;
    }

    public void setNewsLength(int newsLength) {
        this.newsLength = newsLength;
    }

    public void setDPHScoreList(List<Double> DPHScoreList) {
        this.DPHScoreList = DPHScoreList;
    }

    public void setDPHScoreAverage(double DPHScoreAverage) {
        this.DPHScoreAverage = DPHScoreAverage;
    }

    public void setArticle(NewsArticle article) {
        this.article = article;
    }
}
