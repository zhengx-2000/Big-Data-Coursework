package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

import java.io.Serializable;
import java.util.ArrayList;
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
    private short termFrequencyInCurrentDocument; // Term Frequency (count) of the term in the document
    private int currentDocumentLength; // The length of the document (in terms)
    private double DPHScore; // The DPH Score for each query
    private double DPHScoreAverage; // Averaged DPH Score
    private NewsArticle article; // raw data structure
    private Query query; // Current Query information
    private String term; // Current Term searched

    /**
     * Empty Constructor
     */
    public NewsArticleFiltered() {
        titleFiltered = new ArrayList<String>();
        contentsFiltered = new ArrayList<String>();
    };

    public NewsArticleFiltered(String id, List<String> title, List<String> contentsFiltered,
                               short termFrequencyInCurrentDocument, int currentDocumentLength,
                               double DPHScore, double DPHScoreAverage, NewsArticle article,
                               Query query, String term) {
        super();
        this.id = id;
        this.titleFiltered = title;
        this.contentsFiltered = contentsFiltered;
        this.termFrequencyInCurrentDocument = termFrequencyInCurrentDocument;
        this.currentDocumentLength = currentDocumentLength;
        this.DPHScore = DPHScore;
        this.DPHScoreAverage = DPHScoreAverage;
        this.article = article;
        this.query = query;
        this.term = term;
    }

    public NewsArticleFiltered(String id, List<String> title, List<String> contentsFiltered,
		            int currentDocumentLength, NewsArticle article,
		            Query query, String term) {
		super();
		this.id = id;
		this.titleFiltered = title;
		this.contentsFiltered = contentsFiltered;
		this.currentDocumentLength = currentDocumentLength;
		this.article = article;
		this.query = query;
		this.term = term;
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

    public short getTermFrequencyInCurrentDocument() {
        return termFrequencyInCurrentDocument;
    }

    public int getCurrentDocumentLength() {
        return currentDocumentLength;
    }

    public double getDPHScore() {
        return DPHScore;
    }

    public double getDPHScoreAverage() {
        return DPHScoreAverage;
    }

    public NewsArticle getArticle() {
        return article;
    }

    public Query getQuery() {
        return query;
    }

    public String getTerm() {
        return term;
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

    public void setTermFrequencyInCurrentDocument(short termFrequencyInCurrentDocument) {
        this.termFrequencyInCurrentDocument = termFrequencyInCurrentDocument;
    }

    public void setCurrentDocumentLength(int currentDocumentLength) {
        this.currentDocumentLength = currentDocumentLength;
    }

    public void setDPHScore(double DPHScore) {
        this.DPHScore = DPHScore;
    }

    public void setDPHScoreAverage(double DPHScoreAverage) {
        this.DPHScoreAverage = DPHScoreAverage;
    }

    public void setArticle(NewsArticle article) {
        this.article = article;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public void setTerm(String term) {
        this.term = term;
    }
}
