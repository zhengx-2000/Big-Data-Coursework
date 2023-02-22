package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.List;

/**
 * A structure to store the news after filtering.
 * @author Xiao Zheng
 */
public class NewsArticleFiltered implements Serializable {

    private static final long serialVersionUID = 5106114149354544161L;

    String id; // unique article identifier
    List<String> title; // article title after filtered
    List<String> contents; // the contents of the article body after filtered

    public NewsArticleFiltered() {};

    public NewsArticleFiltered(String id, List<String> title, List<String> contents) {
        super();
        this.id = id;
        this.title = title;
        this.contents = contents;
    }

    public String getId() {
        return id;
    }

    public List<String> getTitle() {
        return title;
    }

    public List<String> getContents() {
        return contents;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setTitle(List<String> title) {
        this.title = title;
    }

    public void setContents(List<String> contents) {
        this.contents = contents;
    }
}
