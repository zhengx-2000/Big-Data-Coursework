package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleFiltered;

/**
 * A filter to remove news article with empty title and pre-process the title and contents.
 * Also calculated document length (in terms)
 * Also added key value information (query, term)
 * @author Xiao Zheng
 * @author Zhexu Liu
 * @author Andi Zhang
 */
public class NewsFilterFlatMap implements FlatMapFunction<NewsArticle, NewsArticleFiltered> {

    private static final long serialVersionUID = 2848720760312831312L;

    private List<Query> queries;

    /**
     * Default constructor
     */
    public NewsFilterFlatMap(List<Query> queries) {
        this.queries = queries;
    }

    @Override
    public Iterator<NewsArticleFiltered> call(NewsArticle newsUnfiltered) throws Exception {
        if (newsUnfiltered.getTitle() != null) {
            // News has a title
            List<NewsArticleFiltered> newsFilteredList = new ArrayList<NewsArticleFiltered>();
            TextPreProcessor processor = new TextPreProcessor();
            List<String> contentList = new ArrayList<String>();

            NewsArticleFiltered news = new NewsArticleFiltered();
            news.setId(newsUnfiltered.getId());
            news.setArticle(newsUnfiltered);
            List<String> titleFiltered = processor.process(newsUnfiltered.getTitle());
            news.setTitleFiltered(titleFiltered);

            int count = 0;
            for(ContentItem content : newsUnfiltered.getContents()) {
                if (content != null) {
                    if (content.getContent() != null & content.getSubtype() != null) {
                        if (content.getSubtype().equals("paragraph")) {
                            // Content is not empty and the type is paragraph
                            contentList.addAll(processor.process(content.getContent()));
                            count++;
                            if (count > 4) {
                                break;
                            }
                        }
                    }
                }
            }
            int size = 0;
            size = titleFiltered.size() + contentList.size();
    		news.setCurrentDocumentLength(size);
            news.setContentsFiltered(contentList);

            // Add key values here
            for (Query q : queries){
                for (String t : q.getQueryTerms()) {
                    newsFilteredList.add(new NewsArticleFiltered(news.getId(), news.getTitleFiltered(), news.getContentsFiltered(),
                            news.getTermFrequencyInCurrentDocument(), news.getCurrentDocumentLength(), news.getDPHScore(),
                            news.getDPHScoreAverage(), news.getArticle(), q, t));
                }
            }

            return newsFilteredList.iterator();
        }
        else {
            List<NewsArticleFiltered> newsFilteredList = new ArrayList<NewsArticleFiltered>(0);
            return newsFilteredList.iterator();
        }
    }
}
