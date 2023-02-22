package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleFiltered;

/**
 * A filter to remove news article with empty title and pre-process the title and contents.
 * @author Xiao Zheng
 */
public class NewsFilterFlatMap implements FlatMapFunction<NewsArticle, NewsArticleFiltered> {

    private static final long serialVersionUID = 2848720760312831312L;

    /**
     * Default constructor
     */
    public NewsFilterFlatMap() {

    }

    @Override
    public Iterator<NewsArticleFiltered> call(NewsArticle newsUnfiltered) throws Exception {
        if (newsUnfiltered.getTitle() != null) {
            // News has a title
            List<NewsArticleFiltered> newsFiltered = new ArrayList<NewsArticleFiltered>(1);
            TextPreProcessor processor = new TextPreProcessor();
            List<String> contentList = new ArrayList<String>();

            NewsArticleFiltered news = new NewsArticleFiltered();
            news.setId(newsUnfiltered.getId());
            news.setArticle(newsUnfiltered);
            news.setTitleFiltered(processor.process(newsUnfiltered.getTitle()));

            int count = 0;
            for(ContentItem content : newsUnfiltered.getContents()) {
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
            news.setContentsFiltered(contentList);
            newsFiltered.add(news);
            return newsFiltered.iterator();
        }
        else {
            List<NewsArticleFiltered> newsFiltered = new ArrayList<NewsArticleFiltered>(0);
            return newsFiltered.iterator();
        }
    }
}
