package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleFiltered;

/**
 * A filter calculate the real length of news.
 * @author Zhexu Liu
 * @author Xiao Zheng
 */

public class NewsLengthFlatMap implements FlatMapFunction<NewsArticleFiltered, NewsArticleFiltered>{

	private static final long serialVersionUID = 5669565779118309174L;

    /**
     * Default constructor
     */
    public NewsLengthFlatMap() {}

    @Override
    public Iterator<NewsArticleFiltered> call(NewsArticleFiltered news) throws Exception {
    	int size = 0;
		List<String> titleFiltered = news.getTitleFiltered();
		List<String> contentsFiltered = news.getContentsFiltered();
    	if (titleFiltered != null & contentsFiltered != null) {
    		size = titleFiltered.size() + contentsFiltered.size();
    		news.setNewsLength(size);
    		List<NewsArticleFiltered> temp  = new ArrayList<NewsArticleFiltered>(1);
    		temp.add(news); 
    		return temp.iterator();
		}
    	else {
    		List<NewsArticleFiltered> temp  = new ArrayList<NewsArticleFiltered>(0);
    		return temp.iterator();
    	}
	}
}
