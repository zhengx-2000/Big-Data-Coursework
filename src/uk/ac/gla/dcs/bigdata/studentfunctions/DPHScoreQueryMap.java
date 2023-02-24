package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleFiltered;

import java.util.List;

/**
 * A mapping function to calculate the average DPH score.
 * @author Xiao Zheng
 * TODO: This function is not implemented
 */
public class DPHScoreQueryMap implements MapFunction<NewsArticleFiltered, NewsArticleFiltered> {
    public DPHScoreQueryMap(){}
    @Override
    public NewsArticleFiltered call(NewsArticleFiltered news) throws Exception {
        List<Double> DPHList = news.getDPHScoreList();
        double sum = 0.0;
        for (double DPH : DPHList) {
            sum += DPH;
        }
        news.setDPHScoreAverage(sum/DPHList.size());
        return news;
    }
}
