//package com.fei.streaming;
//
//import com.alibaba.fastjson.JSON;
//import com.dobest.elasticsearch.dto.EsResult;
//import com.dobest.elasticsearch.iterator.DataCollection;
//import org.elasticsearch.action.search.ClearScrollRequest;
//import org.elasticsearch.action.search.ClearScrollResponse;
//import org.elasticsearch.action.search.SearchRequest;
//import org.elasticsearch.action.search.SearchResponse;
//import org.elasticsearch.action.search.SearchScrollRequest;
//import org.elasticsearch.client.RequestOptions;
//import org.elasticsearch.common.unit.TimeValue;
//import org.elasticsearch.search.Scroll;
//import org.elasticsearch.search.SearchHit;
//import org.elasticsearch.search.builder.SearchSourceBuilder;
//
//import java.io.IOException;
//import java.util.Iterator;
//
///**
// * @Description:
// * @ClassName: ScrollSearch
// * @Author chengfei
// * @DateTime 2022/7/13 18:13
// **/
//public class ScrollSearch {
//
//    public <T> DataCollection<EsResult<T>> scrollAll(String index, SearchSourceBuilder searchSourceBuilder, Class<T> clazz, int limit) {
//        final int finalLimit = limit > 0 ? limit : (int) countIndex(index);
//        final int batchSize = Math.min(-1 == searchSourceBuilder.size() ? 5000 : searchSourceBuilder.size(), finalLimit);
//
//        DataCollection.DataCollectionWriter<EsResult<T>> dataCollectionWriter = new DataCollection.DataCollectionWriter<EsResult<T>>() {
//            final Scroll scroll = new Scroll(TimeValue.timeValueSeconds(60));
//            final SearchRequest searchRequest = new SearchRequest(index).scroll(scroll).source(searchSourceBuilder.size(batchSize));
//            String scrollId;
//            SearchResponse searchResponse = null;
//            int count = 0;
//
//            @Override
//            protected boolean hasNext() {
//                if (null == scrollId || scrollId.length() == 0) {
//                    try {
//                        searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
//                    } catch (IOException e) {
//                        logger.error("fetch first batch size failed: ", e);
//                    }
//                    assert searchResponse != null;
//                    scrollId = searchResponse.getScrollId();
//                }
//                if (searchResponse.getHits().getHits().length != 0 && count < finalLimit) {
//                    doTransform();
//                    return true;
//                }
//                closeScroll();
//                return false;
//            }
//
//            @Override
//            protected void nextBatch() {
//                if (count >= finalLimit) {
//                    return;
//                }
//                try {
//                    //进行下次查询
//                    searchResponse = client.scroll(new SearchScrollRequest(scrollId).scroll(scroll), RequestOptions.DEFAULT);
//                } catch (IOException e) {
//                    logger.error("fetch second batch size failed: ", e);
//                }
//                scrollId = searchResponse.getScrollId();
//            }
//
//            private void doTransform() {
//                if (searchResponse.getHits().getHits().length != 0) {
//                    Iterator<SearchHit> it = searchResponse.getHits().iterator();
//                    while (it.hasNext() && count < finalLimit) {
//                        SearchHit searchHit = it.next();
//                        EsResult<T> esResult = new EsResult<T>()
//                                .setIndex(searchHit.getIndex())
//                                .setId(searchHit.getId())
//                                .setSource(JSON.parseObject(searchHit.getSourceAsString(), clazz));
//                        add(esResult);
//                        count++;
//                    }
//                }
//            }
//
//            private boolean closeScroll() {
//                //清除滚屏
//                ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
//                //也可以选择setScrollIds()将多个scrollId一起使用
//                clearScrollRequest.addScrollId(scrollId);
//                ClearScrollResponse clearScrollResponse = null;
//                try {
//                    clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
//                } catch (IOException e) {
//                    logger.error("clean scroll failed: ", e);
//                }
//                boolean succeeded = false;
//                if (clearScrollResponse != null) {
//                    succeeded = clearScrollResponse.isSucceeded();
//                }
//                logger.info(String.format("close scroll %b", succeeded));
//                return succeeded;
//            }
//        };
//
//        return dataCollectionWriter.getCollection();
//    }
//}
