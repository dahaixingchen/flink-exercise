package com.fei.streaming;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @Description:
 * @ClassName: ESSourceFunction
 * @Author chengfei
 * @DateTime 2022/7/12 18:38
 **/
public class ESSourceFunction extends RichParallelSourceFunction<String> implements ResultTypeQueryable<String> {
    Logger logger = LoggerFactory.getLogger(ESSourceFunction.class);
    private final String hosts;
    private final String username;
    private final String password;
    private final String index;
    private final String document_type;
    private Integer fetch_size;
    //...
    private final DeserializationSchema<String> deserializer;
    private volatile boolean isRunning = true;
    private RestClient client;
    private int subTaskIndex = 0;
    private int parallelNum = 0;
    private Long total;
    private Long totalPage;
    private int retryTime = 3;

    public ESSourceFunction(String hosts, String username, String password, String index, String document_type, Integer fetch_size,DeserializationSchema deserializer) {
        this.hosts = hosts;
        this.username = username;
        this.password = password;
        this.index = index;
        this.document_type = document_type;
        this.fetch_size = fetch_size;
        //...
        this.deserializer = deserializer;
    }

    /**
     * @Description: 支持多并行度，多并行度的核心是
     * @author tang
     * @date 2021/11/1 23:16
     */
    @Override
    public TypeInformation<String> getProducedType() {
        return deserializer.getProducedType();
    }
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        if (fetch_size == null) {
            fetch_size = 1000;
        }
        System.out.println(String.format("subTask:%s es source function current task index:%s parallelNum:%s", subTaskIndex, subTaskIndex, parallelNum));
        // 数据源获取
        try {
            System.out.println(String.format("subTask:%s step1:es source function start execute hosts:%s index:%s document_type:%s fetch_size:%s",
                    subTaskIndex, hosts, index, document_type, fetch_size));
            // 修改索引配置 可以查询超10000
            Request settingRequest = new Request("PUT", "/" + index + "/_settings");
            String json = "{\"max_result_window\":\"2147483647\"}";
            settingRequest.setJsonEntity(json);
            client.performRequest(settingRequest);
            System.out.println(String.format("subTask:%s step2:es source function put settings set max_result_window success ", subTaskIndex));

            // 初始化分页
            initPage();
            System.out.println(String.format("subTask:%s step3:es source function get total:%s totalPage:%s", subTaskIndex, total, totalPage));
            int from = 0;
            int size = fetch_size;
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String startTime = format.format(new Date());
            System.out.println(String.format("subTask:%s step4:es source function execute query start from:%s size:%s ", subTaskIndex, from, size));
            SearchHit[] searchHits = null;
            Request request = new Request("GET", "/" + index + "/_search");
            for (int page = 0; page < totalPage; page++) {
                from = (page + 1) * size;
                if (!isRunning) {
                    System.out.println(String.format("subTask:%s es source function query now is not running break while", subTaskIndex));
                    cancel();
                    break;
                }
                if (page % parallelNum != subTaskIndex) {
                    continue;
                }
                System.out.println(String.format("subTask:%s es source function query current page:%s from:%s size:%s", subTaskIndex, page, from, size));
                String queryJson = "{\n" +
                        "  \"query\": {\n" +
                        "    \"match_all\": {}\n" +
                        "  },\n" +
                        "  \"from\":" + from + ",\n" +
                        "  \"size\":" + size + ",\n" +
                        "  \"track_total_hits\":true\n" +
                        "}";
                request.setJsonEntity(queryJson);
                Response response = null;
                try {
                    response = client.performRequest(request);
                } catch (Exception e) {
                    // 请求报错 间隔3秒重试
                    System.err.println(String.format("subTask:%s es source function query request param:%s have error:%s", subTaskIndex, queryJson, e));
                    Thread.sleep(3000);
                    System.err.println(String.format("subTask:%s es source function query sleep 3 s again request param:%s", subTaskIndex, queryJson));
                    response = client.performRequest(request);
                }
                String responseBody = EntityUtils.toString(response.getEntity());
                Map<String, JSONObject> map = JSONObject.parseObject(responseBody, java.util.Map.class);
                JSONObject jsonObject = map.get("hits");
                if (null == jsonObject) {
                    continue;
                }
                JSONArray array = jsonObject.getJSONArray("hits");
                List<String> searchList = array.toJavaList(String.class);
                if (searchList.size() <= 0) {
                    System.out.println(String.format("subTask:%s es source function query result size less than 0 exit query current from:%s page:%s", subTaskIndex, from, page));
                }
                searchList.stream().forEach(sourceAsString -> {
                    if (StringUtils.isBlank(sourceAsString)) {
                        System.out.println(String.format("subTask:%s es source function query row is empty:", subTaskIndex));
                    }
                    Map resultMap = JSONObject.parseObject(sourceAsString, java.util.Map.class);
                    String source = JSONObject.toJSONString(resultMap.get("_source"));
                    try {
                        ctx.collect(source);
                    } catch (Exception e) {
                        System.err.println(String.format("subTask:%s error s source function query ctx collect data:%s have error:%s", subTaskIndex, sourceAsString, ExceptionUtils.getStackTrace(e)));
                        throw new RuntimeException(ExceptionUtils.getStackTrace(e));
                    }
                });
            }
            String endTime = format.format(new Date());
            System.out.println("subTask:"+subTaskIndex+" step5:es source function execute query end startTime:"+startTime+" endTime:"+endTime);
        } catch (Exception e) {
            System.err.println("subTask:"+subTaskIndex+" error es source function query have error:"+ ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(ExceptionUtils.getStackTrace(e));
        } finally {
            System.out.println("subTask:"+subTaskIndex+" step6:es source function query end read cancel client: "+ client);
            cancel();
            System.out.println("subTask:"+subTaskIndex+" step6:es source function query cancel client success.");
            System.out.println();
            System.err.println();

        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        try {
            System.out.println("subTask:"+subTaskIndex+" step6:es source function query end read cancel client: "+ client);
            if (client != null) {
                client.close();
            } else {
                System.out.println("subTask:"+subTaskIndex+" es source function query cancel client but client is null.");
            }
            System.out.println("subTask:"+subTaskIndex+" step6:es source function query cancel client success.");
        } catch (Throwable t) {
            System.err.println("subTask:"+ExceptionUtils.getStackTrace(t)+" error es source function cancel client have error:"+ subTaskIndex);
        }
    }

    /**
     * @Description: 获取总数
     * @author tang
     * @date 2021/10/31 16:09
     */
    public void initPage() throws IOException {
        Request request = new Request("GET", "/" + index + "/_search");
        String queryJson = "{\n" +
                "  \"query\": {\n" +
                "    \"match_all\": {}\n" +
                "  },\n" +
                "  \"track_total_hits\":true\n" +
                "}";
        request.setJsonEntity(queryJson);
        Response response = client.performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        java.util.Map<String, JSONObject> map = JSONObject.parseObject(responseBody, java.util.Map.class);
        JSONObject jsonObject = map.get("hits");
        if (null == jsonObject) {
            total = 0L;
            return;
        }
        total = jsonObject.getJSONObject("total").getLong("value");
        totalPage = total % fetch_size == 0 ? total / fetch_size : (total / fetch_size) + 1;
    }

    /**
     * @Description: 初始化客户端
     * @author tang
     * @date 2021/10/31 16:08
     */
    public void initClient() throws Exception {
        String[] split = hosts.split(";");
        HttpHost[] hosts = new HttpHost[split.length];
        for (int i = 0; i < split.length; i++) {
            String url = split[i];
            String[] s = url.split(":");
            String host = s[1].replaceAll("/", "");
            int port = Integer.parseInt(s[2]);
            String scheme = s[0];
            HttpHost httpHost = new HttpHost(host, port, scheme);
            hosts[i] = httpHost;
        }
        //设置密码
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        //设置超时
        RestClientBuilder builder = RestClient.builder(hosts).setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            @Override
            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                requestConfigBuilder.setConnectTimeout(-1);
                requestConfigBuilder.setSocketTimeout(-1);
                requestConfigBuilder.setConnectionRequestTimeout(-1);
                return requestConfigBuilder;
            }
        }).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                httpClientBuilder.disableAuthCaching();
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });
        client = builder.build();
        if (client == null) {
            System.err.println("subTask:%s es source function init client fail");
            throw new Exception("subTask:%s es source function init client fail");
        }
        System.out.println("subTask:" + subTaskIndex + " step2:es source function init client success ");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RuntimeContext runtimeContext = getRuntimeContext();
        subTaskIndex = runtimeContext.getIndexOfThisSubtask();
        parallelNum = runtimeContext.getNumberOfParallelSubtasks();
        // 初始化生成客户端
        initClient();
    }

    @Override
    public void close() throws Exception {
        System.out.println("subTask:"+subTaskIndex+" step6:es source function query close client start.");
        if (client != null) {
            client.close();
        }
        System.out.println("subTask:"+subTaskIndex+" step6:es source function query close client success.");
    }
}
