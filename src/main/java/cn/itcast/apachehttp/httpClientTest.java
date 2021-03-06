package cn.itcast.apachehttp;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.ParseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * @program: BigDataDemo
 * @description: 测试类
 * @author: 占翔昊
 * @create 2020-12-16 18:45
 **/

public class httpClientTest {
    @lombok.SneakyThrows
    public static String send(String url, Map<String,String> map, String encoding) {
        String body = "";

        //创建httpclient对象
        CloseableHttpClient client = HttpClients.createDefault();
        //创建post方式请求对象
        HttpPost httpPost = new HttpPost(url);
        //装填参数
        List<NameValuePair> nvps = new ArrayList<NameValuePair>();
        if(map!=null){
            for (Map.Entry<String, String> entry : map.entrySet()) {
                nvps.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
            }
        }
        //设置参数到请求对象中
        httpPost.setEntity(new UrlEncodedFormEntity(nvps, encoding));

        System.out.println("请求地址："+url);
        System.out.println("请求参数："+nvps.toString());

        //设置header信息
        //指定报文头【Content-type】、【User-Agent】
        httpPost.setHeader("Content-type", "application/json");
        httpPost.setHeader("User-Agent", "Mozilla/4.0 (compatible; MSIE 5.0; Windows NT; DigExt)");

        //执行请求操作，并拿到结果（同步阻塞）
        CloseableHttpResponse response = client.execute(httpPost);
        //获取结果实体
        HttpEntity entity = response.getEntity();
        if (entity != null) {
            //按指定编码转换结果实体为String类型
            body = EntityUtils.toString(entity, encoding);
        }
        EntityUtils.consume(entity);
        //释放链接
        response.close();
        return body;

    }
    final static Logger log = LoggerFactory.getLogger(httpClientTest.class);
    public static void main(String[] args) throws ParseException, IOException {
//        String url="http://106.75.227.222:9091/v2/xsheetServer/tablemeta/get";
//        Map<String, String> map = new HashMap<String, String>();
//        map.put("type","postgres");
//        map.put("host", "106.75.227.222");
//        map.put("port", "5432");
//        map.put("username", "postgres");
//        map.put("password", "123456");
//        map.put("dbname", "postgres");
//        map.put("tablename","employee");
//        String body = send(url, map,"utf-8");
//        System.out.println("响应结果：");
//        log.info(body);


    }


}
