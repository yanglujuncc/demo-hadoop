/**
 *  @author hzyanglujun
 *  @version  创建时间:2016年3月25日 上午11:30:10
 */
package ylj.demo.spring3.context.annotationconfig;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/**
 * @author hzyanglujun
 *
 */
@Component(value = "mockConfigTest")
public class MockConfigTest {

    @Value("#{'${server.name}'.split(',')}")
    private List<String> servers;

    @Value("#{'${server.id}'.split(',')}")
    private List<Integer> serverId;
    
    @Value("${server.host:127.0.0.1}")
    private String noProKey;
    
    @Autowired
    private Environment environment;
    
    public void readValues() {
        System.out.println("Services Size : " + servers.size());
        for(String s : servers)
            System.out.println(s);
        System.out.println("ServicesId Size : " + serverId.size());
        for(Integer i : serverId)
            System.out.println(i);
        System.out.println("Server Host : " + noProKey);
        String property = environment.getProperty("server.jdbc");
        System.out.println("Server Jdbc : " + property);        
    }

    public static void main(String[] args) {
        AnnotationConfigApplicationContext annotationConfigApplicationContext = new AnnotationConfigApplicationContext(AppConfigTest.class);
        MockConfigTest mockConfigTest = (MockConfigTest) annotationConfigApplicationContext.getBean("mockConfigTest");
        mockConfigTest.readValues();
    }
}