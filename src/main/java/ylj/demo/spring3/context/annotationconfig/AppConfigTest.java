/**
 *  @author hzyanglujun
 *  @version  创建时间:2016年3月25日 上午11:32:33
 */
package ylj.demo.spring3.context.annotationconfig;

import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

/**
 * @author hzyanglujun
 *
 */
@Configurable
@ComponentScan(basePackages = "ylj.demo.spring3.context.annotationconfig")
@PropertySource(value = "classpath:ylj/demo/spring3/context/annotationconfig/config.properties")
public class AppConfigTest {
    
    @Bean
    public PropertySourcesPlaceholderConfigurer propertyConfigInDev() {
        return new PropertySourcesPlaceholderConfigurer();
    }
    
}