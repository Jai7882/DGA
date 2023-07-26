package com.jia.dga.common.util;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * ClassName: SpringBeanProvider
 * Package: com.jia.dga.common.util
 * Description:
 *
 * @Author jjy
 * @Create 2023/7/25 16:50
 * @Version 1.0
 */
@Component
public class SpringBeanProvider implements ApplicationContextAware {

    ApplicationContext applicationContext = null;

    public <T> T getBean(String beanName,Class<T> tClass){
        return applicationContext.getBean(beanName,tClass);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
