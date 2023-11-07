package com.cn.pojo;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
@Data
@Component
@ConfigurationProperties(prefix = "curator")
public class ZKProperties {

    private String connectString;

    private int sessionTimeOut;

    private int connectionTimeOut;

    private String nameSpace;

    private Retry retry;
}
