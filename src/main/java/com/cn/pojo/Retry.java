package com.cn.pojo;

import lombok.Data;
import org.springframework.stereotype.Component;
@Data
@Component
public class Retry {

    private int baseSleepTimeMs;

    private int maxRetries;
}
