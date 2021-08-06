package com.chris.es.entity;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @Author Lilun
 * @Date 2021-08-06 13:21
 * @Description
 **/
@Data
@Accessors(chain = true)
public class Book {
    private String id;
    private String name;
    private String sex;
    private Integer age;
    private String content;
}
