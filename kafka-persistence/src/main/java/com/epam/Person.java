package com.epam;

import java.io.Serializable;

/**
 * Created by Oleksiy_Dyagilev
 */
public class Person implements Serializable {
    private String name;

    public Person(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
