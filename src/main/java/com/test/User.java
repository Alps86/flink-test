package com.test;

// define function logic
public class User {
    private String name;
    private Integer score;

    public User(String name, Integer score) {
        this.name = name;
        this.score = score;
    }

    public String getName() {
        return name;
    }

    public Integer getScore() {
        return score;
    }
}
