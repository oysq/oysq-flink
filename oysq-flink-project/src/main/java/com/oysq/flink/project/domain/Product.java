package com.oysq.flink.project.domain;

public class Product {

    /**
     * 商品名称
     * 矿泉水
     */
    private String name;

    /**
     * 商品类别
     * 食品饮料
     */
    private String category;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    @Override
    public String toString() {
        return "Product{" +
                "name='" + name + '\'' +
                ", category='" + category + '\'' +
                '}';
    }
}
