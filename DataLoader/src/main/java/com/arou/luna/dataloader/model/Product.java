package com.arou.luna.dataloader.model;

/**
 * Product数据集
 * 3982                            商品ID
 * Fuhlen 富勒 M8眩光舞者时尚节能    商品名称
 * 1057,439,736                    商品分类ID，不需要
 * B009EJN4T2                      亚马逊ID，不需要
 * https://images-cn-4.ssl-image   商品的图片URL
 * 外设产品|鼠标|电脑/办公           商品分类
 * 富勒|鼠标|电子产品|好用|外观漂亮   商品UGC标签
 */
public class Product {
    private long productId;
    private String name;
    private String imageUrl;
    private String categories;
    private String tags;

    public Product(long productId, String name, String imageUrl, String categories, String tags) {
        this.productId = productId;
        this.name = name;
        this.imageUrl = imageUrl;
        this.categories = categories;
        this.tags = tags;
    }

    public long getProductId() {
        return productId;
    }

    public void setProductId(long productId) {
        this.productId = productId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getImageUrl() {
        return imageUrl;
    }

    public void setImageUrl(String imageUrl) {
        this.imageUrl = imageUrl;
    }

    public String getCategories() {
        return categories;
    }

    public void setCategories(String categories) {
        this.categories = categories;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    @Override
    public String toString() {
        return "Product{" +
                "productId=" + productId +
                ", name='" + name + '\'' +
                ", imageUrl='" + imageUrl + '\'' +
                ", categories='" + categories + '\'' +
                ", tags='" + tags + '\'' +
                '}';
    }
}
