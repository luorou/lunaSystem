package com.arou.luna.dataloader.model;

public class Ratting {
    private long userId;
    private long productId;
    private double score;
    private long timestamp;

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getProductId() {
        return productId;
    }

    public void setProductId(long productId) {
        this.productId = productId;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Ratting{" +
                "userId=" + userId +
                ", productId=" + productId +
                ", score=" + score +
                ", timestamp=" + timestamp +
                '}';
    }
}
