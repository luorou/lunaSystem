package com.arou.luna.statistics.model;

import lombok.Data;

import java.util.Objects;
@Data
public class Ratting  {
    private long userId;
    private long productId;
    private double score;
    private long timestamp;

    public Ratting(long userId, long productId, double score, long timestamp) {
        this.userId = userId;
        this.productId = productId;
        this.score = score;
        this.timestamp = timestamp;
    }

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Ratting ratting = (Ratting) o;
        return userId == ratting.userId &&
                productId == ratting.productId &&
                Double.compare(ratting.score, score) == 0 &&
                timestamp == ratting.timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, productId, score, timestamp);
    }
}
