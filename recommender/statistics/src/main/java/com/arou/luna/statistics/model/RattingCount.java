package com.arou.luna.statistics.model;

import lombok.Data;

import java.util.Objects;

@Data
public class RattingCount {
    private Long productId;
    private long count;

    public RattingCount(Long productId, long count) {
        this.productId = productId;
        this.count = count;
    }

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "RattingCount{" +
                "productId=" + productId +
                ", count=" + count +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RattingCount that = (RattingCount) o;
        return count == that.count &&
                Objects.equals(productId, that.productId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(productId, count);
    }
}
