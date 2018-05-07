package com.upplication.s3fs.channels.multipart;

import lombok.Data;

@Data
public class PartKey implements Comparable<PartKey> {

    private final long start;
    private final long end;

    public static PartKeyBuilder builder() {
        return new PartKeyBuilder();
    }

    @Override
    public int compareTo(PartKey o) {
        if(equals(o)) return 0;
        else if(isAfter(o)) return 1;
        else return -1;
    }

    public boolean isAfter(PartKey partKey) {
        return this.end > partKey.end;
    }

    public long getLength() {
        return end - start;
    }

    public int startAsInt() {
        return Math.toIntExact(start);
    }

    public int lengthAsInt() {
        return Math.toIntExact(getLength());
    }


    public PartKey unionWith(PartKey otherPart) {
        return new PartKey(Math.min(start, otherPart.start), Math.max(end, otherPart.end));
    }

    public static class PartKeyBuilder {

        private long start;
        private long offset;

        public PartKeyBuilder start(long start) {
            this.start = start;
            return this;
        }

        public PartKeyBuilder length(long offset) {
            this.offset = offset;
            return this;
        }

        public PartKey build() {
            return new PartKey(start, start + offset);
        }

    }

}
