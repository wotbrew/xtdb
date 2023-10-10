package xtdb.trie;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.sorting.IndirectSort;
import org.apache.arrow.flatbuf.Int;
import org.apache.arrow.memory.util.ArrowBufPointer;
import xtdb.vector.IVectorReader;

import java.util.Arrays;
import java.util.function.IntBinaryOperator;
import java.util.stream.IntStream;

public record LiveHashTrie(Node rootNode, IVectorReader iidReader) implements HashTrie<LiveHashTrie.Node> {

    private static final int LOG_LIMIT = 64;
    private static final int PAGE_LIMIT = 1024;
    private static final int PAGE_BYTE_LIMIT = 512 * 1024;
    private static final int MAX_LEVEL = 64;

    public sealed interface Node extends HashTrie.Node<Node> {
        default Node add(LiveHashTrie trie, int idx) {
            return add(trie, idx, 0);
        }

        Node add(LiveHashTrie trie, int idx, int size);

        Node compactLogs(LiveHashTrie trie);
    }

    public static class Builder {
        private final IVectorReader iidReader;
        private int logLimit = LOG_LIMIT;
        private int pageLimit = PAGE_LIMIT;
        private int pageByteLimit = PAGE_BYTE_LIMIT;
        private byte[] rootPath = new byte[0];

        private Builder(IVectorReader iidReader) {
            this.iidReader = iidReader;
        }

        public void setLogLimit(int logLimit) {
            this.logLimit = logLimit;
        }

        public void setPageLimit(int pageLimit) {
            this.pageLimit = pageLimit;
        }

        public void setPageByteLimit(int pageByteLimit) {
            this.pageByteLimit = pageByteLimit;
        }

        public void setRootPath(byte[] path) {
            this.rootPath = path;
        }

        public LiveHashTrie build() {
            return new LiveHashTrie(new Leaf(logLimit, pageLimit, pageByteLimit, rootPath), iidReader);
        }
    }

    public static Builder builder(IVectorReader iidReader) {
        return new Builder(iidReader);
    }

    @SuppressWarnings("unused")
    public static LiveHashTrie emptyTrie(IVectorReader iidReader) {
        return builder(iidReader).build();
    }

    public LiveHashTrie add(int idx, int size) {
        return new LiveHashTrie(rootNode.add(this, idx, size), iidReader);
    }

    @SuppressWarnings("unused")
    public LiveHashTrie withIidReader(IVectorReader iidReader) {
        return new LiveHashTrie(rootNode, iidReader);
    }

    public LiveHashTrie compactLogs() {
        return new LiveHashTrie(rootNode.compactLogs(this), iidReader);
    }

    private static final ThreadLocal<ArrowBufPointer> BUCKET_BUF_PTR = ThreadLocal.withInitial(ArrowBufPointer::new);

    private int bucketFor(int idx, int level) {
        return HashTrie.bucketFor(iidReader.getPointer(idx, BUCKET_BUF_PTR.get()), level);
    }

    private static final ThreadLocal<ArrowBufPointer> LEFT_BUF_PTR = ThreadLocal.withInitial(ArrowBufPointer::new);
    private static final ThreadLocal<ArrowBufPointer> RIGHT_BUF_PTR = ThreadLocal.withInitial(ArrowBufPointer::new);

    private int compare(int leftIdx, int rightIdx) {
        int cmp = iidReader.getPointer(leftIdx, LEFT_BUF_PTR.get()).compareTo(iidReader.getPointer(rightIdx, RIGHT_BUF_PTR.get()));
        if (cmp != 0) return cmp;

        // sort by idx desc
        return Integer.compare(rightIdx, leftIdx);
    }

    private static byte[] conjPath(byte[] path, byte idx) {
        int currentPathLength = path.length;
        var childPath = new byte[currentPathLength + 1];
        System.arraycopy(path, 0, childPath, 0, currentPathLength);
        childPath[currentPathLength] = idx;
        return childPath;
    }

    public record Branch(int logLimit, int pageLimit, int pageByteLimit, byte[] path, Node[] children) implements Node {

        @Override
        public Node add(LiveHashTrie trie, int idx, int size) {
            var bucket = trie.bucketFor(idx, path.length);

            var newChildren = IntStream.range(0, children.length)
                    .mapToObj(childIdx -> {
                        var child = children[childIdx];
                        if (bucket == childIdx) {
                            if (child == null) {
                                child = new Leaf(logLimit, pageLimit, pageByteLimit, conjPath(path, (byte) childIdx));
                            }
                            child = child.add(trie, idx, size);
                        }
                        return child;
                    }).toArray(Node[]::new);

            return new Branch(logLimit, pageLimit, pageByteLimit, path, newChildren);
        }

        @Override
        public Node compactLogs(LiveHashTrie trie) {
            var children =
                    Arrays.stream(this.children)
                            .map(child -> child == null ? null : child.compactLogs(trie))
                            .toArray(Node[]::new);

            return new Branch(logLimit, pageLimit, pageByteLimit, path, children);
        }
    }

    public record Leaf(int logLimit, int pageLimit, int pageByteLimit, byte[] path, int[] data, int[] dataSizes, int[] log, int[] logSizes, int logCount) implements Node {

        static int[] permutation(LiveHashTrie trie, int[] positions, int len) {
            return IndirectSort.mergesort(0, len, (l, r) -> trie.compare(positions[l], positions[r]));
        }

        static void permute(int[] permutation, int[] in, int[] out) {
            for (int i = 0; i < permutation.length; i++) {
                var p = permutation[i];
                out[i] = in[p];
            }
        }

        Leaf(int logLimit, int pageLimit, int pageByteLimit, byte[] path) {
            this(logLimit, pageLimit, pageByteLimit, path, new int[0], new int[0]);
        }

        private Leaf(int logLimit, int pageLimit, int pageByteLimit, byte[] path, int[] data, int[] dataSizes) {
            this(logLimit, pageLimit, pageByteLimit, path, data, dataSizes, new int[logLimit], new int[logLimit], 0);
        }

        @Override
        public Node[] children() {
            return null;
        }

        private Branch partitionLeaf(LiveHashTrie trie, int[] newData, int[] newSizes) {
            var buckets = new IntArrayList[LEVEL_WIDTH];
            var nodes = new Node[LEVEL_WIDTH];

            for (int i = 0; i < newData.length; i++) {
                var dataPos = newData[i];
                var bucket = trie.bucketFor(dataPos, path.length);
                var bucketList = buckets[bucket];
                if (bucketList == null) {
                    bucketList = new IntArrayList();
                    buckets[bucket] = bucketList;
                }
                bucketList.add(i);
            }

            for (int bucket = 0; bucket < LEVEL_WIDTH; bucket++) {
                var bucketList = buckets[bucket];
                if (bucketList != null) {
                    var bucketData = new int[bucketList.size()];
                    var bucketSizes = new int[bucketList.size()];
                    for (int i = 0; i < bucketList.size(); i++) {
                        var p = bucketList.get(i);
                        bucketData[i] = newData[p];
                        bucketSizes[i] = newSizes[p];
                    }
                    nodes[bucket] = new Leaf(logLimit, pageLimit, pageByteLimit, conjPath(path, (byte) bucket), bucketData, bucketSizes);
                }
            }
            return new Branch(logLimit, pageLimit, pageByteLimit, path, nodes);
        }

        @Override
        public Node compactLogs(LiveHashTrie trie) {
            if (logCount == 0) return this;

            var newLength = this.data.length + logCount;
            var concatenatedData = new int[newLength];
            var concatenatedSizes = new int[newLength];

            System.arraycopy(this.data, 0, concatenatedData, 0, this.data.length);
            System.arraycopy(this.log, 0, concatenatedData, this.data.length, logCount);
            System.arraycopy(this.dataSizes, 0, concatenatedSizes, 0, this.data.length);
            System.arraycopy(this.logSizes, 0, concatenatedSizes, this.data.length, logCount);

            var permutation = permutation(trie, concatenatedData, newLength);
            var newData = new int[newLength];
            var newSizes = new int[newLength];
            permute(permutation, concatenatedData, newData);
            permute(permutation, concatenatedSizes, newSizes);

            long byteCount = 0;
            for (int newSize : newSizes) {
                byteCount += newSize;
            }

            var pageLimitExceeded = newData.length > this.pageLimit;
            var byteLimitExceeded = byteCount > this.pageByteLimit;
            var canBranch = path.length < MAX_LEVEL;

            if ((pageLimitExceeded || byteLimitExceeded) && canBranch) {
                return partitionLeaf(trie, newData, newSizes);
            } else {
                return new Leaf(logLimit, pageLimit, pageByteLimit, path, newData, newSizes, new int[this.logLimit], new int[this.logLimit], 0);
            }
        }

        @Override

        public Node add(LiveHashTrie trie, int newIdx, int size) {
            var logCount = this.logCount;
            log[logCount] = newIdx;
            logSizes[logCount] = size;
            logCount++;
            var newLeaf = new Leaf(logLimit, pageLimit, pageByteLimit, path, data, dataSizes, log, logSizes, logCount);
            return logCount == this.logLimit ? newLeaf.compactLogs(trie) : newLeaf;
        }
    }
}
