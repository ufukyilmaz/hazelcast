package com.hazelcast.internal.bplustree;

/**
 * The B+tree composite key comparison result. The composite key consists of the index key and entry key.
 * <p>
 * The index keys comparison relies on the result of Comparable.compareTo(). "null" is less than any other
 * "non-null" index key.
 * <p>
 * The entry keys are compared using serialized (native) form. First, the size of the serialized data
 * is compared and the bigger key is greater. If the entry keys size is equal,
 * the entry keys are compared byte-by-byte. "null" is less than any other "non-null" entry key.
 * <p>
 * The composite key1 is less than composite key2 if the index key1 is less than the index key 2
 * or if the index key1 is equal to the index key2, but the entry key1 is less than the entry key2.
 * <p>
 * The composite key1 is greater than composite key2 if the index key1 is greater than the index key 2
 * or if the index key1 is equal to the index key2, but the entry key1 is greater than the entry key2.
 * <p>
 * The composite key1 is equal to the composite key 2 if both index key and entry key components are equal.
 */
enum CompositeKeyComparison {

    /**
     * The left-hand index key is less than the right-hand index key.
     * The entry key component is not compared.
     */
    INDEX_KEY_LESS(-2),

    /**
     * The left-hand index key is equal to the right-hand index key and the
     * left-hand entry key is less than the right-hand entry-key;
     */
    INDEX_KEY_EQUAL_ENTRY_KEY_LESS(-1),

    /**
     * Both the left-hand index key and entry key are equal to the
     * right-hand index key and entry key; If the caller requested a comparison
     * of the index key component only, the entry keys are not compared.
     */
    KEYS_EQUAL(0),

    /**
     * The left-hand index key is equal to the right-hand index key and the
     * left-hand entry key is greater than the right-hand entry-key.
     */
    INDEX_KEY_EQUAL_ENTRY_KEY_GREATER(1),

    /**
     * The left-hand index key is greater than right-hand index key.
     * The entry key component is not compared.
     */
    INDEX_KEY_GREATER(2);

    private final int id;

    CompositeKeyComparison(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    /**
     * @param cmp the comparison result of the composite keys
     * @return {@code true} if the index key components of the compared keys are equal,
     * {@code false} otherwise.
     */
    public static boolean indexKeysEqual(CompositeKeyComparison cmp) {
        return cmp == KEYS_EQUAL || cmp == INDEX_KEY_EQUAL_ENTRY_KEY_GREATER
                || cmp == INDEX_KEY_EQUAL_ENTRY_KEY_LESS;
    }

    /**
     * @param cmp the comparison result of the composite keys
     * @return {@code true} if both the index key and entry key components are equal,
     * {@code false} otherwise.
     */
    public static boolean keysEqual(CompositeKeyComparison cmp) {
        return cmp == KEYS_EQUAL;
    }

    /**
     * @param cmp the comparison result of the composite keys
     * @return {@code true} if the left-hand composite key is less than the right-hand composite key,
     * {@code false} otherwise.
     */
    public static boolean less(CompositeKeyComparison cmp) {
        return cmp == INDEX_KEY_LESS || cmp == INDEX_KEY_EQUAL_ENTRY_KEY_LESS;
    }

    /**
     * @param cmp the comparison result of the composite keys
     * @return {@code true} if the left-hand composite key is less or equal to the right-hand composite key,
     * {@code false} otherwise.
     */
    public static boolean lessOrEqual(CompositeKeyComparison cmp) {
        return less(cmp) || cmp == KEYS_EQUAL;
    }

    /**
     * @param cmp the comparison result of the composite keys
     * @return {@code true} if the left-hand composite key is greater than the right-hand composite key,
     * {@code false} otherwise.
     */
    public static boolean greater(CompositeKeyComparison cmp) {
        return cmp == INDEX_KEY_GREATER || cmp == INDEX_KEY_EQUAL_ENTRY_KEY_GREATER;
    }

    /**
     * @param cmp the comparison result of the composite keys
     * @return {@code true} if the left-hand composite key is greater or equal than the right-hind composite key,
     * {@code false} otherwise.
     */
    public static boolean greaterOrEqual(CompositeKeyComparison cmp) {
        return greater(cmp) || cmp == KEYS_EQUAL;
    }
}
