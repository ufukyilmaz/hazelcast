package info.jerrinot.compatibilityguardian;

import com.hazelcast.util.collection.ArrayUtils;

import java.lang.reflect.Array;
import java.util.Arrays;

public class Utils {
    public static final boolean DEBUG_ENABLED = true;

    public static RuntimeException rethrow(Exception e) {
        if (e instanceof RuntimeException) {
            throw (RuntimeException)e;
        }
        throw new GuardianException(e);
    }

    public static <T> T[] concatItems(T[] source, T...items) {
        Class<?> componentType = source.getClass().getComponentType();
        T[] newArray = (T[]) Array.newInstance(componentType, source.length + items.length);
        ArrayUtils.concat(source, items, newArray);
        return newArray;
    }

    public static void debug(String text) {
        if (DEBUG_ENABLED) {
            System.out.println(text);
        }
    }
}
