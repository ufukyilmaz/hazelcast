package info.jerrinot.compatibilityguardian;

public class Utils {
    public static RuntimeException rethrow(Exception e) {
        if (e instanceof RuntimeException) {
            throw (RuntimeException)e;
        }
        throw new GuardianException(e);
    }
}
