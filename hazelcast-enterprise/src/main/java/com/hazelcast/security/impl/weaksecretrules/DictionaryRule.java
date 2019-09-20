package com.hazelcast.security.impl.weaksecretrules;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.security.impl.SecretStrengthRule;
import com.hazelcast.security.impl.WeakSecretError;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.EnumSet;

import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.security.impl.WeakSecretError.DICT_WORD;
import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static java.lang.System.getProperty;

public class DictionaryRule
        implements SecretStrengthRule {

    private static final String CUSTOM_WORD_LIST_PATH_PROPERTY = "hazelcast.security.dictionary.policy.wordlist.path";
    private static final String DEFAULT_WORD_LIST_PATH = "/usr/share/dict/words";

    private final ILogger logger = Logger.getLogger(getClass());

    @Override
    public EnumSet<WeakSecretError> check(CharSequence secret) {
        String wordListPath = getProperty(CUSTOM_WORD_LIST_PATH_PROPERTY, DEFAULT_WORD_LIST_PATH);
        File wordList = new File(wordListPath);
        if (!wordList.exists()) {
            logger.warning("Dictionary secret policy disabled, word-list file <" + wordListPath + "> not found.");
            return EnumSet.noneOf(WeakSecretError.class);
        }

        BufferedReader br = null;
        try {
            br = new BufferedReader(
                    new InputStreamReader(
                            new FileInputStream(new File(wordListPath)), "UTF-8"));
            String line;
            while ((line = br.readLine()) != null) {
                if (line.equals(secret)) {
                    return EnumSet.of(DICT_WORD);
                }
            }
        } catch (IOException e) {
            sneakyThrow(e);
        } finally {
            closeResource(br);
        }

        return EnumSet.noneOf(WeakSecretError.class);
    }

    public static boolean isAvailable() {
        String wordListPath = getProperty(CUSTOM_WORD_LIST_PATH_PROPERTY, DEFAULT_WORD_LIST_PATH);
        return new File(wordListPath).exists();
    }
}
