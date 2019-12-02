package com.hazelcast.security;

import com.hazelcast.security.impl.SecretStrengthRule;
import com.hazelcast.security.impl.weaksecretrules.DictionaryRule;
import com.hazelcast.security.impl.weaksecretrules.LargeKeySpaceRule;
import com.hazelcast.security.impl.weaksecretrules.MinLengthRule;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.EnumSet;

import static com.hazelcast.security.impl.WeakSecretError.DICT_WORD;
import static com.hazelcast.security.impl.WeakSecretError.MIN_LEN;
import static com.hazelcast.security.impl.WeakSecretError.NO_ALPHA;
import static com.hazelcast.security.impl.WeakSecretError.NO_MIXED_CASE;
import static com.hazelcast.security.impl.WeakSecretError.NO_NUMERAL;
import static com.hazelcast.security.impl.WeakSecretError.NO_SPECIAL_CHARS;
import static com.hazelcast.test.HazelcastTestSupport.assertContains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WeakSecretPolicyTest {

    @Test
    public void invalidLengthShort() {
        SecretStrengthRule rule = new MinLengthRule();
        assertEquals(EnumSet.of(MIN_LEN), rule.check("one"));
    }

    @Test
    public void invalidLengthEmpty() {
        SecretStrengthRule rule = new MinLengthRule();
        assertEquals(EnumSet.of(MIN_LEN), rule.check(""));
    }

    @Test
    public void invalidLengthNull() {
        SecretStrengthRule rule = new MinLengthRule();
        assertEquals(EnumSet.of(MIN_LEN), rule.check(null));
    }

    @Test
    public void noNumerals() {
        SecretStrengthRule rule = new LargeKeySpaceRule();
        assertContains(rule.check("one"), NO_NUMERAL);
    }

    @Test
    public void noMixedCase_whenAllLower() {
        SecretStrengthRule rule = new LargeKeySpaceRule();
        assertContains(rule.check("one"), NO_MIXED_CASE);
    }

    @Test
    public void noMixedCase_whenAllCaps() {
        SecretStrengthRule rule = new LargeKeySpaceRule();
        assertContains(rule.check("ONE"), NO_MIXED_CASE);
    }

    @Test
    public void noSpecialChars() {
        SecretStrengthRule rule = new LargeKeySpaceRule();
        assertContains(rule.check("1one2"), NO_SPECIAL_CHARS);
    }

    @Test
    public void noAlphabetic() {
        SecretStrengthRule rule = new LargeKeySpaceRule();
        assertContains(rule.check("1234"), NO_ALPHA);
    }

    @Test
    public void noDictionaryWord() {
        assumeTrue(DictionaryRule.isAvailable());

        SecretStrengthRule rule = new DictionaryRule();
        assertContains(rule.check("alpha"), DICT_WORD);
        assertContains(rule.check("beta"), DICT_WORD);
        assertContains(rule.check("dictionary"), DICT_WORD);
    }
}
