package io.github.chrisalbright.utility;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.Locale;

public final class WordFunctions {
    private WordFunctions() {
    }

    public static Iterable<String> lineToWords(final String line) {
        Preconditions.checkArgument(line != null, "Must not pass a null string");
        assert line != null;
        return Lists.newArrayList(line.split("\\s+"));
    }

    public static Iterable<String> trimNonWordCharacters(final Iterable<String> wordList) {
        Preconditions.checkArgument(wordList != null, "Must not pass a null wordlist");
        return Iterables.transform(wordList, new Function<String, String>() {
            @Override
            public String apply(String s) {
                char[] chars = s.toCharArray();
                for (int i = 0; i < chars.length; i++) {
                    if (Character.isAlphabetic(chars[i]) ||
                              Character.isDigit(chars[i])) {
                        String strippedLine = s.replaceAll("[^a-zA-Z]+", " ");
                        return strippedLine;
                    }
                }
                return "";
            }
        });
    }

    public static Iterable<String> lowercaseWords(final Iterable<String> wordList) {
        return lowercaseWords(wordList, Locale.getDefault());
    }

    public static Iterable<String> lowercaseWords(final Iterable<String> wordList, final Locale locale) {
        Preconditions.checkArgument(wordList != null, "Must not pass a null wordlist");
        Preconditions.checkArgument(locale != null, "Must not pass a null locale");
        assert locale != null;
        return Iterables.transform(wordList, new Function<String, String>() {
            @Override
            public String apply(String s) {
                return s.toLowerCase(locale);
            }
        });
    }
}
