package io.github.chrisalbright.utility;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.Locale;

public final class WordFunctions {
    private WordFunctions() {
    }

    public static Iterable<String> lineToWords(final String line) {
        Preconditions.checkArgument(line != null, "Must not pass a null string");
        assert line != null;
        return lowercaseWords(filterAllNumbers(filterEmptyWords(Lists.newArrayList(trimNonWordCharacters(line).split("\\s+")))));
    }

    public static String trimNonWordCharacters(final String word) {
        String strippedWord = word.replaceAll("[^a-zA-Z0-9]+", " ");
        return strippedWord;
    }
    public static Iterable<String> trimNonWordCharacters(final Iterable<String> wordList) {
        Preconditions.checkArgument(wordList != null, "Must not pass a null wordlist");
        return Iterables.transform(wordList, new Function<String, String>() {
            @Override
            public String apply(String word) {
                return trimNonWordCharacters(word);
            }
        });
    }

    public static Iterable<String> filterAllNumbers(final Iterable<String> wordList) {
        Preconditions.checkArgument(wordList != null, "Must not pass a null wordlist");
        return Iterables.filter(wordList, new Predicate<String>() {
            @Override
            public boolean apply(String s) {
                return ! s.matches("[0-9]+");
            }
        });

    }

    public static Iterable<String> filterEmptyWords(final Iterable<String> wordList) {
        Preconditions.checkArgument(wordList != null, "Must not pass a null wordlist");
        return Iterables.filter(wordList, new Predicate<String>() {
            @Override
            public boolean apply(String s) {
                return null != s && !s.isEmpty();
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
