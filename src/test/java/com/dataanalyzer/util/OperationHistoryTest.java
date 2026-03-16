package com.dataanalyzer.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link OperationHistory}.
 */
class OperationHistoryTest {

    private OperationHistory history;

    @BeforeEach
    void setUp() {
        history = new OperationHistory();
    }

    @Test
    void record_addsEntry() {
        history.record("Filter", "Categoria = Moveis");

        assertEquals(1, history.getEntries().size());
        assertTrue(history.getEntries().get(0).contains("Filter"));
        assertTrue(history.getEntries().get(0).contains("Categoria = Moveis"));
    }

    @Test
    void record_multipleEntries_allPresent() {
        history.record("Load", "test.csv");
        history.record("Filter", "x > 10");
        history.record("Export", "out/");

        assertEquals(3, history.getEntries().size());
    }

    @Test
    void clear_removesAll() {
        history.record("Load", "test.csv");
        history.record("Filter", "x > 10");

        history.clear();

        assertTrue(history.getEntries().isEmpty());
    }

    @Test
    void toText_containsEntries() {
        history.record("Load", "test.csv");
        history.record("Filter", "x > 10");

        String text = history.toText();

        assertTrue(text.contains("Load"));
        assertTrue(text.contains("Filter"));
        assertTrue(text.contains("\n"));
    }

    @Test
    void toText_emptyHistory_returnsEmptyString() {
        assertEquals("", history.toText());
    }

    @Test
    void getEntries_isUnmodifiable() {
        history.record("Load", "test.csv");

        List<String> entries = history.getEntries();

        assertThrows(UnsupportedOperationException.class, () ->
            entries.add("manual entry")
        );
    }
}
