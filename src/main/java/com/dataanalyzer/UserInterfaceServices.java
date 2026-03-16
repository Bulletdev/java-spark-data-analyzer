package com.dataanalyzer;

import com.dataanalyzer.core.DataAggregator;
import com.dataanalyzer.core.DataExporter;
import com.dataanalyzer.core.DataJoiner;
import com.dataanalyzer.core.DataLoader;
import com.dataanalyzer.core.DataProfiler;
import com.dataanalyzer.core.DataTransformer;
import com.dataanalyzer.core.SqlExecutor;
import com.dataanalyzer.ui.InputReader;
import com.dataanalyzer.ui.MenuRenderer;
import com.dataanalyzer.util.OperationHistory;

/**
 * Groups all service dependencies required by {@link UserInterface}.
 *
 * <p>Using this holder reduces the {@code UserInterface} constructor
 * parameter count while keeping dependencies explicit. Use the
 * {@link Builder} to create instances.
 */
public final class UserInterfaceServices {

    /** Data loading service. */
    private final DataLoader loader;

    /** Data transformation service. */
    private final DataTransformer transformer;

    /** Data aggregation service. */
    private final DataAggregator aggregator;

    /** Data export service. */
    private final DataExporter exporter;

    /** User input reader. */
    private final InputReader input;

    /** Menu renderer. */
    private final MenuRenderer menu;

    /** SQL execution service. */
    private final SqlExecutor sqlExecutor;

    /** Join service. */
    private final DataJoiner joiner;

    /** Data profiling service. */
    private final DataProfiler profiler;

    /** Operation history tracker. */
    private final OperationHistory history;

    /**
     * Private constructor — use {@link Builder}.
     *
     * @param builder the builder instance
     */
    private UserInterfaceServices(final Builder builder) {
        this.loader      = builder.loader;
        this.transformer = builder.transformer;
        this.aggregator  = builder.aggregator;
        this.exporter    = builder.exporter;
        this.input       = builder.input;
        this.menu        = builder.menu;
        this.sqlExecutor = builder.sqlExecutor;
        this.joiner      = builder.joiner;
        this.profiler    = builder.profiler;
        this.history     = builder.history;
    }

    /**
     * Returns the data loader.
     *
     * @return data loader service
     */
    public DataLoader getLoader() {
        return loader;
    }

    /**
     * Returns the data transformer.
     *
     * @return transformation service
     */
    public DataTransformer getTransformer() {
        return transformer;
    }

    /**
     * Returns the data aggregator.
     *
     * @return aggregation service
     */
    public DataAggregator getAggregator() {
        return aggregator;
    }

    /**
     * Returns the data exporter.
     *
     * @return export service
     */
    public DataExporter getExporter() {
        return exporter;
    }

    /**
     * Returns the input reader.
     *
     * @return user input reader
     */
    public InputReader getInput() {
        return input;
    }

    /**
     * Returns the menu renderer.
     *
     * @return menu renderer
     */
    public MenuRenderer getMenu() {
        return menu;
    }

    /**
     * Returns the SQL executor.
     *
     * @return SQL execution service
     */
    public SqlExecutor getSqlExecutor() {
        return sqlExecutor;
    }

    /**
     * Returns the data joiner.
     *
     * @return join service
     */
    public DataJoiner getJoiner() {
        return joiner;
    }

    /**
     * Returns the data profiler.
     *
     * @return data profiling service
     */
    public DataProfiler getProfiler() {
        return profiler;
    }

    /**
     * Returns the operation history.
     *
     * @return operation history tracker
     */
    public OperationHistory getHistory() {
        return history;
    }

    /**
     * Builder for {@link UserInterfaceServices}.
     */
    public static final class Builder {

        /** Data loading service. */
        private DataLoader loader;

        /** Data transformation service. */
        private DataTransformer transformer;

        /** Data aggregation service. */
        private DataAggregator aggregator;

        /** Data export service. */
        private DataExporter exporter;

        /** User input reader. */
        private InputReader input;

        /** Menu renderer. */
        private MenuRenderer menu;

        /** SQL execution service. */
        private SqlExecutor sqlExecutor;

        /** Join service. */
        private DataJoiner joiner;

        /** Data profiling service. */
        private DataProfiler profiler;

        /** Operation history tracker. */
        private OperationHistory history;

        /**
         * Sets the data loader.
         *
         * @param val the loader
         * @return this builder
         */
        public Builder loader(final DataLoader val) {
            this.loader = val;
            return this;
        }

        /**
         * Sets the data transformer.
         *
         * @param val the transformer
         * @return this builder
         */
        public Builder transformer(final DataTransformer val) {
            this.transformer = val;
            return this;
        }

        /**
         * Sets the data aggregator.
         *
         * @param val the aggregator
         * @return this builder
         */
        public Builder aggregator(final DataAggregator val) {
            this.aggregator = val;
            return this;
        }

        /**
         * Sets the data exporter.
         *
         * @param val the exporter
         * @return this builder
         */
        public Builder exporter(final DataExporter val) {
            this.exporter = val;
            return this;
        }

        /**
         * Sets the input reader.
         *
         * @param val the input reader
         * @return this builder
         */
        public Builder input(final InputReader val) {
            this.input = val;
            return this;
        }

        /**
         * Sets the menu renderer.
         *
         * @param val the menu renderer
         * @return this builder
         */
        public Builder menu(final MenuRenderer val) {
            this.menu = val;
            return this;
        }

        /**
         * Sets the SQL executor.
         *
         * @param val the SQL executor
         * @return this builder
         */
        public Builder sqlExecutor(final SqlExecutor val) {
            this.sqlExecutor = val;
            return this;
        }

        /**
         * Sets the data joiner.
         *
         * @param val the joiner
         * @return this builder
         */
        public Builder joiner(final DataJoiner val) {
            this.joiner = val;
            return this;
        }

        /**
         * Sets the data profiler.
         *
         * @param val the profiler
         * @return this builder
         */
        public Builder profiler(final DataProfiler val) {
            this.profiler = val;
            return this;
        }

        /**
         * Sets the operation history.
         *
         * @param val the history tracker
         * @return this builder
         */
        public Builder history(final OperationHistory val) {
            this.history = val;
            return this;
        }

        /**
         * Builds the {@link UserInterfaceServices} instance.
         *
         * @return a new UserInterfaceServices
         */
        public UserInterfaceServices build() {
            return new UserInterfaceServices(this);
        }
    }
}
