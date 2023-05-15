/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.connect.jdbc.util;

import org.apache.rocketmq.connect.jdbc.schema.column.ColumnId;

public class ExpressionBuilder {

    @FunctionalInterface
    public interface Expressable {

        void appendTo(
            ExpressionBuilder builder,
            boolean useQuotes
        );
        default void appendTo(
            ExpressionBuilder builder,
            QuoteMethod useQuotes
        ) {
            switch (useQuotes) {
                case ALWAYS:
                    appendTo(builder, true);
                    break;
                case NEVER:
                default:
                    // do nothing
                    break;
            }
        }
    }

    @FunctionalInterface
    public interface Transform<T> {
        void apply(
            ExpressionBuilder builder,
            T input
        );
    }
    public interface ListBuilder<T> {
        ListBuilder<T> delimitedBy(String delimiter);
        <R> ListBuilder<R> transformedBy(Transform<R> transform);

        ExpressionBuilder of(Iterable<? extends T> objects);
        default ExpressionBuilder of(Iterable<? extends T> objects1, Iterable<? extends T> objects2) {
            of(objects1);
            return of(objects2);
        }

        default ExpressionBuilder of(
                Iterable<? extends T> objects1,
                Iterable<? extends T> objects2,
                Iterable<? extends T> objects3
        ) {
            of(objects1);
            of(objects2);
            return of(objects3);
        }
    }

    public static Transform<String> quote() {
        return (builder, input) -> builder.appendColumnName(input);
    }

    public static Transform<ColumnId> columnNames() {
        return (builder, input) -> builder.appendColumnName(input.name());
    }

    public static Transform<ColumnId> columnNamesWith(final String appended) {
        return (builder, input) -> {
            builder.appendColumnName(input.name());
            builder.append(appended);
        };
    }

    public static Transform<ColumnId> placeholderInsteadOfColumnNames(final String str) {
        return (builder, input) -> builder.append(str);
    }

    public static Transform<ColumnId> columnNamesWithPrefix(final String prefix) {
        return (builder, input) -> {
            builder.append(prefix);
            builder.appendColumnName(input.name());
        };
    }

    public static ExpressionBuilder create() {
        return new ExpressionBuilder();
    }

    protected static final QuoteMethod DEFAULT_QUOTE_METHOD = QuoteMethod.ALWAYS;

    private final IdentifierRules rules;
    private final StringBuilder sb = new StringBuilder();
    private QuoteMethod quoteSqlIdentifiers = DEFAULT_QUOTE_METHOD;

    public ExpressionBuilder() {
        this(null);
    }

    public ExpressionBuilder(IdentifierRules rules) {
        this.rules = rules != null ? rules : IdentifierRules.DEFAULT;
    }

    public ExpressionBuilder setQuoteIdentifiers(QuoteMethod method) {
        this.quoteSqlIdentifiers = method != null ? method : DEFAULT_QUOTE_METHOD;
        return this;
    }

    public ExpressionBuilder escapeQuotesWith(String prefix) {
        if (prefix == null || prefix.isEmpty()) {
            return this;
        }
        return new ExpressionBuilder(this.rules.escapeQuotesWith(prefix));
    }
    public ExpressionBuilder appendIdentifierDelimiter() {
        sb.append(rules.identifierDelimiter());
        return this;
    }

    public ExpressionBuilder appendLeadingQuote() {
        return appendLeadingQuote(QuoteMethod.ALWAYS);
    }


    protected ExpressionBuilder appendLeadingQuote(QuoteMethod method) {
        switch (method) {
            case ALWAYS:
                sb.append(rules.leadingQuoteString());
                break;
            case NEVER:
            default:
                break;
        }
        return this;
    }

    public ExpressionBuilder appendTrailingQuote() {
        return appendTrailingQuote(QuoteMethod.ALWAYS);
    }

    protected ExpressionBuilder appendTrailingQuote(QuoteMethod method) {
        switch (method) {
            case ALWAYS:
                sb.append(rules.trailingQuoteString());
                break;
            case NEVER:
            default:
                break;
        }
        return this;
    }

    public ExpressionBuilder appendStringQuote() {
        sb.append("'");
        return this;
    }

    public ExpressionBuilder appendStringQuoted(Object name) {
        appendStringQuote();
        sb.append(name);
        appendStringQuote();
        return this;
    }

    @Deprecated
    public ExpressionBuilder appendIdentifier(
            String name,
            boolean quoted
    ) {
        return appendIdentifier(name, quoted ? QuoteMethod.ALWAYS : QuoteMethod.NEVER);
    }

    public ExpressionBuilder appendIdentifier(
            String name,
            QuoteMethod quoted
    ) {
        appendLeadingQuote(quoted);
        sb.append(name);
        appendTrailingQuote(quoted);
        return this;
    }

    public ExpressionBuilder appendTableName(String name) {
        return appendTableName(name, quoteSqlIdentifiers);
    }

    public ExpressionBuilder appendTableName(String name, QuoteMethod quote) {
        appendLeadingQuote(quote);
        sb.append(name);
        appendTrailingQuote(quote);
        return this;
    }

    public ExpressionBuilder appendColumnName(String name) {
        return appendColumnName(name, quoteSqlIdentifiers);
    }

    public ExpressionBuilder appendColumnName(String name, QuoteMethod quote) {
        appendLeadingQuote(quote);
        sb.append(name);
        appendTrailingQuote(quote);
        return this;
    }

    public ExpressionBuilder appendIdentifierQuoted(String name) {
        appendLeadingQuote();
        sb.append(name);
        appendTrailingQuote();
        return this;
    }

    public ExpressionBuilder appendBinaryLiteral(byte[] value) {
        return append("x'").append(BytesUtil.toHex(value)).append("'");
    }

    public ExpressionBuilder appendNewLine() {
        sb.append(System.lineSeparator());
        return this;
    }

    @Deprecated
    public ExpressionBuilder append(
            Object obj,
            boolean useQuotes
    ) {
        return append(obj, useQuotes ? QuoteMethod.ALWAYS : QuoteMethod.NEVER);
    }

    public ExpressionBuilder append(
            Object obj,
            QuoteMethod useQuotes
    ) {
        if (obj instanceof Expressable) {
            ((Expressable) obj).appendTo(this, useQuotes);
        } else if (obj != null) {
            sb.append(obj);
        }
        return this;
    }

    public ExpressionBuilder append(Object obj) {
        return append(obj, quoteSqlIdentifiers);
    }

    public <T> ExpressionBuilder append(
            T obj,
            Transform<T> transform
    ) {
        if (transform != null) {
            transform.apply(this, obj);
        } else {
            append(obj);
        }
        return this;
    }

    protected class BasicListBuilder<T> implements ListBuilder<T> {
        private final String delimiter;
        private final Transform<T> transform;
        private boolean first = true;

        BasicListBuilder() {
            this(", ", null);
        }

        BasicListBuilder(String delimiter, Transform<T> transform) {
            this.delimiter = delimiter;
            this.transform = transform != null ? transform : ExpressionBuilder::append;
        }

        @Override
        public ListBuilder<T> delimitedBy(String delimiter) {
            return new BasicListBuilder<T>(delimiter, transform);
        }

        @Override
        public <R> ListBuilder<R> transformedBy(Transform<R> transform) {
            return new BasicListBuilder<>(delimiter, transform);
        }

        @Override
        public ExpressionBuilder of(Iterable<? extends T> objects) {
            for (T obj : objects) {
                if (first) {
                    first = false;
                } else {
                    append(delimiter);
                }
                append(obj, transform);
            }
            return ExpressionBuilder.this;
        }
    }

    public ListBuilder<Object> appendList() {
        return new BasicListBuilder<>();
    }

    public ExpressionBuilder appendMultiple(
            String delimiter,
            String expression,
            int times
    ) {
        for (int i = 0; i < times; i++) {
            if (i > 0) {
                append(delimiter);
            }
            append(expression);
        }
        return this;
    }

    @Override
    public String toString() {
        return sb.toString();
    }
}
