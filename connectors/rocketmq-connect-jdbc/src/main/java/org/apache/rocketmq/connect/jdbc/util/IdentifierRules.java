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

import java.util.ArrayList;
import java.util.List;

/**
 * The rules for how identifiers are parsed and quoted.
 */
public class IdentifierRules {
    public static final String UNSUPPORTED_QUOTE = " ";
    private static final String DEFAULT_QUOTE = "\"";
    private static final String DEFAULT_ID_DELIM = ".";

    public static final IdentifierRules DEFAULT = new IdentifierRules(DEFAULT_ID_DELIM,
            DEFAULT_QUOTE
    );

    private final String leadingQuoteString;
    private final String trailingQuoteString;
    private final String identifierDelimiter;

    public IdentifierRules(
            String delimiter,
            String quoteString
    ) {
        this(delimiter, quoteString, quoteString);
    }

    public IdentifierRules(
            String identifierDelimiter,
            String leadingQuoteString,
            String trailingQuoteString
    ) {
        this.leadingQuoteString = leadingQuoteString != null ? leadingQuoteString : DEFAULT_QUOTE;
        this.trailingQuoteString = trailingQuoteString != null ? trailingQuoteString : DEFAULT_QUOTE;
        this.identifierDelimiter = identifierDelimiter != null ? identifierDelimiter : DEFAULT_ID_DELIM;
    }

    /**
     * Get the delimiter that is used to delineate segments within fully-qualified identifiers.
     *
     * @return the identifier delimiter; never null
     */
    public String identifierDelimiter() {
        return identifierDelimiter;
    }

    /**
     * Get the string used as a leading quote.
     *
     * @return the leading quote string; never null
     */
    public String leadingQuoteString() {
        return leadingQuoteString;
    }

    /**
     * Get the string used as a trailing quote.
     *
     * @return the trailing quote string; never null
     */
    public String trailingQuoteString() {
        return trailingQuoteString;
    }

    public ExpressionBuilder expressionBuilder() {
        return new ExpressionBuilder(this);
    }

    public List<String> parseQualifiedIdentifier(String fqn) {
        String orig = fqn;
        String delim = identifierDelimiter();
        String lead = leadingQuoteString();
        String trail = trailingQuoteString();
        List<String> parts = new ArrayList<>();
        int index;
        String segment;
        do {
            if (!lead.equals(UNSUPPORTED_QUOTE) && fqn.startsWith(lead)) {
                int end = fqn.indexOf(trail, lead.length());
                if (end < 0) {
                    throw new IllegalArgumentException(
                            "Failure parsing fully qualified identifier; missing trailing quote in " + orig);
                }
                segment = fqn.substring(lead.length(), end);
                fqn = fqn.substring(end + trail.length());
                if (fqn.startsWith(delim)) {
                    fqn = fqn.substring(delim.length());
                    if (fqn.isEmpty()) {
                        throw new IllegalArgumentException(
                                "Failure parsing fully qualified identifier; ends in delimiter " + orig);
                    }
                }
            } else {
                index = fqn.indexOf(delim, 0);
                if (index == -1) {
                    segment = fqn;
                    fqn = "";
                } else {
                    segment = fqn.substring(0, index);
                    fqn = fqn.substring(index + delim.length());
                    if (fqn.isEmpty()) {
                        throw new IllegalArgumentException(
                                "Failure parsing fully qualified identifier; ends in delimiter " + orig);
                    }
                }
            }
            parts.add(segment);
        } while (fqn.length() > 0);
        return parts;
    }

    public IdentifierRules escapeQuotesWith(String prefix) {
        if (prefix == null || prefix.isEmpty()) {
            return this;
        }
        return new IdentifierRules(
                identifierDelimiter,
                prefix + leadingQuoteString,
                prefix + trailingQuoteString
        );
    }
}
