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
package org.apache.rocketmq.connect.jdbc.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.jdbc.exception.ConfigException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * config def
 */
public class ConfigDef {

    private static final Pattern COMMA_WITH_WHITESPACE = Pattern.compile("\\s*,\\s*");
    /**
     * A unique Java object which represents the lack of a default value.
     */
    public static final Object NO_DEFAULT_VALUE = new Object();

    private final Map<String, ConfigKey> configKeys;

    public ConfigDef() {
        configKeys = new LinkedHashMap<>();
    }


    public ConfigDef define(ConfigKey key) {
        if (configKeys.containsKey(key.name)) {
            throw new ConfigException("Configuration " + key.name + " is defined twice.");
        }
        configKeys.put(key.name, key);
        return this;
    }

    /**
     * no validate
     * @param name
     * @param type
     * @param defaultValue
     * @param validator
     * @param documentation
     * @return
     */
    public ConfigDef define(String name, Type type, Object defaultValue, Validator validator, String documentation) {
        return define(new ConfigKey(name, type, defaultValue, validator, documentation));
    }

    /**
     * validate config
     * @param name
     * @param type
     * @param defaultValue
     * @param documentation
     * @return
     */
    public ConfigDef define(String name, Type type, Object defaultValue, String documentation) {
        return define(new ConfigKey(name, type, defaultValue, null, documentation));
    }


    /**
     * Get the configuration keys
     *
     * @return a map containing all configuration keys
     */
    public Map<String, ConfigKey> configKeys() {
        return configKeys;
    }

    /**
     * parse props
     *
     * @param props
     * @return
     */
    public Map<String, Object> parse(Map<?, ?> props) {
        // parse all known keys
        Map<String, Object> values = new HashMap<>();
        for (ConfigKey key : configKeys.values()) {
            values.put(key.name, parseValue(key, props.get(key.name), props.containsKey(key.name)));
        }
        return values;
    }

    /**
     * convert + validate
     *
     * @param key
     * @param value
     * @param isSet
     * @return
     */
    Object parseValue(ConfigKey key, Object value, boolean isSet) {
        Object parsedValue;
        if (isSet) {
            parsedValue = parseType(key.name, value, key.type);
            // props map doesn't contain setting, the key is required because no default value specified - its an error
        } else if (NO_DEFAULT_VALUE.equals(key.defaultValue)) {
            throw new ConfigException("Missing required configuration \"" + key.name + "\" which has no default value.");
        } else {
            parsedValue = key.defaultValue;
        }
        if (key.validator != null) {
            key.validator.ensureValid(key.name, parsedValue);
        }
        return parsedValue;
    }


    /**
     * Parse a value according to its expected type.
     *
     * @param name  The config name
     * @param value The config value
     * @param type  The expected type
     * @return The parsed object
     */
    public static Object parseType(String name, Object value, Type type) {
        try {
            if (value == null) {
                return null;
            }
            String trimmed = null;
            if (value instanceof String) {
                trimmed = ((String) value).trim();
            }
            switch (type) {
                case BOOLEAN:
                    if (value instanceof String) {
                        if (trimmed.equalsIgnoreCase("true")) {
                            return true;
                        } else if (trimmed.equalsIgnoreCase("false")) {
                            return false;
                        } else {
                            throw new ConfigException(name, value, "Expected value to be either true or false");
                        }
                    } else if (value instanceof Boolean) {
                        return value;
                    } else {
                        throw new ConfigException(name, value, "Expected value to be either true or false");
                    }
                case STRING:
                    if (value instanceof String) {
                        return trimmed;
                    } else {
                        throw new ConfigException(name, value, "Expected value to be a string, but it was a " + value.getClass().getName());
                    }
                case INT:
                    if (value instanceof Integer) {
                        return value;
                    } else if (value instanceof String) {
                        return Integer.parseInt(trimmed);
                    } else {
                        throw new ConfigException(name, value, "Expected value to be a 32-bit integer, but it was a " + value.getClass().getName());
                    }
                case SHORT:
                    if (value instanceof Short) {
                        return value;
                    } else if (value instanceof String) {
                        return Short.parseShort(trimmed);
                    } else {
                        throw new ConfigException(name, value, "Expected value to be a 16-bit integer (short), but it was a " + value.getClass().getName());
                    }
                case LONG:
                    if (value instanceof Integer) {
                        return ((Integer) value).longValue();
                    }
                    if (value instanceof Long) {
                        return value;
                    } else if (value instanceof String) {
                        return Long.parseLong(trimmed);
                    } else {
                        throw new ConfigException(name, value, "Expected value to be a 64-bit integer (long), but it was a " + value.getClass().getName());
                    }
                case DOUBLE:
                    if (value instanceof Number) {
                        return ((Number) value).doubleValue();
                    } else if (value instanceof String) {
                        return Double.parseDouble(trimmed);
                    } else {
                        throw new ConfigException(name, value, "Expected value to be a double, but it was a " + value.getClass().getName());
                    }
                case LIST:
                    if (value instanceof List) {
                        return value;
                    } else if (value instanceof String) {
                        if (trimmed.isEmpty()) {
                            return Collections.emptyList();
                        } else {
                            return Arrays.asList(COMMA_WITH_WHITESPACE.split(trimmed, -1));
                        }
                    } else {
                        throw new ConfigException(name, value, "Expected a comma separated list.");
                    }
                default:
                    throw new IllegalStateException("Unknown type.");
            }
        } catch (NumberFormatException e) {
            throw new ConfigException(name, value, "Not a number of type " + type);
        }
    }

    public static String convertToString(Object parsedValue, Type type) {
        if (parsedValue == null) {
            return null;
        }

        if (type == null) {
            return parsedValue.toString();
        }

        switch (type) {
            case BOOLEAN:
            case SHORT:
            case INT:
            case LONG:
            case DOUBLE:
            case STRING:
            case PASSWORD:
                return parsedValue.toString();
            case LIST:
                List<?> valueList = (List<?>) parsedValue;
                return StringUtils.join(valueList, ",");
            case CLASS:
                Class<?> clazz = (Class<?>) parsedValue;
                return clazz.getName();
            default:
                throw new IllegalStateException("Unknown type.");
        }
    }

    /**
     * The config types
     */
    public enum Type {
        BOOLEAN, STRING, INT, SHORT, LONG, DOUBLE, LIST, CLASS, PASSWORD
    }


    /**
     * Validation logic the user may provide to perform single configuration validation.
     */
    public interface Validator {
        /**
         * Perform single configuration validation.
         *
         * @param name  The name of the configuration
         * @param value The value of the configuration
         * @throws ConfigException if the value is invalid.
         */
        void ensureValid(String name, Object value);
    }

    /**
     * Validation logic for numeric ranges
     */
    public static class Range implements Validator {
        private final Number min;
        private final Number max;

        /**
         * A numeric range with inclusive upper bound and inclusive lower bound
         *
         * @param min the lower bound
         * @param max the upper bound
         */
        private Range(Number min, Number max) {
            this.min = min;
            this.max = max;
        }

        /**
         * A numeric range that checks only the lower bound
         *
         * @param min The minimum acceptable value
         */
        public static Range atLeast(Number min) {
            return new Range(min, null);
        }

        /**
         * A numeric range that checks both the upper (inclusive) and lower bound
         */
        public static Range between(Number min, Number max) {
            return new Range(min, max);
        }

        @Override
        public void ensureValid(String name, Object o) {
            if (o == null) {
                throw new ConfigException(name, null, "Value must be non-null");
            }
            Number n = (Number) o;
            if (min != null && n.doubleValue() < min.doubleValue()) {
                throw new ConfigException(name, o, "Value must be at least " + min);
            }
            if (max != null && n.doubleValue() > max.doubleValue()) {
                throw new ConfigException(name, o, "Value must be no more than " + max);
            }
        }

        @Override
        public String toString() {
            if (min == null && max == null) {
                return "[...]";
            } else if (min == null) {
                return "[...," + max + "]";
            } else if (max == null) {
                return "[" + min + ",...]";
            } else {
                return "[" + min + ",...," + max + "]";
            }
        }
    }

    public static class ValidList implements Validator {

        final ValidString validString;

        private ValidList(List<String> validStrings) {
            this.validString = new ValidString(validStrings);
        }

        public static ValidList in(String... validStrings) {
            return new ValidList(Arrays.asList(validStrings));
        }

        @Override
        public void ensureValid(final String name, final Object value) {
            @SuppressWarnings("unchecked")
            List<String> values = (List<String>) value;
            for (String string : values) {
                validString.ensureValid(name, string);
            }
        }

        @Override
        public String toString() {
            return validString.toString();
        }
    }

    public static class ValidString implements Validator {
        final List<String> validStrings;

        private ValidString(List<String> validStrings) {
            this.validStrings = validStrings;
        }

        public static ValidString in(String... validStrings) {
            return new ValidString(Arrays.asList(validStrings));
        }

        @Override
        public void ensureValid(String name, Object o) {
            String s = (String) o;
            if (!validStrings.contains(s)) {
                throw new ConfigException(name, o, "String must be one of: " + StringUtils.join(validStrings, ", "));
            }

        }

        @Override
        public String toString() {
            return "[" + StringUtils.join(validStrings, ", ") + "]";
        }
    }

    public static class CaseInsensitiveValidString implements Validator {

        final Set<String> validStrings;

        private CaseInsensitiveValidString(List<String> validStrings) {
            this.validStrings = validStrings.stream()
                    .map(s -> s.toUpperCase(Locale.ROOT))
                    .collect(Collectors.toSet());
        }

        public static CaseInsensitiveValidString in(String... validStrings) {
            return new CaseInsensitiveValidString(Arrays.asList(validStrings));
        }

        @Override
        public void ensureValid(String name, Object o) {
            String s = (String) o;
            if (s == null || !validStrings.contains(s.toUpperCase(Locale.ROOT))) {
                throw new ConfigException(name, o, "String must be one of (case insensitive): " + StringUtils.join(validStrings, ", "));
            }
        }

        @Override
        public String toString() {
            return "(case insensitive) [" + StringUtils.join(validStrings, ", ") + "]";
        }
    }

    public static class NonNullValidator implements Validator {
        @Override
        public void ensureValid(String name, Object value) {
            if (value == null) {
                // Pass in the string null to avoid the spotbugs warning
                throw new ConfigException(name, "null", "entry must be non null");
            }
        }

        @Override
        public String toString() {
            return "non-null string";
        }
    }

    public static class LambdaValidator implements Validator {
        BiConsumer<String, Object> ensureValid;
        Supplier<String> toStringFunction;

        private LambdaValidator(BiConsumer<String, Object> ensureValid,
                                Supplier<String> toStringFunction) {
            this.ensureValid = ensureValid;
            this.toStringFunction = toStringFunction;
        }

        public static LambdaValidator with(BiConsumer<String, Object> ensureValid,
                                           Supplier<String> toStringFunction) {
            return new LambdaValidator(ensureValid, toStringFunction);
        }

        @Override
        public void ensureValid(String name, Object value) {
            ensureValid.accept(name, value);
        }

        @Override
        public String toString() {
            return toStringFunction.get();
        }
    }

    public static class CompositeValidator implements Validator {
        private final List<Validator> validators;

        private CompositeValidator(List<Validator> validators) {
            this.validators = Collections.unmodifiableList(validators);
        }

        public static CompositeValidator of(Validator... validators) {
            return new CompositeValidator(Arrays.asList(validators));
        }

        @Override
        public void ensureValid(String name, Object value) {
            for (Validator validator : validators) {
                validator.ensureValid(name, value);
            }
        }

        @Override
        public String toString() {
            if (validators == null) {
                return "";
            }
            StringBuilder desc = new StringBuilder();
            for (Validator v : validators) {
                if (desc.length() > 0) {
                    desc.append(',').append(' ');
                }
                desc.append(String.valueOf(v));
            }
            return desc.toString();
        }
    }

    public static class NonEmptyString implements Validator {

        @Override
        public void ensureValid(String name, Object o) {
            String s = (String) o;
            if (s != null && s.isEmpty()) {
                throw new ConfigException(name, o, "String must be non-empty");
            }
        }

        @Override
        public String toString() {
            return "non-empty string";
        }
    }

    public static class NonEmptyStringWithoutControlChars implements Validator {

        public static NonEmptyStringWithoutControlChars nonEmptyStringWithoutControlChars() {
            return new NonEmptyStringWithoutControlChars();
        }

        @Override
        public void ensureValid(String name, Object value) {
            String s = (String) value;

            if (s == null) {
                // This can happen during creation of the config object due to no default value being defined for the
                // name configuration - a missing name parameter is caught when checking for mandatory parameters,
                // thus we can ok a null value here
                return;
            } else if (s.isEmpty()) {
                throw new ConfigException(name, value, "String may not be empty");
            }

            // Check name string for illegal characters
            ArrayList<Integer> foundIllegalCharacters = new ArrayList<>();

            for (int i = 0; i < s.length(); i++) {
                if (Character.isISOControl(s.codePointAt(i))) {
                    foundIllegalCharacters.add(s.codePointAt(i));
                }
            }
            if (!foundIllegalCharacters.isEmpty()) {
                throw new ConfigException(name, value, "String may not contain control sequences but had the following ASCII chars: " + StringUtils.join(foundIllegalCharacters, ","));
            }
        }

        @Override
        public String toString() {
            return "non-empty string without ISO control characters";
        }
    }

    public static class ConfigKey {
        public final String name;
        public final Type type;
        public final String documentation;
        public final Object defaultValue;
        public final Validator validator;

        public ConfigKey(String name, Type type, Object defaultValue, Validator validator, String documentation) {
            this.name = name;
            this.type = type;
            this.defaultValue = NO_DEFAULT_VALUE.equals(defaultValue) ? NO_DEFAULT_VALUE : parseType(name, defaultValue, type);
            this.validator = validator;
            if (this.validator != null && hasDefault()) {
                this.validator.ensureValid(name, this.defaultValue);
            }
            this.documentation = documentation;
        }

        public boolean hasDefault() {
            return !NO_DEFAULT_VALUE.equals(this.defaultValue);
        }

        public Type type() {
            return type;
        }
    }

}
