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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.rocketmq.connect.runtime.controller.isolation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

public class PluginUtils {

    private static final Logger log = LoggerFactory.getLogger(PluginUtils.class);

    // Be specific about javax packages and exclude those existing in Java SE and Java EE libraries.
    private static final Pattern BLACKLIST = Pattern.compile("^(?:"
            + "java"
            + "|javax\\.accessibility"
            + "|javax\\.activation"
            + "|javax\\.activity"
            + "|javax\\.annotation"
            + "|javax\\.batch\\.api"
            + "|javax\\.batch\\.operations"
            + "|javax\\.batch\\.runtime"
            + "|javax\\.crypto"
            + "|javax\\.decorator"
            + "|javax\\.ejb"
            + "|javax\\.el"
            + "|javax\\.enterprise\\.concurrent"
            + "|javax\\.enterprise\\.context"
            + "|javax\\.enterprise\\.context\\.spi"
            + "|javax\\.enterprise\\.deploy\\.model"
            + "|javax\\.enterprise\\.deploy\\.shared"
            + "|javax\\.enterprise\\.deploy\\.spi"
            + "|javax\\.enterprise\\.event"
            + "|javax\\.enterprise\\.inject"
            + "|javax\\.enterprise\\.inject\\.spi"
            + "|javax\\.enterprise\\.util"
            + "|javax\\.faces"
            + "|javax\\.imageio"
            + "|javax\\.inject"
            + "|javax\\.interceptor"
            + "|javax\\.jms"
            + "|javax\\.json"
            + "|javax\\.jws"
            + "|javax\\.lang\\.model"
            + "|javax\\.mail"
            + "|javax\\.management"
            + "|javax\\.management\\.j2ee"
            + "|javax\\.naming"
            + "|javax\\.net"
            + "|javax\\.persistence"
            + "|javax\\.print"
            + "|javax\\.resource"
            + "|javax\\.rmi"
            + "|javax\\.script"
            + "|javax\\.security\\.auth"
            + "|javax\\.security\\.auth\\.message"
            + "|javax\\.security\\.cert"
            + "|javax\\.security\\.jacc"
            + "|javax\\.security\\.sasl"
            + "|javax\\.servlet"
            + "|javax\\.sound\\.midi"
            + "|javax\\.sound\\.sampled"
            + "|javax\\.sql"
            + "|javax\\.swing"
            + "|javax\\.tools"
            + "|javax\\.transaction"
            + "|javax\\.validation"
            + "|javax\\.websocket"
            + "|javax\\.ws\\.rs"
            + "|javax\\.xml"
            + "|javax\\.xml\\.bind"
            + "|javax\\.xml\\.registry"
            + "|javax\\.xml\\.rpc"
            + "|javax\\.xml\\.soap"
            + "|javax\\.xml\\.ws"
            + "|org\\.ietf\\.jgss"
            + "|org\\.omg\\.CORBA"
            + "|org\\.omg\\.CosNaming"
            + "|org\\.omg\\.Dynamic"
            + "|org\\.omg\\.DynamicAny"
            + "|org\\.omg\\.IOP"
            + "|org\\.omg\\.Messaging"
            + "|org\\.omg\\.PortableInterceptor"
            + "|org\\.omg\\.PortableServer"
            + "|org\\.omg\\.SendingContext"
            + "|org\\.omg\\.stub\\.java\\.rmi"
            + "|org\\.w3c\\.dom"
            + "|org\\.xml\\.sax"
            + "|io\\.openmessaging\\.connector\\.api"
            + "|org\\.slf4j"
            + "|org\\.apache\\.rocketmq"
            + ")\\..*$"
            + "|io\\.openmessaging\\.KeyValue");


    // If the base interface or class that will be used to identify Connect plugins resides within
    // the same java package as the plugins that need to be loaded in isolation (and thus are
    // added to the INCLUDE pattern), then this base interface or class needs to be excluded in the
    // regular expression pattern
    private static final Pattern INCLUDE = Pattern.compile("^(?:"
            + "|org\\.apache\\.rocketmq\\.connect"
            + "|org\\.apache\\.rocketmq\\.replicator"
            + ")\\..*$");


    private static final DirectoryStream.Filter<Path> PLUGIN_PATH_FILTER = new DirectoryStream
            .Filter<Path>() {
        @Override
        public boolean accept(Path path) {
            return Files.isDirectory(path) || isArchive(path) || isClassFile(path);
        }
    };

    public static boolean isArchive(Path path) {
        String archivePath = path.toString().toLowerCase(Locale.ROOT);
        return archivePath.endsWith(".jar") || archivePath.endsWith(".zip");
    }

    public static boolean isClassFile(Path path) {
        return path.toString().toLowerCase(Locale.ROOT).endsWith(".class");
    }

    public static List<Path> pluginLocations(Path topPath) throws IOException {
        List<Path> locations = new ArrayList<>();
        try (
                DirectoryStream<Path> listing = Files.newDirectoryStream(
                        topPath,
                        PLUGIN_PATH_FILTER
                )
        ) {
            for (Path dir : listing) {
                locations.add(dir);
            }
        }
        return locations;
    }

    /**
     * Verify the given class corresponds to a concrete class and not to an abstract class or
     */
    public static boolean isConcrete(Class<?> klass) {
        int mod = klass.getModifiers();
        return !Modifier.isAbstract(mod) && !Modifier.isInterface(mod);
    }

    /**
     * Return whether the class with the given name should be loaded in isolation using a plugin
     * classloader.
     *
     * @param name the fully qualified name of the class.
     * @return true if this class should be loaded in isolation, false otherwise.
     */
    public static boolean shouldLoadInIsolation(String name) {
        return !(BLACKLIST.matcher(name).matches() && !INCLUDE.matcher(name).matches());
    }

    /**
     * Verify whether a given plugin's alias matches another alias in a collection of plugins.
     *
     * @param alias   the plugin descriptor to test for alias matching.
     * @param plugins the collection of plugins to test against.
     * @param <U>     the plugin type.
     * @return false if a match was found in the collection, otherwise true.
     */
    public static <U> boolean isAliasUnique(
            PluginWrapper<U> alias,
            Collection<PluginWrapper<U>> plugins
    ) {
        boolean matched = false;
        for (PluginWrapper<U> plugin : plugins) {
            if (simpleName(alias).equals(simpleName(plugin))
                    || prunedName(alias).equals(prunedName(plugin))) {
                if (matched) {
                    return false;
                }
                matched = true;
            }
        }
        return true;
    }

    /**
     * Return the simple class name of a plugin as {@code String}.
     *
     * @param plugin the plugin descriptor.
     * @return the plugin's simple class name.
     */
    public static String simpleName(PluginWrapper<?> plugin) {
        return plugin.pluginClass().getSimpleName();
    }

    /**
     * Remove the plugin type name at the end of a plugin class name, if such suffix is present.
     * This method is meant to be used to extract plugin aliases.
     */
    public static String prunedName(PluginWrapper<?> plugin) {
        switch (plugin.type()) {
            case SOURCE:
            case SINK:
            case CONNECTOR:
                return prunePluginName(plugin, "Connector");
            default:
                return prunePluginName(plugin, plugin.type().simpleName());
        }
    }

    private static String prunePluginName(PluginWrapper<?> plugin, String suffix) {
        String simple = plugin.pluginClass().getSimpleName();
        int pos = simple.lastIndexOf(suffix);
        if (pos > 0) {
            return simple.substring(0, pos);
        }
        return simple;
    }

    public static List<Path> pluginUrls(Path topPath) throws IOException {
        boolean containsClassFiles = false;
        Set<Path> archives = new TreeSet<>();
        LinkedList<DirectoryEntry> dfs = new LinkedList<>();
        Set<Path> visited = new HashSet<>();

        if (isArchive(topPath)) {
            return Collections.singletonList(topPath);
        }

        DirectoryStream<Path> topListing = Files.newDirectoryStream(
                topPath,
                PLUGIN_PATH_FILTER
        );
        dfs.push(new DirectoryEntry(topListing));
        visited.add(topPath);
        try {
            while (!dfs.isEmpty()) {
                Iterator<Path> neighbors = dfs.peek().iterator;
                if (!neighbors.hasNext()) {
                    dfs.pop().stream.close();
                    continue;
                }

                Path adjacent = neighbors.next();
                if (Files.isSymbolicLink(adjacent)) {
                    try {
                        Path symlink = Files.readSymbolicLink(adjacent);
                        Path parent = adjacent.getParent();
                        if (parent == null) {
                            continue;
                        }
                        Path absolute = parent.resolve(symlink).toRealPath();
                        if (Files.exists(absolute)) {
                            adjacent = absolute;
                        } else {
                            continue;
                        }
                    } catch (IOException e) {
                        log.warn(
                                "Resolving symbolic link '{}' failed. Ignoring this path.",
                                adjacent,
                                e
                        );
                        continue;
                    }
                }

                if (!visited.contains(adjacent)) {
                    visited.add(adjacent);
                    if (isArchive(adjacent)) {
                        archives.add(adjacent);
                    } else if (isClassFile(adjacent)) {
                        containsClassFiles = true;
                    } else {
                        DirectoryStream<Path> listing = Files.newDirectoryStream(
                                adjacent,
                                PLUGIN_PATH_FILTER
                        );
                        dfs.push(new DirectoryEntry(listing));
                    }
                }
            }
        } finally {
            while (!dfs.isEmpty()) {
                dfs.pop().stream.close();
            }
        }

        if (containsClassFiles) {
            if (archives.isEmpty()) {
                return Collections.singletonList(topPath);
            }
            log.warn("Plugin path contains both java archives and class files. Returning only the"
                    + " archives");
        }
        return Arrays.asList(archives.toArray(new Path[0]));
    }

    public static boolean shouldNotLoadInIsolation(String name) {
        return BLACKLIST.matcher(name).matches();
    }

    private static class DirectoryEntry {
        final DirectoryStream<Path> stream;
        final Iterator<Path> iterator;

        DirectoryEntry(DirectoryStream<Path> stream) {
            this.stream = stream;
            this.iterator = stream.iterator();
        }
    }

}
