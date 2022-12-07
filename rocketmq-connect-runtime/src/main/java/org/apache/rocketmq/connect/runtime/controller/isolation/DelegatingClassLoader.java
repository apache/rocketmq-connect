/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.connect.runtime.controller.isolation;

import io.openmessaging.connector.api.component.Transform;
import io.openmessaging.connector.api.component.connector.Connector;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.component.task.source.SourceConnector;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.data.RecordConverter;
import org.reflections.Configuration;
import org.reflections.Reflections;
import org.reflections.ReflectionsException;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.Driver;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Delegating class Loader
 */
public class DelegatingClassLoader extends URLClassLoader {
    private static final Logger log = LoggerFactory.getLogger(DelegatingClassLoader.class);
    private static final String CLASSPATH_NAME = "classpath";
    private static final String UNDEFINED_VERSION = "undefined";

    private final Map<String, SortedMap<PluginWrapper<?>, ClassLoader>> pluginLoaders;
    private final Map<String, String> aliases;
    private final SortedSet<PluginWrapper<SinkConnector>> sinkConnectors;
    private final SortedSet<PluginWrapper<SourceConnector>> sourceConnectors;
    private final Collection<PluginWrapper<SourceTask>> sourceTasks;
    private final Collection<PluginWrapper<SinkTask>> sinkTasks;
    private final SortedSet<PluginWrapper<RecordConverter>> converters;
    private final SortedSet<PluginWrapper<Transform<?>>> transformations;
    private final List<String> pluginPaths;


    public DelegatingClassLoader(List<String> pluginPaths, ClassLoader parent) {
        super(new URL[0], parent);
        this.pluginPaths = pluginPaths;
        this.pluginLoaders = new HashMap<>();
        this.aliases = new HashMap<>();
        this.sinkConnectors = new TreeSet<>();
        this.sourceConnectors = new TreeSet<>();
        this.sourceTasks = new TreeSet<>();
        this.sinkTasks = new TreeSet<>();
        this.converters = new TreeSet<>();
        this.transformations = new TreeSet<>();
    }

    public DelegatingClassLoader(List<String> pluginPaths) {
        this(pluginPaths, DelegatingClassLoader.class.getClassLoader());
    }

    private static PluginClassLoader newPluginClassLoader(
            final URL pluginLocation,
            final URL[] urls,
            final ClassLoader parent
    ) {
        return AccessController.doPrivileged(
                (PrivilegedAction<PluginClassLoader>) () -> new PluginClassLoader(pluginLocation, urls, parent)
        );
    }

    private static <T> String versionFor(T pluginImpl) {
//        return pluginImpl instanceof Versioned ? ((Versioned) pluginImpl).version() : UNDEFINED_VERSION;
        return UNDEFINED_VERSION;
    }

    private static <T> String versionFor(Class<? extends T> pluginKlass) throws IllegalAccessException, InstantiationException {
        // Temporary workaround until all the plugins are versioned.
        return Connector.class.isAssignableFrom(pluginKlass) ? versionFor(pluginKlass.newInstance()) : UNDEFINED_VERSION;
    }

    public Set<PluginWrapper<Connector>> connectors() {
        Set<PluginWrapper<Connector>> connectors = new TreeSet<>((Set) sinkConnectors);
        connectors.addAll((Set) sourceConnectors);
        return connectors;
    }

    public Set<PluginWrapper<SinkConnector>> sinkConnectors() {
        return sinkConnectors;
    }

    public Set<PluginWrapper<SourceConnector>> sourceConnectors() {
        return sourceConnectors;
    }

    public Set<PluginWrapper<RecordConverter>> converters() {
        return converters;
    }

    public Set<PluginWrapper<Transform<?>>> transformations() {
        return transformations;
    }

    /**
     * Retrieve the PluginClassLoader associated with a plugin class
     *
     * @param name
     * @return
     */
    public PluginClassLoader pluginClassLoader(String name) {
//        if (!PluginUtils.shouldLoadInIsolation(name)) {
//            return null;
//        }
        SortedMap<PluginWrapper<?>, ClassLoader> inner = pluginLoaders.get(name);
        if (inner == null) {
            return null;
        }
        ClassLoader pluginLoader = inner.get(inner.lastKey());
        return pluginLoader instanceof PluginClassLoader
                ? (PluginClassLoader) pluginLoader
                : null;
    }

    public ClassLoader connectorLoader(Connector connector) {
        return connectorLoader(connector.getClass().getName());
    }

    public ClassLoader connectorLoader(String connectorClassOrAlias) {
        String fullName = aliases.containsKey(connectorClassOrAlias)
                ? aliases.get(connectorClassOrAlias)
                : connectorClassOrAlias;
        ClassLoader classLoader = pluginClassLoader(fullName);
        if (classLoader == null) {
            classLoader = this;
        }
        log.debug(
                "Getting plugin class loader: '{}' for connector: {}",
                classLoader,
                connectorClassOrAlias
        );
        return classLoader;
    }

    private <T> void addPlugins(Collection<PluginWrapper<T>> plugins, ClassLoader loader) {
        for (PluginWrapper<T> plugin : plugins) {
            String pluginClassName = plugin.className();
            SortedMap<PluginWrapper<?>, ClassLoader> inner = pluginLoaders.get(pluginClassName);
            if (inner == null) {
                inner = new TreeMap<>();
                pluginLoaders.put(pluginClassName, inner);
                log.info("Added plugin '{}'", pluginClassName);
            }
            inner.put(plugin, loader);
        }
    }

    public void initLoaders() {
        long beginTime = System.currentTimeMillis();
        for (String configPath : pluginPaths) {
            initPluginLoader(configPath);
        }
        initPluginLoader(CLASSPATH_NAME);
        addAllAliases();
        log.info("Init all plugins cost time = {} ms", System.currentTimeMillis() - beginTime);
    }

    private void initPluginLoader(String path) {
        try {
            if (CLASSPATH_NAME.equals(path)) {
                scanUrlsAndAddPlugins(
                        getParent(),
                        ClasspathHelper.forJavaClassPath().toArray(new URL[0])
                );
            } else {
                Path pluginPath = Paths.get(path).toAbsolutePath();
                path = pluginPath.toString();
                if (Files.isDirectory(pluginPath)) {
                    for (Path pluginLocation : PluginUtils.pluginLocations(pluginPath)) {
                        registerPlugin(pluginLocation);
                    }
                } else if (PluginUtils.isArchive(pluginPath)) {
                    registerPlugin(pluginPath);
                }
            }
        } catch (InvalidPathException | MalformedURLException e) {
            log.error("Invalid path in plugin path: {}. Ignoring.", path, e);
        } catch (IOException e) {
            log.error("Could not get listing for plugin path: {}. Ignoring.", path, e);
        } catch (ReflectiveOperationException e) {
            log.error("Could not instantiate plugins in: {}. Ignoring.", path, e);
        }
    }

    private void registerPlugin(Path pluginLocation)
            throws ReflectiveOperationException, IOException {
        log.info("Loading plugin from: {}", pluginLocation);
        List<URL> pluginUrls = new ArrayList<>();
        for (Path path : PluginUtils.pluginUrls(pluginLocation)) {
            pluginUrls.add(path.toUri().toURL());
        }
        URL[] urls = pluginUrls.toArray(new URL[0]);
        if (log.isDebugEnabled()) {
            log.debug("Loading plugin urls: {}", Arrays.toString(urls));
        }
        PluginClassLoader loader = newPluginClassLoader(
                pluginLocation.toUri().toURL(),
                urls,
                this
        );
        scanUrlsAndAddPlugins(loader, urls);
    }

    private void scanUrlsAndAddPlugins(
            ClassLoader loader,
            URL[] urls
    ) throws ReflectiveOperationException {
        long beginTime = System.currentTimeMillis();
        PluginScanResult plugins = doLoad(loader, urls);
        log.info("Registered loader: {}, cost time = {} ms", loader, System.currentTimeMillis() - beginTime);
        if (!plugins.isEmpty()) {
            addPlugins(plugins.sinkConnectors(), loader);
            sinkConnectors.addAll(plugins.sinkConnectors());
            addPlugins(plugins.sourceConnectors(), loader);
            sourceConnectors.addAll(plugins.sourceConnectors());
            addPlugins(plugins.sourceTasks(), loader);
            sourceTasks.addAll(plugins.sourceTasks());
            addPlugins(plugins.sinkTasks(), loader);
            sinkTasks.addAll(plugins.sinkTasks());
            addPlugins(plugins.converters(), loader);
            converters.addAll(plugins.converters());
            addPlugins(plugins.transformations(), loader);
            transformations.addAll(plugins.transformations());
        }
        loadJdbcDrivers(loader);
    }

    private void loadJdbcDrivers(final ClassLoader loader) {
        AccessController.doPrivileged(
                new PrivilegedAction<Void>() {
                    @Override
                    public Void run() {
                        ServiceLoader<Driver> loadedDrivers = ServiceLoader.load(
                                Driver.class,
                                loader
                        );
                        Iterator<Driver> driversIterator = loadedDrivers.iterator();
                        try {
                            while (driversIterator.hasNext()) {
                                Driver driver = driversIterator.next();
                                log.debug(
                                        "Registered java.sql.Driver: {} to java.sql.DriverManager",
                                        driver
                                );
                            }
                        } catch (Throwable t) {
                            log.debug(
                                    "Ignoring java.sql.Driver classes listed in resources but not"
                                            + " present in class loader's classpath: ",
                                    t
                            );
                        }
                        return null;
                    }
                }
        );
    }

    private PluginScanResult doLoad(
            ClassLoader loader,
            URL[] urls
    ) throws ReflectiveOperationException {
        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.setClassLoaders(new ClassLoader[]{loader});
        builder.addUrls(urls);
        builder.setScanners(new SubTypesScanner());
        builder.setParallel(true);
        Reflections reflections = new InternalReflections(builder);

        return new PluginScanResult(
                getPluginWrapper(reflections, SourceConnector.class, loader),
                getPluginWrapper(reflections, SinkConnector.class, loader),
                getPluginWrapper(reflections, SourceTask.class, loader),
                getPluginWrapper(reflections, SinkTask.class, loader),
                getPluginWrapper(reflections, RecordConverter.class, loader),
                getTransformationPluginWrapper(loader, reflections)
        );
    }

    private Collection<PluginWrapper<Transform<?>>> getTransformationPluginWrapper(ClassLoader loader, Reflections reflections) throws ReflectiveOperationException {
        return (Collection<PluginWrapper<Transform<?>>>) (Collection<?>) getPluginWrapper(reflections, Transform.class, loader);
    }

    private <T> Collection<PluginWrapper<T>> getPluginWrapper(
            Reflections reflections,
            Class<T> klass,
            ClassLoader loader
    ) throws InstantiationException, IllegalAccessException {
        Set<Class<? extends T>> plugins;
        try {
            plugins = reflections.getSubTypesOf(klass);
        } catch (ReflectionsException e) {
            log.debug("Reflections scanner could not find any classes for URLs: " +
                    reflections.getConfiguration().getUrls(), e);
            return Collections.emptyList();
        }

        Collection<PluginWrapper<T>> result = new ArrayList<>();
        for (Class<? extends T> plugin : plugins) {
            if (PluginUtils.isConcrete(plugin)) {
                result.add(new PluginWrapper<>(plugin, versionFor(plugin), loader));
            } else {
                log.debug("Skipping {} as it is not concrete implementation", plugin);
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private <T> Collection<PluginWrapper<T>> getServiceLoaderPluginWrapper(Class<T> klass, ClassLoader loader) {
        ClassLoader savedLoader = Plugin.compareAndSwapLoaders(loader);
        Collection<PluginWrapper<T>> result = new ArrayList<>();
        try {
            ServiceLoader<T> serviceLoader = ServiceLoader.load(klass, loader);
            for (T pluginImpl : serviceLoader) {
                result.add(new PluginWrapper<>((Class<? extends T>) pluginImpl.getClass(),
                        versionFor(pluginImpl), loader));
            }
        } finally {
            Plugin.compareAndSwapLoaders(savedLoader);
        }
        return result;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        String fullName = aliases.containsKey(name) ? aliases.get(name) : name;
        PluginClassLoader pluginLoader = pluginClassLoader(fullName);
        if (pluginLoader != null) {
            log.trace("Retrieving loaded class '{}' from '{}'", fullName, pluginLoader);
            return pluginLoader.loadClass(fullName, resolve);
        }

        return super.loadClass(fullName, resolve);
    }

    private void addAllAliases() {
        addAliases(sourceConnectors);
        addAliases(sinkConnectors);
        addAliases(sourceTasks);
        addAliases(sinkTasks);
        addAliases(converters);
        addAliases(transformations);
    }

    private <S> void addAliases(Collection<PluginWrapper<S>> plugins) {
        for (PluginWrapper<S> plugin : plugins) {
            if (PluginUtils.isAliasUnique(plugin, plugins)) {
                String simple = PluginUtils.simpleName(plugin);
                String pruned = PluginUtils.prunedName(plugin);
                aliases.put(simple, plugin.className());
                if (simple.equals(pruned)) {
                    log.info("Added alias '{}' to plugin '{}'", simple, plugin.className());
                } else {
                    aliases.put(pruned, plugin.className());
                    log.info(
                            "Added aliases '{}' and '{}' to plugin '{}'",
                            simple,
                            pruned,
                            plugin.className()
                    );
                }
            }
        }
    }

    private static class InternalReflections extends Reflections {

        public InternalReflections(Configuration configuration) {
            super(configuration);
        }
    }

}
