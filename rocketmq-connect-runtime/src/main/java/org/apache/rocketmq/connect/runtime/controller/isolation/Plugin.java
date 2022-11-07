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

import io.openmessaging.connector.api.component.Transform;
import io.openmessaging.connector.api.component.connector.Connector;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import io.openmessaging.connector.api.component.task.source.SourceConnector;
import io.openmessaging.connector.api.data.RecordConverter;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.config.ConnectorConfig;
import org.apache.rocketmq.connect.runtime.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Plugin {

    private static final Logger log = LoggerFactory.getLogger(Plugin.class);
    private final DelegatingClassLoader delegatingLoader;

    public Plugin(List<String> pluginLocations) {
        delegatingLoader = newDelegatingClassLoader(pluginLocations);
        delegatingLoader.initLoaders();
    }

    public static ClassLoader compareAndSwapLoaders(ClassLoader loader) {
        ClassLoader current = Thread.currentThread().getContextClassLoader();
        if (null == current || !current.equals(loader)) {
            Thread.currentThread().setContextClassLoader(loader);
        }
        return current;
    }

    protected static <U> Class<? extends U> pluginClass(
            DelegatingClassLoader loader,
            String classOrAlias,
            Class<U> pluginClass
    ) throws ClassNotFoundException {
        Class<?> klass = loader.loadClass(classOrAlias, false);
        if (pluginClass.isAssignableFrom(klass)) {
            return (Class<? extends U>) klass;
        }

        throw new ClassNotFoundException(
                "Requested class: "
                        + classOrAlias
                        + " does not extend " + pluginClass.getSimpleName()
        );
    }

    protected static <T> T newPlugin(Class<T> klass) {
        ClassLoader savedLoader = compareAndSwapLoaders(klass.getClassLoader());
        try {
            return Utils.newInstance(klass);
        } catch (Throwable t) {
            throw new ConnectException("Instantiation error", t);
        } finally {
            compareAndSwapLoaders(savedLoader);
        }
    }

    private static <T> String pluginNames(Collection<PluginWrapper<T>> plugins) {
        return Utils.join(plugins, ", ");
    }

    public void initLoaders() {
        delegatingLoader.initLoaders();
    }

    protected DelegatingClassLoader newDelegatingClassLoader(final List<String> paths) {
        return AccessController.doPrivileged(
                (PrivilegedAction<DelegatingClassLoader>) () -> new DelegatingClassLoader(paths)
        );
    }

    public DelegatingClassLoader delegatingLoader() {
        return delegatingLoader;
    }

    public Set<PluginWrapper<SinkConnector>> sinkConnectors() {
        return delegatingLoader.sinkConnectors();
    }

    public Set<PluginWrapper<SourceConnector>> sourceConnectors() {
        return delegatingLoader.sourceConnectors();
    }

    public Set<PluginWrapper<RecordConverter>> converters() {
        return delegatingLoader.converters();
    }

    public Set<PluginWrapper<Transform<?>>> transformations() {
        return delegatingLoader.transformations();
    }

    public ClassLoader currentThreadLoader() {
        return Thread.currentThread().getContextClassLoader();
    }

    public Connector newConnector(String connectorClassOrAlias) {
        Class<? extends Connector> klass = connectorClass(connectorClassOrAlias);
        return newPlugin(klass);
    }

    public Class<? extends Connector> connectorClass(String connectorClassOrAlias) {
        Class<? extends Connector> klass;
        try {
            klass = pluginClass(delegatingLoader, connectorClassOrAlias, Connector.class);
        } catch (ClassNotFoundException e) {
            List<PluginWrapper<? extends Connector>> matches = new ArrayList<>();
            Set<PluginWrapper<Connector>> connectors = delegatingLoader.connectors();
            for (PluginWrapper<? extends Connector> plugin : connectors) {
                Class<?> pluginClass = plugin.pluginClass();
                String simpleName = pluginClass.getSimpleName();
                if (simpleName.equals(connectorClassOrAlias)
                        || simpleName.equals(connectorClassOrAlias + "Connector")) {
                    matches.add(plugin);
                }
            }
            if (matches.isEmpty()) {
                throw new ConnectException(
                        "Failed to find any class that implements Connector and which name matches "
                                + connectorClassOrAlias
                                + ", available connectors are: "
                                + Utils.join(connectors, ", ")
                );
            }

            // conflict connector
            if (matches.size() > 1) {
                throw new ConnectException(
                        "More than one connector matches alias "
                                + connectorClassOrAlias
                                + ". Please use full package and class name instead. Classes found: "
                                + Utils.join(connectors, ", ")
                );
            }

            PluginWrapper<? extends Connector> entry = matches.get(0);
            klass = entry.pluginClass();
        }
        return klass;
    }

    public Task newTask(Class<? extends Task> taskClass) {
        return newPlugin(taskClass);
    }

    public RecordConverter newConverter(ConnectKeyValue config, boolean isKey, String classPropertyName, String defaultConverter, ClassLoaderUsage classLoaderUsage) {
        if (!config.containsKey(classPropertyName)) {
            config.put(classPropertyName, defaultConverter);
        }
        Class<? extends RecordConverter> klass = null;
        switch (classLoaderUsage) {
            case CURRENT_CLASSLOADER:
                klass = pluginClassFromConfig(config, classPropertyName, RecordConverter.class, delegatingLoader.converters());
                break;
            case PLUGINS:
                String converterClassOrAlias = Utils.getClass(config, classPropertyName).getName();
                try {
                    klass = pluginClass(delegatingLoader, converterClassOrAlias, RecordConverter.class);
                } catch (ClassNotFoundException e) {
                    throw new ConnectException(
                            "Failed to find any class that implements Converter and which name matches "
                                    + converterClassOrAlias + ", available converters are: "
                                    + pluginNames(delegatingLoader.converters())
                    );
                }
                break;
        }
        if (klass == null) {
            throw new ConnectException("Unable to initialize the Converter specified in '" + classPropertyName + "'");
        }

        // Configure the Converter using only the old configuration mechanism ...
        String configPrefix = classPropertyName + ".";
        Map<String, String> converterConfig = config.originalsWithPrefix(configPrefix);
        log.debug("Configuring the converter with configuration keys:{}{}", System.lineSeparator(), converterConfig.keySet());

        RecordConverter plugin;
        ClassLoader savedLoader = compareAndSwapLoaders(klass.getClassLoader());
        try {
            plugin = newPlugin(klass);
            converterConfig.put(ConnectorConfig.IS_KEY, String.valueOf(isKey));
            plugin.configure(converterConfig);
        } finally {
            compareAndSwapLoaders(savedLoader);
        }
        return plugin;
    }

    public <T> List<T> newPlugins(List<String> klassNames, ConnectKeyValue config, Class<T> pluginKlass) {
        List<T> plugins = new ArrayList<>();
        if (klassNames != null) {
            for (String klassName : klassNames) {
                plugins.add(newPlugin(klassName, config, pluginKlass));
            }
        }
        return plugins;
    }

    public <T> T newPlugin(String klassName, ConnectKeyValue config, Class<T> pluginKlass) {
        T plugin;
        Class<? extends T> klass;
        try {
            klass = pluginClass(delegatingLoader, klassName, pluginKlass);
        } catch (ClassNotFoundException e) {
            String msg = String.format("Failed to find any class that implements %s and which "
                    + "name matches %s", pluginKlass, klassName);
            throw new ConnectException(msg);
        }
        ClassLoader savedLoader = compareAndSwapLoaders(klass.getClassLoader());
        try {
            plugin = newPlugin(klass);
        } finally {
            compareAndSwapLoaders(savedLoader);
        }
        return plugin;
    }

    protected <U> Class<? extends U> pluginClassFromConfig(
            ConnectKeyValue config,
            String propertyName,
            Class<U> pluginClass,
            Collection<PluginWrapper<U>> plugins
    ) {
        Class<?> klass = Utils.getClass(config, propertyName);
        if (pluginClass.isAssignableFrom(klass)) {
            return (Class<? extends U>) klass;
        }
        throw new ConnectException(
                "Failed to find any class that implements " + pluginClass.getSimpleName()
                        + " for the config "
                        + propertyName + ", available classes are: "
                        + pluginNames(plugins)
        );
    }


    public enum ClassLoaderUsage {
        CURRENT_CLASSLOADER,
        PLUGINS
    }

}
