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

package org.apache.rocketmq.connect.runtime.controller.isolation;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class PluginUtilsTest {

    private static final String PATH = "src/test/java/org/apache/rocketmq/connect/runtime";
    private Path path = new TestPath();

    @Test
    public void isArchiveTest() {
        final boolean archive = PluginUtils.isArchive(path);
        assert archive == false;
    }

    @Test
    public void isClassFileTest() {
        final boolean file = PluginUtils.isClassFile(path);
        assert file == false;
    }

    @Test
    public void pluginLocationsTest() throws IOException {
        final List<Path> paths = PluginUtils.pluginLocations(Paths.get(PATH).toAbsolutePath());
        assert paths.size() > 0;
    }

    @Test
    public void pluginUrlsTest() throws IOException {
        final List<Path> paths = PluginUtils.pluginUrls(Paths.get(PATH).toAbsolutePath());
        assert paths.size() == 0;
    }

    @Test
    public void shouldNotLoadInIsolationTest() {
        final boolean flag = PluginUtils.shouldNotLoadInIsolation(PATH);
        assert flag == false;

    }
}
