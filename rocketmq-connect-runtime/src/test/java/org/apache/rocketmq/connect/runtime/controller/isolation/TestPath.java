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

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Iterator;

public class TestPath implements Path {
    @NotNull
    @Override
    public FileSystem getFileSystem() {
        return new TestFileSystem();
    }

    @Override
    public boolean isAbsolute() {
        return false;
    }

    @Override
    public Path getRoot() {
        return null;
    }

    @Override
    public Path getFileName() {
        return null;
    }

    @Override
    public Path getParent() {
        return null;
    }

    @Override
    public int getNameCount() {
        return 0;
    }

    @NotNull
    @Override
    public Path getName(int index) {
        return null;
    }

    @NotNull
    @Override
    public Path subpath(int beginIndex, int endIndex) {
        return null;
    }

    @Override
    public boolean startsWith(@NotNull Path other) {
        return false;
    }

    @Override
    public boolean startsWith(@NotNull String other) {
        return false;
    }

    @Override
    public boolean endsWith(@NotNull Path other) {
        return false;
    }

    @Override
    public boolean endsWith(@NotNull String other) {
        return false;
    }

    @NotNull
    @Override
    public Path normalize() {
        return null;
    }

    @NotNull
    @Override
    public Path resolve(@NotNull Path other) {
        return null;
    }

    @NotNull
    @Override
    public Path resolve(@NotNull String other) {
        return null;
    }

    @NotNull
    @Override
    public Path resolveSibling(@NotNull Path other) {
        return null;
    }

    @NotNull
    @Override
    public Path resolveSibling(@NotNull String other) {
        return null;
    }

    @NotNull
    @Override
    public Path relativize(@NotNull Path other) {
        return null;
    }

    @NotNull
    @Override
    public URI toUri() {
        return null;
    }

    @NotNull
    @Override
    public Path toAbsolutePath() {
        return null;
    }

    @NotNull
    @Override
    public Path toRealPath(@NotNull LinkOption... options) throws IOException {
        return null;
    }

    @NotNull
    @Override
    public File toFile() {
        return null;
    }

    @NotNull
    @Override
    public WatchKey register(@NotNull WatchService watcher, @NotNull WatchEvent.Kind<?>[] events,
                             WatchEvent.Modifier... modifiers) throws IOException {
        return null;
    }

    @NotNull
    @Override
    public WatchKey register(@NotNull WatchService watcher, @NotNull WatchEvent.Kind<?>... events) throws IOException {
        return null;
    }

    @NotNull
    @Override
    public Iterator<Path> iterator() {
        return null;
    }

    @Override
    public int compareTo(@NotNull Path other) {
        return 0;
    }
}
