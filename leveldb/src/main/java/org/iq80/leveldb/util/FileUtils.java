/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.iq80.leveldb.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

public final class FileUtils
{
    private FileUtils()
    {
    }

    public static ImmutableList<Path> listFiles(Path dir)
            throws IOException
    {
        if (!Files.exists(dir)) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<Path> result = ImmutableList.builder();
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(dir)) {
            for (Path path : directoryStream) {
                result.add(path);
            }
        }
        return result.build();
    }

    public static Path createTempDir(String prefix)
            throws IOException
    {
        return Files.createTempDirectory(prefix);
    }

    public static void deleteDirectoryContents(Path directory)
            throws IOException
    {
        Preconditions.checkArgument(Files.isDirectory(directory), "Not a directory: %s", directory);

        // Don't delete symbolic link directories
        if (Files.isSymbolicLink(directory)) {
            return;
        }

        for (Path file : listFiles(directory)) {
            deleteRecursively(file);
        }
    }

    public static void deleteRecursively(Path file)
            throws IOException
    {
        if (Files.isDirectory(file)) {
            deleteDirectoryContents(file);
        }

        Files.deleteIfExists(file);
    }

    public static boolean copyDirectoryContents(Path src, Path target)
            throws IOException
    {
        Preconditions.checkArgument(Files.isDirectory(src), "Source dir is not a directory: %s", src);

        // Don't delete symbolic link directories
        if (Files.isSymbolicLink(src)) {
            return false;
        }

        Files.createDirectories(target);
        Preconditions.checkArgument(Files.isDirectory(target), "Target dir is not a directory: %s", src);

        boolean success = true;
        for (Path file : listFiles(src)) {
            success = copyRecursively(file, target.resolve(file.getFileName().toString())) && success;
        }
        return success;
    }

    public static boolean copyRecursively(Path src, Path target)
            throws IOException
    {
        if (Files.isDirectory(src)) {
            return copyDirectoryContents(src, target);
        }
        else {
            try {
                Files.copy(src, target);
                return true;
            }
            catch (IOException e) {
                return false;
            }
        }
    }
}
