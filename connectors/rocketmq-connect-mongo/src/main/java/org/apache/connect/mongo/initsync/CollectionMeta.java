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

package org.apache.connect.mongo.initsync;

public class CollectionMeta {

    private String databaseName;
    private String collectionName;

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public CollectionMeta(String databaseName, String collectionName) {

        this.databaseName = databaseName;
        this.collectionName = collectionName;
    }

    public String getNameSpace() {
        return databaseName + "." + collectionName;
    }

    @Override
    public String toString() {
        return "CollectionMeta{" +
            "databaseName='" + databaseName + '\'' +
            ", collectionName='" + collectionName + '\'' +
            '}';
    }
}
