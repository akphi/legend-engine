// Copyright 2020 Goldman Sachs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.finos.legend.engine.application.query.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DataCubeQuery
{
    public String id;
    public String name;
    public String description;

    public Map<String, ?> query;
    public Map<String, ?> source;
    public Map<String, ?> executionContext;

    public Long lastUpdatedAt;
    public Long createdAt;
    public Long lastOpenAt;

    // We make it clear that we only allow a single owner
    public String owner;
}
