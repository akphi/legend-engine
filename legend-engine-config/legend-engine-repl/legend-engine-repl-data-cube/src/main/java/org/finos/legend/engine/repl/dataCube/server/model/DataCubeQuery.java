// Copyright 2024 Goldman Sachs
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

package org.finos.legend.engine.repl.dataCube.server.model;

import java.util.List;
import java.util.Map;

public class DataCubeQuery
{
    public String name;
    public String query;
    // NOTE: this is an information that is needed for initialization
    // we might want to move this to the source
    public List<DataCubeQueryColumn> columns;

    // NOTE: we don't need to process the config, so we will leave it as raw JSON
    public Map<String, ?> configuration;
}
