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

package org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.connection;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.util.StdConverter;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.connection.ConnectionVisitor;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.connection.authentication.AuthenticationStrategy;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.connection.postprocessor.PostProcessor;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.connection.specification.DatasourceSpecification;

import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;

@JsonDeserialize(converter = RelationalDatabaseConnection.RelationalDatabaseConnectionConverter.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RelationalDatabaseConnection extends DatabaseConnection
{
    public DatasourceSpecification datasourceSpecification;
    public AuthenticationStrategy authenticationStrategy;
    public DatabaseType databaseType;
    public List<PostProcessor> postProcessors = Collections.emptyList();
    public Boolean localMode;

    public RelationalDatabaseConnection()
    {
        // jackson
    }

    public RelationalDatabaseConnection(DatasourceSpecification datasourceSpecification, AuthenticationStrategy authenticationStrategy, DatabaseType databaseType)
    {
        this.datasourceSpecification = datasourceSpecification;
        this.authenticationStrategy = authenticationStrategy;
        this.databaseType = databaseType;
    }

    @Override
    public <T> T accept(ConnectionVisitor<T> connectionVisitor)
    {
        return connectionVisitor.visit(this);
    }

    public static class RelationalDatabaseConnectionConverter extends StdConverter<RelationalDatabaseConnection, RelationalDatabaseConnection>
    {
        private final List<RelationalDatabaseConnectionProtocolConverter> converters;

        public RelationalDatabaseConnectionConverter()
        {
            this.converters = org.eclipse.collections.api.factory.Lists.mutable.withAll(ServiceLoader.load(RelationalDatabaseConnectionProtocolConverter.class));
        }

        @Override
        public RelationalDatabaseConnection convert(RelationalDatabaseConnection value)
        {
            if (Boolean.parseBoolean(System.getenv("org.finos.legend.engine.enableNewConnectionFramework")))
            {
                for (RelationalDatabaseConnectionProtocolConverter converter : this.converters)
                {
                    RelationalDatabaseConnection newValue = converter.convert(value);
                    if (newValue != null)
                    {
                        return newValue;
                    }
                }
            }
            return value;
        }
    }
}
