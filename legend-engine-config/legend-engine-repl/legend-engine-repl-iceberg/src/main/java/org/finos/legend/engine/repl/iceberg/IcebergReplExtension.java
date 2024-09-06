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

package org.finos.legend.engine.repl.iceberg;

import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;
import org.finos.legend.engine.plan.execution.result.Result;
import org.finos.legend.engine.repl.client.Client;
import org.finos.legend.engine.repl.core.Command;
import org.finos.legend.engine.repl.core.ReplExtension;
import org.finos.legend.engine.repl.iceberg.commands.LoadIcebergS3;

public class IcebergReplExtension implements ReplExtension
{
    private Client client;

    @Override
    public String type()
    {
        return "relational";
    }

    public void initialize(Client client)
    {
        this.client = client;
    }

    @Override
    public MutableList<Command> getExtraCommands()
    {
        return Lists.mutable.with(
                new LoadIcebergS3(this.client)
        );
    }

    @Override
    public boolean supports(Result res)
    {
        return false;
    }

    @Override
    public String print(Result res)
    {
        return null;
    }

    @Override
    public MutableList<String> generateDynamicContent(String code)
    {
        return Lists.mutable.empty();
    }
}
