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

package org.finos.legend.engine.repl.core.commands;

import org.eclipse.collections.api.list.MutableList;
import org.finos.legend.engine.repl.client.Client;
import org.finos.legend.engine.repl.core.Command;
import org.jline.reader.Candidate;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

public class Debug implements Command
{
    private final Client client;

    public Debug(Client client)
    {
        this.client = client;
    }

    @Override
    public String documentation()
    {
        return "debug (<boolean>)";
    }

    @Override
    public String description()
    {
        return "toggle debug mode";
    }

    @Override
    public boolean process(String line) throws Exception
    {
        if (line.startsWith("debug"))
        {
            String[] cmd = line.split(" ");
            if (cmd.length == 1)
            {
                this.client.setDebug(!this.client.isDebug());
            }
            else
            {
                this.client.setDebug(Boolean.parseBoolean(cmd[1]));
            }
            this.client.getTerminal().writer().println("debug: " + this.client.isDebug());
            return true;
        }
        return false;
    }

    @Override
    public MutableList<Candidate> complete(String cmd, LineReader lineReader, ParsedLine parsedLine)
    {
        return null;
    }
}
