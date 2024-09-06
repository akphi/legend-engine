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

package org.finos.legend.engine.repl.iceberg.commands;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.utility.ListIterate;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.connection.DatabaseConnection;
import org.finos.legend.engine.repl.client.Client;
import org.finos.legend.engine.repl.client.jline3.JLine3Parser;
import org.finos.legend.engine.repl.core.Command;
import org.finos.legend.engine.repl.dataCube.commands.DataCube;
import org.finos.legend.engine.repl.relational.shared.ConnectionHelper;
import org.jline.builtins.Completers;
import org.jline.reader.Candidate;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Properties;

public class LoadIcebergS3 implements Command
{
    private final Client client;
    private final Completers.FilesCompleter completer = new Completers.FilesCompleter(new File("/"));

    public LoadIcebergS3(Client client)
    {
        this.client = client;
    }

    @Override
    public String documentation()
    {
        return "load_iceberg_s3 <path> <table name>";
    }

    @Override
    public String description()
    {
        return "[DEV] Load data from Iceberg S3 into table";
    }

    @Override
    public boolean process(String line) throws Exception
    {
        if (line.startsWith("load"))
        {
            String[] tokens = line.split(" ");
            if (tokens.length != 3)
            {
                throw new RuntimeException("Command should be used as '" + this.documentation() + "'");
            }

            DatabaseConnection databaseConnection = ConnectionHelper.getDatabaseConnection(this.client.getModelState().parse(), DataCube.getLocalConnectionPath());
            String path = tokens[1];
            String tableName = tokens[2];
            Properties props = new Properties();
            props.load(Files.newInputStream(Paths.get(path.startsWith("~") ? FileUtils.getUserDirectoryPath() + path.substring("~".length()) : path)));

            String initSQL = props.getProperty("INIT_SQL");
            String s3Url = props.getProperty("S3_URL");
            String sql = initSQL + "CREATE TABLE " + tableName + " AS SELECT * FROM iceberg_scan('" + s3Url + "')";

            try (Connection connection = ConnectionHelper.getConnection(databaseConnection, client.getPlanExecutor()))
            {
                try (Statement statement = connection.createStatement())
                {
                    statement.executeUpdate(sql);
                    this.client.println("Loaded into table: '" + tokens[2] + "'");
                }
            }

            return true;
        }
        return false;
    }

    @Override
    public MutableList<Candidate> complete(String inScope, LineReader lineReader, ParsedLine parsedLine)
    {
        if (StringUtils.stripStart(inScope, null).startsWith("load_iceberg_s3"))
        {
            MutableList<String> words = Lists.mutable.withAll(parsedLine.words()).drop(3);
            String compressed = StringUtils.stripStart(words.makeString(""), null);
            MutableList<Candidate> list = Lists.mutable.empty();
            completer.complete(lineReader, new JLine3Parser.MyParsedLine(new JLine3Parser.ParserResult(parsedLine.line(), Lists.mutable.with(compressed))), list);
            MutableList<Candidate> ca = ListIterate.collect(list, c ->
            {
                String val = compressed.length() == 1 ? c.value() : c.value().substring(1);
                return new Candidate(val, val, null, null, null, null, false, 0);
            });
            list.clear();
            list.addAll(ca);
            return list;
        }
        return null;
    }
}
