// Copyright 2023 Goldman Sachs
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
//

package org.finos.legend.engine.repl.iceberg;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.finos.legend.tableformat.iceberg.testsupport.IceboxSpark;

public class IceboxSparkWithNessieCatalog
{
    public static void main(String[] args) throws Exception
    {
        try (IceboxSpark ICEBOX = new IceboxSpark())
        {
            ICEBOX.start();

            // create namespace (spark won't create it)
            Namespace namespace = ICEBOX.createNamespace("nyc");
            Catalog catalog = ICEBOX.getCatalog();
            TableIdentifier table1 = TableIdentifier.of(namespace, "taxis");

            String createTable = "CREATE TABLE demo.nyc.taxis " +
                    "(" +
                    "  vendor_id bigint," +
                    "  trip_id bigint," +
                    "  trip_distance float," +
                    "  fare_amount double," +
                    "  store_and_fwd_flag string" +
                    ") PARTITIONED BY (vendor_id);";
            ICEBOX.runSparkQL(createTable);

            String insertSql = "INSERT INTO demo.nyc.taxis VALUES" +
                    "(1, 1000371, 1.8, 15.32, 'N'), " +
                    "(2, 1000372, 2.5, 22.15, 'N'), " +
                    "(2, 1000373, 0.9, 9.01, 'N'), " +
                    "(1, 1000374, 8.4, 42.13, 'Y');";
            ICEBOX.runSparkQL(insertSql);

            System.out.println("\nINIT_SQL=" +
                    "INSTALL iceberg;" +
                    "LOAD iceberg;" +
                    "INSTALL httpfs;" +
                    "LOAD httpfs;" +
                    "SET s3_region='" + IceboxSpark.S3_REGION + "';" +
                    "SET s3_url_style='path';" +
                    "SET s3_use_ssl = false;" +
                    "SET s3_endpoint='" + ICEBOX.getS3EndPoint().substring("http://".length()) + "';" +
                    "SET s3_access_key_id='" + IceboxSpark.S3_ACCESS_KEY + "';" +
                    "SET s3_secret_access_key='" + IceboxSpark.S3_SECRET_KEY + "';"
            );
            Table table = catalog.loadTable(table1);
            String metadataFileLocation = ((HasTableOperations) table).operations().current().metadataFileLocation();
            String s3Url = "s3://" + metadataFileLocation.substring("s3a://".length());
            System.out.println("S3_URL=" + s3Url);
            System.out.println("\nSQL: SELECT * FROM iceberg_scan('" + s3Url + "');");

            while (true)
            {
                Thread.sleep(1000);
            }
        }
    }
}
