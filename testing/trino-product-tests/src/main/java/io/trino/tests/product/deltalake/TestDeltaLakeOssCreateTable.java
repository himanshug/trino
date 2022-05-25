/*
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
package io.trino.tests.product.deltalake;

import io.trino.tempto.ProductTest;
import io.trino.tempto.query.QueryResult;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestDeltaLakeOssCreateTable
        extends ProductTest
{
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testCreateTableWithTableComment()
    {
        String tableName = "delta_table_comment_" + randomTableSuffix();
        onTrino().executeQuery("CREATE TABLE default." + tableName + "(col INT) COMMENT 'test comment'");

        try {
            assertThat(onTrino().executeQuery("SELECT comment FROM system.metadata.table_comments WHERE catalog_name = 'delta' AND schema_name = 'default' AND table_name = '" + tableName + "'"))
                    .containsOnly(row("test comment"));

            assertEquals(getTableCommentOnDelta("default", tableName), "test comment");
        }
        finally {
            onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);
        }
    }

    private static String getTableCommentOnDelta(String schemaName, String tableName)
    {
        QueryResult result = onDelta().executeQuery(format("DESCRIBE EXTENDED %s.%s", schemaName, tableName));
        return (String) result.rows().stream()
                .filter(row -> row.get(0).equals("Comment"))
                .map(row -> row.get(1))
                .findFirst().orElseThrow();
    }
}
