/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package test.org.apache.spark.sql;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.InMemoryTableCatalog;
import org.apache.spark.sql.execution.ExplainMode;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.SparkStrategy;
import org.apache.spark.sql.test.TestSparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import static org.apache.spark.sql.functions.*;

public class JavaSparkStrategyOptimizationSuite {

  class ExperimentingWithStrategy extends SparkStrategy {

    @Override
    public Seq<SparkPlan> apply(LogicalPlan plan) {
      // If this SparkStrategy does not apply to the input plan, we must return an empty list.
      return JavaConverters.asScalaIteratorConverter(
          Collections.<SparkPlan>emptyIterator()).asScala().toSeq();
    }
  }
  private static StructType schema = new StructType().add("s", "string");
  private SparkSession spark = null;

  public Dataset<Row> df() {
    return spark.read().schema(schema).text();
  }

  @Before
  public void createTestTable() {
    this.spark = new TestSparkSession();
    spark.conf().set("spark.sql.catalog.testcat", InMemoryTableCatalog.class.getName());
    spark.sql("CREATE TABLE testcat.t (s string) USING foo");
    Dataset<Row> ds = spark.read().parquet("src/test/resources/test-data/index-opt/premier_league.parquet");
    ds.createOrReplaceTempView("premier_league_data");
    /*
     *  Schema of the parquet file with premier league information:
     *
     * For more detailed info: https://www.kaggle.com/irkaal/english-premier-league-results
     *
     *  <pyarrow._parquet.ParquetSchema object at 0x7ff147f62280>
     * required group field_id=0 schema {
     *   optional binary field_id=1 Season (String);
     *   optional int64 field_id=2 DateTime (Timestamp(isAdjustedToUTC=true, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false));
     *   optional binary field_id=3 HomeTeam (String);
     *   optional binary field_id=4 AwayTeam (String);
     *   optional int32 field_id=5 FTHG;
     *   optional int32 field_id=6 FTAG;
     *   optional binary field_id=7 FTR (String);
     *   optional int32 field_id=8 HTHG;
     *   optional int32 field_id=9 HTAG;
     *   optional binary field_id=10 HTR (String);
     *   optional binary field_id=11 Referee (String);
     *   optional int32 field_id=12 HS;
     *   optional int32 field_id=13 AS;
     *   optional int32 field_id=14 HST;
     *   optional int32 field_id=15 AST;
     *   optional int32 field_id=16 HC;
     *   optional int32 field_id=17 AC;
     *   optional int32 field_id=18 HF;
     *   optional int32 field_id=19 AF;
     *   optional int32 field_id=20 HY;
     *   optional int32 field_id=21 AY;
     *   optional int32 field_id=22 HR;
     *   optional int32 field_id=23 AR;
     * }
     */
  }

  @After
  public void dropTestTable() {
    spark.sql("DROP TABLE testcat.t");
    spark.stop();
  }

  @Test
  public void testPlayWithSparkStrategy() throws Exception {
    spark.extensions().injectPlannerStrategy(
        builder -> {
          return new ExperimentingWithStrategy();
        }
    );
    Dataset<Row> ds = spark.sql("SELECT HomeTeam, AwayTeam, COUNT(HomeTeam, AwayTeam) "
        + "FROM premier_league_data WHERE FTHG > 0 "
        + "GROUP BY HomeTeam, AwayTeam");

    System.out.println("PABLOEM - HERE IS INFO ABOUT THE EXECUTION PLAN");
    System.out.println(ds.queryExecution().explainString(ExplainMode.fromString("extended")));
  }
}
