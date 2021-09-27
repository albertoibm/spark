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

import java.util.Collections;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;
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
      // If this is a Join, and it joins on a column that we index, then...
      // 1. Retrieve the full index for the column
      // 2. Return a physical plan that uses the index to read the table
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
    /*
     * Dataset information: https://www.kaggle.com/olistbr/brazilian-ecommerce
     */
    this.spark = new TestSparkSession();
    spark.conf().set("spark.sql.catalog.testcat", InMemoryTableCatalog.class.getName());
    Dataset<Row> customers = spark.read().parquet("src/test/resources/test-data/index-opt/olist_customers_dataset.csv.parquet");
    Dataset<Row> orders = spark.read().parquet("src/test/resources/test-data/index-opt/olist_orders_dataset.csv.parquet");
    customers.createOrReplaceTempView("customers");
    orders.createOrReplaceTempView("orders");
  }

  @After
  public void dropTestTable() {
    spark.stop();
  }

  @Test
  public void testPlayWithSparkStrategy() throws Exception {
    spark.extensions()
        .injectPlannerStrategy(
            builder -> {
              return new ExperimentingWithStrategy();
            });

    spark.extensions().injectOptimizerRule(
        builder -> {
          return new Rule<LogicalPlan>() {
            @Override
            public LogicalPlan apply(LogicalPlan plan) {
              return plan;
            }
          };
        }
    );
    // Dataset<Row> ds = spark.sql("SELECT customer_id, order_status, COUNT(DISTINCT order_id) "
    //     + "FROM orders WHERE order_estimated_delivery_date != 'aidiosmio' "
    //     + "GROUP BY customer_id, order_status");

    // System.out.println("PABLOEM - HERE IS INFO ABOUT THE EXECUTION PLAN");
    // System.out.println(ds.queryExecution().explainString(ExplainMode.fromString("extended")));

    Dataset<Row> ds2 = spark.sql("SELECT c.customer_id, c.customer_state, o.order_status, COUNT(DISTINCT o.order_id) "
        + "FROM customers as c JOIN orders as o ON c.customer_id = o.customer_id "
        + "WHERE c.customer_state != 'Washington' AND o.order_status != 'oioioi' "
        + "GROUP BY c.customer_id, c.customer_state, o.order_status");

    System.out.println(ds2.queryExecution().explainString(ExplainMode.fromString("extended")));
  }
}
