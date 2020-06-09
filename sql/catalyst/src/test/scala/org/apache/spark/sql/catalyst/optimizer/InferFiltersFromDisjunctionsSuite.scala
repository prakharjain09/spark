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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf

class InferFiltersFromDisjunctionsSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val filterPushdownRules = Seq(PushDownPredicates)

    val batches =
      Batch("Filter Pushdown before", FixedPoint(10), filterPushdownRules: _*) ::
        Batch("InferFilters", Once, InferFiltersFromDisjunctions) ::
        Batch("Filter Pushdown after", FixedPoint(10), filterPushdownRules: _*) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  test("filters inferred in inner join") {
    withSQLConf(SQLConf.INFER_FILTERS_FROM_DISJUNCTIONS_ENABLED.key -> "true") {
      val x = testRelation.subquery('x)
      val y = testRelation.subquery('y)
      val joinCondition = Some(("x.a".attr === "y.a".attr) &&
        (
          (("x.b".attr === 1) && ("y.c".attr === 5)) ||
            (("x.b".attr === 2) && ("y.c".attr === 6))
          )
      )
      val originalQuery = x.join(y,
        condition = joinCondition
      ).analyze
      val left = x.where(("x.b".attr === 1) || ("x.b".attr === 2))
      val right = y.where(("y.c".attr === 5) || ("y.c".attr === 6))
      val correctAnswer = left.join(right, condition = joinCondition).analyze
      val optimized = Optimize.execute(originalQuery)
      comparePlans(optimized, correctAnswer)
    }
  }

  test("filters should not be inferred when config is disabled") {
    withSQLConf(SQLConf.INFER_FILTERS_FROM_DISJUNCTIONS_ENABLED.key -> "false") {
      val x = testRelation.subquery('x)
      val y = testRelation.subquery('y)
      val joinCondition = Some(("x.a".attr === "y.a".attr) &&
        (
          (("x.b".attr === 1) && ("y.c".attr === 5)) ||
            (("x.b".attr === 2) && ("y.c".attr === 6))
          )
      )
      val originalQuery = x.join(y,
        condition = joinCondition
      ).analyze
      val left = x
      val right = y
      val correctAnswer = left.join(right, condition = joinCondition).analyze
      val optimized = Optimize.execute(originalQuery)
      comparePlans(optimized, correctAnswer)
    }
  }

  test("filters cannot be inferred when one of the" +
    " disjunction doesn't give anything for 1 side") {
    withSQLConf(SQLConf.INFER_FILTERS_FROM_DISJUNCTIONS_ENABLED.key -> "true") {
      val x = testRelation.subquery('x)
      val y = testRelation.subquery('y)
      val joinCondition = Some(("x.a".attr === "y.a".attr) &&
        (
          ("x.b".attr === 1) ||
            (("x.b".attr === 2) && ("y.c".attr === 6))
          ))
      val originalQuery = x.join(y,
        condition = joinCondition
      ).analyze
      val left = x.where(("x.b".attr === 1) || (("x.b".attr === 2)))
      val right = y
      val correctAnswer = left.join(right, condition = joinCondition).analyze
      val optimized = Optimize.execute(originalQuery)
      comparePlans(optimized, correctAnswer)
    }
  }

  test("filters are recursively inferred from each disjunction") {
    withSQLConf(SQLConf.INFER_FILTERS_FROM_DISJUNCTIONS_ENABLED.key -> "true") {
      val x = testRelation.subquery('x)
      val y = testRelation.subquery('y)
      val joinCondition = Some(("x.a".attr === "y.a".attr) &&
        (
          (("x.b".attr === 1) && (
            (
              ("x.c".attr === 1) && ("y.c".attr === 2)) ||
              (("x.c".attr === 3) && ("y.c".attr === 4))
            )) ||
            (("x.b".attr === 2) && ("y.b".attr === 6))
          )
      )
      val originalQuery = x.join(y,
        condition = joinCondition
      ).analyze
      val left = x.where(
        ("x.b".attr === 1 && (("x.c".attr === 1) || ("x.c".attr === 3))) || (("x.b".attr === 2)))
      val right = y.where(
        (("y.c".attr === 2) || ("y.c".attr === 4)) || ("y.b".attr === 6))
      val correctAnswer = left.join(right, condition = joinCondition).analyze
      val optimized = Optimize.execute(originalQuery)
      comparePlans(optimized, correctAnswer)
    }
  }

  Seq(
    ("fullouter", false, false),
    ("leftouter", false, true),
    ("rightouter", true, false)
  ).foreach { case (joinTypeStr, pushdownExpectedOnLeft, pushdownExpectedOnRight) =>
    test(s"filters inferred in $joinTypeStr join," +
      s" pushdownExpectedOnLeft: $pushdownExpectedOnLeft, " +
      s"pushdownExpectedOnRight: $pushdownExpectedOnRight") {
      withSQLConf(SQLConf.INFER_FILTERS_FROM_DISJUNCTIONS_ENABLED.key -> "true") {
        val x = testRelation.subquery('x)
        val y = testRelation.subquery('y)
        val joinType = JoinType(joinTypeStr)
        val joinCondition = Some(("x.a".attr === "y.a".attr) &&
          (
            (("x.b".attr === 1) && ("y.c".attr === 5)) ||
              (("x.b".attr === 2) && ("y.c".attr === 6))
            )
        )
        val originalQuery = x.join(y,
          condition = joinCondition,
          joinType = joinType
        ).analyze
        val left = x
        val right = y
        val leftWithPushdown = x.where(("x.b".attr === 1) || ("x.b".attr === 2))
        val rightWithPushdown = y.where(("y.c".attr === 5) || ("y.c".attr === 6))

        val expectedLeft = if (pushdownExpectedOnLeft) leftWithPushdown else left
        val expectedRight = if (pushdownExpectedOnRight) rightWithPushdown else right
        val correctAnswer = expectedLeft.join(expectedRight,
          condition = joinCondition,
          joinType = joinType
        ).analyze
        val optimized = Optimize.execute(originalQuery)
        comparePlans(optimized, correctAnswer)
      }
    }
  }

  Seq(
    ("fullouter", false, false),
    ("leftouter", true, false),
    ("rightouter", false, true)
  ).foreach { case (joinTypeStr, pushdownExpectedOnLeft, pushdownExpectedOnRight) =>
    test(s"filters inferred from Filter " +
      s"followed by $joinTypeStr, pushdownExpectedOnLeft: $pushdownExpectedOnLeft, " +
      s"pushdownExpectedOnRight: $pushdownExpectedOnRight") {
      withSQLConf(SQLConf.INFER_FILTERS_FROM_DISJUNCTIONS_ENABLED.key -> "true") {
        val x = testRelation.subquery('x)
        val y = testRelation.subquery('y)
        val joinType = JoinType(joinTypeStr)
        val joinCondition = Some("x.a".attr === "y.a".attr)
        val filterCondition = ((("x.b".attr === 1) && ("y.c".attr === 5)) ||
          (("x.b".attr === 2) && ("y.c".attr === 6)))
        val originalQuery = x.join(y,
          condition = joinCondition,
          joinType = joinType
        ).where(filterCondition).analyze

        val left = x
        val leftWithPushdown = x.where(("x.b".attr === 1) || ("x.b".attr === 2))
        val right = y
        val rightWithPushdown = y.where(("y.c".attr === 5) || ("y.c".attr === 6))

        val expectedLeft = if (pushdownExpectedOnLeft) leftWithPushdown else left
        val expectedRight = if (pushdownExpectedOnRight) rightWithPushdown else right

        val expectedAnswer = expectedLeft.join(expectedRight,
          condition = joinCondition,
          joinType = joinType
        ).where(filterCondition).analyze

        val optimized = Optimize.execute(originalQuery)
        comparePlans(optimized, expectedAnswer)
      }
    }
  }
}
