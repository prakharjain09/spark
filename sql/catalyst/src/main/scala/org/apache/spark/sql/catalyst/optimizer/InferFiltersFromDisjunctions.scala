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

import org.apache.spark.sql.catalyst.expressions.{And, Expression, Or, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, InnerLike, JoinType, LeftAnti, LeftExistence, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

/**
 * Infer extra filters which can be pushed down through join from the
 * remaining non-pushable filters
 * Ex- Filter(t1.c1 > 2 && t2.c1 < 5 && ( (t1.c2 > 5 && t2.c2 > 4) || (t1.c3 > 20 && t2.c3 > 6) ))
 *                                               |
 *                                               Join
 *                                            left  right
 *
 * Here t1.c1 > 2 can be pushed down to left
 *      t2.c1 < 5 can be pushed down to right
 * ( (t1.c2 > 5 && t2.c2 > 4) || (t1.c3 > 20 && t2.c3 > 6) ) cannot be pushed down to
 * left or right as it contains attributes of both left and right table
 * But we can infer extra filters for left side: t1.c2 > 5 || t1.c3 > 20
 * we can also infer extra filters for the right side: t2.c2 > 4 || t2.c3 > 6
 */
object InferFiltersFromDisjunctions extends Rule[LogicalPlan]
  with PredicateHelper {

  /**
   * Filters out deterministic predicates which contains references
   *   from both left and right logical plan
   */
  private def getNonPushableDeterministicPredicates(predicates: Seq[Expression],
                                                    left: LogicalPlan,
                                                    right: LogicalPlan): Seq[Expression] = {
    val deterministicPredicates = predicates.filter(_.deterministic)
    deterministicPredicates.filterNot { pred =>
      pred.references.subsetOf(left.outputSet) || pred.references.subsetOf(right.outputSet)
    }
  }

  /**
   * Returns new inferred predicates from existing predicates which can
   * be pushed down to left and right.
   * This function will analyze each element in predicatesInConjunction list and
   * tries to infer new predicates which can be pushed down from it.
   * Ex - Say left=t1, right=t2 and
   * predicatesInConjunction =Seq(
   *     (t1.c1 > 2 && t2.c1 < 5),
   *     ((t1.c2 > 5 && t2.c2 > 4) || (t1.c3 > 20 && t2.c3 > 6))
   *   )
   *
   * In this case, predicates that can be inferred for left: (t1.c2 > 5 || t1.c3 > 20)
   * predicates that can be inferred for right: (t2.c2 > 4 || t2.c3 > 6)
   */
  private def inferFiltersForPushown(predicatesInConjunction: Seq[Expression],
                                     left: LogicalPlan,
                                     right: LogicalPlan): (Seq[Expression], Seq[Expression]) = {
    val extraFiltersInferredTupples = predicatesInConjunction.map { cond =>
      val allOrPredicates = splitDisjunctivePredicates(cond)
      if (allOrPredicates.size >= 2) {
        logDebug(s"Processing $cond to infer more filters")
        val inferredFilterTupplesForEachDisjunction = allOrPredicates.map { orPredicate =>
          val allAndsInsideOr = splitConjunctivePredicates(orPredicate)
          val (leftEvaluateCondition, rest) =
            allAndsInsideOr.partition(_.references.subsetOf(left.outputSet))
          val (rightEvaluateCondition, commonCondition) =
            rest.partition(expr => expr.references.subsetOf(right.outputSet))
          val (recursiveLeftExtraFilters, recursiveRightExtraFilters) =
            inferFiltersForPushown(commonCondition, left, right)
          (leftEvaluateCondition ++ recursiveLeftExtraFilters,
            rightEvaluateCondition ++ recursiveRightExtraFilters)
        }
        val inferredFilterListForLeft = inferredFilterTupplesForEachDisjunction.map(_._1)
        val inferredFilterListForRight = inferredFilterTupplesForEachDisjunction.map(_._2)
        val canInferFilterForLeft = inferredFilterListForLeft.forall(_.nonEmpty)
        val canInferFilterForRight = inferredFilterListForRight.forall(_.nonEmpty)
        val newLeftFilter = if (canInferFilterForLeft) {
          Some(inferredFilterListForLeft.map(_.reduce(And)).reduce(Or))
        } else {
          None
        }
        val newRightFilter = if (canInferFilterForRight) {
          Some(inferredFilterListForRight.map(_.reduce(And)).reduce(Or))
        } else {
          None
        }
        (newLeftFilter, newRightFilter)
      } else {
        (None, None)
      }
    }
    (extraFiltersInferredTupples.flatMap(_._1), extraFiltersInferredTupples.flatMap(_._2))
  }

  private def getNewFilterOrJoinCondition(
      oldFilterOrJoinCondition: Expression,
      left: LogicalPlan,
      right: LogicalPlan,
      canPushdownOnLeft: Boolean,
      canPushdownOnRight: Boolean): Expression = {
    if (!canPushdownOnLeft && !canPushdownOnRight) {
      return oldFilterOrJoinCondition
    }
    val predicates = splitConjunctivePredicates(oldFilterOrJoinCondition)
    val nonPushablePredicates = getNonPushableDeterministicPredicates(predicates, left, right)
    val (filtersInferredForLeft, filtersInferredForRight) =
      inferFiltersForPushown(nonPushablePredicates, left, right)
    val filtersPushableOnLeftSide = if (canPushdownOnLeft) {
      filtersInferredForLeft.filterNot(p => predicates.exists(_.semanticEquals(p)))
    } else {
      Seq()
    }
    val filtersPushableOnRightSide = if (canPushdownOnRight) {
      filtersInferredForRight.filterNot(p => predicates.exists(_.semanticEquals(p)))
    } else {
      Seq()
    }
    (predicates ++ filtersPushableOnLeftSide ++ filtersPushableOnRightSide).reduceLeft(And)
  }

  /**
   * Infer new predicates and updates the filterCondition/joinCondition so that they
   * can be pushed down by the [[PushDownPredicates]] rule
   */
  private def inferFilters(plan: LogicalPlan): LogicalPlan = plan transformDown {
    // [[PushPredicateThroughJoin]] rules pushes down predicates based on join type.
    // Check https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior for more details
    case Filter(filterCondition, join@Join(left, right, joinType, _, _)) =>
      val (canPushdownOnLeft, canPushdownOnRight) = joinType match {
        case _: InnerLike => (true, true)
        case RightOuter => (false, true)
        case LeftOuter | LeftExistence(_) => (true, false)
        case _ => (false, false)
      }
      val newFilterCond = getNewFilterOrJoinCondition(filterCondition, left, right,
        canPushdownOnLeft, canPushdownOnRight)
      Filter(newFilterCond, join)
    case j@Join(left, right, joinType, Some(joinCondition), _) =>
      val (canPushdownOnLeft, canPushdownOnRight) = joinType match {
        case _: InnerLike | LeftSemi => (true, true)
        case RightOuter =>
          (true, false)
        case LeftOuter | LeftAnti | ExistenceJoin(_) =>
          (false, true)
        case _ => (false, false)
      }
      val newJoinCond = getNewFilterOrJoinCondition(joinCondition, left, right,
        canPushdownOnLeft, canPushdownOnRight)
      j.copy(condition = Some(newJoinCond))
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (SQLConf.get.inferFilterFromDisjunctionsEnabled) {
      inferFilters(plan)
    } else {
      plan
    }
  }
}
