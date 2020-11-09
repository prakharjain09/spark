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
package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}

/**
 * A trait that provides functionality to handle aliases in the `outputExpressions`.
 */
trait AliasAwareOutputExpression extends UnaryExecNode {
  protected def outputExpressions: Seq[NamedExpression]

  lazy val aliasMap = AttributeMap(outputExpressions.collect {
    case a @ Alias(child: AttributeReference, _) => (child, a.toAttribute)
  })

  protected def hasAlias: Boolean = aliasMap.nonEmpty

  protected def replaceAlias(attr: AttributeReference): Option[Attribute] = {
    aliasMap.get(attr)
  }
}

/**
 * A trait that handles aliases in the `outputExpressions` to produce `outputPartitioning` that
 * satisfies distribution requirements.
 */
trait AliasAwareOutputPartitioning extends AliasAwareOutputExpression {
  final override def outputPartitioning: Partitioning = {
    child.outputPartitioning match {
      case e: Expression if hasAlias =>
        val normalizedExp = e.transformDown {
          case attr: AttributeReference => replaceAlias(attr).getOrElse(attr)
        }
        normalizedExp.asInstanceOf[Partitioning]
      case other => other
    }
  }
}

/**
 * A trait that handles aliases in the `orderingExpressions` to produce `outputOrdering` that
 * satisfies ordering requirements.
 */
trait AliasAwareOutputOrdering extends AliasAwareOutputExpression {
  protected def orderingExpressions: Seq[SortOrder]

  final override def outputOrdering: Seq[SortOrder] = {
    if (hasAlias) {
      def normalizeExprs(expr: Expression): Expression = expr.transformDown {
        case attr: AttributeReference => replaceAlias(attr).getOrElse(attr)
      }
      orderingExpressions.map { sortOrder =>
        val normalizedSameOrderExpressions = sortOrder.sameOrderExpressions.map(normalizeExprs)
        val newSortOrder = normalizeExprs(sortOrder).asInstanceOf[SortOrder]
        newSortOrder.copy(sameOrderExpressions = normalizedSameOrderExpressions)
      }
    } else {
      orderingExpressions
    }
  }
}
