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

package org.apache.spark.sql.catalyst.statsEstimation

import java.sql.Date

import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils.{getOutputSize, nullColumnStat}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._

class FilterJoinEstimationSuite extends StatsEstimationTestBase {

  /** prepare columns */
  private val attrInt_201_400 = AttributeReference("cint_201_400", IntegerType)()
  private val colStatInt_201_400 = ColumnStat(distinctCount = 200, min = Some(201), max = Some(400),
    nullCount = 0, avgLen = 4, maxLen = 4)

  private val attrInt_1_200 = AttributeReference("cint_1_200", IntegerType)()
  private val colStatInt_1_200 = ColumnStat(distinctCount = 200, min = Some(1), max = Some(200),
    nullCount = 0, avgLen = 4, maxLen = 4)

  private val attrInt_1_100 = AttributeReference("cint_1_100", IntegerType)()
  private val colStatInt_1_100 = ColumnStat(distinctCount = 100, min = Some(1), max = Some(100),
    nullCount = 0, avgLen = 4, maxLen = 4)

  private val attrInt_101_300 = AttributeReference("cint_101_300", IntegerType)()
  private val colStatInt_101_300 = ColumnStat(distinctCount = 100, min = Some(101), max = Some(300),
    nullCount = 0, avgLen = 4, maxLen = 4)

  private val attrBool = AttributeReference("cbool", BooleanType)()
  private val colStatBool = ColumnStat(distinctCount = 2, min = Some(false), max = Some(true),
    nullCount = 0, avgLen = 1, maxLen = 1)

  private val dMin_200 = DateTimeUtils.fromJavaDate(Date.valueOf("2019-01-01"))
  private val dMax_200 = DateTimeUtils.fromJavaDate(Date.valueOf("2019-07-20"))
  private val attrDate_200 = AttributeReference("cdate_200", DateType)()
  private val colStatDate_200 = ColumnStat(distinctCount = 200,
    min = Some(dMin_200), max = Some(dMax_200), nullCount = 0, avgLen = 4, maxLen = 4)

  private val dMin_100 = DateTimeUtils.fromJavaDate(Date.valueOf("2019-01-01"))
  private val dMax_100 = DateTimeUtils.fromJavaDate(Date.valueOf("2019-04-11"))
  private val attrDate_100 = AttributeReference("cdate_100", DateType)()
  private val colStatDate_100 = ColumnStat(distinctCount = 100, min = Some(dMin_100),
    max = Some(dMax_100), nullCount = 0, avgLen = 4, maxLen = 4)


  private val decMin_200 = Decimal("0.010000000000000000")
  private val decMax_200 = Decimal("2.000000000000000000")
  private val attrDecimal_200 = AttributeReference("cdecimal_200", DecimalType(18, 18))()
  private val colStatDecimal_200 = ColumnStat(distinctCount = 200, min = Some(decMin_200),
    max = Some(decMax_200), nullCount = 0, avgLen = 8, maxLen = 8)

  private val decMin_100 = Decimal("0.010000000000000000")
  private val decMax_100 = Decimal("1.000000000000000000")
  private val attrDecimal_100 = AttributeReference("cdecimal_100", DecimalType(18, 18))()
  private val colStatDecimal_100 = ColumnStat(distinctCount = 100, min = Some(decMin_100),
    max = Some(decMax_100), nullCount = 0, avgLen = 8, maxLen = 8)

  private val attrDouble_200 = AttributeReference("cdouble_200", DoubleType)()
  private val colStatDouble_200 = ColumnStat(distinctCount = 200, min = Some(1.0),
    max = Some(200.0), nullCount = 0, avgLen = 8, maxLen = 8)

  private val attrDouble_1_100 = AttributeReference("cdouble_1_100", DoubleType)()
  private val colStatDouble_1_100 = ColumnStat(distinctCount = 100, min = Some(1.0),
    max = Some(100.0), nullCount = 0, avgLen = 8, maxLen = 8)

  private val attrDouble_50_149 = AttributeReference("cdouble_50_149", DoubleType)()
  private val colDouble_50_149 = ColumnStat(distinctCount = 100, min = Some(50.0),
    max = Some(149.0), nullCount = 0, avgLen = 8, maxLen = 8)

  private val attrFloat_200 = AttributeReference("cfloat_200", FloatType)()
  private val colStatFloat_200 = ColumnStat(distinctCount = 200, min = Some(1.0),
    max = Some(200.0), nullCount = 0, avgLen = 4, maxLen = 4)

  private val attrFloat_100 = AttributeReference("cfloat_100", FloatType)()
  private val colStatFloat_100 = ColumnStat(distinctCount = 100, min = Some(1.0),
    max = Some(100.0), nullCount = 0, avgLen = 4, maxLen = 4)

  // string define length is 2 but program estimation is 14
  private val attrString_200 = AttributeReference("cstring_200", StringType)()
  private val colStatString_200 = ColumnStat(distinctCount = 200, min = None, max = None,
    nullCount = 0, avgLen = 2, maxLen = 2)

  private val attrString_100 = AttributeReference("cstring_100", StringType)()
  private val colStatString_100 = ColumnStat(distinctCount = 100, min = None, max = None,
    nullCount = 0, avgLen = 2, maxLen = 2)

  // to do~!
  // column histogram

  private val attributeMap = AttributeMap(Seq(
    attrInt_1_200 -> colStatInt_1_200, // 1
    attrInt_1_100 -> colStatInt_1_100, // 4
    attrInt_101_300 -> colStatInt_101_300, // 3
    attrInt_201_400 -> colStatInt_201_400, // 2

    attrDouble_200 -> colStatDouble_200, // 1
    attrDouble_1_100 -> colStatDouble_1_100, // 2
    attrDouble_50_149 -> colDouble_50_149, // 4

    attrFloat_200 -> colStatFloat_200, // 2
    attrFloat_100 -> colStatFloat_100, // 3

    attrDecimal_200 -> colStatDecimal_200, // 1
    attrDecimal_100 -> colStatDecimal_100, // 2

    attrDate_200 -> colStatDate_200, // 1
    attrDate_100 -> colStatDate_100, // 4

    attrString_200 -> colStatString_200, // 1
    attrString_100 -> colStatString_100, // 4

    attrBool -> colStatBool // 3
  ))

  /*
  *  table1_String (attrInt_1_200, attrDouble_200, attrDecimal_200, attrDate_200, attrString_200 )
  *  table1 (attrInt_1_200, attrDouble_200, attrDecimal_200, attrDate_200)
  *  table2 (attrInt_201_400, attrFloat_200, attrDecimal_100, attrDouble_1_100)
  *  table3 (attrInt_101_300, attrFloat_100, attrBool)
  *  table4 (attrInt_1_100, attrDouble_50_149, attrDate_100)
  *  table4_String (attrInt_1_100, attrDouble_50_149, attrDate_100, attrString_100)
  */

  private val nameToAttr: Map[String, Attribute] = attributeMap.map(kv => kv._1.name -> kv._1)
  private val nameToColInfo: Map[String, (Attribute, ColumnStat)] =
    attributeMap.map(kv => kv._1.name -> kv)

  // table1_String (cint_1_200, cdouble_200, cdecimal_200, cdate_200, cstring_200)
  private val table1_String = StatsTestPlan(
    outputList = Seq("cint_1_200", "cdouble_200",
      "cdecimal_200", "cdate_200", "cstring_200").map(nameToAttr), rowCount = 200,
    attributeStats = AttributeMap(Seq("cint_1_200", "cdouble_200",
      "cdecimal_200", "cdate_200", "cstring_200").map(nameToColInfo)))

  // table1 (cint_1_200, cdouble_200, cdecimal_200, cdate_200)
  private val table1 = StatsTestPlan(
    outputList = Seq("cint_1_200", "cdouble_200",
      "cdecimal_200", "cdate_200").map(nameToAttr), rowCount = 200,
    attributeStats = AttributeMap(Seq("cint_1_200", "cdouble_200",
      "cdecimal_200", "cdate_200").map(nameToColInfo)))

  // table2 (cint_201_400, cfloat_200, cdecimal_100, cdouble_1_100)
  private val table2 = StatsTestPlan(
    outputList = Seq("cint_201_400", "cfloat_200",
      "cdecimal_100", "cdouble_1_100").map(nameToAttr), rowCount = 200,
    attributeStats = AttributeMap(Seq("cint_201_400", "cfloat_200",
      "cdecimal_100", "cdouble_1_100").map(nameToColInfo)))

  // table3 (cint_101_300, cfloat_100, cbool)
  private val table3 = StatsTestPlan(
    outputList = Seq("cint_101_300", "cfloat_100", "cbool").map(nameToAttr), rowCount = 200,
    attributeStats = AttributeMap(Seq("cint_101_300", "cfloat_100", "cbool").map(nameToColInfo)))

  // table4 (cint_1_100, cdouble_50_149, cdate_100)
  private val table4 = StatsTestPlan(
    outputList = Seq("cint_1_100", "cdouble_50_149",
      "cdate_100").map(nameToAttr),
    rowCount = 100,
    attributeStats = AttributeMap(Seq("cint_1_100", "cdouble_50_149",
      "cdate_100").map(nameToColInfo)))

  // table4_String (cint_1_100, cdouble_50_149, cdate_100, cstring_100)
  private val table4_String = StatsTestPlan(
    outputList = Seq("cint_1_100", "cdouble_50_149",
      "cdate_100", "cstring_100").map(nameToAttr),
    rowCount = 100,
    attributeStats = AttributeMap(Seq("cint_1_100", "cdouble_50_149",
      "cdate_100", "cstring_100").map(nameToColInfo)))

  // function
  private def childStatsTestPlan(outList: Seq[Attribute], tableRowCount: BigInt): StatsTestPlan = {
    StatsTestPlan(
      outputList = outList,
      rowCount = tableRowCount,
      attributeStats = AttributeMap(outList.map(a => a -> attributeMap(a))))
  }

  private def validateEstimatedStats(
                                      filterNode: Filter,
                                      expectedColStats: Seq[(Attribute, ColumnStat)],
                                      expectedRowCount: BigInt): Unit = {

    val swappedFilter = filterNode transformExpressionsDown {
      case EqualTo(attr: Attribute, l: Literal) =>
        EqualTo(l, attr)

      case LessThan(attr: Attribute, l: Literal) =>
        GreaterThan(l, attr)

      case LessThanOrEqual(attr: Attribute, l: Literal) =>
        GreaterThanOrEqual(l, attr)

      case GreaterThan(attr: Attribute, l: Literal) =>
        LessThan(l, attr)

      case GreaterThanOrEqual(attr: Attribute, l: Literal) =>
        LessThanOrEqual(l, attr)
    }

    val testFilters = if (swappedFilter != filterNode) {
      Seq(swappedFilter, filterNode)
    } else {
      Seq(filterNode)
    }

    testFilters.foreach { filter =>
      val expectedAttributeMap = AttributeMap(expectedColStats)
      val expectedStats = Statistics(
        sizeInBytes = getOutputSize(filter.output, expectedRowCount, expectedAttributeMap),
        rowCount = Some(expectedRowCount),
        attributeStats = expectedAttributeMap)

      val filterStats = filter.stats

      // assert
      assert(filterStats.sizeInBytes == expectedStats.sizeInBytes,
        "filter size don't match expected size")

      assert(filterStats.rowCount == expectedStats.rowCount,
        "filter rowcount don't math expected rowcount")

      val rowCountValue = filterStats.rowCount.getOrElse(0)

      if (rowCountValue != 0) {
        assert(expectedColStats.size == filterStats.attributeStats.size, "col size different")

        expectedColStats.foreach { kv =>
          val filterColumnStat = filterStats.attributeStats.get(kv._1).get
          assert(filterColumnStat == kv._2, "col data different")

        }
      }

    }
  }

  /**
    * filter condition operator:
    * =, <, <=, >, >=
    * AND, OR, NOT
    * IS NULL, IS NOT NULL, IN, NOT IN
    */

  /** join operator:
    * cross join
    * disjoint inner join
    * disjoint left outer join
    * disjoint right outer join
    * disjoint full outer join
    * inner join
    * inner join with multiple equi-join keys
    * left outer join
    * right outer join
    * full outer join
    * left semi/anti join
    * test join keys of different types
    * join with null column
    */


  /*
 * table1 (cint_1_200, cdouble_200, cdecimal_200, cdate_200)
 * table2 (cint_201_400, cfloat_200, cdecimal_100, cdouble_1_100)
 * table3 (cint_101_300, cfloat_100, cbool)
 *
 * select *
 * from table1 cross join table2
 * cross join table3
 * where true
 *
 */
  test("3 tables cross join where true") {

    /** execute join */
    val join = Join(table1, table2, Cross, None)

    val join2 = Join(join, table3, Cross, None)

    /** join size define */
    val joinSizeInBytes = BigInt(200L * 200 * 200 * (8 + (4 + 8 + 8 + 4) + (4 + 4 + 8 + 8) + (4 + 4 + 1)))
    val joinRowCount = BigInt(200L * 200 * 200)

    val expectedColName = Seq("cint_1_200", "cdouble_200", "cdecimal_200", "cdate_200",
      "cint_201_400", "cfloat_200", "cdecimal_100", "cdouble_1_100",
      "cint_101_300", "cfloat_100", "cbool")

    val joinExpectedStats = Statistics(
      sizeInBytes = joinSizeInBytes,
      rowCount = Some(joinRowCount),
      // Keep the column stat from both sides unchanged.
      attributeStats = AttributeMap(expectedColName.map(nameToColInfo)))

    assert(join2.stats == joinExpectedStats, "join size don't match expected size")

    /** define filter condition */
    val condition = TrueLiteral

    /** define expected stats */
    val expectedRowCount = 200 * 200 * 200
    val expectedColStats = Seq(
      nameToAttr("cint_1_200") -> ColumnStat(distinctCount = 200,
        min = Some(1), max = Some(200), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cdouble_200") -> ColumnStat(distinctCount = 200,
        min = Some(1.0), max = Some(200.0), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdecimal_200") -> ColumnStat(distinctCount = 200,
        min = Some(decMin_200), max = Some(decMax_200), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdate_200") -> ColumnStat(distinctCount = 200,
        min = Some(dMin_200), max = Some(dMax_200), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cint_201_400") -> ColumnStat(distinctCount = 200,
        min = Some(201), max = Some(400), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cfloat_200") -> ColumnStat(distinctCount = 200,
        min = Some(1.0), max = Some(200.0), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cdecimal_100") -> ColumnStat(distinctCount = 100,
        min = Some(decMin_100), max = Some(decMax_100), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdouble_1_100") -> ColumnStat(distinctCount = 100,
        min = Some(1.0), max = Some(100.0), nullCount = 0, avgLen = 8, maxLen = 8),

      /** actual 200 but estimation 100 */
      nameToAttr("cint_101_300") -> ColumnStat(distinctCount = 100,
        min = Some(101), max = Some(300), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cfloat_100") -> ColumnStat(distinctCount = 100,
        min = Some(1.0), max = Some(100.0), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cbool") -> ColumnStat(distinctCount = 2,
        min = Some(false), max = Some(true), nullCount = 0, avgLen = 1, maxLen = 1))

    validateEstimatedStats(Filter(condition, join2), expectedColStats,
      expectedRowCount = expectedRowCount)
  }

  /*
   * table1 (cint_1_200, cdouble_200, cdecimal_200, cdate_200)
   * table2 (cint_201_400, cfloat_200, cdecimal_100, cdouble_1_100)
   * table3 (cint_101_300, cfloat_100， cbool)
   * table4 (cint_1_100, cdouble_50_149, cdate_100)
   *
   * select *
   * from table1 t1 cross join table2 t2
   * cross join table3 t3
   * cross join table4 t4
   * where true
   *
   */

  test("4 tables cross join where true") {

    /** execute join */
    val join = Join(table1, table2, Cross, None)

    val join2 = Join(join, table3, Cross, None)

    val join3 = Join(join2, table4, Cross, None)

    /** join size define */
    val joinSizeInBytes = BigInt(200L * 200 * 200 * 100 * (8 + (4 + 8 + 8 + 4) + (4 + 4 + 8 + 8) + (4 + 4 + 1) + (4 + 8 + 4)))
    val joinRowCount = BigInt(200L * 200 * 200 * 100)

    val expectedColName = Seq("cint_1_200", "cdouble_200", "cdecimal_200", "cdate_200",
      "cint_201_400", "cfloat_200", "cdecimal_100", "cdouble_1_100",
      "cint_101_300", "cfloat_100", "cbool",
      "cint_1_100", "cdouble_50_149", "cdate_100")

    val joinExpectedStats = Statistics(
      sizeInBytes = joinSizeInBytes,
      rowCount = Some(joinRowCount),
      // Keep the column stat from both sides unchanged.
      attributeStats = AttributeMap(expectedColName.map(nameToColInfo)))

    assert(join3.stats == joinExpectedStats, "join size don't match expected size")

    /** define filter condition */
    val condition = TrueLiteral

    /** define expected stats */
    val expectedRowCount = 200 * 200 * 200 * 100
    val expectedColStats = Seq(
      nameToAttr("cint_1_200") -> ColumnStat(distinctCount = 200,
        min = Some(1), max = Some(200), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cdouble_200") -> ColumnStat(distinctCount = 200,
        min = Some(1.0), max = Some(200.0), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdecimal_200") -> ColumnStat(distinctCount = 200,
        min = Some(decMin_200), max = Some(decMax_200), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdate_200") -> ColumnStat(distinctCount = 200,
        min = Some(dMin_200), max = Some(dMax_200), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cint_201_400") -> ColumnStat(distinctCount = 200,
        min = Some(201), max = Some(400), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cfloat_200") -> ColumnStat(distinctCount = 200,
        min = Some(1.0), max = Some(200.0), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cdecimal_100") -> ColumnStat(distinctCount = 100,
        min = Some(decMin_100), max = Some(decMax_100), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdouble_1_100") -> ColumnStat(distinctCount = 100,
        min = Some(1.0), max = Some(100.0), nullCount = 0, avgLen = 8, maxLen = 8),

      /** actual 200 but estimation 100 */
      nameToAttr("cint_101_300") -> ColumnStat(distinctCount = 100,
        min = Some(101), max = Some(300), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cfloat_100") -> ColumnStat(distinctCount = 100,
        min = Some(1.0), max = Some(100.0), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cbool") -> ColumnStat(distinctCount = 2,
        min = Some(false), max = Some(true), nullCount = 0, avgLen = 1, maxLen = 1),

      nameToAttr("cint_1_100") -> ColumnStat(distinctCount = 100,
        min = Some(1), max = Some(100), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cdouble_50_149") -> ColumnStat(distinctCount = 100,
        min = Some(50.0), max = Some(149.0), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdate_100") -> ColumnStat(distinctCount = 100,
        min = Some(dMin_100), max = Some(dMax_100), nullCount = 0, avgLen = 4, maxLen = 4))

    validateEstimatedStats(Filter(condition, join3), expectedColStats,
      expectedRowCount = expectedRowCount)
  }


  /*
   * table1 (cint_1_200, cdouble_200, cdecimal_200, cdate_200)
   * table2 (cint_201_400, cfloat_200, cdecimal_100, cdouble_1_100)
   * table3 (cint_101_300, cfloat_100， cbool)
   * table4 (cint_1_100, cdouble_50_149, cdate_100)
   *
   * select *
   * from table1 t1 cross join table t2
   * cross join table3 t3
   * where false
   *
   */
  test("4 tables cross join where false") {

    /** execute join */
    val join = Join(table1, table2, Cross, None)

    val join2 = Join(join, table3, Cross, None)

    val join3 = Join(join2, table4, Cross, None)

    /** join size define */
    val joinSizeInBytes = BigInt(200L * 200 * 200 * 100 * (8 + (4 + 8 + 8 + 4) + (4 + 4 + 8 + 8) + (4 + 4 + 1) + (4 + 8 + 4)))
    val joinRowCount = BigInt(200L * 200 * 200 * 100)

    val expectedColName = Seq("cint_1_200", "cdouble_200", "cdecimal_200", "cdate_200",
      "cint_201_400", "cfloat_200", "cdecimal_100", "cdouble_1_100",
      "cint_101_300", "cfloat_100", "cbool",
      "cint_1_100", "cdouble_50_149", "cdate_100")

    val joinExpectedStats = Statistics(
      sizeInBytes = joinSizeInBytes,
      rowCount = Some(joinRowCount),
      // Keep the column stat from both sides unchanged.
      attributeStats = AttributeMap(expectedColName.map(nameToColInfo)))

    assert(join3.stats == joinExpectedStats, "join size don't match expected size")

    /** define filter condition */
    val condition = FalseLiteral

    /** define expected stats */
    val expectedRowCount = 0
    val expectedColStats = Nil

    validateEstimatedStats(Filter(condition, join3), expectedColStats,
      expectedRowCount = expectedRowCount)
  }


  /*
   * table1 (cint_1_200, cdouble_200, cdecimal_200, cdate_200)
   * table2 (cint_201_400, cfloat_200, cdecimal_100, cdouble_1_100)
   * table3 (cint_101_300, cfloat_100， cbool)
   *
   * select *
   * from table1 t1 left outer join table2 t2 on t1.cint_1_200 = t2.cint_201_400
   * inner join table3 t3 on t1.cint_1_200 = t3.cint_101_300
   * where true
   *
   */
  test("""3 tables disjoint left outer join and inner join
    where true""") {

    /** execute join */
    val joinType = Some(EqualTo(nameToAttr("cint_1_200"), nameToAttr("cint_201_400")))

    val join = Join(table1, table2, LeftOuter, joinType)

    val joinType2 = Some(EqualTo(nameToAttr("cint_1_200"), nameToAttr("cint_101_300")))

    val join2 = Join(join, table3, Inner, joinType2)

    /** join size define */
    /** actual joinrowcount is 100 but estimation 200 */
    val joinSizeInBytes = BigInt(200L * (8 + (4 + 8 + 8 + 4) + (4 + 4 + 8 + 8) + (4 + 4 + 1)))
    val joinRowCount = BigInt(200L)

    val joinExpectedStats = Statistics(
      sizeInBytes = joinSizeInBytes,
      rowCount = Some(joinRowCount),
      // Keep the column stat from both sides unchanged.
      attributeStats = AttributeMap(Seq(
        nameToAttr("cint_1_200") -> ColumnStat(distinctCount = 100, min = Some(101),
          max = Some(200), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cdouble_200") -> ColumnStat(distinctCount = 200, min = Some(1.0),
          max = Some(200.0), nullCount = 0, avgLen = 8, maxLen = 8),
        nameToAttr("cdecimal_200") -> ColumnStat(distinctCount = 200, min = Some(decMin_200),
          max = Some(decMax_200), nullCount = 0, avgLen = 8, maxLen = 8),
        nameToAttr("cdate_200") -> ColumnStat(distinctCount = 200, min = Some(dMin_200),
          max = Some(dMax_200), nullCount = 0, avgLen = 4, maxLen = 4),

        nameToAttr("cint_201_400") -> nullColumnStat(nameToAttr("cint_201_400").dataType, 200),
        nameToAttr("cfloat_200") -> nullColumnStat(nameToAttr("cfloat_200").dataType, 200),
        nameToAttr("cdecimal_100") -> nullColumnStat(nameToAttr("cdecimal_100").dataType, 200),
        nameToAttr("cdouble_1_100") -> nullColumnStat(nameToAttr("cdouble_1_100").dataType, 200),
        nameToAttr("cint_101_300") -> ColumnStat(distinctCount = 100, min = Some(101),
          max = Some(200), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cfloat_100") -> ColumnStat(distinctCount = 100, min = Some(1.0),
          max = Some(100.0), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cbool") -> ColumnStat(distinctCount = 2, min = Some(false),
          max = Some(true), nullCount = 0, avgLen = 1, maxLen = 1))))

    assert(join2.stats == joinExpectedStats, "joined table don't match expected table")

    /** define filter condition */
    val condition = TrueLiteral

    /** define expected stats */
    val expectedRowCount = 200L
    val expectedColStats = Seq(
      nameToAttr("cint_1_200") -> ColumnStat(distinctCount = 100,
        min = Some(101), max = Some(200), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cdouble_200") -> ColumnStat(distinctCount = 200,
        min = Some(1.0), max = Some(200.0), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdecimal_200") -> ColumnStat(distinctCount = 200,
        min = Some(decMin_200), max = Some(decMax_200), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdate_200") -> ColumnStat(distinctCount = 200,
        min = Some(dMin_200), max = Some(dMax_200), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cint_201_400") -> ColumnStat(distinctCount = 0,
        None, None, nullCount = 200, avgLen = 4, maxLen = 4),

      nameToAttr("cfloat_200") -> ColumnStat(distinctCount = 0,
        None, None, nullCount = 200, avgLen = 4, maxLen = 4),

      nameToAttr("cdecimal_100") -> ColumnStat(distinctCount = 0,
        None, None, nullCount = 200, avgLen = 8, maxLen = 8),

      nameToAttr("cdouble_1_100") -> ColumnStat(distinctCount = 0,
        None, None, nullCount = 200, avgLen = 8, maxLen = 8),

      nameToAttr("cint_101_300") -> ColumnStat(distinctCount = 100,
        min = Some(101), max = Some(200), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cfloat_100") -> ColumnStat(distinctCount = 100,
        min = Some(1.0), max = Some(100.0), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cbool") -> ColumnStat(distinctCount = 2,
        min = Some(false), max = Some(true), nullCount = 0, avgLen = 1, maxLen = 1))

    validateEstimatedStats(Filter(condition, join2), expectedColStats,
      expectedRowCount = expectedRowCount)
  }



/*
 * table1 (cint_1_200, cdouble_200, cdecimal_200, cdate_200)
 * table2 (cint_201_400, cfloat_200, cdecimal_100, cdouble_1_100)
 * table3 (cint_101_300, cfloat_100， cbool)
 *
 * select *
 * from table1 t1 left outer join table2 t2 on t1.cint_1_200 = t2.cint_201_400
 * inner join table3 t3 on t1.cint_1_200 = t3.cint_101_300
 * where t1.cint_1_200 >= 101 and t1.cint_1_200 <= 150
 *
 */
  test("""3 tables disjoint left outer join and inner join
    where cint_1_200 >= 101 AND cint_1_200 <= 150""") {

    /** execute join */
    val joinType = Some(EqualTo(nameToAttr("cint_1_200"), nameToAttr("cint_201_400")))

    val join = Join(table1, table2, LeftOuter, joinType)

    val joinType2 = Some(EqualTo(nameToAttr("cint_1_200"), nameToAttr("cint_101_300")))

    val join2 = Join(join, table3, Inner, joinType2)

    /** join size define */
    /** actual joinrowcount is 100 but estimation 200 */
    val joinSizeInBytes = BigInt(200L * (8 + (4 + 8 + 8 + 4) + (4 + 4 + 8 + 8) + (4 + 4 + 1)))
    val joinRowCount = BigInt(200L)

    val joinExpectedStats = Statistics(
      sizeInBytes = joinSizeInBytes,
      rowCount = Some(joinRowCount),
      // Keep the column stat from both sides unchanged.
      attributeStats = AttributeMap(Seq(
        nameToAttr("cint_1_200") -> ColumnStat(distinctCount = 100, min = Some(101),
          max = Some(200), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cdouble_200") -> ColumnStat(distinctCount = 200, min = Some(1.0),
          max = Some(200.0), nullCount = 0, avgLen = 8, maxLen = 8),
        nameToAttr("cdecimal_200") -> ColumnStat(distinctCount = 200, min = Some(decMin_200),
          max = Some(decMax_200), nullCount = 0, avgLen = 8, maxLen = 8),
        nameToAttr("cdate_200") -> ColumnStat(distinctCount = 200, min = Some(dMin_200),
          max = Some(dMax_200), nullCount = 0, avgLen = 4, maxLen = 4),

        nameToAttr("cint_201_400") -> nullColumnStat(nameToAttr("cint_201_400").dataType, 200),
        nameToAttr("cfloat_200") -> nullColumnStat(nameToAttr("cfloat_200").dataType, 200),
        nameToAttr("cdecimal_100") -> nullColumnStat(nameToAttr("cdecimal_100").dataType, 200),
        nameToAttr("cdouble_1_100") -> nullColumnStat(nameToAttr("cdouble_1_100").dataType, 200),
        nameToAttr("cint_101_300") -> ColumnStat(distinctCount = 100, min = Some(101),
          max = Some(200), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cfloat_100") -> ColumnStat(distinctCount = 100, min = Some(1.0),
          max = Some(100.0), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cbool") -> ColumnStat(distinctCount = 2, min = Some(false),
          max = Some(true), nullCount = 0, avgLen = 1, maxLen = 1))))

    assert(join2.stats == joinExpectedStats, "joined table don't match expected table")

    /** define filter condition */
    val condition = And(GreaterThanOrEqual(attrInt_1_200, Literal(101)), LessThanOrEqual(attrInt_1_200, Literal(150)))

    /** define expected stats */
    val expectedRowCount = 99L
    val expectedColStats = Seq(
      nameToAttr("cint_1_200") -> ColumnStat(distinctCount = 50,
        min = Some(101), max = Some(150), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cdouble_200") -> ColumnStat(distinctCount = 99,
        min = Some(1.0), max = Some(200.0), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdecimal_200") -> ColumnStat(distinctCount = 99,
        min = Some(decMin_200), max = Some(decMax_200), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdate_200") -> ColumnStat(distinctCount = 99,
        min = Some(dMin_200), max = Some(dMax_200), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cint_201_400") -> ColumnStat(distinctCount = 0,
        None, None, nullCount = 200, avgLen = 4, maxLen = 4),

      nameToAttr("cfloat_200") -> ColumnStat(distinctCount = 0,
        None, None, nullCount = 200, avgLen = 4, maxLen = 4),

      nameToAttr("cdecimal_100") -> ColumnStat(distinctCount = 0,
        None, None, nullCount = 200, avgLen = 8, maxLen = 8),

      nameToAttr("cdouble_1_100") -> ColumnStat(distinctCount = 0,
        None, None, nullCount = 200, avgLen = 8, maxLen = 8),

      nameToAttr("cint_101_300") -> ColumnStat(distinctCount = 50,
        min = Some(101), max = Some(200), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cfloat_100") -> ColumnStat(distinctCount = 50,
        min = Some(1.0), max = Some(100.0), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cbool") -> ColumnStat(distinctCount = 1,
        min = Some(false), max = Some(true), nullCount = 0, avgLen = 1, maxLen = 1))

    validateEstimatedStats(Filter(condition, join2), expectedColStats,
      expectedRowCount = expectedRowCount)
  }


  /*
 * table1 (cint_1_200, cdouble_200, cdecimal_200, cdate_200)
 * table2 (cint_201_400, cfloat_200, cdecimal_100, cdouble_1_100)
 * table4 (cint_1_100, cdouble_50_149, cdate_100)
 *
 * select *
 * from table1 t1 left outer join table2 t2 on t1.cdouble_200 = t2.cdouble_1_100
 * inner join table4 t4 on t1.cdate_200 = t4.cdate_100
 * where true
 *
 */
  test(
    """3 tables left outer join and inner join
    where true""") {

    /** execute join */
    val joinType = Some(EqualTo(nameToAttr("cdouble_200"), nameToAttr("cdouble_1_100")))

    val join = Join(table1, table2, LeftOuter, joinType)

    val joinType2 = Some(EqualTo(nameToAttr("cdate_200"), nameToAttr("cdate_100")))

    val join2 = Join(join, table4, Inner, joinType2)

    /** join size define */
    val joinSizeInBytes = BigInt(100L * (8 + (4 + 8 + 8 + 4) + (4 + 4 + 8 + 8) + (4 + 8 + 4)))
    val joinRowCount = BigInt(100L)

    val joinExpectedStats = Statistics(
      sizeInBytes = joinSizeInBytes,
      rowCount = Some(joinRowCount),
      // Keep the column stat from both sides unchanged.
      attributeStats = AttributeMap(Seq(
        nameToAttr("cint_1_200") -> ColumnStat(distinctCount = 100, min = Some(1),
          max = Some(200), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cdouble_200") -> ColumnStat(distinctCount = 100, min = Some(1.0),
          max = Some(200.0), nullCount = 0, avgLen = 8, maxLen = 8),
        nameToAttr("cdecimal_200") -> ColumnStat(distinctCount = 100, min = Some(decMin_200),
          max = Some(decMax_200), nullCount = 0, avgLen = 8, maxLen = 8),
        nameToAttr("cdate_200") -> ColumnStat(distinctCount = 100, min = Some(dMin_200),
          max = Some(dMax_100), nullCount = 0, avgLen = 4, maxLen = 4),

        nameToAttr("cint_201_400") -> ColumnStat(distinctCount = 100, min = Some(201),
          max = Some(400), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cfloat_200") -> ColumnStat(distinctCount = 100, min = Some(1.0),
          max = Some(200.0), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cdecimal_100") -> ColumnStat(distinctCount = 50, min = Some(decMin_100),
          max = Some(decMax_100), nullCount = 0, avgLen = 8, maxLen = 8),
        nameToAttr("cdouble_1_100") -> ColumnStat(distinctCount = 50, min = Some(1.0),
          max = Some(100.0), nullCount = 0, avgLen = 8, maxLen = 8),

        nameToAttr("cint_1_100") -> ColumnStat(distinctCount = 100, min = Some(1),
          max = Some(100), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cdouble_50_149") -> ColumnStat(distinctCount = 100, min = Some(50.0),
          max = Some(149.0), nullCount = 0, avgLen = 8, maxLen = 8),
        nameToAttr("cdate_100") -> ColumnStat(distinctCount = 100, min = Some(dMin_100),
          max = Some(dMax_100), nullCount = 0, avgLen = 4, maxLen = 4))))

    assert(join2.stats == joinExpectedStats, "joined table don't match expected table")

    /** define filter condition */
    val condition = TrueLiteral

    /** define expected stats */
    val expectedRowCount = 100L
    val expectedColStats = Seq(
      nameToAttr("cint_1_200") -> ColumnStat(distinctCount = 100,
        min = Some(1), max = Some(200), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cdouble_200") -> ColumnStat(distinctCount = 100,
        min = Some(1.0), max = Some(200.0), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdecimal_200") -> ColumnStat(distinctCount = 100,
        min = Some(decMin_200), max = Some(decMax_200), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdate_200") -> ColumnStat(distinctCount = 100,
        min = Some(dMin_200), max = Some(dMax_100), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cint_201_400") -> ColumnStat(distinctCount = 100,
        min = Some(201), max = Some(400), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cfloat_200") -> ColumnStat(distinctCount = 100,
        min = Some(1.0), max = Some(200.0), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cdecimal_100") -> ColumnStat(distinctCount = 50,
        min = Some(decMin_100), max = Some(decMax_100), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdouble_1_100") -> ColumnStat(distinctCount = 50,
        min = Some(1.0), max = Some(100.0), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cint_1_100") -> ColumnStat(distinctCount = 100,
        min = Some(1), max = Some(100), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cdouble_50_149") -> ColumnStat(distinctCount = 100,
        min = Some(50.0), max = Some(149.0), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdate_100") -> ColumnStat(distinctCount = 100,
        min = Some(dMin_100), max = Some(dMax_100), nullCount = 0, avgLen = 4, maxLen = 4))

    validateEstimatedStats(Filter(condition, join2), expectedColStats,
      expectedRowCount = expectedRowCount)
  }


  /*
* table1 (cint_1_200, cdouble_200, cdecimal_200, cdate_200)
* table2 (cint_201_400, cfloat_200, cdecimal_100, cdouble_1_100)
* table4 (cint_1_100, cdouble_50_149, cdate_100)
*
* select *
* from table1 t1 left outer join table2 t2 on t1.cdouble_200 = t2.cdouble_1_100
* inner join table4 t4 on t1.cdate_200 = t4.cdate_100
* where t1.cdecimal_200 is not null and t2.cfloat_200 >= 50.0
*
*/
  test(
    """3 tables left outer join and inner join
    where cdecimal_200 is not null and cfloat_200 >= 50.0""") {

    /** execute join */
    val joinType = Some(EqualTo(nameToAttr("cdouble_200"), nameToAttr("cdouble_1_100")))

    val join = Join(table1, table2, LeftOuter, joinType)

    val joinType2 = Some(EqualTo(nameToAttr("cdate_200"), nameToAttr("cdate_100")))

    val join2 = Join(join, table4, Inner, joinType2)

    /** join size define */
    val joinSizeInBytes = BigInt(100L * (8 + (4 + 8 + 8 + 4) + (4 + 4 + 8 + 8) + (4 + 8 + 4)))
    val joinRowCount = BigInt(100L)

    val joinExpectedStats = Statistics(
      sizeInBytes = joinSizeInBytes,
      rowCount = Some(joinRowCount),
      // Keep the column stat from both sides unchanged.
      attributeStats = AttributeMap(Seq(
        nameToAttr("cint_1_200") -> ColumnStat(distinctCount = 100, min = Some(1),
          max = Some(200), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cdouble_200") -> ColumnStat(distinctCount = 100, min = Some(1.0),
          max = Some(200.0), nullCount = 0, avgLen = 8, maxLen = 8),
        nameToAttr("cdecimal_200") -> ColumnStat(distinctCount = 100, min = Some(decMin_200),
          max = Some(decMax_200), nullCount = 0, avgLen = 8, maxLen = 8),
        nameToAttr("cdate_200") -> ColumnStat(distinctCount = 100, min = Some(dMin_200),
          max = Some(dMax_100), nullCount = 0, avgLen = 4, maxLen = 4),

        nameToAttr("cint_201_400") -> ColumnStat(distinctCount = 100, min = Some(201),
          max = Some(400), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cfloat_200") -> ColumnStat(distinctCount = 100, min = Some(1.0),
          max = Some(200.0), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cdecimal_100") -> ColumnStat(distinctCount = 50, min = Some(decMin_100),
          max = Some(decMax_100), nullCount = 0, avgLen = 8, maxLen = 8),
        nameToAttr("cdouble_1_100") -> ColumnStat(distinctCount = 50, min = Some(1.0),
          max = Some(100.0), nullCount = 0, avgLen = 8, maxLen = 8),

        nameToAttr("cint_1_100") -> ColumnStat(distinctCount = 100, min = Some(1),
          max = Some(100), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cdouble_50_149") -> ColumnStat(distinctCount = 100, min = Some(50.0),
          max = Some(149.0), nullCount = 0, avgLen = 8, maxLen = 8),
        nameToAttr("cdate_100") -> ColumnStat(distinctCount = 100, min = Some(dMin_100),
          max = Some(dMax_100), nullCount = 0, avgLen = 4, maxLen = 4))))

    assert(join2.stats == joinExpectedStats, "joined table don't match expected table")

    /** define filter condition */
      // t1.cdecimal_200 is not null and t2.cfloat_200 >= 50.0
    val condition = And(IsNotNull(attrDecimal_200), GreaterThanOrEqual(attrFloat_200, Literal(50.0, FloatType)))

    /** define expected stats */
    val expectedRowCount = 76L
    val expectedColStats = Seq(
      nameToAttr("cint_1_200") -> ColumnStat(distinctCount = 76,
        min = Some(1), max = Some(200), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cdouble_200") -> ColumnStat(distinctCount = 76,
        min = Some(1.0), max = Some(200.0), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdecimal_200") -> ColumnStat(distinctCount = 76,
        min = Some(decMin_200), max = Some(decMax_200), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdate_200") -> ColumnStat(distinctCount = 76,
        min = Some(dMin_200), max = Some(dMax_100), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cint_201_400") -> ColumnStat(distinctCount = 76,
        min = Some(201), max = Some(400), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cfloat_200") -> ColumnStat(distinctCount = 76,
        min = Some(50.0), max = Some(200.0), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cdecimal_100") -> ColumnStat(distinctCount = 38,
        min = Some(decMin_100), max = Some(decMax_100), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdouble_1_100") -> ColumnStat(distinctCount = 38,
        min = Some(1.0), max = Some(100.0), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cint_1_100") -> ColumnStat(distinctCount = 76,
        min = Some(1), max = Some(100), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cdouble_50_149") -> ColumnStat(distinctCount = 76,
        min = Some(50.0), max = Some(149.0), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdate_100") -> ColumnStat(distinctCount = 76,
        min = Some(dMin_100), max = Some(dMax_100), nullCount = 0, avgLen = 4, maxLen = 4))

    validateEstimatedStats(Filter(condition, join2), expectedColStats,
      expectedRowCount = expectedRowCount)
  }



  /*
 * table1 (cint_1_200, cdouble_200, cdecimal_200, cdate_200)
 * table2 (cint_201_400, cfloat_200, cdecimal_100, cdouble_1_100)
 * table4 (cint_1_100, cdouble_50_149, cdate_100)
 *
 * select *
 * from table1 t1 left outer join table2 t2 on t1.cdouble_200 = t2.cdouble_1_100
 * and t1.cdecimal_200 = t2.cdecimal_100
 * inner join table4 t4 on t1.cdate_200 = t4.cdate_100
 * and t2.cdouble_1_100 = t4.cdouble_50_149
 * where true
 *
 */
  test("""3 tables left outer join and inner join with multi condiction
    where true""") {

    /** execute join */
    val joinTypeLeft1 = EqualTo(nameToAttr("cdouble_200"), nameToAttr("cdouble_1_100"))

    val joinTypeRight1 = EqualTo(nameToAttr("cdecimal_200"), nameToAttr("cdecimal_100"))

    val joinType = Some(And(joinTypeLeft1, joinTypeRight1))

    val join = Join(table1, table2, LeftOuter, joinType)

    val joinTypeLeft2 = EqualTo(nameToAttr("cdate_200"), nameToAttr("cdate_100"))

    val joinTypeRight2 = EqualTo(nameToAttr("cdouble_1_100"), nameToAttr("cdouble_50_149"))

    val joinType2 = Some(And(joinTypeLeft2, joinTypeRight2))

    val join2 = Join(join, table4, Inner, joinType2)

    /** join size define */
    val joinSizeInBytes = BigInt(100L * (8 + (4 + 8 + 8 + 4) + (4 + 4 + 8 + 8) + (4 + 8 + 4)))
    val joinRowCount = BigInt(100L)

    val joinExpectedStats = Statistics(
      sizeInBytes = joinSizeInBytes,
      rowCount = Some(joinRowCount),
      // Keep the column stat from both sides unchanged.
      attributeStats = AttributeMap(Seq(
        nameToAttr("cint_1_200") -> ColumnStat(distinctCount = 100, min = Some(1),
          max = Some(200), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cdouble_200") -> ColumnStat(distinctCount = 100, min = Some(1.0),
          max = Some(200.0), nullCount = 0, avgLen = 8, maxLen = 8),
        nameToAttr("cdecimal_200") -> ColumnStat(distinctCount = 100, min = Some(decMin_200),
          max = Some(decMax_200), nullCount = 0, avgLen = 8, maxLen = 8),
        nameToAttr("cdate_200") -> ColumnStat(distinctCount = 100, min = Some(dMin_200),
          max = Some(dMax_100), nullCount = 0, avgLen = 4, maxLen = 4),

        nameToAttr("cint_201_400") -> ColumnStat(distinctCount = 100, min = Some(201),
          max = Some(400), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cfloat_200") -> ColumnStat(distinctCount = 100, min = Some(1.0),
          max = Some(200.0), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cdecimal_100") -> ColumnStat(distinctCount = 50, min = Some(decMin_100),
          max = Some(decMax_100), nullCount = 0, avgLen = 8, maxLen = 8),
        nameToAttr("cdouble_1_100") -> ColumnStat(distinctCount = 100, min = Some(50.0),
          max = Some(100.0), nullCount = 0, avgLen = 8, maxLen = 8),

        nameToAttr("cint_1_100") -> ColumnStat(distinctCount = 100, min = Some(1),
          max = Some(100), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cdouble_50_149") -> ColumnStat(distinctCount = 100, min = Some(50.0),
          max = Some(100.0), nullCount = 0, avgLen = 8, maxLen = 8),
        nameToAttr("cdate_100") -> ColumnStat(distinctCount = 100, min = Some(dMin_100),
          max = Some(dMax_100), nullCount = 0, avgLen = 4, maxLen = 4))))

    assert(join2.stats == joinExpectedStats, "joined table don't match expected table")

    /** define filter condition */
    val condition = TrueLiteral

    /** define expected stats */
    val expectedRowCount = 100L
    val expectedColStats = Seq(
      nameToAttr("cint_1_200") -> ColumnStat(distinctCount = 100,
        min = Some(1), max = Some(200), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cdouble_200") -> ColumnStat(distinctCount = 100,
        min = Some(1.0), max = Some(200.0), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdecimal_200") -> ColumnStat(distinctCount = 100,
        min = Some(decMin_200), max = Some(decMax_200), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdate_200") -> ColumnStat(distinctCount = 100,
        min = Some(dMin_200), max = Some(dMax_100), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cint_201_400") -> ColumnStat(distinctCount = 100,
        min = Some(201), max = Some(400), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cfloat_200") -> ColumnStat(distinctCount = 100,
        min = Some(1.0), max = Some(200.0), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cdecimal_100") -> ColumnStat(distinctCount = 50,
        min = Some(decMin_100), max = Some(decMax_100), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdouble_1_100") -> ColumnStat(distinctCount = 100,
        min = Some(50.0), max = Some(100.0), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cint_1_100") -> ColumnStat(distinctCount = 100,
        min = Some(1), max = Some(100), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cdouble_50_149") -> ColumnStat(distinctCount = 100,
        min = Some(50.0), max = Some(100.0), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdate_100") -> ColumnStat(distinctCount = 100,
        min = Some(dMin_100), max = Some(dMax_100), nullCount = 0, avgLen = 4, maxLen = 4))

    validateEstimatedStats(Filter(condition, join2), expectedColStats,
      expectedRowCount = expectedRowCount)
  }

  /*
   * table1 (cint_1_200, cdouble_200, cdecimal_200, cdate_200)
   * table2 (cint_201_400, cfloat_200, cdecimal_100, cdouble_1_100)
   * table4 (cint_1_100, cdouble_50_149, cdate_100)
   *
   * select *
   * from table1 t1 left outer join table2 t2 on t1.cdouble_200 = t2.cdouble_1_100
   * and t1.cdecimal_200 = t2.cdecimal_100
   * inner join table4 t4 on t1.cdate_200 = t4.cdate_100
   * and t2.cdouble_1_100 = t4.cdouble_50_149
   * where t2.cdecimal_100 >= 0.10 and t1.cdate_100 <= Date('2019-01-30')
   *
   */
  test("""3 tables left outer join and inner join with multi condiction
    where cdecimal_100 >= 0.10 and cdate_100 <= Date('2019-01-30')""") {

    /** execute join */
    val joinTypeLeft1 = EqualTo(nameToAttr("cdouble_200"), nameToAttr("cdouble_1_100"))

    val joinTypeRight1 = EqualTo(nameToAttr("cdecimal_200"), nameToAttr("cdecimal_100"))

    val joinType = Some(And(joinTypeLeft1, joinTypeRight1))

    val join = Join(table1, table2, LeftOuter, joinType)

    val joinTypeLeft2 = EqualTo(nameToAttr("cdate_200"), nameToAttr("cdate_100"))

    val joinTypeRight2 = EqualTo(nameToAttr("cdouble_1_100"), nameToAttr("cdouble_50_149"))

    val joinType2 = Some(And(joinTypeLeft2, joinTypeRight2))

    val join2 = Join(join, table4, Inner, joinType2)

    /** join size define */
    val joinSizeInBytes = BigInt(100L * (8 + (4 + 8 + 8 + 4) + (4 + 4 + 8 + 8) + (4 + 8 + 4)))
    val joinRowCount = BigInt(100L)

    val joinExpectedStats = Statistics(
      sizeInBytes = joinSizeInBytes,
      rowCount = Some(joinRowCount),
      // Keep the column stat from both sides unchanged.
      attributeStats = AttributeMap(Seq(
        nameToAttr("cint_1_200") -> ColumnStat(distinctCount = 100, min = Some(1),
          max = Some(200), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cdouble_200") -> ColumnStat(distinctCount = 100, min = Some(1.0),
          max = Some(200.0), nullCount = 0, avgLen = 8, maxLen = 8),
        nameToAttr("cdecimal_200") -> ColumnStat(distinctCount = 100, min = Some(decMin_200),
          max = Some(decMax_200), nullCount = 0, avgLen = 8, maxLen = 8),
        nameToAttr("cdate_200") -> ColumnStat(distinctCount = 100, min = Some(dMin_200),
          max = Some(dMax_100), nullCount = 0, avgLen = 4, maxLen = 4),

        nameToAttr("cint_201_400") -> ColumnStat(distinctCount = 100, min = Some(201),
          max = Some(400), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cfloat_200") -> ColumnStat(distinctCount = 100, min = Some(1.0),
          max = Some(200.0), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cdecimal_100") -> ColumnStat(distinctCount = 50, min = Some(decMin_100),
          max = Some(decMax_100), nullCount = 0, avgLen = 8, maxLen = 8),
        nameToAttr("cdouble_1_100") -> ColumnStat(distinctCount = 100, min = Some(50.0),
          max = Some(100.0), nullCount = 0, avgLen = 8, maxLen = 8),

        nameToAttr("cint_1_100") -> ColumnStat(distinctCount = 100, min = Some(1),
          max = Some(100), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cdouble_50_149") -> ColumnStat(distinctCount = 100, min = Some(50.0),
          max = Some(100.0), nullCount = 0, avgLen = 8, maxLen = 8),
        nameToAttr("cdate_100") -> ColumnStat(distinctCount = 100, min = Some(dMin_100),
          max = Some(dMax_100), nullCount = 0, avgLen = 4, maxLen = 4))))

    assert(join2.stats == joinExpectedStats, "joined table don't match expected table")

    /** define filter condition */

    val d20190130 = DateTimeUtils.fromJavaDate(Date.valueOf("2019-01-30"))

    val dec_0_10 = Decimal("0.100000000000000000")

    val condition = And(GreaterThanOrEqual(attrDecimal_100, Literal(dec_0_10)),
      LessThanOrEqual(attrDate_100, Literal(d20190130, DateType)))

    /** define expected stats */
    val expectedRowCount = 27L
    val expectedColStats = Seq(
      nameToAttr("cint_1_200") -> ColumnStat(distinctCount = 27,
        min = Some(1), max = Some(200), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cdouble_200") -> ColumnStat(distinctCount = 27,
        min = Some(1.0), max = Some(200.0), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdecimal_200") -> ColumnStat(distinctCount = 27,
        min = Some(decMin_200), max = Some(decMax_200), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdate_200") -> ColumnStat(distinctCount = 27,
        min = Some(dMin_200), max = Some(dMax_100), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cint_201_400") -> ColumnStat(distinctCount = 27,
        min = Some(201), max = Some(400), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cfloat_200") -> ColumnStat(distinctCount = 27,
        min = Some(1.0), max = Some(200.0), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cdecimal_100") -> ColumnStat(distinctCount = 14,
        min = Some(Decimal("0.100000000000000000")), max = Some(decMax_100),
        nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdouble_1_100") -> ColumnStat(distinctCount = 27,
        min = Some(50.0), max = Some(100.0), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cint_1_100") -> ColumnStat(distinctCount = 27,
        min = Some(1), max = Some(100), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cdouble_50_149") -> ColumnStat(distinctCount = 27,
        min = Some(50.0), max = Some(100.0), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdate_100") -> ColumnStat(distinctCount = 27,
        min = Some(dMin_100), max = Some(DateTimeUtils.fromJavaDate(Date.valueOf("2019-01-30"))),
        nullCount = 0, avgLen = 4, maxLen = 4))

    validateEstimatedStats(Filter(condition, join2), expectedColStats,
      expectedRowCount = expectedRowCount)
  }


  /*
 * table1 (cint_1_200, cdouble_200, cdecimal_200, cdate_200)
 * table2 (cint_201_400, cfloat_200, cdecimal_100, cdouble_1_100)
 * table4 (cint_1_100, cdouble_50_149, cdate_100)
 *
 * select *
 * from table1 t1 left outer join table4 t4 on t1.cint_1_200 = t4.cint_1_100
 * and t1.cdouble_200 = t4.cdouble_50_149
 * inner join table2 t2 on t1.cdecimal_200 = t2.cdecimal_100
 * and t2.cdouble_1_100 = t4.cdouble_50_149
 * where Not(t1.cint_1_200 < 3 AND t2.cfloat_200 >= 150.0)
 *
 */
  test("""3 tables left outer join and inner join with multi condiction2
    where Not(t1.cint_1_200 < 3 AND t2.cfloat_200 >= 150.0)""") {

    /** execute join */
    val joinTypeLeft1 = EqualTo(nameToAttr("cint_1_200"), nameToAttr("cint_1_100"))

    val joinTypeRight1 = EqualTo(nameToAttr("cdouble_200"), nameToAttr("cdouble_50_149"))

    val joinType = Some(And(joinTypeLeft1, joinTypeRight1))

    val join = Join(table1, table4, LeftOuter, joinType)

    val joinTypeLeft2 = EqualTo(nameToAttr("cdecimal_200"), nameToAttr("cdecimal_100"))

    val joinTypeRight2 = EqualTo(nameToAttr("cdouble_1_100"), nameToAttr("cdouble_50_149"))

    val joinType2 = Some(And(joinTypeLeft2, joinTypeRight2))

    val join2 = Join(join, table2, Inner, joinType2)

    /** join size define */
    val joinSizeInBytes = BigInt(200L * (8 + (4 + 8 + 8 + 4) + (4 + 4 + 8 + 8) + (4 + 8 + 4)))
    val joinRowCount = BigInt(200L)

    val joinExpectedStats = Statistics(
      sizeInBytes = joinSizeInBytes,
      rowCount = Some(joinRowCount),
      // Keep the column stat from both sides unchanged.
      attributeStats = AttributeMap(Seq(
        nameToAttr("cint_1_200") -> ColumnStat(distinctCount = 200, min = Some(1),
          max = Some(200), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cdouble_200") -> ColumnStat(distinctCount = 200, min = Some(1.0),
          max = Some(200.0), nullCount = 0, avgLen = 8, maxLen = 8),

        /** decimal type change */
        nameToAttr("cdecimal_200") -> ColumnStat(distinctCount = 100, min = Some(Decimal(0.01)),
          max = Some(Decimal(1.0)), nullCount = 0, avgLen = 8, maxLen = 8),
        nameToAttr("cdate_200") -> ColumnStat(distinctCount = 200, min = Some(dMin_200),
          max = Some(dMax_200), nullCount = 0, avgLen = 4, maxLen = 4),

        nameToAttr("cint_1_100") -> ColumnStat(distinctCount = 100, min = Some(1),
          max = Some(100), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cdouble_50_149") -> ColumnStat(distinctCount = 100, min = Some(50.0),
          max = Some(100.0), nullCount = 0, avgLen = 8, maxLen = 8),
        nameToAttr("cdate_100") -> ColumnStat(distinctCount = 100, min = Some(dMin_100),
          max = Some(dMax_100), nullCount = 0, avgLen = 4, maxLen = 4),

        nameToAttr("cint_201_400") -> ColumnStat(distinctCount = 200, min = Some(201),
          max = Some(400), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cfloat_200") -> ColumnStat(distinctCount = 200, min = Some(1.0),
          max = Some(200.0), nullCount = 0, avgLen = 4, maxLen = 4),

        /** decimal type change */
        nameToAttr("cdecimal_100") -> ColumnStat(distinctCount = 100, min = Some(Decimal(0.01)),
          max = Some(Decimal(1.0)), nullCount = 0, avgLen = 8, maxLen = 8),
        nameToAttr("cdouble_1_100") -> ColumnStat(distinctCount = 100, min = Some(50.0),
          max = Some(100.0), nullCount = 0, avgLen = 8, maxLen = 8))))

    /** col data compare */
    join2.stats.attributeStats.foreach { kv =>
      val filterColumnStat = joinExpectedStats.attributeStats.get(kv._1).get
      assert(filterColumnStat == kv._2, "join col data different")

    }
    assert(join2.stats == joinExpectedStats, "joined table don't match expected table")

    /** define filter condition */
    val condition = Not(And(LessThan(attrInt_1_200, Literal(3)),
      GreaterThanOrEqual(attrFloat_200, Literal(150.0, FloatType))))

    /** define expected stats */
    val expectedRowCount = 200L
    val expectedColStats = Seq(
      nameToAttr("cint_1_200") -> ColumnStat(distinctCount = 200,
        min = Some(1), max = Some(200), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cdouble_200") -> ColumnStat(distinctCount = 200,
        min = Some(1.0), max = Some(200.0), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdecimal_200") -> ColumnStat(distinctCount = 100,
        min = Some(Decimal(0.01)), max = Some(Decimal(1.0)), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdate_200") -> ColumnStat(distinctCount = 200,
        min = Some(dMin_200), max = Some(dMax_200), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cint_1_100") -> ColumnStat(distinctCount = 100,
        min = Some(1), max = Some(100), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cdouble_50_149") -> ColumnStat(distinctCount = 100,
        min = Some(50.0), max = Some(100.0), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdate_100") -> ColumnStat(distinctCount = 100,
        min = Some(dMin_100), max = Some(dMax_100),
        nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cint_201_400") -> ColumnStat(distinctCount = 200,
        min = Some(201), max = Some(400), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cfloat_200") -> ColumnStat(distinctCount = 200,
        min = Some(1.0), max = Some(200.0), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cdecimal_100") -> ColumnStat(distinctCount = 100,
        min = Some(Decimal(0.01)), max = Some(Decimal(1.0)), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdouble_1_100") -> ColumnStat(distinctCount = 100,
        min = Some(50.0), max = Some(100.0),
        nullCount = 0, avgLen = 8, maxLen = 8))

    validateEstimatedStats(Filter(condition, join2), expectedColStats,
      expectedRowCount = expectedRowCount)
  }


  /*
* table1 (cint_1_200, cdouble_200, cdecimal_200, cdate_200)
* table2 (cint_201_400, cfloat_200, cdecimal_100, cdouble_1_100)
* table4 (cint_1_100, cdouble_50_149, cdate_100)
*
* select *
* from table1 t1 left semi join table4 t4 on t1.cint_1_200 = t4.cint_1_100
* left anti join table2 t2 on t1.cdecimal_200 = t2.cdecimal_100
* and t2.cdouble_1_100 = t4.cdouble_50_149
* where true
*
*/
  test("""3 tables left semi join and left anti join with  condiction
    where true""") {

    /** execute join */
    val joinTypeLeft1 = EqualTo(nameToAttr("cint_1_200"), nameToAttr("cint_1_100"))

    val joinTypeRight1 = EqualTo(nameToAttr("cdouble_200"), nameToAttr("cdouble_50_149"))

    val joinType = Some(And(joinTypeLeft1, joinTypeRight1))

    val join = Join(table1, table4, LeftOuter, joinType)

    val joinTypeLeft2 = EqualTo(nameToAttr("cdecimal_200"), nameToAttr("cdecimal_100"))

    val joinTypeRight2 = EqualTo(nameToAttr("cdouble_1_100"), nameToAttr("cdouble_50_149"))

    val joinType2 = Some(And(joinTypeLeft2, joinTypeRight2))

    val join2 = Join(join, table2, Inner, joinType2)

    /** join size define */
    val joinSizeInBytes = BigInt(200L * (8 + (4 + 8 + 8 + 4) + (4 + 4 + 8 + 8) + (4 + 8 + 4)))
    val joinRowCount = BigInt(200L)

    val joinExpectedStats = Statistics(
      sizeInBytes = joinSizeInBytes,
      rowCount = Some(joinRowCount),
      // Keep the column stat from both sides unchanged.
      attributeStats = AttributeMap(Seq(
        nameToAttr("cint_1_200") -> ColumnStat(distinctCount = 200, min = Some(1),
          max = Some(200), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cdouble_200") -> ColumnStat(distinctCount = 200, min = Some(1.0),
          max = Some(200.0), nullCount = 0, avgLen = 8, maxLen = 8),

        /** decimal type change */
        nameToAttr("cdecimal_200") -> ColumnStat(distinctCount = 100, min = Some(Decimal(0.01)),
          max = Some(Decimal(1.0)), nullCount = 0, avgLen = 8, maxLen = 8),
        nameToAttr("cdate_200") -> ColumnStat(distinctCount = 200, min = Some(dMin_200),
          max = Some(dMax_200), nullCount = 0, avgLen = 4, maxLen = 4),

        nameToAttr("cint_1_100") -> ColumnStat(distinctCount = 100, min = Some(1),
          max = Some(100), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cdouble_50_149") -> ColumnStat(distinctCount = 100, min = Some(50.0),
          max = Some(100.0), nullCount = 0, avgLen = 8, maxLen = 8),
        nameToAttr("cdate_100") -> ColumnStat(distinctCount = 100, min = Some(dMin_100),
          max = Some(dMax_100), nullCount = 0, avgLen = 4, maxLen = 4),

        nameToAttr("cint_201_400") -> ColumnStat(distinctCount = 200, min = Some(201),
          max = Some(400), nullCount = 0, avgLen = 4, maxLen = 4),
        nameToAttr("cfloat_200") -> ColumnStat(distinctCount = 200, min = Some(1.0),
          max = Some(200.0), nullCount = 0, avgLen = 4, maxLen = 4),

        /** decimal type change */
        nameToAttr("cdecimal_100") -> ColumnStat(distinctCount = 100, min = Some(Decimal(0.01)),
          max = Some(Decimal(1.0)), nullCount = 0, avgLen = 8, maxLen = 8),
        nameToAttr("cdouble_1_100") -> ColumnStat(distinctCount = 100, min = Some(50.0),
          max = Some(100.0), nullCount = 0, avgLen = 8, maxLen = 8))))

    /** col data compare */
    join2.stats.attributeStats.foreach { kv =>
      val filterColumnStat = joinExpectedStats.attributeStats.get(kv._1).get
      assert(filterColumnStat == kv._2, "join col data different")

    }
    assert(join2.stats == joinExpectedStats, "joined table don't match expected table")

    /** define filter condition */
    val condition = Not(And(LessThan(attrInt_1_200, Literal(3)),
      GreaterThanOrEqual(attrFloat_200, Literal(150.0, FloatType))))

    /** define expected stats */
    val expectedRowCount = 200L
    val expectedColStats = Seq(
      nameToAttr("cint_1_200") -> ColumnStat(distinctCount = 200,
        min = Some(1), max = Some(200), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cdouble_200") -> ColumnStat(distinctCount = 200,
        min = Some(1.0), max = Some(200.0), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdecimal_200") -> ColumnStat(distinctCount = 100,
        min = Some(Decimal(0.01)), max = Some(Decimal(1.0)), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdate_200") -> ColumnStat(distinctCount = 200,
        min = Some(dMin_200), max = Some(dMax_200), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cint_1_100") -> ColumnStat(distinctCount = 100,
        min = Some(1), max = Some(100), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cdouble_50_149") -> ColumnStat(distinctCount = 100,
        min = Some(50.0), max = Some(100.0), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdate_100") -> ColumnStat(distinctCount = 100,
        min = Some(dMin_100), max = Some(dMax_100),
        nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cint_201_400") -> ColumnStat(distinctCount = 200,
        min = Some(201), max = Some(400), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cfloat_200") -> ColumnStat(distinctCount = 200,
        min = Some(1.0), max = Some(200.0), nullCount = 0, avgLen = 4, maxLen = 4),

      nameToAttr("cdecimal_100") -> ColumnStat(distinctCount = 100,
        min = Some(Decimal(0.01)), max = Some(Decimal(1.0)), nullCount = 0, avgLen = 8, maxLen = 8),

      nameToAttr("cdouble_1_100") -> ColumnStat(distinctCount = 100,
        min = Some(50.0), max = Some(100.0),
        nullCount = 0, avgLen = 8, maxLen = 8))

    validateEstimatedStats(Filter(condition, join2), expectedColStats,
      expectedRowCount = expectedRowCount)
  }

  // 4 tables join: 8 unit

}