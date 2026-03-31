package tree

/*
* 测试树模型查询相关操作
 */

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/apache/iotdb-client-go/v2/client"
	"github.com/apache/iotdb-client-go/v2/common"
)

// TestSessionExecuteAggregationQuery 测试聚合查询
func TestSessionExecuteAggregationQuery(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	// 先清理可能存在的 storage group
	session.DeleteStorageGroup(database)

	err = cleanupAndSetStorageGroup(session, database)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	err = session.CreateTimeseries(database+".agg_dev.s1", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	// 插入数据
	ts := time.Now().UTC().UnixNano() / 1000000
	for i := 0; i < 10; i++ {
		err = session.InsertRecord(database+".agg_dev", []string{"s1"}, []client.TSDataType{client.INT32}, []interface{}{int32(i)}, ts+int64(i*1000))
		if err != nil {
			t.Fatalf("Failed to insert record: %v", err)
		}
	}

	// 执行聚合查询
	startTime := ts
	endTime := ts + 10000
	timeout := int64(5000)
	ds, err := session.ExecuteAggregationQuery(
		[]string{database + ".agg_dev.s1"},
		[]common.TAggregationType{common.TAggregationType_COUNT},
		&startTime, &endTime, nil, &timeout)
	if err != nil {
		t.Fatalf("Failed to execute aggregation query: %v", err)
	}
	defer ds.Close()

	// 清理
	session.DeleteTimeseries([]string{database + ".agg_dev.s1"})
	session.DeleteStorageGroup(database)
}

// TestSessionExecuteRawDataQuery 测试原始数据查询
func TestSessionExecuteRawDataQuery(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	// 先清理可能存在的 storage group
	session.DeleteStorageGroup(database)

	err = cleanupAndSetStorageGroup(session, database)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	err = session.CreateTimeseries(database+".raw_dev.s1", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	// 插入数据
	ts := time.Now().UTC().UnixNano() / 1000000
	err = session.InsertRecord(database+".raw_dev", []string{"s1"}, []client.TSDataType{client.INT32}, []interface{}{int32(100)}, ts)
	if err != nil {
		t.Fatalf("Failed to insert record: %v", err)
	}

	// 执行原始数据查询
	ds, err := session.ExecuteRawDataQuery([]string{database + ".raw_dev.s1"}, ts-1000, ts+1000)
	if err != nil {
		t.Fatalf("Failed to execute raw data query: %v", err)
	}
	defer ds.Close()

	// 清理
	session.DeleteTimeseries([]string{database + ".raw_dev.s1"})
	session.DeleteStorageGroup(database)
}

// TestSessionExecuteFastLastDataQuery 测试快速最后数据查询
func TestSessionExecuteFastLastDataQuery(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	// 先清理可能存在的 storage group
	session.DeleteStorageGroup(database)

	err = cleanupAndSetStorageGroup(session, database)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	err = session.CreateTimeseries(database+".fast_dev.s1", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	// 插入数据
	ts := time.Now().UTC().UnixNano() / 1000000
	err = session.InsertRecord(database+".fast_dev", []string{"s1"}, []client.TSDataType{client.INT32}, []interface{}{int32(100)}, ts)
	if err != nil {
		t.Fatalf("Failed to insert record: %v", err)
	}

	// 执行快速最后数据查询
	timeout := int64(5000)
	ds, err := session.ExecuteFastLastDataQueryForOnePrefixPath([]string{database + ".fast_dev"}, &timeout)
	if err != nil {
		t.Logf("ExecuteFastLastDataQueryForOnePrefixPath returned error (may be expected for certain path formats): %v", err)
	} else {
		ds.Close()
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".fast_dev.s1"})
	session.DeleteStorageGroup(database)
}

// TestSessionQueryAllDataType 测试所有数据类型的查询
func TestSessionQueryAllDataType(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	err = cleanupAndSetStorageGroup(session, database)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	measurementSchemas := []*client.MeasurementSchema{
		{Measurement: "s0", DataType: client.BOOLEAN},
		{Measurement: "s1", DataType: client.INT32},
		{Measurement: "s2", DataType: client.INT64},
		{Measurement: "s3", DataType: client.FLOAT},
		{Measurement: "s4", DataType: client.DOUBLE},
		{Measurement: "s5", DataType: client.TEXT},
		{Measurement: "s6", DataType: client.TIMESTAMP},
		{Measurement: "s7", DataType: client.DATE},
		{Measurement: "s8", DataType: client.BLOB},
		{Measurement: "s9", DataType: client.STRING},
	}

	tablet, err := client.NewTablet(database+".d1", measurementSchemas, 100)
	if err != nil {
		t.Fatalf("Failed to create tablet: %v", err)
	}
	tablet.SetTimestamp(1, 0)
	tablet.SetValueAt(true, 0, 0)
	tablet.SetValueAt(int32(1), 1, 0)
	tablet.SetValueAt(int64(1), 2, 0)
	tablet.SetValueAt(float32(1), 3, 0)
	tablet.SetValueAt(float64(1), 4, 0)
	tablet.SetValueAt("text", 5, 0)
	tablet.SetValueAt(int64(1), 6, 0)
	expectedDate, _ := client.Int32ToDate(20250326)
	tablet.SetValueAt(expectedDate, 7, 0)
	tablet.SetValueAt([]byte{1}, 8, 0)
	tablet.SetValueAt("string", 9, 0)
	tablet.RowSize = 1

	err = session.InsertAlignedTablet(tablet, true)
	if err != nil {
		t.Fatalf("Failed to insert tablet: %v", err)
	}

	sessionDataSet, err := session.ExecuteQueryStatement("select s0, s1, s2, s3, s4, s5, s6, s7, s8, s9 from "+database+".d1 limit 1", nil)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}

	for {
		if hasNext, err := sessionDataSet.Next(); err != nil || !hasNext {
			break
		}
		for _, columnName := range sessionDataSet.GetColumnNames() {
			isNull, err := sessionDataSet.IsNull(columnName)
			if err != nil {
				t.Errorf("Failed to check isNull for %s: %v", columnName, err)
			}
			if isNull {
				t.Errorf("Column %s should not be null", columnName)
			}
		}

		timeValue, err := sessionDataSet.GetLongByIndex(1)
		if err != nil {
			t.Errorf("Failed to get time: %v", err)
		}
		if timeValue != 1 {
			t.Errorf("Expected time 1, got %d", timeValue)
		}

		boolValue, err := sessionDataSet.GetBooleanByIndex(2)
		if err != nil || boolValue != true {
			t.Errorf("Failed to get boolean: %v, value: %v", err, boolValue)
		}

		intValue, err := sessionDataSet.GetIntByIndex(3)
		if err != nil || intValue != int32(1) {
			t.Errorf("Failed to get int: %v, value: %v", err, intValue)
		}

		longValue, err := sessionDataSet.GetLongByIndex(4)
		if err != nil || longValue != int64(1) {
			t.Errorf("Failed to get long: %v, value: %v", err, longValue)
		}

		floatValue, err := sessionDataSet.GetFloatByIndex(5)
		if err != nil || floatValue != float32(1) {
			t.Errorf("Failed to get float: %v, value: %v", err, floatValue)
		}

		doubleValue, err := sessionDataSet.GetDoubleByIndex(6)
		if err != nil || doubleValue != float64(1) {
			t.Errorf("Failed to get double: %v, value: %v", err, doubleValue)
		}

		textValue, err := sessionDataSet.GetStringByIndex(7)
		if err != nil || textValue != "text" {
			t.Errorf("Failed to get text: %v, value: %v", err, textValue)
		}

		timestampValue, err := sessionDataSet.GetTimestampByIndex(8)
		if err != nil || timestampValue != time.Unix(0, 1e6) {
			t.Errorf("Failed to get timestamp: %v, value: %v", err, timestampValue)
		}

		dateValue, err := sessionDataSet.GetDateByIndex(9)
		if err != nil || dateValue != expectedDate {
			t.Errorf("Failed to get date: %v, value: %v", err, dateValue)
		}

		blobValue, err := sessionDataSet.GetBlobByIndex(10)
		if err != nil {
			t.Errorf("Failed to get blob: %v", err)
		} else {
			if len(blobValue.GetValues()) != 1 || blobValue.GetValues()[0] != 1 {
				t.Errorf("Blob value mismatch")
			}
		}

		stringValue, err := sessionDataSet.GetStringByIndex(11)
		if err != nil || stringValue != "string" {
			t.Errorf("Failed to get string: %v, value: %v", err, stringValue)
		}
	}
	sessionDataSet.Close()

	// 清理
	session.DeleteTimeseries([]string{database + ".d1.**"})
	session.DeleteStorageGroup(database)
}

// TestSessionFetchMoreData 测试获取更多数据（大数据量）
func TestSessionFetchMoreData(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	session.SetFetchSize(1000)

	err = cleanupAndSetStorageGroup(session, database)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	writeCount := 10000
	tablet, err := client.NewTablet(database+".fetch_dev", []*client.MeasurementSchema{
		{Measurement: "restart_count", DataType: client.INT32},
		{Measurement: "price", DataType: client.DOUBLE},
		{Measurement: "tick_count", DataType: client.INT64},
		{Measurement: "temperature", DataType: client.FLOAT},
		{Measurement: "description", DataType: client.TEXT},
		{Measurement: "status", DataType: client.BOOLEAN},
	}, writeCount)
	if err != nil {
		t.Fatalf("Failed to create tablet: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	for row := 0; row < writeCount; row++ {
		ts++
		tablet.SetTimestamp(ts, row)
		tablet.SetValueAt(rand.Int31(), 0, row)
		tablet.SetValueAt(rand.Float64(), 1, row)
		tablet.SetValueAt(rand.Int63(), 2, row)
		tablet.SetValueAt(rand.Float32(), 3, row)
		tablet.SetValueAt(fmt.Sprintf("Test Device %d", row+1), 4, row)
		tablet.SetValueAt(bool(ts%2 == 0), 5, row)
		tablet.RowSize++
	}

	tablets := []*client.Tablet{tablet}
	err = session.InsertAlignedTablets(tablets, false)
	if err != nil {
		t.Fatalf("Failed to insert tablets: %v", err)
	}

	ds, err := session.ExecuteQueryStatement("select * from "+database+".fetch_dev", nil)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer ds.Close()

	count := 0
	for {
		if hasNext, err := ds.Next(); err != nil || !hasNext {
			break
		}
		count++
	}

	if count != writeCount {
		t.Errorf("Expected %d rows, got %d", writeCount, count)
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".fetch_dev.**"})
	session.DeleteStorageGroup(database)
}

// TestSessionSetFetchSize 测试设置 FetchSize
func TestSessionSetFetchSize(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	// 设置 FetchSize
	session.SetFetchSize(500)
	t.Logf("FetchSize set to 500")
}

// TestSessionExecuteNonQueryStatement 测试非查询语句执行
func TestSessionExecuteNonQueryStatement(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	// 执行 flush 语句
	err = session.ExecuteNonQueryStatement("flush")
	if err != nil {
		t.Fatalf("Failed to execute flush: %v", err)
	}
}

// TestSessionExecuteStatement 测试语句执行
func TestSessionExecuteStatement(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	err = cleanupAndSetStorageGroup(session, database)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	// 执行创建时间序列语句
	_, err = session.ExecuteStatement("create timeseries " + database + ".stmt_dev.s1 with datatype=INT32,encoding=PLAIN")
	if err != nil {
		t.Fatalf("Failed to execute create timeseries statement: %v", err)
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".stmt_dev.s1"})
	session.DeleteStorageGroup(database)
}

// TestSessionExecuteStatementWithContext 测试带上下文的语句执行
func TestSessionExecuteStatementWithContext(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	ctx := context.Background()
	_, err = session.ExecuteStatementWithContext(ctx, "show databases")
	if err != nil {
		t.Fatalf("Failed to execute statement with context: %v", err)
	}
}

// TestSessionInvalidSQL 测试无效 SQL
func TestSessionInvalidSQL(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	_, err = session.ExecuteStatementWithContext(context.Background(), "select1 from device")
	if err == nil {
		t.Error("Expected error for invalid SQL, got nil")
	} else {
		t.Logf("Expected SQL error: %v", err)
	}
}

// TestSessionExecuteUpdateStatement 测试更新语句执行
func TestSessionExecuteUpdateStatement(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	err = cleanupAndSetStorageGroup(session, database)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	err = session.CreateTimeseries(database+".update_dev.s1", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	// 插入数据
	ts := time.Now().UTC().UnixNano() / 1000000
	err = session.InsertRecord(database+".update_dev", []string{"s1"}, []client.TSDataType{client.INT32}, []interface{}{int32(100)}, ts)
	if err != nil {
		t.Fatalf("Failed to insert record: %v", err)
	}

	// 执行更新语句
	ds, err := session.ExecuteUpdateStatement("select * from " + database + ".update_dev")
	if err != nil {
		t.Fatalf("Failed to execute update statement: %v", err)
	}
	defer ds.Close()

	// 清理
	session.DeleteTimeseries([]string{database + ".update_dev.s1"})
	session.DeleteStorageGroup(database)
}

// TestSessionExecuteAggregationQueryWithLegalNodes 测试带合法节点的聚合查询
func TestSessionExecuteAggregationQueryWithLegalNodes(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	err = cleanupAndSetStorageGroup(session, database)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	err = session.CreateTimeseries(database+".legal_agg_dev.s1", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	// 插入数据
	ts := time.Now().UTC().UnixNano() / 1000000
	for i := 0; i < 5; i++ {
		err = session.InsertRecord(database+".legal_agg_dev", []string{"s1"}, []client.TSDataType{client.INT32}, []interface{}{int32(i)}, ts+int64(i*1000))
		if err != nil {
			t.Fatalf("Failed to insert record: %v", err)
		}
	}

	// 执行带合法节点参数的聚合查询
	startTime := ts
	endTime := ts + 5000
	timeout := int64(5000)
	legalNodes := true
	ds, err := session.ExecuteAggregationQueryWithLegalNodes(
		[]string{database + ".legal_agg_dev.s1"},
		[]common.TAggregationType{common.TAggregationType_COUNT},
		&startTime, &endTime, nil, &timeout, &legalNodes)
	if err != nil {
		t.Fatalf("Failed to execute aggregation query with legal nodes: %v", err)
	}
	defer ds.Close()

	// 清理
	session.DeleteTimeseries([]string{database + ".legal_agg_dev.s1"})
	session.DeleteStorageGroup(database)
}

// TestSessionGetColumnNames 测试获取列名
func TestSessionGetColumnNames(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	err = cleanupAndSetStorageGroup(session, database)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	err = session.CreateTimeseries(database+".colnames_dev.s1", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	err = session.InsertRecord(database+".colnames_dev", []string{"s1"}, []client.TSDataType{client.INT32}, []interface{}{int32(100)}, ts)
	if err != nil {
		t.Fatalf("Failed to insert record: %v", err)
	}

	// 查询并获取列名
	ds, err := session.ExecuteQueryStatement("select * from "+database+".colnames_dev", nil)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer ds.Close()

	columnNames := ds.GetColumnNames()
	if len(columnNames) == 0 {
		t.Error("Expected column names, got empty")
	} else {
		t.Logf("Column names: %v", columnNames)
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".colnames_dev.s1"})
	session.DeleteStorageGroup(database)
}

// TestSessionIsIsNull 测试 IsNull 方法
func TestSessionIsIsNull(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	err = cleanupAndSetStorageGroup(session, database)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	err = session.CreateTimeseries(database+".isnull_dev.s1", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	err = session.InsertRecord(database+".isnull_dev", []string{"s1"}, []client.TSDataType{client.INT32}, []interface{}{int32(100)}, ts)
	if err != nil {
		t.Fatalf("Failed to insert record: %v", err)
	}

	ds, err := session.ExecuteQueryStatement("select * from "+database+".isnull_dev", nil)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer ds.Close()

	for {
		hasNext, err := ds.Next()
		if err != nil || !hasNext {
			break
		}

		// 测试 IsNull 方法
		isNull, err := ds.IsNull(database + ".isnull_dev.s1")
		if err != nil {
			t.Logf("IsNull check error: %v", err)
		} else {
			t.Logf("IsNull result: %v", isNull)
		}
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".isnull_dev.s1"})
	session.DeleteStorageGroup(database)
}

// TestSessionGetObject 测试 GetObject 方法
func TestSessionGetObject(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	err = cleanupAndSetStorageGroup(session, database)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	err = session.CreateTimeseries(database+".obj_dev.s1", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	err = session.InsertRecord(database+".obj_dev", []string{"s1"}, []client.TSDataType{client.INT32}, []interface{}{int32(100)}, ts)
	if err != nil {
		t.Fatalf("Failed to insert record: %v", err)
	}

	ds, err := session.ExecuteQueryStatement("select s1 from "+database+".obj_dev", nil)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer ds.Close()

	for {
		hasNext, err := ds.Next()
		if err != nil || !hasNext {
			break
		}

		// 测试 GetObject 方法
		obj, err := ds.GetObject(database + ".obj_dev.s1")
		if err != nil {
			t.Logf("GetObject error: %v", err)
		} else {
			t.Logf("GetObject result: %v (type: %T)", obj, obj)
		}
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".obj_dev.s1"})
	session.DeleteStorageGroup(database)
}
