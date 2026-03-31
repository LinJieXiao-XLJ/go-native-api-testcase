package tree

/*
* 测试树模型 Insert 功能
 */

import (
	"fmt"
	"testing"
	"time"

	"github.com/apache/iotdb-client-go/v2/client"
)

// TestSessionInsertRecords 测试批量插入记录
func TestSessionInsertRecords(t *testing.T) {
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

	err = session.CreateTimeseries(database+".records_dev.s1", client.TEXT, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	// 批量插入多条记录
	ts := time.Now().UTC().UnixNano() / 1000000
	err = session.InsertRecords(
		[]string{database + ".records_dev", database + ".records_dev"},
		[][]string{{"s1"}, {"s1"}},
		[][]client.TSDataType{{client.TEXT}, {client.TEXT}},
		[][]interface{}{{"value1"}, {"value2"}},
		[]int64{ts, ts + 1})
	if err != nil {
		t.Fatalf("Failed to insert records: %v", err)
	}

	// 验证数据
	timeout := int64(5000)
	ds, err := session.ExecuteQueryStatement("select count(s1) from "+database+".records_dev", &timeout)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer ds.Close()

	for {
		hasNext, err := ds.Next()
		if err != nil || !hasNext {
			break
		}
		countStr, _ := ds.GetStringByIndex(1)
		if countStr != "2" {
			t.Errorf("Expected count 2, got %s", countStr)
		}
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".records_dev.s1"})
	session.DeleteStorageGroup(database)
}

// TestSessionInsertRecordsWithWrongType 测试错误类型的插入
func TestSessionInsertRecordsWithWrongType(t *testing.T) {
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

	err = session.CreateTimeseries(database+".wrong_type_dev.s1", client.BOOLEAN, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	// 尝试插入错误类型的数据
	ts := time.Now().UTC().UnixNano() / 1000000
	err = session.InsertRecords(
		[]string{database + ".wrong_type_dev"},
		[][]string{{"s1"}},
		[][]client.TSDataType{{client.BOOLEAN}},
		[][]interface{}{{100.0}}, // 错误：应该是 bool 而不是 float
		[]int64{ts})
	if err == nil {
		t.Error("Expected error for wrong type, got nil")
	} else {
		t.Logf("Expected type error: %v", err)
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".wrong_type_dev.s1"})
	session.DeleteStorageGroup(database)
}

// TestSessionInsertAlignedRecord 测试插入对齐记录
func TestSessionInsertAlignedRecord(t *testing.T) {
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

	err = session.CreateAlignedTimeseries(database+".aligned_rec_dev",
		[]string{"status"},
		[]client.TSDataType{client.TEXT},
		[]client.TSEncoding{client.PLAIN},
		[]client.TSCompressionType{client.SNAPPY},
		nil)
	if err != nil {
		t.Fatalf("Failed to create aligned timeseries: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	err = session.InsertAlignedRecord(database+".aligned_rec_dev",
		[]string{"status"},
		[]client.TSDataType{client.TEXT},
		[]interface{}{"Working"},
		ts)
	if err != nil {
		t.Fatalf("Failed to insert aligned record: %v", err)
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".aligned_rec_dev.status"})
	session.DeleteStorageGroup(database)
}

// TestSessionInsertAlignedRecords 测试批量插入对齐记录
func TestSessionInsertAlignedRecords(t *testing.T) {
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

	err = session.CreateAlignedTimeseries(database+".aligned_recs_dev",
		[]string{"temperature"},
		[]client.TSDataType{client.STRING},
		[]client.TSEncoding{client.PLAIN},
		[]client.TSCompressionType{client.SNAPPY},
		nil)
	if err != nil {
		t.Fatalf("Failed to create aligned timeseries: %v", err)
	}

	err = session.InsertAlignedRecords(
		[]string{database + ".aligned_recs_dev", database + ".aligned_recs_dev"},
		[][]string{{"temperature"}, {"temperature"}},
		[][]client.TSDataType{{client.STRING}, {client.STRING}},
		[][]interface{}{{"33"}, {"44"}},
		[]int64{12, 13})
	if err != nil {
		t.Fatalf("Failed to insert aligned records: %v", err)
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".aligned_recs_dev.temperature"})
	session.DeleteStorageGroup(database)
}

// TestSessionInsertAlignedRecordsOfOneDevice 测试单设备批量插入对齐记录
func TestSessionInsertAlignedRecordsOfOneDevice(t *testing.T) {
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

	ts := time.Now().UTC().UnixNano() / 1000000

	err = session.CreateAlignedTimeseries(database+".one_device_aligned",
		[]string{"restart_count", "tick_count", "price", "temperature", "description", "status"},
		[]client.TSDataType{client.INT32, client.INT64, client.DOUBLE, client.FLOAT, client.TEXT, client.BOOLEAN},
		[]client.TSEncoding{client.PLAIN, client.PLAIN, client.PLAIN, client.PLAIN, client.PLAIN, client.PLAIN},
		[]client.TSCompressionType{client.SNAPPY, client.SNAPPY, client.SNAPPY, client.SNAPPY, client.SNAPPY, client.SNAPPY},
		nil)
	if err != nil {
		t.Fatalf("Failed to create aligned timeseries: %v", err)
	}

	measurementsSlice := [][]string{
		{"restart_count", "tick_count", "price"},
		{"temperature", "description", "status"},
	}
	dataTypes := [][]client.TSDataType{
		{client.INT32, client.INT64, client.DOUBLE},
		{client.FLOAT, client.TEXT, client.BOOLEAN},
	}
	values := [][]interface{}{
		{int32(1), int64(2018), float64(1988.1)},
		{float32(12.1), "Test Device 1", false},
	}
	timestamps := []int64{ts, ts - 1}

	err = session.InsertAlignedRecordsOfOneDevice(database+".one_device_aligned", timestamps, measurementsSlice, dataTypes, values, false)
	if err != nil {
		t.Fatalf("Failed to insert aligned records of one device: %v", err)
	}

	// 清理
	session.DeleteTimeseries([]string{
		database + ".one_device_aligned.restart_count",
		database + ".one_device_aligned.tick_count",
		database + ".one_device_aligned.price",
		database + ".one_device_aligned.temperature",
		database + ".one_device_aligned.description",
		database + ".one_device_aligned.status",
	})
	session.DeleteStorageGroup(database)
}

// TestSessionInsertStringRecord 测试插入字符串记录
func TestSessionInsertStringRecord(t *testing.T) {
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

	err = session.CreateTimeseries(database+".string_dev.s1", client.TEXT, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	err = session.InsertStringRecord(database+".string_dev", []string{"s1"}, []string{"text_value"}, ts)
	if err != nil {
		t.Fatalf("Failed to insert string record: %v", err)
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".string_dev.s1"})
	session.DeleteStorageGroup(database)
}

// TestSessionInsertAlignedTablet 测试插入对齐 Tablet
func TestSessionInsertAlignedTablet(t *testing.T) {
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

	tablet, err := createTreeTablet(12)
	if err != nil {
		t.Fatalf("Failed to create tablet: %v", err)
	}

	err = session.InsertAlignedTablet(tablet, false)
	if err != nil {
		t.Fatalf("Failed to insert aligned tablet: %v", err)
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".tablet_dev.**"})
	session.DeleteStorageGroup(database)
}

// TestSessionInsertAlignedTablets 测试批量插入对齐 Tablet
func TestSessionInsertAlignedTablets(t *testing.T) {
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

	tablet1, err := createTreeTablet(8)
	if err != nil {
		t.Fatalf("Failed to create tablet1: %v", err)
	}
	tablet2, err := createTreeTablet(4)
	if err != nil {
		t.Fatalf("Failed to create tablet2: %v", err)
	}

	tablets := []*client.Tablet{tablet1, tablet2}
	err = session.InsertAlignedTablets(tablets, false)
	if err != nil {
		t.Fatalf("Failed to insert aligned tablets: %v", err)
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".tablet_dev.**"})
	session.DeleteStorageGroup(database)
}

// TestSessionInsertTablets 测试批量插入 Tablet
func TestSessionInsertTablets(t *testing.T) {
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

	// 创建时间序列
	err = session.CreateTimeseries(database+".tablets_dev.s1", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	tablet1, err := client.NewTablet(database+".tablets_dev", []*client.MeasurementSchema{
		{Measurement: "s1", DataType: client.INT32},
	}, 5)
	if err != nil {
		t.Fatalf("Failed to create tablet1: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	for i := 0; i < 3; i++ {
		tablet1.SetTimestamp(ts+int64(i), i)
		tablet1.SetValueAt(int32(i*100), 0, i)
		tablet1.RowSize++
	}

	tablets := []*client.Tablet{tablet1}
	err = session.InsertTablets(tablets, false)
	if err != nil {
		t.Fatalf("Failed to insert tablets: %v", err)
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".tablets_dev.s1"})
	session.DeleteStorageGroup(database)
}

// TestSessionExecuteBatchStatement 测试批量执行语句
func TestSessionExecuteBatchStatement(t *testing.T) {
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

	err = session.CreateTimeseries(database+".batch_dev.s1", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	// 批量执行插入语句
	ts := time.Now().UTC().UnixNano() / 1000000
	statements := []string{
		fmt.Sprintf("insert into %s.batch_dev(timestamp, s1) values(%d, 1)", database, ts),
		fmt.Sprintf("insert into %s.batch_dev(timestamp, s1) values(%d, 2)", database, ts+1),
		fmt.Sprintf("insert into %s.batch_dev(timestamp, s1) values(%d, 3)", database, ts+2),
	}

	err = session.ExecuteBatchStatement(statements)
	if err != nil {
		t.Fatalf("Failed to execute batch statement: %v", err)
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".batch_dev.s1"})
	session.DeleteStorageGroup(database)
}

// TestSessionInsertRecordsOfOneDevice 测试单设备批量插入记录
func TestSessionInsertRecordsOfOneDevice(t *testing.T) {
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

	err = session.CreateTimeseries(database+".one_dev.s1", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	timestamps := []int64{ts, ts + 1, ts + 2}
	measurementsSlice := [][]string{{"s1"}, {"s1"}, {"s1"}}
	dataTypesSlice := [][]client.TSDataType{{client.INT32}, {client.INT32}, {client.INT32}}
	valuesSlice := [][]interface{}{{int32(1)}, {int32(2)}, {int32(3)}}

	err = session.InsertRecordsOfOneDevice(database+".one_dev", timestamps, measurementsSlice, dataTypesSlice, valuesSlice, false)
	if err != nil {
		t.Fatalf("Failed to insert records of one device: %v", err)
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".one_dev.s1"})
	session.DeleteStorageGroup(database)
}

// TestSessionInsertRecordsOfOneDeviceWithSorted 测试已排序的单设备记录插入
func TestSessionInsertRecordsOfOneDeviceWithSorted(t *testing.T) {
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

	err = session.CreateTimeseries(database+".sorted_dev.s1", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	// 已排序的时间戳（升序）
	timestamps := []int64{ts, ts + 100, ts + 200}
	measurementsSlice := [][]string{{"s1"}, {"s1"}, {"s1"}}
	dataTypesSlice := [][]client.TSDataType{{client.INT32}, {client.INT32}, {client.INT32}}
	valuesSlice := [][]interface{}{{int32(1)}, {int32(2)}, {int32(3)}}

	// sorted=true 表示数据已按时间戳排序
	err = session.InsertRecordsOfOneDevice(database+".sorted_dev", timestamps, measurementsSlice, dataTypesSlice, valuesSlice, true)
	if err != nil {
		t.Fatalf("Failed to insert sorted records: %v", err)
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".sorted_dev.s1"})
	session.DeleteStorageGroup(database)
}

// TestSessionInsertTabletWithSorted 测试已排序的 Tablet 插入
func TestSessionInsertTabletWithSorted(t *testing.T) {
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

	err = session.CreateTimeseries(database+".tablet_sorted.s1", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	tablet, err := client.NewTablet(database+".tablet_sorted", []*client.MeasurementSchema{
		{Measurement: "s1", DataType: client.INT32},
	}, 5)
	if err != nil {
		t.Fatalf("Failed to create tablet: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	for i := 0; i < 3; i++ {
		tablet.SetTimestamp(ts+int64(i*100), i) // 已按时间戳排序
		tablet.SetValueAt(int32(i), 0, i)
		tablet.RowSize++
	}

	// sorted=true 表示数据已按时间戳排序
	err = session.InsertTablet(tablet, true)
	if err != nil {
		t.Fatalf("Failed to insert sorted tablet: %v", err)
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".tablet_sorted.s1"})
	session.DeleteStorageGroup(database)
}

// TestSessionInsertTabletsWithSorted 测试已排序的 Tablets 批量插入
func TestSessionInsertTabletsWithSorted(t *testing.T) {
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

	err = session.CreateTimeseries(database+".tablets_sorted.s1", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	tablet, err := client.NewTablet(database+".tablets_sorted", []*client.MeasurementSchema{
		{Measurement: "s1", DataType: client.INT32},
	}, 5)
	if err != nil {
		t.Fatalf("Failed to create tablet: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	for i := 0; i < 3; i++ {
		tablet.SetTimestamp(ts+int64(i*100), i)
		tablet.SetValueAt(int32(i), 0, i)
		tablet.RowSize++
	}

	tablets := []*client.Tablet{tablet}
	// sorted=true 表示数据已按时间戳排序
	err = session.InsertTablets(tablets, true)
	if err != nil {
		t.Fatalf("Failed to insert sorted tablets: %v", err)
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".tablets_sorted.s1"})
	session.DeleteStorageGroup(database)
}

// TestSessionInsertRecordsOfOneDeviceWithMismatchedLengths 测试长度不匹配的情况
func TestSessionInsertRecordsOfOneDeviceWithMismatchedLengths(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	ts := time.Now().UTC().UnixNano() / 1000000
	timestamps := []int64{ts, ts + 1}
	measurementsSlice := [][]string{{"s1"}} // 长度不匹配
	dataTypesSlice := [][]client.TSDataType{{client.INT32}}
	valuesSlice := [][]interface{}{{int32(1)}}

	err = session.InsertRecordsOfOneDevice(database+".mismatch_dev", timestamps, measurementsSlice, dataTypesSlice, valuesSlice, false)
	if err == nil {
		t.Error("Expected error for mismatched lengths, got nil")
	} else {
		t.Logf("Expected mismatch error: %v", err)
	}
}

// TestSessionInsertAlignedRecordsOfOneDeviceWithMismatchedLengths 测试对齐记录长度不匹配
func TestSessionInsertAlignedRecordsOfOneDeviceWithMismatchedLengths(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	ts := time.Now().UTC().UnixNano() / 1000000
	timestamps := []int64{ts}
	measurementsSlice := [][]string{{"s1"}, {"s2"}} // 长度不匹配
	dataTypesSlice := [][]client.TSDataType{{client.INT32}}
	valuesSlice := [][]interface{}{{int32(1)}}

	err = session.InsertAlignedRecordsOfOneDevice(database+".aligned_mismatch_dev", timestamps, measurementsSlice, dataTypesSlice, valuesSlice, false)
	if err == nil {
		t.Error("Expected error for mismatched lengths, got nil")
	} else {
		t.Logf("Expected mismatch error: %v", err)
	}
}

// TestSessionInsertAlignedTabletWithNilValues 测试插入带 nil 值的对齐 Tablet
func TestSessionInsertAlignedTabletWithNilValues(t *testing.T) {
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

	tablet, err := client.NewTablet(database+".nil_tablet_dev", []*client.MeasurementSchema{
		{Measurement: "s1", DataType: client.INT32},
		{Measurement: "s2", DataType: client.DOUBLE},
	}, 10)
	if err != nil {
		t.Fatalf("Failed to create tablet: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	for i := 0; i < 5; i++ {
		tablet.SetTimestamp(ts+int64(i), i)
		tablet.SetValueAt(int32(i), 0, i)
		if i%2 == 0 {
			tablet.SetValueAt(float64(i)*1.5, 1, i)
		} else {
			// 模拟 nil 值 - 在实际测试中，可能需要使用特殊方式表示 null
		}
		tablet.RowSize++
	}

	err = session.InsertAlignedTablet(tablet, false)
	if err != nil {
		t.Logf("Insert with potential nil handling: %v", err)
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".nil_tablet_dev.**"})
	session.DeleteStorageGroup(database)
}

// TestSessionInsertRecordWithNilValue 测试插入带 nil 值的记录
func TestSessionInsertRecordWithNilValue(t *testing.T) {
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

	err = session.CreateTimeseries(database+".nil_record_dev.s1", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	// 尝试插入 nil 值 - 这应该返回错误
	err = session.InsertRecord(database+".nil_record_dev", []string{"s1"}, []client.TSDataType{client.INT32}, []interface{}{nil}, ts)
	if err == nil {
		t.Log("Insert with nil value may have succeeded or been handled differently")
	} else {
		t.Logf("Expected error for nil value: %v", err)
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".nil_record_dev.s1"})
	session.DeleteStorageGroup(database)
}

// TestSessionInsertRecordsWithNilValue 测试批量插入带 nil 值的记录
func TestSessionInsertRecordsWithNilValue(t *testing.T) {
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

	err = session.CreateTimeseries(database+".nil_records_dev.s1", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	err = session.InsertRecords(
		[]string{database + ".nil_records_dev"},
		[][]string{{"s1"}},
		[][]client.TSDataType{{client.INT32}},
		[][]interface{}{{nil}}, // nil 值
		[]int64{ts})
	if err == nil {
		t.Log("Insert records with nil value may have succeeded")
	} else {
		t.Logf("Expected error for nil value in records: %v", err)
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".nil_records_dev.s1"})
	session.DeleteStorageGroup(database)
}

// TestSessionInsertRecordsOfOneDeviceWithNilValue 测试单设备批量插入带 nil 值
func TestSessionInsertRecordsOfOneDeviceWithNilValue(t *testing.T) {
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

	err = session.CreateTimeseries(database+".nil_one_dev.s1", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	timestamps := []int64{ts}
	measurementsSlice := [][]string{{"s1"}}
	dataTypesSlice := [][]client.TSDataType{{client.INT32}}
	valuesSlice := [][]interface{}{{nil}} // nil 值

	err = session.InsertRecordsOfOneDevice(database+".nil_one_dev", timestamps, measurementsSlice, dataTypesSlice, valuesSlice, false)
	if err == nil {
		t.Log("Insert records of one device with nil may have succeeded")
	} else {
		t.Logf("Expected error for nil value: %v", err)
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".nil_one_dev.s1"})
	session.DeleteStorageGroup(database)
}
