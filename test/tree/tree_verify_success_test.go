package tree

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/apache/iotdb-client-go/v2/client"
)

// TestSessionSetStorageGroupReturnsExecutionError 测试重复设置存储组返回执行错误
func TestSessionSetStorageGroupReturnsExecutionError(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	storageGroup := fmt.Sprintf("%s.verify_set_sg_%d", database, time.Now().UnixNano())
	_ = session.DeleteStorageGroup(storageGroup)
	t.Cleanup(func() {
		_ = session.DeleteStorageGroup(storageGroup)
	})

	err = session.SetStorageGroup(storageGroup)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	err = session.SetStorageGroup(storageGroup)
	if err == nil {
		t.Fatal("Expected execution error when setting the same storage group twice, got nil")
	}

	var execErr *client.ExecutionError
	if !errors.As(err, &execErr) {
		t.Fatalf("Expected *client.ExecutionError, got %T: %v", err, err)
	}

	err = session.ExecuteNonQueryStatement("flush")
	if err != nil {
		t.Fatalf("Session should remain usable after execution error: %v", err)
	}
}

// TestSessionDeleteStorageGroupReturnsStatusError 测试重复删除存储组返回状态错误
func TestSessionDeleteStorageGroupReturnsStatusError(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	storageGroup := fmt.Sprintf("%s.verify_delete_sg_%d", database, time.Now().UnixNano())
	_ = session.DeleteStorageGroup(storageGroup)

	err = session.SetStorageGroup(storageGroup)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	err = session.DeleteStorageGroup(storageGroup)
	if err != nil {
		t.Fatalf("Failed to delete storage group: %v", err)
	}

	err = session.DeleteStorageGroup(storageGroup)
	if err == nil {
		t.Fatal("Expected status error when deleting a nonexistent storage group, got nil")
	}

	var execErr *client.ExecutionError
	var batchErr *client.BatchError
	if !errors.As(err, &execErr) && !errors.As(err, &batchErr) {
		t.Fatalf("Expected *client.ExecutionError or *client.BatchError, got %T: %v", err, err)
	}

	err = session.ExecuteNonQueryStatement("flush")
	if err != nil {
		t.Fatalf("Session should remain usable after delete storage group error: %v", err)
	}
}

// TestSessionDeleteStorageGroupsReturnsStatusError 测试重复批量删除存储组返回状态错误
func TestSessionDeleteStorageGroupsReturnsStatusError(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	storageGroup1 := fmt.Sprintf("%s.verify_delete_sgs1_%d", database, time.Now().UnixNano())
	storageGroup2 := fmt.Sprintf("%s.verify_delete_sgs2_%d", database, time.Now().UnixNano())
	_ = session.DeleteStorageGroups(storageGroup1, storageGroup2)

	err = session.SetStorageGroup(storageGroup1)
	if err != nil {
		t.Fatalf("Failed to set storage group 1: %v", err)
	}

	err = session.SetStorageGroup(storageGroup2)
	if err != nil {
		t.Fatalf("Failed to set storage group 2: %v", err)
	}

	err = session.DeleteStorageGroups(storageGroup1, storageGroup2)
	if err != nil {
		t.Fatalf("Failed to delete storage groups: %v", err)
	}

	err = session.DeleteStorageGroups(storageGroup1, storageGroup2)
	if err == nil {
		t.Fatal("Expected status error when deleting nonexistent storage groups, got nil")
	}

	var execErr *client.ExecutionError
	var batchErr *client.BatchError
	if !errors.As(err, &execErr) && !errors.As(err, &batchErr) {
		t.Fatalf("Expected *client.ExecutionError or *client.BatchError, got %T: %v", err, err)
	}

	err = session.ExecuteNonQueryStatement("flush")
	if err != nil {
		t.Fatalf("Session should remain usable after delete storage groups error: %v", err)
	}
}

// TestSessionCreateTimeseriesReturnsExecutionError 测试重复创建 timeseries 返回执行错误
func TestSessionCreateTimeseriesReturnsExecutionError(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	storageGroup := fmt.Sprintf("%s.verify_create_ts_%d", database, time.Now().UnixNano())
	_ = session.DeleteStorageGroup(storageGroup)
	t.Cleanup(func() {
		_ = session.DeleteStorageGroup(storageGroup)
	})

	err = session.SetStorageGroup(storageGroup)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	path := storageGroup + ".create_ts_dev.s1"
	err = session.CreateTimeseries(path, client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create initial timeseries: %v", err)
	}

	err = session.CreateTimeseries(path, client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err == nil {
		t.Fatal("Expected execution error when creating the same timeseries twice, got nil")
	}

	var execErr *client.ExecutionError
	if !errors.As(err, &execErr) {
		t.Fatalf("Expected *client.ExecutionError, got %T: %v", err, err)
	}

	err = session.ExecuteNonQueryStatement("flush")
	if err != nil {
		t.Fatalf("Session should remain usable after create timeseries error: %v", err)
	}
}

// TestSessionCreateAlignedTimeseriesReturnsStatusError 测试重复创建对齐 timeseries 返回状态错误
func TestSessionCreateAlignedTimeseriesReturnsStatusError(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	storageGroup := fmt.Sprintf("%s.verify_create_aligned_ts_%d", database, time.Now().UnixNano())
	_ = session.DeleteStorageGroup(storageGroup)
	t.Cleanup(func() {
		_ = session.DeleteStorageGroup(storageGroup)
	})

	err = session.SetStorageGroup(storageGroup)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	device := storageGroup + ".aligned_dev"
	err = session.CreateAlignedTimeseries(device,
		[]string{"s1"},
		[]client.TSDataType{client.INT32},
		[]client.TSEncoding{client.PLAIN},
		[]client.TSCompressionType{client.SNAPPY},
		nil)
	if err != nil {
		t.Fatalf("Failed to create initial aligned timeseries: %v", err)
	}

	err = session.CreateAlignedTimeseries(device,
		[]string{"s1"},
		[]client.TSDataType{client.INT32},
		[]client.TSEncoding{client.PLAIN},
		[]client.TSCompressionType{client.SNAPPY},
		nil)
	if err == nil {
		t.Fatal("Expected status error when creating the same aligned timeseries twice, got nil")
	}

	var execErr *client.ExecutionError
	var batchErr *client.BatchError
	if !errors.As(err, &execErr) && !errors.As(err, &batchErr) {
		t.Fatalf("Expected *client.ExecutionError or *client.BatchError, got %T: %v", err, err)
	}

	err = session.ExecuteNonQueryStatement("flush")
	if err != nil {
		t.Fatalf("Session should remain usable after create aligned timeseries error: %v", err)
	}
}

// TestSessionCreateMultiTimeseriesReturnsBatchError 测试批量创建 timeseries 的部分失败返回批量错误
func TestSessionCreateMultiTimeseriesReturnsBatchError(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	storageGroup := fmt.Sprintf("%s.verify_create_multi_ts_%d", database, time.Now().UnixNano())
	_ = session.DeleteStorageGroup(storageGroup)
	t.Cleanup(func() {
		_ = session.DeleteStorageGroup(storageGroup)
	})

	err = session.SetStorageGroup(storageGroup)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	existingPath := storageGroup + ".multi_dev.s1"
	err = session.CreateTimeseries(existingPath, client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create initial timeseries: %v", err)
	}

	err = session.CreateMultiTimeseries(
		[]string{existingPath, storageGroup + ".multi_dev.s2"},
		[]client.TSDataType{client.INT32, client.INT32},
		[]client.TSEncoding{client.PLAIN, client.PLAIN},
		[]client.TSCompressionType{client.SNAPPY, client.SNAPPY})
	if err == nil {
		t.Fatal("Expected batch error, got nil")
	}

	var batchErr *client.BatchError
	if !errors.As(err, &batchErr) {
		t.Fatalf("Expected *client.BatchError, got %T: %v", err, err)
	}
	if len(batchErr.GetStatuses()) == 0 {
		t.Fatal("Expected batch error to contain sub-statuses")
	}
	if batchErr.Error() == "" {
		t.Fatal("Expected batch error message to be populated")
	}

	err = session.ExecuteNonQueryStatement("flush")
	if err != nil {
		t.Fatalf("Session should remain usable after create multi timeseries error: %v", err)
	}
}

// TestSessionDeleteTimeseriesReturnsStatusError 测试重复删除 timeseries 返回状态错误
func TestSessionDeleteTimeseriesReturnsStatusError(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	storageGroup := fmt.Sprintf("%s.verify_delete_ts_%d", database, time.Now().UnixNano())
	_ = session.DeleteStorageGroup(storageGroup)
	t.Cleanup(func() {
		_ = session.DeleteStorageGroup(storageGroup)
	})

	err = session.SetStorageGroup(storageGroup)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	paths := []string{storageGroup + ".delete_ts_dev.s1", storageGroup + ".delete_ts_dev.s2"}
	err = session.CreateTimeseries(paths[0], client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries s1: %v", err)
	}

	err = session.CreateTimeseries(paths[1], client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries s2: %v", err)
	}

	err = session.DeleteTimeseries(paths)
	if err != nil {
		t.Fatalf("Failed to delete timeseries: %v", err)
	}

	err = session.DeleteTimeseries(paths)
	if err == nil {
		t.Fatal("Expected status error when deleting nonexistent timeseries, got nil")
	}

	var execErr *client.ExecutionError
	var batchErr *client.BatchError
	if !errors.As(err, &execErr) && !errors.As(err, &batchErr) {
		t.Fatalf("Expected *client.ExecutionError or *client.BatchError, got %T: %v", err, err)
	}

	err = session.ExecuteNonQueryStatement("flush")
	if err != nil {
		t.Fatalf("Session should remain usable after delete timeseries error: %v", err)
	}
}

// TestSessionDeleteDataOnNonexistentPathSucceeds 测试删除不存在路径的数据返回成功
func TestSessionDeleteDataOnNonexistentPathSucceeds(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	err = session.DeleteData([]string{"illegal_path"}, 1, 2)
	if err != nil {
		t.Fatalf("DeleteData on nonexistent path should succeed, got: %v", err)
	}

	err = session.ExecuteNonQueryStatement("flush")
	if err != nil {
		t.Fatalf("Session should remain usable after delete data no-op: %v", err)
	}
}

// TestSessionInsertStringRecordReturnsExecutionError 测试字符串写入错误类型返回执行错误
func TestSessionInsertStringRecordReturnsExecutionError(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	storageGroup := fmt.Sprintf("%s.verify_insert_string_%d", database, time.Now().UnixNano())
	_ = session.DeleteStorageGroup(storageGroup)
	t.Cleanup(func() {
		_ = session.DeleteStorageGroup(storageGroup)
	})

	err = session.SetStorageGroup(storageGroup)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	device := storageGroup + ".string_dev"
	err = session.CreateTimeseries(device+".s1", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	err = session.InsertStringRecord(device, []string{"s1"}, []string{"not-an-int"}, ts)
	if err == nil {
		t.Fatal("Expected execution error when inserting wrong string value, got nil")
	}

	var execErr *client.ExecutionError
	if !errors.As(err, &execErr) {
		t.Fatalf("Expected *client.ExecutionError, got %T: %v", err, err)
	}

	err = session.ExecuteNonQueryStatement("flush")
	if err != nil {
		t.Fatalf("Session should remain usable after insert string record error: %v", err)
	}
}

// TestSessionSetTimeZoneReturnsExecutionError 测试设置非法时区返回执行错误
func TestSessionSetTimeZoneReturnsExecutionError(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	err = session.SetTimeZone("Invalid/Timezone")
	if err == nil {
		t.Fatal("Expected execution error for invalid timezone, got nil")
	}

	var execErr *client.ExecutionError
	if !errors.As(err, &execErr) {
		t.Fatalf("Expected *client.ExecutionError, got %T: %v", err, err)
	}

	err = session.ExecuteNonQueryStatement("flush")
	if err != nil {
		t.Fatalf("Session should remain usable after set timezone error: %v", err)
	}
}

// TestSessionExecuteNonQueryReturnsExecutionError 测试执行非法 SQL 返回执行错误
func TestSessionExecuteNonQueryReturnsExecutionError(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	err = session.ExecuteNonQueryStatement("invalid sql statement with syntax error")
	if err == nil {
		t.Fatal("Expected execution error for invalid SQL, got nil")
	}

	var execErr *client.ExecutionError
	if !errors.As(err, &execErr) {
		t.Fatalf("Expected *client.ExecutionError, got %T: %v", err, err)
	}

	err = session.ExecuteNonQueryStatement("flush")
	if err != nil {
		t.Fatalf("Session should remain usable after execution error: %v", err)
	}
}

// TestSessionInsertRecordsOfOneDeviceReturnsBatchError 测试单设备批量写入部分失败返回批量错误
func TestSessionInsertRecordsOfOneDeviceReturnsBatchError(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	storageGroup := fmt.Sprintf("%s.virod_%d", database, time.Now().UnixNano())
	_ = session.DeleteStorageGroup(storageGroup)
	t.Cleanup(func() {
		_ = session.DeleteStorageGroup(storageGroup)
	})

	err = session.SetStorageGroup(storageGroup)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	device := storageGroup + ".one_dev"
	err = session.CreateTimeseries(device+".s1", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	err = session.InsertRecordsOfOneDevice(
		device,
		[]int64{ts, ts + 1},
		[][]string{{"s1"}, {"s1"}},
		[][]client.TSDataType{{client.INT32}, {client.BOOLEAN}},
		[][]interface{}{{int32(1)}, {true}},
		false)
	if err == nil {
		t.Fatal("Expected execution error, got nil")
	}

	var execErr *client.ExecutionError
	if !errors.As(err, &execErr) {
		t.Fatalf("Expected *client.ExecutionError, got %T: %v", err, err)
	}
	if execErr.Code != 507 {
		t.Fatalf("Expected error code 507, got %d: %v", execErr.Code, err)
	}
	if execErr.Message == "" {
		t.Fatal("Expected execution error message to be populated")
	}

	err = session.ExecuteNonQueryStatement("flush")
	if err != nil {
		t.Fatalf("Session should remain usable after InsertRecordsOfOneDevice error: %v", err)
	}
}

// TestSessionInsertAlignedRecordsOfOneDeviceReturnsExecutionError 测试对齐单设备批量写入返回执行错误
func TestSessionInsertAlignedRecordsOfOneDeviceReturnsExecutionError(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	storageGroup := fmt.Sprintf("%s.viaod_%d", database, time.Now().UnixNano())
	_ = session.DeleteStorageGroup(storageGroup)
	t.Cleanup(func() {
		_ = session.DeleteStorageGroup(storageGroup)
	})

	err = session.SetStorageGroup(storageGroup)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	device := storageGroup + ".aligned_one_dev"
	err = session.CreateAlignedTimeseries(device,
		[]string{"s1"},
		[]client.TSDataType{client.INT32},
		[]client.TSEncoding{client.PLAIN},
		[]client.TSCompressionType{client.SNAPPY},
		nil)
	if err != nil {
		t.Fatalf("Failed to create aligned timeseries: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	err = session.InsertAlignedRecordsOfOneDevice(
		device,
		[]int64{ts, ts + 1},
		[][]string{{"s1"}, {"s1"}},
		[][]client.TSDataType{{client.INT32}, {client.BOOLEAN}},
		[][]interface{}{{int32(1)}, {true}},
		false)
	if err == nil {
		t.Fatal("Expected execution error, got nil")
	}

	var execErr *client.ExecutionError
	if !errors.As(err, &execErr) {
		t.Fatalf("Expected *client.ExecutionError, got %T: %v", err, err)
	}

	err = session.ExecuteNonQueryStatement("flush")
	if err != nil {
		t.Fatalf("Session should remain usable after InsertAlignedRecordsOfOneDevice error: %v", err)
	}
}

// TestSessionInsertRecordsReturnsBatchError 测试批量写入记录部分失败返回批量错误
func TestSessionInsertRecordsReturnsBatchError(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	storageGroup := fmt.Sprintf("%s.verify_insert_records_%d", database, time.Now().UnixNano())
	_ = session.DeleteStorageGroup(storageGroup)
	t.Cleanup(func() {
		_ = session.DeleteStorageGroup(storageGroup)
	})

	err = session.SetStorageGroup(storageGroup)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	device := storageGroup + ".records_dev"
	err = session.CreateTimeseries(device+".s1", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	err = session.InsertRecords(
		[]string{device, device},
		[][]string{{"s1"}, {"s1"}},
		[][]client.TSDataType{{client.INT32}, {client.BOOLEAN}},
		[][]interface{}{{int32(1)}, {true}},
		[]int64{ts, ts + 1})
	if err == nil {
		t.Fatal("Expected execution error, got nil")
	}

	var execErr *client.ExecutionError
	if !errors.As(err, &execErr) {
		t.Fatalf("Expected *client.ExecutionError, got %T: %v", err, err)
	}
	if execErr.Code != 507 {
		t.Fatalf("Expected error code 507, got %d: %v", execErr.Code, err)
	}
	if execErr.Message == "" {
		t.Fatal("Expected execution error message to be populated")
	}

	err = session.ExecuteNonQueryStatement("flush")
	if err != nil {
		t.Fatalf("Session should remain usable after InsertRecords error: %v", err)
	}
}

// TestSessionInsertAlignedRecordsReturnsBatchError 测试批量写入对齐记录部分失败返回批量错误
func TestSessionInsertAlignedRecordsReturnsBatchError(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	storageGroup := fmt.Sprintf("%s.verify_insert_aligned_records_%d", database, time.Now().UnixNano())
	_ = session.DeleteStorageGroup(storageGroup)
	t.Cleanup(func() {
		_ = session.DeleteStorageGroup(storageGroup)
	})

	err = session.SetStorageGroup(storageGroup)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	device := storageGroup + ".aligned_records_dev"
	err = session.CreateAlignedTimeseries(device,
		[]string{"s1"},
		[]client.TSDataType{client.INT32},
		[]client.TSEncoding{client.PLAIN},
		[]client.TSCompressionType{client.SNAPPY},
		nil)
	if err != nil {
		t.Fatalf("Failed to create aligned timeseries: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	err = session.InsertAlignedRecords(
		[]string{device, device},
		[][]string{{"s1"}, {"s1"}},
		[][]client.TSDataType{{client.INT32}, {client.BOOLEAN}},
		[][]interface{}{{int32(1)}, {true}},
		[]int64{ts, ts + 1})
	if err == nil {
		t.Fatal("Expected execution error, got nil")
	}

	var execErr *client.ExecutionError
	if !errors.As(err, &execErr) {
		t.Fatalf("Expected *client.ExecutionError, got %T: %v", err, err)
	}
	if execErr.Code != 507 {
		t.Fatalf("Expected error code 507, got %d: %v", execErr.Code, err)
	}
	if execErr.Message == "" {
		t.Fatal("Expected execution error message to be populated")
	}

	err = session.ExecuteNonQueryStatement("flush")
	if err != nil {
		t.Fatalf("Session should remain usable after InsertAlignedRecords error: %v", err)
	}
}

// TestSessionInsertTabletsReturnsBatchError 测试批量写入 tablets 部分失败返回批量错误
func TestSessionInsertTabletsReturnsBatchError(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	storageGroup := fmt.Sprintf("%s.verify_insert_tablets_%d", database, time.Now().UnixNano())
	_ = session.DeleteStorageGroup(storageGroup)
	t.Cleanup(func() {
		_ = session.DeleteStorageGroup(storageGroup)
	})

	err = session.SetStorageGroup(storageGroup)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	goodDevice := storageGroup + ".good_tablet_dev"
	badDevice := storageGroup + ".bad_tablet_dev"
	err = session.CreateTimeseries(goodDevice+".s1", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create good device timeseries: %v", err)
	}

	err = session.CreateTimeseries(badDevice+".s1", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create bad device timeseries: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	goodTablet, err := client.NewTablet(goodDevice, []*client.MeasurementSchema{
		{Measurement: "s1", DataType: client.INT32},
	}, 1)
	if err != nil {
		t.Fatalf("Failed to create good tablet: %v", err)
	}
	goodTablet.SetTimestamp(ts, 0)
	goodTablet.SetValueAt(int32(1), 0, 0)
	goodTablet.RowSize = 1

	badTablet, err := client.NewTablet(badDevice, []*client.MeasurementSchema{
		{Measurement: "s1", DataType: client.BOOLEAN},
	}, 1)
	if err != nil {
		t.Fatalf("Failed to create bad tablet: %v", err)
	}
	badTablet.SetTimestamp(ts+1, 0)
	badTablet.SetValueAt(true, 0, 0)
	badTablet.RowSize = 1

	err = session.InsertTablets([]*client.Tablet{goodTablet, badTablet}, false)
	if err == nil {
		t.Fatal("Expected execution error, got nil")
	}

	var execErr *client.ExecutionError
	if !errors.As(err, &execErr) {
		t.Fatalf("Expected *client.ExecutionError, got %T: %v", err, err)
	}
	if execErr.Code != 507 {
		t.Fatalf("Expected error code 507, got %d: %v", execErr.Code, err)
	}
	if execErr.Message == "" {
		t.Fatal("Expected execution error message to be populated")
	}

	err = session.ExecuteNonQueryStatement("flush")
	if err != nil {
		t.Fatalf("Session should remain usable after InsertTablets error: %v", err)
	}
}

// TestSessionInsertAlignedTabletsReturnsBatchError 测试批量写入对齐 tablets 部分失败返回批量错误
func TestSessionInsertAlignedTabletsReturnsBatchError(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	storageGroup := fmt.Sprintf("%s.verify_insert_aligned_tablets_%d", database, time.Now().UnixNano())
	_ = session.DeleteStorageGroup(storageGroup)
	t.Cleanup(func() {
		_ = session.DeleteStorageGroup(storageGroup)
	})

	err = session.SetStorageGroup(storageGroup)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	goodDevice := storageGroup + ".good_aligned_tablet_dev"
	badDevice := storageGroup + ".bad_aligned_tablet_dev"
	err = session.CreateAlignedTimeseries(goodDevice,
		[]string{"s1"},
		[]client.TSDataType{client.INT32},
		[]client.TSEncoding{client.PLAIN},
		[]client.TSCompressionType{client.SNAPPY},
		nil)
	if err != nil {
		t.Fatalf("Failed to create good aligned timeseries: %v", err)
	}

	err = session.CreateAlignedTimeseries(badDevice,
		[]string{"s1"},
		[]client.TSDataType{client.INT32},
		[]client.TSEncoding{client.PLAIN},
		[]client.TSCompressionType{client.SNAPPY},
		nil)
	if err != nil {
		t.Fatalf("Failed to create bad aligned timeseries: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	goodTablet, err := client.NewTablet(goodDevice, []*client.MeasurementSchema{
		{Measurement: "s1", DataType: client.INT32},
	}, 1)
	if err != nil {
		t.Fatalf("Failed to create good aligned tablet: %v", err)
	}
	goodTablet.SetTimestamp(ts, 0)
	goodTablet.SetValueAt(int32(1), 0, 0)
	goodTablet.RowSize = 1

	badTablet, err := client.NewTablet(badDevice, []*client.MeasurementSchema{
		{Measurement: "s1", DataType: client.BOOLEAN},
	}, 1)
	if err != nil {
		t.Fatalf("Failed to create bad aligned tablet: %v", err)
	}
	badTablet.SetTimestamp(ts+1, 0)
	badTablet.SetValueAt(true, 0, 0)
	badTablet.RowSize = 1

	err = session.InsertAlignedTablets([]*client.Tablet{goodTablet, badTablet}, false)
	if err == nil {
		t.Fatal("Expected execution error, got nil")
	}

	var execErr *client.ExecutionError
	if !errors.As(err, &execErr) {
		t.Fatalf("Expected *client.ExecutionError, got %T: %v", err, err)
	}
	if execErr.Code != 507 {
		t.Fatalf("Expected error code 507, got %d: %v", execErr.Code, err)
	}
	if execErr.Message == "" {
		t.Fatal("Expected execution error message to be populated")
	}

	err = session.ExecuteNonQueryStatement("flush")
	if err != nil {
		t.Fatalf("Session should remain usable after InsertAlignedTablets error: %v", err)
	}
}

// TestSessionExecuteBatchStatementReturnsBatchError 测试批量执行语句部分失败返回批量错误
func TestSessionExecuteBatchStatementReturnsBatchError(t *testing.T) {
	t.Skip("待在Java中验证是否效果一致，因为ExecuteBatchStatement认为重复创建时间戳异常操作是“正常返回失败状态”，不是 Java 异常，导致最后仍然返回 success，所以 Go client 拿到 nil")
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	storageGroup := fmt.Sprintf("%s.verify_batch_%d", database, time.Now().UnixNano())
	_ = session.DeleteStorageGroup(storageGroup)
	t.Cleanup(func() {
		_ = session.DeleteStorageGroup(storageGroup)
	})

	err = session.SetStorageGroup(storageGroup)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	existingPath := storageGroup + ".batch_dev.s1"
	err = session.CreateTimeseries(existingPath, client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create initial timeseries: %v", err)
	}

	statements := []string{
		fmt.Sprintf("create timeseries %s.batch_dev.s2 with datatype=INT32,encoding=PLAIN,compressor=SNAPPY", storageGroup),
		fmt.Sprintf("create timeseries %s with datatype=INT32,encoding=PLAIN,compressor=SNAPPY", existingPath),
	}

	err = session.ExecuteBatchStatement(statements)
	if err == nil {
		t.Fatal("Expected batch error, got nil")
	}

	var batchErr *client.BatchError
	if !errors.As(err, &batchErr) {
		t.Fatalf("Expected *client.BatchError, got %T: %v", err, err)
	}
	if len(batchErr.GetStatuses()) == 0 {
		t.Fatal("Expected batch error to contain sub-statuses")
	}
	if batchErr.Error() == "" {
		t.Fatal("Expected batch error message to be populated")
	}

	err = session.CreateTimeseries(storageGroup+".batch_dev.s3", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Session should remain usable after batch execution error: %v", err)
	}
}

// TestSessionInsertTabletReturnsExecutionError 测试 tablet 写入错误类型返回执行错误
func TestSessionInsertTabletReturnsExecutionError(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	storageGroup := fmt.Sprintf("%s.verify_insert_tablet_%d", database, time.Now().UnixNano())
	_ = session.DeleteStorageGroup(storageGroup)
	t.Cleanup(func() {
		_ = session.DeleteStorageGroup(storageGroup)
	})

	err = session.SetStorageGroup(storageGroup)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	device := storageGroup + ".tablet_dev"
	err = session.CreateTimeseries(device+".s1", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	tablet, err := client.NewTablet(device, []*client.MeasurementSchema{
		{Measurement: "s1", DataType: client.BOOLEAN},
	}, 1)
	if err != nil {
		t.Fatalf("Failed to create tablet: %v", err)
	}
	tablet.SetTimestamp(ts, 0)
	tablet.SetValueAt(true, 0, 0)
	tablet.RowSize = 1

	err = session.InsertTablet(tablet, false)
	if err == nil {
		t.Fatal("Expected execution error when inserting wrong tablet type, got nil")
	}

	var execErr *client.ExecutionError
	if !errors.As(err, &execErr) {
		t.Fatalf("Expected *client.ExecutionError, got %T: %v", err, err)
	}

	err = session.ExecuteNonQueryStatement("flush")
	if err != nil {
		t.Fatalf("Session should remain usable after InsertTablet error: %v", err)
	}
}
