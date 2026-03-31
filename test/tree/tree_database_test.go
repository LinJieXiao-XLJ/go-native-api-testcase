package tree

/*
* 测试树模型数据库相关操作
 */

import (
	"testing"
	"time"

	"github.com/apache/iotdb-client-go/v2/client"
)

// TestSessionDeleteData 测试删除数据
func TestSessionDeleteData(t *testing.T) {
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

	err = session.CreateTimeseries(database+".delete_dev.s1", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	// 插入数据
	ts := time.Now().UTC().UnixNano() / 1000000
	err = session.InsertRecords(
		[]string{database + ".delete_dev", database + ".delete_dev"},
		[][]string{{"s1"}, {"s1"}},
		[][]client.TSDataType{{client.INT32}, {client.INT32}},
		[][]interface{}{{int32(1)}, {int32(2)}},
		[]int64{ts, ts + 1000})
	if err != nil {
		t.Fatalf("Failed to insert records: %v", err)
	}

	// 删除部分数据
	err = session.DeleteData([]string{database + ".delete_dev.s1"}, ts, ts+500)
	if err != nil {
		t.Fatalf("Failed to delete data: %v", err)
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".delete_dev.s1"})
	session.DeleteStorageGroup(database)
}

// TestSessionDeleteStorageGroups 测试批量删除存储组
func TestSessionDeleteStorageGroups(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	// 先清理可能存在的存储组
	session.DeleteStorageGroups(database+".sg1", database+".sg2")

	// 创建多个存储组
	err = session.SetStorageGroup(database + ".sg1")
	if err != nil {
		t.Fatalf("Failed to set storage group sg1: %v", err)
	}

	err = session.SetStorageGroup(database + ".sg2")
	if err != nil {
		t.Fatalf("Failed to set storage group sg2: %v", err)
	}

	// 批量删除存储组
	err = session.DeleteStorageGroups(database+".sg1", database+".sg2")
	if err != nil {
		t.Fatalf("Failed to delete storage groups: %v", err)
	}
}

// TestSessionDeleteTimeseries 测试删除时间序列
func TestSessionDeleteTimeseries(t *testing.T) {
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
	err = session.CreateTimeseries(database+".del_ts_dev.s1", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	err = session.CreateTimeseries(database+".del_ts_dev.s2", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	// 删除时间序列
	err = session.DeleteTimeseries([]string{database + ".del_ts_dev.s1", database + ".del_ts_dev.s2"})
	if err != nil {
		t.Fatalf("Failed to delete timeseries: %v", err)
	}

	// 清理
	session.DeleteStorageGroup(database)
}

// TestSessionSetStorageGroup 测试设置存储组
func TestSessionSetStorageGroup(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	// 先清理可能存在的存储组
	session.DeleteStorageGroup(database + ".sg_test")

	// 设置存储组
	err = session.SetStorageGroup(database + ".sg_test")
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	// 清理
	session.DeleteStorageGroup(database + ".sg_test")
}

// TestSessionDeleteStorageGroup 测试删除单个存储组
func TestSessionDeleteStorageGroup(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	// 先清理可能存在的存储组
	session.DeleteStorageGroup(database + ".del_sg_test")

	// 设置存储组
	err = session.SetStorageGroup(database + ".del_sg_test")
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	// 删除存储组
	err = session.DeleteStorageGroup(database + ".del_sg_test")
	if err != nil {
		t.Fatalf("Failed to delete storage group: %v", err)
	}
}
