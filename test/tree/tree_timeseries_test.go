package tree

/*
* 测试树模型 Timeseries 相关操作
 */

import (
	"testing"

	"github.com/apache/iotdb-client-go/v2/client"
)

// TestSessionCreateAlignedTimeseries 测试创建对齐时间序列
func TestSessionCreateAlignedTimeseries(t *testing.T) {
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

	// 创建对齐时间序列
	err = session.CreateAlignedTimeseries(database+".aligned_dev",
		[]string{"s1", "s2"},
		[]client.TSDataType{client.FLOAT, client.INT64},
		[]client.TSEncoding{client.PLAIN, client.PLAIN},
		[]client.TSCompressionType{client.SNAPPY, client.SNAPPY},
		[]string{"alias_s1", "alias_s2"})
	if err != nil {
		t.Fatalf("Failed to create aligned timeseries: %v", err)
	}

	// 验证创建成功
	timeout := int64(5000)
	ds, err := session.ExecuteQueryStatement("show timeseries "+database+".aligned_dev.s1", &timeout)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer ds.Close()

	hasNext, err := ds.Next()
	if err != nil || !hasNext {
		t.Errorf("Expected to find timeseries, got error: %v", err)
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".aligned_dev.s1", database + ".aligned_dev.s2"})
	session.DeleteStorageGroup(database)
}

// TestSessionCreateMultiTimeseries 测试创建多个时间序列
func TestSessionCreateMultiTimeseries(t *testing.T) {
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

	// 批量创建时间序列
	err = session.CreateMultiTimeseries(
		[]string{database + ".multi_dev.s1", database + ".multi_dev.s2"},
		[]client.TSDataType{client.INT32, client.DOUBLE},
		[]client.TSEncoding{client.PLAIN, client.PLAIN},
		[]client.TSCompressionType{client.SNAPPY, client.SNAPPY})
	if err != nil {
		t.Fatalf("Failed to create multi timeseries: %v", err)
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".multi_dev.s1", database + ".multi_dev.s2"})
	session.DeleteStorageGroup(database)
}

// TestSessionCreateTimeseriesWithAttributesAndTags 测试创建带属性和标签的时间序列
func TestSessionCreateTimeseriesWithAttributesAndTags(t *testing.T) {
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

	attributes := map[string]string{"attr1": "value1", "attr2": "value2"}
	tags := map[string]string{"tag1": "tagvalue1", "tag2": "tagvalue2"}

	err = session.CreateTimeseries(database+".attr_tags_dev.s1", client.INT32, client.PLAIN, client.SNAPPY, attributes, tags)
	if err != nil {
		t.Fatalf("Failed to create timeseries with attributes and tags: %v", err)
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".attr_tags_dev.s1"})
	session.DeleteStorageGroup(database)
}
