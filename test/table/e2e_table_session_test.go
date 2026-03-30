package table

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/iotdb-client-go/v2/client"
)

var (
	nodeUrls []string
	host     string
	port     string
	username string
	password string
	database string
)

// init 初始化时读取配置文件
func init() {
	// 配置文件路径
	configPath := filepath.Join("..", "..", "conf", "config.properties")

	// 检验配置文件是否合法
	data, err := os.ReadFile(configPath)
	if err != nil {
		panic("failed to read config file: " + err.Error())
	}

	// 解析 properties 文件
	props := make(map[string]string)
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if kv := strings.SplitN(line, "=", 2); len(kv) == 2 {
			props[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		}
	}

	// 读取配置项
	nodeUrls = strings.Split(props["nodeUrls"], ",")
	host = props["host"]
	port = props["port"]
	username = props["username"]
	password = props["password"]
	database = props["table_database"]
}

// NewTableSession 创建 TableSession 配置
func NewTableSessionConfig() *client.Config {
	return &client.Config{
		Host:     host,
		Port:     port,
		UserName: username,
		Password: password,
		Database: database,
	}
}

// NewTableSessionPoolConfig 创建 SessionPool 配置
func NewTableSessionPoolConfig() *client.PoolConfig {
	return &client.PoolConfig{
		NodeUrls: nodeUrls,
		UserName: username,
		Password: password,
		Database: database,
	}
}

// TestTableSessionBasic 测试基本的 TableSession 操作
// 测试流程：
// 1. 创建 TableSession 连接
// 2. 创建数据库和表
// 3. 使用 Tablet 批量插入数据
// 4. 查询并验证数据
// 5. 清理测试表
func TestTableSessionBasic(t *testing.T) {
	session, err := client.NewTableSession(NewTableSessionConfig(), false, 10000)
	if err != nil {
		t.Fatalf("Failed to create TableSession: %v", err)
	}
	defer session.Close()

	err = session.ExecuteNonQueryStatement("create database if not exists " + database)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	err = session.ExecuteNonQueryStatement("use " + database)
	if err != nil {
		t.Fatalf("Failed to use database: %v", err)
	}

	err = session.ExecuteNonQueryStatement("create table if not exists test_table (tag1 string tag, tag2 string tag, s1 text field, s2 text field)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	insertRelationalTablet(t, session, "test_table")
	queryAndVerify(t, session, "select * from test_table")

	err = session.ExecuteNonQueryStatement("drop table if exists test_table")
	if err != nil {
		t.Logf("Warning: Failed to drop table: %v", err)
	}
}

// TestTableSessionPoolBasic 测试 TableSessionPool 的基本功能
func TestTableSessionPoolBasic(t *testing.T) {
	pool := client.NewTableSessionPool(NewTableSessionPoolConfig(), 5, 10000, 30000, false)
	defer pool.Close()

	session, err := pool.GetSession()
	if err != nil {
		t.Fatalf("Failed to get session from pool: %v", err)
	}
	defer session.Close()

	err = session.ExecuteNonQueryStatement("create database if not exists " + database)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	err = session.ExecuteNonQueryStatement("use " + database)
	if err != nil {
		t.Fatalf("Failed to use database: %v", err)
	}

	err = session.ExecuteNonQueryStatement("create table if not exists pool_test_table (tag1 string tag, value int32 field)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tablet, err := client.NewRelationalTablet("pool_test_table", []*client.MeasurementSchema{
		{Measurement: "tag1", DataType: client.STRING},
		{Measurement: "value", DataType: client.INT32},
	}, []client.ColumnCategory{client.TAG, client.FIELD}, 10)
	if err != nil {
		t.Fatalf("Failed to create tablet: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	for i := 0; i < 5; i++ {
		tablet.SetTimestamp(ts+int64(i), i)
		tablet.SetValueAt("tag_"+strconv.Itoa(i), 0, i)
		tablet.SetValueAt(int32(i*100), 1, i)
		tablet.RowSize++
	}

	err = session.Insert(tablet)
	if err != nil {
		t.Fatalf("Failed to insert tablet: %v", err)
	}

	timeout := int64(5000)
	dataSet, err := session.ExecuteQueryStatement("select * from pool_test_table", &timeout)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer dataSet.Close()

	count := 0
	for {
		hasNext, err := dataSet.Next()
		if err != nil {
			t.Fatalf("Failed to get next row: %v", err)
		}
		if !hasNext {
			break
		}
		count++
	}

	if count != 5 {
		t.Errorf("Expected 5 rows, got %d", count)
	}

	err = session.ExecuteNonQueryStatement("drop table if exists pool_test_table")
	if err != nil {
		t.Logf("Warning: Failed to drop table: %v", err)
	}
}

// TestPooledTableSessionClosedBehavior 测试已关闭 session 的行为
func TestPooledTableSessionClosedBehavior(t *testing.T) {
	pool := client.NewTableSessionPool(NewTableSessionPoolConfig(), 1, 10000, 30000, false)
	defer pool.Close()

	session, err := pool.GetSession()
	if err != nil {
		t.Fatalf("Failed to get session from pool: %v", err)
	}

	err = session.Close()
	if err != nil {
		t.Fatalf("Failed to close session: %v", err)
	}

	err = session.ExecuteNonQueryStatement("show tables")
	if err == nil {
		t.Error("Expected ErrTableSessionClosed, got nil")
	} else if !errors.Is(err, client.ErrTableSessionClosed) {
		t.Errorf("Expected ErrTableSessionClosed, got: %v", err)
	}

	tablet, _ := client.NewRelationalTablet("dummy", []*client.MeasurementSchema{
		{Measurement: "s1", DataType: client.TEXT},
	}, []client.ColumnCategory{client.FIELD}, 1)
	err = session.Insert(tablet)
	if err == nil {
		t.Error("Expected ErrTableSessionClosed for Insert, got nil")
	} else if !errors.Is(err, client.ErrTableSessionClosed) {
		t.Errorf("Expected ErrTableSessionClosed for Insert, got: %v", err)
	}

	timeout := int64(1000)
	_, err = session.ExecuteQueryStatement("show tables", &timeout)
	if err == nil {
		t.Error("Expected ErrTableSessionClosed for ExecuteQueryStatement, got nil")
	} else if !errors.Is(err, client.ErrTableSessionClosed) {
		t.Errorf("Expected ErrTableSessionClosed for ExecuteQueryStatement, got: %v", err)
	}
}

// TestPooledTableSessionConcurrentClose 测试并发 Close 调用的安全性
func TestPooledTableSessionConcurrentClose(t *testing.T) {
	pool := client.NewTableSessionPool(NewTableSessionPoolConfig(), 1, 10000, 30000, false)
	defer pool.Close()

	session, err := pool.GetSession()
	if err != nil {
		t.Fatalf("Failed to get session from pool: %v", err)
	}

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			session.Close()
		}()
	}

	wg.Wait()

	err = session.ExecuteNonQueryStatement("show tables")
	if err == nil {
		t.Error("Expected ErrTableSessionClosed after close, got nil")
	} else if !errors.Is(err, client.ErrTableSessionClosed) {
		t.Errorf("Expected ErrTableSessionClosed, got: %v", err)
	}
}

// TestTableSessionPoolConcurrentAccess 测试连接池的并发访问能力
func TestTableSessionPoolConcurrentAccess(t *testing.T) {
	pool := client.NewTableSessionPool(NewTableSessionPoolConfig(), 3, 10000, 60000, false)
	defer pool.Close()

	setupSession, err := pool.GetSession()
	if err != nil {
		t.Fatalf("Failed to get setup session: %v", err)
	}

	err = setupSession.ExecuteNonQueryStatement("create database if not exists " + database)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	err = setupSession.ExecuteNonQueryStatement("use " + database)
	if err != nil {
		t.Fatalf("Failed to use database: %v", err)
	}

	err = setupSession.ExecuteNonQueryStatement("create table if not exists concurrent_table (id string tag, value double field)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	setupSession.Close()

	var wg sync.WaitGroup
	errCount := 0
	var mu sync.Mutex

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()

			session, err := pool.GetSession()
			if err != nil {
				mu.Lock()
				errCount++
				mu.Unlock()
				return
			}
			defer session.Close()

			tablet, err := client.NewRelationalTablet("concurrent_table", []*client.MeasurementSchema{
				{Measurement: "id", DataType: client.STRING},
				{Measurement: "value", DataType: client.DOUBLE},
			}, []client.ColumnCategory{client.TAG, client.FIELD}, 10)
			if err != nil {
				return
			}

			ts := time.Now().UTC().UnixNano() / 1000000
			for j := 0; j < 5; j++ {
				tablet.SetTimestamp(ts+int64(workerId*100+j), j)
				tablet.SetValueAt(fmt.Sprintf("worker_%d", workerId), 0, j)
				tablet.SetValueAt(float64(workerId*100+j), 1, j)
				tablet.RowSize++
			}

			err = session.Insert(tablet)
			if err != nil {
				mu.Lock()
				errCount++
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	if errCount > 0 {
		t.Errorf("Got %d errors during concurrent operations", errCount)
	}

	cleanupSession, _ := pool.GetSession()
	if cleanupSession != nil {
		cleanupSession.ExecuteNonQueryStatement("drop table if exists concurrent_table")
		cleanupSession.Close()
	}
}

// insertRelationalTablet 辅助函数：插入关系型 Tablet 数据
func insertRelationalTablet(t *testing.T, session client.ITableSession, tableName string) {
	tablet, err := client.NewRelationalTablet(tableName, []*client.MeasurementSchema{
		{Measurement: "tag1", DataType: client.STRING},
		{Measurement: "tag2", DataType: client.STRING},
		{Measurement: "s1", DataType: client.TEXT},
		{Measurement: "s2", DataType: client.TEXT},
	}, []client.ColumnCategory{client.TAG, client.TAG, client.FIELD, client.FIELD}, 1024)
	if err != nil {
		t.Fatalf("Failed to create relational tablet: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	for row := 0; row < 16; row++ {
		ts++
		tablet.SetTimestamp(ts, row)
		tablet.SetValueAt("tag1_value_"+strconv.Itoa(row), 0, row)
		tablet.SetValueAt("tag2_value_"+strconv.Itoa(row), 1, row)
		tablet.SetValueAt("s1_value_"+strconv.Itoa(row), 2, row)
		tablet.SetValueAt("s2_value_"+strconv.Itoa(row), 3, row)
		tablet.RowSize++
	}

	err = session.Insert(tablet)
	if err != nil {
		t.Fatalf("Failed to insert tablet: %v", err)
	}

	tablet.Reset()
	for row := 0; row < 8; row++ {
		ts++
		tablet.SetTimestamp(ts, row)
		tablet.SetValueAt("tag1_null_test", 0, row)
		tablet.SetValueAt("tag2_null_test", 1, row)
		tablet.SetValueAt("s1_value_"+strconv.Itoa(row), 2, row)
		tablet.SetValueAt(nil, 3, row)
		tablet.RowSize++
	}

	err = session.Insert(tablet)
	if err != nil {
		t.Fatalf("Failed to insert tablet with null values: %v", err)
	}
}

// queryAndVerify 辅助函数：查询并验证数据行数
func queryAndVerify(t *testing.T, session client.ITableSession, sql string) {
	timeout := int64(5000)
	dataSet, err := session.ExecuteQueryStatement(sql, &timeout)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer dataSet.Close()

	rowCount := 0
	for {
		hasNext, err := dataSet.Next()
		if err != nil {
			t.Fatalf("Failed to get next row: %v", err)
		}
		if !hasNext {
			break
		}
		rowCount++
	}

	if rowCount != 24 {
		t.Errorf("Expected 24 rows, got %d", rowCount)
	}
}

// TestErrTableSessionClosedIsDefined 测试 ErrTableSessionClosed 错误定义
func TestErrTableSessionClosedIsDefined(t *testing.T) {
	if client.ErrTableSessionClosed == nil {
		t.Error("ErrTableSessionClosed should not be nil")
	}

	if client.ErrTableSessionClosed.Error() != "table session has been closed" {
		t.Errorf("ErrTableSessionClosed message mismatch: got '%s'", client.ErrTableSessionClosed.Error())
	}
}
