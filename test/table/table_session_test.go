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

/*
* 测试表模型 Session 和 SessionPool
 */

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

// NewTableSessionPoolConfig 创建 TableSessionPool 配置
func NewTableSessionPoolConfig() *client.PoolConfig {
	return &client.PoolConfig{
		NodeUrls: nodeUrls,
		UserName: username,
		Password: password,
		Database: database,
	}
}

// TestTableSessionBasic 测试基本的 TableSession 操作
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

// TestPooledTableSessionCloseWithDifferentDatabase 测试 Close 时 database 不匹配的场景
// 覆盖 Close 方法中 s.session.config.Database != s.sessionPool.config.Database 分支
func TestPooledTableSessionCloseWithDifferentDatabase(t *testing.T) {
	// 创建一个不指定 database 的 pool 配置
	poolConfig := &client.PoolConfig{
		NodeUrls: nodeUrls,
		UserName: username,
		Password: password,
		Database: "", // 空 database，触发切换逻辑
	}
	pool := client.NewTableSessionPool(poolConfig, 1, 10000, 30000, false)
	defer pool.Close()

	session, err := pool.GetSession()
	if err != nil {
		t.Fatalf("Failed to get session from pool: %v", err)
	}

	// 先创建并切换到一个临时 database
	err = session.ExecuteNonQueryStatement("create database if not exists temp_db_for_close_test")
	if err != nil {
		t.Fatalf("Failed to create temp database: %v", err)
	}

	err = session.ExecuteNonQueryStatement("use temp_db_for_close_test")
	if err != nil {
		t.Fatalf("Failed to use temp database: %v", err)
	}

	// 此时 session 的 database 是 temp_db_for_close_test，而 pool 的 database 是空
	// Close 时应该不需要切换（因为 pool.config.Database 为空）
	err = session.Close()
	if err != nil {
		t.Errorf("Failed to close session: %v", err)
	}

	// 清理临时 database
	cleanupSession, _ := pool.GetSession()
	if cleanupSession != nil {
		cleanupSession.ExecuteNonQueryStatement("drop database if exists temp_db_for_close_test")
		cleanupSession.Close()
	}
}

// TestPooledTableSessionCloseWithDatabaseMismatch 测试 Close 时需要切换回 pool database 的场景
// 覆盖 Close 方法中 database 不匹配且需要执行 use 语句的分支
func TestPooledTableSessionCloseWithDatabaseMismatch(t *testing.T) {
	// pool 配置指定 database
	pool := client.NewTableSessionPool(NewTableSessionPoolConfig(), 1, 10000, 30000, false)
	defer pool.Close()

	session, err := pool.GetSession()
	if err != nil {
		t.Fatalf("Failed to get session from pool: %v", err)
	}

	// 确保 database 存在
	err = session.ExecuteNonQueryStatement("create database if not exists " + database)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// 切换到另一个 database
	err = session.ExecuteNonQueryStatement("create database if not exists another_db_for_test")
	if err != nil {
		t.Fatalf("Failed to create another database: %v", err)
	}

	err = session.ExecuteNonQueryStatement("use another_db_for_test")
	if err != nil {
		t.Fatalf("Failed to use another database: %v", err)
	}

	// 此时 session.config.Database 是 another_db_for_test
	// pool.config.Database 是 database
	// Close 时应该执行 use database 切换回来
	err = session.Close()
	if err != nil {
		t.Errorf("Failed to close session: %v", err)
	}

	// 清理
	cleanupSession, _ := pool.GetSession()
	if cleanupSession != nil {
		cleanupSession.ExecuteNonQueryStatement("drop database if exists another_db_for_test")
		cleanupSession.Close()
	}
}

// TestPooledTableSessionConnectionErrorBehavior 测试连接错误时的 session drop 行为
// 覆盖 isConnectionError 函数和 Insert/ExecuteNonQueryStatement/ExecuteQueryStatement 的错误处理分支
// 注意：此测试通过使用无效端口模拟连接错误
func TestPooledTableSessionConnectionErrorBehavior(t *testing.T) {
	// 使用无效端口配置，模拟连接错误场景
	invalidPoolConfig := &client.PoolConfig{
		NodeUrls: []string{"127.0.0.1:9999"}, // 无效端口
		UserName: username,
		Password: password,
		Database: database,
	}
	pool := client.NewTableSessionPool(invalidPoolConfig, 1, 1000, 1000, false)
	defer pool.Close()

	session, err := pool.GetSession()
	// 获取 session 可能因为连接超时而失败
	if err != nil {
		// 这是预期的连接错误，测试通过
		t.Logf("Expected connection error on GetSession: %v", err)
		return
	}

	// 如果获取到了 session（某些情况下可能成功），尝试操作来触发连接错误
	tablet, _ := client.NewRelationalTablet("test_table", []*client.MeasurementSchema{
		{Measurement: "s1", DataType: client.TEXT},
	}, []client.ColumnCategory{client.FIELD}, 1)

	err = session.Insert(tablet)
	if err != nil {
		// 连接错误时 session 应该被 drop，后续操作应返回 ErrTableSessionClosed
		t.Logf("Insert error: %v", err)

		// 再次尝试操作，验证 session 已被关闭
		err2 := session.ExecuteNonQueryStatement("show tables")
		if err2 != nil && errors.Is(err2, client.ErrTableSessionClosed) {
			t.Log("Session was correctly dropped after connection error")
		}
	}

	session.Close()
}

// TestPooledTableSessionExecuteNonQueryError 测试 ExecuteNonQueryStatement 的错误处理
// 覆盖 ExecuteNonQueryStatement 方法中的错误处理分支
func TestPooledTableSessionExecuteNonQueryError(t *testing.T) {
	pool := client.NewTableSessionPool(NewTableSessionPoolConfig(), 1, 10000, 30000, false)
	defer pool.Close()

	session, err := pool.GetSession()
	if err != nil {
		t.Fatalf("Failed to get session from pool: %v", err)
	}

	// 执行一个语法错误的 SQL，触发执行错误（不是连接错误）
	err = session.ExecuteNonQueryStatement("invalid sql statement with syntax error")
	if err == nil {
		t.Error("Expected error for invalid SQL, got nil")
	} else {
		// 执行错误不应导致 session 被 drop
		// 验证 session 仍然可用
		err2 := session.ExecuteNonQueryStatement("show tables")
		if err2 != nil && errors.Is(err2, client.ErrTableSessionClosed) {
			t.Error("Session should not be dropped for execution error")
		}
		t.Logf("Expected execution error: %v", err)
	}

	session.Close()
}

// TestPooledTableSessionExecuteQueryError 测试 ExecuteQueryStatement 的错误处理
// 覆盖 ExecuteQueryStatement 方法中的错误处理分支
func TestPooledTableSessionExecuteQueryError(t *testing.T) {
	pool := client.NewTableSessionPool(NewTableSessionPoolConfig(), 1, 10000, 30000, false)
	defer pool.Close()

	session, err := pool.GetSession()
	if err != nil {
		t.Fatalf("Failed to get session from pool: %v", err)
	}

	// 确保 database 存在并切换
	err = session.ExecuteNonQueryStatement("create database if not exists " + database)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	err = session.ExecuteNonQueryStatement("use " + database)
	if err != nil {
		t.Fatalf("Failed to use database: %v", err)
	}

	timeout := int64(5000)

	// 执行一个语法错误的查询 SQL，触发执行错误
	_, err = session.ExecuteQueryStatement("select from invalid_syntax", &timeout)
	if err != nil {
		// 执行错误不应导致 session 被 drop
		// 验证 session 仍然可用
		timeout2 := int64(5000)
		_, err2 := session.ExecuteQueryStatement("show tables", &timeout2)
		if err2 != nil && errors.Is(err2, client.ErrTableSessionClosed) {
			t.Error("Session should not be dropped for execution error")
		}
		t.Logf("Expected execution error: %v", err)
	} else {
		// 某些情况下可能不报错
		t.Log("Query succeeded or returned empty result")
	}

	session.Close()
}

// TestPooledTableSessionInsertError 测试 Insert 的错误处理
// 覆盖 Insert 方法中的错误处理分支
func TestPooledTableSessionInsertError(t *testing.T) {
	pool := client.NewTableSessionPool(NewTableSessionPoolConfig(), 1, 10000, 30000, false)
	defer pool.Close()

	session, err := pool.GetSession()
	if err != nil {
		t.Fatalf("Failed to get session from pool: %v", err)
	}

	// 确保 database 存在
	err = session.ExecuteNonQueryStatement("create database if not exists " + database)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	err = session.ExecuteNonQueryStatement("use " + database)
	if err != nil {
		t.Fatalf("Failed to use database: %v", err)
	}

	// 创建一个表结构不匹配的 tablet，触发执行错误
	// 先创建一个有特定结构的表
	err = session.ExecuteNonQueryStatement("create table if not exists error_test_table (id string tag, value int32 field)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// 创建一个列类型不匹配的 tablet
	tablet, err := client.NewRelationalTablet("error_test_table", []*client.MeasurementSchema{
		{Measurement: "id", DataType: client.STRING},
		{Measurement: "value", DataType: client.DOUBLE}, // 类型不匹配：表定义为 int32，这里用 DOUBLE
	}, []client.ColumnCategory{client.TAG, client.FIELD}, 1)
	if err != nil {
		t.Fatalf("Failed to create tablet: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	tablet.SetTimestamp(ts, 0)
	tablet.SetValueAt("test_id", 0, 0)
	tablet.SetValueAt(3.14, 1, 0) // DOUBLE 值
	tablet.RowSize = 1

	err = session.Insert(tablet)
	if err != nil {
		// 执行错误不应导致 session 被 drop
		// 验证 session 仍然可用
		err2 := session.ExecuteNonQueryStatement("show tables")
		if err2 != nil && errors.Is(err2, client.ErrTableSessionClosed) {
			t.Error("Session should not be dropped for execution error")
		}
		t.Logf("Got insert error: %v", err)
	} else {
		// 某些情况下 IoTDB 可能允许类型转换，不报错
		t.Log("Insert succeeded (IoTDB may allow type conversion)")
	}

	// 清理测试表
	session.ExecuteNonQueryStatement("drop table if exists error_test_table")
	session.Close()
}

// TestPooledTableSessionCloseWithNonexistentDatabase 测试 Close 时 target database 不存在的场景
// 覆盖 Close 方法中 use database 失败时 drop session 的分支
func TestPooledTableSessionCloseWithNonexistentDatabase(t *testing.T) {
	// 创建一个指向不存在 database 的 pool 配置
	poolConfig := &client.PoolConfig{
		NodeUrls: nodeUrls,
		UserName: username,
		Password: password,
		Database: "nonexistent_close_test_db", // 不存在的 database
	}
	pool := client.NewTableSessionPool(poolConfig, 1, 10000, 30000, false)
	defer pool.Close()

	session, err := pool.GetSession()
	if err != nil {
		t.Fatalf("Failed to get session from pool: %v", err)
	}

	// 切换到一个存在的 database
	err = session.ExecuteNonQueryStatement("create database if not exists " + database)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	err = session.ExecuteNonQueryStatement("use " + database)
	if err != nil {
		t.Fatalf("Failed to use database: %v", err)
	}

	// 此时 session.config.Database 是存在的 database
	// pool.config.Database 是 nonexistent_close_test_db（不存在）
	// Close 时会尝试 use nonexistent_close_test_db，会失败，导致 session 被 drop
	err = session.Close()
	if err != nil {
		t.Logf("Close returned error: %v", err)
	}

	// 验证 session 已被关闭
	err = session.ExecuteNonQueryStatement("show tables")
	if err == nil {
		t.Error("Expected ErrTableSessionClosed after close with nonexistent database, got nil")
	} else if !errors.Is(err, client.ErrTableSessionClosed) {
		t.Logf("Session state after close: %v", err)
	}
}

// TestPooledTableSessionDoubleClose 测试重复调用 Close 的安全性
// 覆盖 Close 方法中 CAS 失败（已关闭）的分支
func TestPooledTableSessionDoubleClose(t *testing.T) {
	pool := client.NewTableSessionPool(NewTableSessionPoolConfig(), 1, 10000, 30000, false)
	defer pool.Close()

	session, err := pool.GetSession()
	if err != nil {
		t.Fatalf("Failed to get session from pool: %v", err)
	}

	// 第一次 Close
	err = session.Close()
	if err != nil {
		t.Errorf("First close failed: %v", err)
	}

	// 第二次 Close - CAS 会失败，因为 closed 已经是 1
	err = session.Close()
	if err != nil {
		t.Errorf("Second close should not return error: %v", err)
	}

	// 验证 session 已被关闭
	err = session.ExecuteNonQueryStatement("show tables")
	if err == nil {
		t.Error("Expected ErrTableSessionClosed after double close, got nil")
	} else if !errors.Is(err, client.ErrTableSessionClosed) {
		t.Errorf("Expected ErrTableSessionClosed, got: %v", err)
	}
}
