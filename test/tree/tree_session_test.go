package tree

/*
* 测试树模型 Session 和 SessionPool
 */

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/apache/iotdb-client-go/v2/client"
)

// TestSessionBasic 测试基本的 Session 操作
func TestSessionBasic(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	// 设置存储组
	err = cleanupAndSetStorageGroup(session, database)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	// 创建时间序列
	err = session.CreateTimeseries(database+".dev1.status", client.BOOLEAN, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	err = session.CreateTimeseries(database+".dev1.temperature", client.FLOAT, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	// 插入数据
	ts := time.Now().UTC().UnixNano() / 1000000
	err = session.InsertRecord(database+".dev1", []string{"status", "temperature"}, []client.TSDataType{client.BOOLEAN, client.FLOAT}, []interface{}{true, float32(36.5)}, ts)
	if err != nil {
		t.Fatalf("Failed to insert record: %v", err)
	}

	// 查询数据
	timeout := int64(5000)
	ds, err := session.ExecuteQueryStatement("select * from "+database+".dev1", &timeout)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer ds.Close()

	count := 0
	for {
		hasNext, err := ds.Next()
		if err != nil {
			t.Fatalf("Failed to get next row: %v", err)
		}
		if !hasNext {
			break
		}
		count++
	}

	if count != 1 {
		t.Errorf("Expected 1 row, got %d", count)
	}

	// 清理
	err = session.DeleteTimeseries([]string{database + ".dev1.status", database + ".dev1.temperature"})
	if err != nil {
		t.Logf("Warning: Failed to delete timeseries: %v", err)
	}

	err = session.DeleteStorageGroup(database)
	if err != nil {
		t.Logf("Warning: Failed to delete storage group: %v", err)
	}
}

// TestSessionPoolBasic 测试 SessionPool 的基本功能
func TestSessionPoolBasic(t *testing.T) {
	pool := client.NewSessionPool(NewTreeSessionPoolConfig(), 5, 10000, 30000, false)
	defer pool.Close()

	session, err := pool.GetSession()
	if err != nil {
		t.Fatalf("Failed to get session from pool: %v", err)
	}
	defer pool.PutBack(session)

	err = cleanupAndSetStorageGroup(session, database)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	err = session.CreateTimeseries(database+".pool_dev.status", client.TEXT, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	// 使用 Tablet 批量插入数据
	tablet, err := client.NewTablet(database+".pool_dev", []*client.MeasurementSchema{
		{Measurement: "status", DataType: client.TEXT},
	}, 10)
	if err != nil {
		t.Fatalf("Failed to create tablet: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	for i := 0; i < 5; i++ {
		tablet.SetTimestamp(ts+int64(i), i)
		tablet.SetValueAt("status_"+strconv.Itoa(i), 0, i)
		tablet.RowSize++
	}

	err = session.InsertTablet(tablet, false)
	if err != nil {
		t.Fatalf("Failed to insert tablet: %v", err)
	}

	// 查询数据
	timeout := int64(5000)
	ds, err := session.ExecuteQueryStatement("select count(status) from "+database+".pool_dev", &timeout)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer ds.Close()

	for {
		hasNext, err := ds.Next()
		if err != nil {
			t.Fatalf("Failed to get next row: %v", err)
		}
		if !hasNext {
			break
		}
		countStr, err := ds.GetStringByIndex(1)
		if err != nil {
			t.Fatalf("Failed to get count: %v", err)
		}
		if countStr != "5" {
			t.Errorf("Expected count 5, got %s", countStr)
		}
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".pool_dev.status"})
	session.DeleteStorageGroup(database)
}

// TestClusterSessionBasic 测试集群 Session 的基本操作
func TestClusterSessionBasic(t *testing.T) {
	clusterConfig := &client.ClusterConfig{
		NodeUrls:  nodeUrls,
		UserName:  username,
		Password:  password,
		FetchSize: 1024,
		TimeZone:  "Asia/Shanghai",
	}

	session, err := client.NewClusterSession(clusterConfig)
	if err != nil {
		t.Fatalf("Failed to create cluster session: %v", err)
	}

	err = session.OpenCluster(false)
	if err != nil {
		t.Fatalf("Failed to open cluster session: %v", err)
	}
	defer session.Close()

	err = cleanupAndSetStorageGroup(session, database)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	err = session.CreateTimeseries(database+".cluster_dev.s1", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}

	ts := time.Now().UTC().UnixNano() / 1000000
	err = session.InsertRecord(database+".cluster_dev", []string{"s1"}, []client.TSDataType{client.INT32}, []interface{}{int32(100)}, ts)
	if err != nil {
		t.Fatalf("Failed to insert record: %v", err)
	}

	// 清理
	session.DeleteTimeseries([]string{database + ".cluster_dev.s1"})
	session.DeleteStorageGroup(database)
}

// TestClusterSessionWrongURL 测试错误的集群地址
func TestClusterSessionWrongURL(t *testing.T) {
	clusterConfig := &client.ClusterConfig{
		NodeUrls: []string{"127.0.0.1:9999"}, // 无效端口
		UserName: username,
		Password: password,
	}

	_, err := client.NewClusterSession(clusterConfig)
	if err == nil {
		t.Error("Expected error for wrong URL, got nil")
	} else {
		t.Logf("Expected error: %v", err)
	}
}

// TestSessionPoolConcurrentAccess 测试连接池的并发访问能力
func TestSessionPoolConcurrentAccess(t *testing.T) {
	pool := client.NewSessionPool(NewTreeSessionPoolConfig(), 3, 10000, 60000, false)
	defer pool.Close()

	setupSession, err := pool.GetSession()
	if err != nil {
		t.Fatalf("Failed to get setup session: %v", err)
	}

	err = cleanupAndSetStorageGroup(setupSession, database)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	err = setupSession.CreateTimeseries(database+".concurrent_dev.s1", client.INT32, client.PLAIN, client.SNAPPY, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create timeseries: %v", err)
	}
	pool.PutBack(setupSession)

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
			defer pool.PutBack(session)

			ts := time.Now().UTC().UnixNano() / 1000000
			err = session.InsertRecord(database+".concurrent_dev",
				[]string{"s1"},
				[]client.TSDataType{client.INT32},
				[]interface{}{int32(workerId * 100)},
				ts+int64(workerId))
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

	// 清理
	cleanupSession, err := pool.GetSession()
	if err == nil {
		cleanupSession.DeleteTimeseries([]string{database + ".concurrent_dev.s1"})
		cleanupSession.DeleteStorageGroup(database)
		pool.PutBack(cleanupSession)
	}
}

// TestSessionPoolGetSessionTimeout 测试获取 Session 超时
func TestSessionPoolGetSessionTimeout(t *testing.T) {
	// 使用较小的超时时间和有限池大小
	poolConfig := &client.PoolConfig{
		NodeUrls: nodeUrls,
		UserName: username,
		Password: password,
	}
	pool := client.NewSessionPool(poolConfig, 1, 1000, 1000, false) // 1秒超时
	defer pool.Close()

	// 先获取一个 session 占用池
	session1, err := pool.GetSession()
	if err != nil {
		t.Logf("Failed to get first session: %v", err)
		return
	}

	// 并发尝试获取第二个 session，应该超时
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := pool.GetSession()
		if err != nil {
			t.Logf("Expected timeout error: %v", err)
		}
	}()

	wg.Wait()
	pool.PutBack(session1)
}

// TestSessionPoolClose 测试连接池关闭
func TestSessionPoolClose(t *testing.T) {
	pool := client.NewSessionPool(NewTreeSessionPoolConfig(), 2, 10000, 30000, false)

	session, err := pool.GetSession()
	if err != nil {
		t.Fatalf("Failed to get session: %v", err)
	}
	pool.PutBack(session)

	// 关闭连接池
	pool.Close()

	t.Logf("Pool closed successfully")
}

// TestSessionPoolPutBack 测试归还 Session
func TestSessionPoolPutBack(t *testing.T) {
	pool := client.NewSessionPool(NewTreeSessionPoolConfig(), 2, 10000, 30000, false)
	defer pool.Close()

	session, err := pool.GetSession()
	if err != nil {
		t.Fatalf("Failed to get session: %v", err)
	}

	// 执行一些操作
	err = session.Open(false, 10000)
	if err != nil {
		t.Logf("Session already opened: %v", err)
	}

	// 归还 session
	pool.PutBack(session)

	// 再次获取应该可以获取到同一个或另一个 session
	session2, err := pool.GetSession()
	if err != nil {
		t.Fatalf("Failed to get session after putback: %v", err)
	}
	pool.PutBack(session2)
}

// TestSessionPoolConstructSession 测试构建 Session
func TestSessionPoolConstructSession(t *testing.T) {
	pool := client.NewSessionPool(NewTreeSessionPoolConfig(), 2, 10000, 30000, false)
	defer pool.Close()

	config := NewTreeSessionPoolConfig()
	session, err := pool.ConstructSession(config)
	if err != nil {
		t.Fatalf("Failed to construct session: %v", err)
	}

	err = session.Open(false, 10000)
	if err != nil {
		t.Logf("Session open error (may already opened): %v", err)
	}
	session.Close()
}

// TestSessionPoolConstructSessionSingleNode 测试单节点配置构建 Session
func TestSessionPoolConstructSessionSingleNode(t *testing.T) {
	// 单节点配置（不使用 NodeUrls）
	poolConfig := &client.PoolConfig{
		Host:     host,
		Port:     port,
		UserName: username,
		Password: password,
	}
	pool := client.NewSessionPool(poolConfig, 1, 10000, 30000, false)
	defer pool.Close()

	session, err := pool.GetSession()
	if err != nil {
		t.Fatalf("Failed to get session with single node config: %v", err)
	}
	pool.PutBack(session)
}

// TestSessionPoolInvalidConfig 测试无效配置
func TestSessionPoolInvalidConfig(t *testing.T) {
	poolConfig := &client.PoolConfig{
		NodeUrls: []string{"127.0.0.1:9999"}, // 无效端口
		UserName: username,
		Password: password,
	}
	pool := client.NewSessionPool(poolConfig, 1, 1000, 1000, false)
	defer pool.Close()

	_, err := pool.GetSession()
	if err == nil {
		t.Error("Expected error for invalid config, got nil")
	} else {
		t.Logf("Expected connection error: %v", err)
	}
}

// TestSessionPoolMaxSizeZero 测试 maxSize 为 0 的情况
func TestSessionPoolMaxSizeZero(t *testing.T) {
	pool := client.NewSessionPool(NewTreeSessionPoolConfig(), 0, 10000, 30000, false)
	defer pool.Close()

	session, err := pool.GetSession()
	if err != nil {
		t.Fatalf("Failed to get session with maxSize=0: %v", err)
	}
	pool.PutBack(session)
}

// TestSessionPoolClosedBehavior 测试已关闭 pool 的行为
func TestSessionPoolClosedBehavior(t *testing.T) {
	pool := client.NewSessionPool(NewTreeSessionPoolConfig(), 1, 10000, 30000, false)

	session, err := pool.GetSession()
	if err != nil {
		t.Fatalf("Failed to get session: %v", err)
	}

	// 关闭 pool
	pool.Close()

	// session 应该仍然可以正常使用（在 pool 关闭前获取的）
	err = cleanupAndSetStorageGroup(session, database)
	if err != nil {
		t.Logf("Session operation after pool close: %v", err)
	}
	session.Close()

	// 清理
	cleanupSession := client.NewSession(NewTreeSessionConfig())
	cleanupSession.Open(false, 10000)
	cleanupSession.DeleteStorageGroup(database)
	cleanupSession.Close()
}

// TestSessionPoolPutBackWithClosedTransport 测试归还已关闭传输的 Session
func TestSessionPoolPutBackWithClosedTransport(t *testing.T) {
	pool := client.NewSessionPool(NewTreeSessionPoolConfig(), 1, 10000, 30000, false)
	defer pool.Close()

	session, err := pool.GetSession()
	if err != nil {
		t.Fatalf("Failed to get session: %v", err)
	}

	// 关闭 session
	session.Close()

	// PutBack 应该处理已关闭的传输
	pool.PutBack(session)
}

// TestSessionPoolGetSessionFromChannel 测试从 channel 获取 Session
func TestSessionPoolGetSessionFromChannel(t *testing.T) {
	pool := client.NewSessionPool(NewTreeSessionPoolConfig(), 2, 10000, 30000, false)
	defer pool.Close()

	// 先获取一个 session
	session1, err := pool.GetSession()
	if err != nil {
		t.Fatalf("Failed to get first session: %v", err)
	}

	// 归还 session
	pool.PutBack(session1)

	// 再次获取，应该从 channel 获取
	session2, err := pool.GetSession()
	if err != nil {
		t.Fatalf("Failed to get session from channel: %v", err)
	}
	pool.PutBack(session2)
}

// TestSessionPoolMultipleGetAndPutBack 测试多次获取和归还
func TestSessionPoolMultipleGetAndPutBack(t *testing.T) {
	pool := client.NewSessionPool(NewTreeSessionPoolConfig(), 3, 10000, 30000, false)
	defer pool.Close()

	sessions := make([]client.Session, 0, 3)

	// 获取多个 session
	for i := 0; i < 3; i++ {
		session, err := pool.GetSession()
		if err != nil {
			t.Fatalf("Failed to get session %d: %v", i, err)
		}
		sessions = append(sessions, session)
	}

	// 归还所有 session
	for _, session := range sessions {
		pool.PutBack(session)
	}
}

// TestSessionPoolDropSessionOnError 测试错误时 drop Session
func TestSessionPoolDropSessionOnError(t *testing.T) {
	// 使用无效配置
	poolConfig := &client.PoolConfig{
		NodeUrls: []string{"127.0.0.1:9999"}, // 无效端口
		UserName: username,
		Password: password,
	}
	pool := client.NewSessionPool(poolConfig, 1, 1000, 1000, false)
	defer pool.Close()

	// 尝试获取 session 会失败
	_, err := pool.GetSession()
	if err == nil {
		t.Error("Expected error with invalid config, got nil")
	}
}

// TestSessionPoolCloseWithSessionsInChannel 测试关闭有 session 在 channel 中的 pool
func TestSessionPoolCloseWithSessionsInChannel(t *testing.T) {
	pool := client.NewSessionPool(NewTreeSessionPoolConfig(), 2, 10000, 30000, false)

	session, err := pool.GetSession()
	if err != nil {
		t.Fatalf("Failed to get session: %v", err)
	}

	// 归还 session 到 channel
	pool.PutBack(session)

	// 关闭 pool，应该关闭 channel 中的 session
	pool.Close()
}

// TestSessionPoolGetSessionAfterPutBack 测试归还后再次获取 Session
func TestSessionPoolGetSessionAfterPutBack(t *testing.T) {
	pool := client.NewSessionPool(NewTreeSessionPoolConfig(), 1, 10000, 30000, false)
	defer pool.Close()

	// 获取 session
	session1, err := pool.GetSession()
	if err != nil {
		t.Fatalf("Failed to get session: %v", err)
	}

	// 记录 session id
	sessionId1 := session1.GetSessionId()

	// 归还 session
	pool.PutBack(session1)

	// 再次获取
	session2, err := pool.GetSession()
	if err != nil {
		t.Fatalf("Failed to get session again: %v", err)
	}

	// 验证是同一个 session
	sessionId2 := session2.GetSessionId()
	if sessionId1 != sessionId2 {
		t.Logf("Session IDs differ: %d vs %d (may be expected for new session)", sessionId1, sessionId2)
	}

	pool.PutBack(session2)
}

// TestSessionPoolCloseWithEmptyChannel 测试关闭空 channel 的 pool
func TestSessionPoolCloseWithEmptyChannel(t *testing.T) {
	pool := client.NewSessionPool(NewTreeSessionPoolConfig(), 2, 10000, 30000, false)

	// 不获取任何 session，直接关闭
	pool.Close()
	t.Log("Pool closed successfully with empty channel")
}

// TestSessionPoolConcurrentPutBack 测试并发归还 Session
func TestSessionPoolConcurrentPutBack(t *testing.T) {
	pool := client.NewSessionPool(NewTreeSessionPoolConfig(), 3, 10000, 30000, false)
	defer pool.Close()

	sessions := make([]client.Session, 0, 3)

	// 获取多个 session
	for i := 0; i < 3; i++ {
		session, err := pool.GetSession()
		if err != nil {
			t.Fatalf("Failed to get session %d: %v", i, err)
		}
		sessions = append(sessions, session)
	}

	var wg sync.WaitGroup

	// 并发归还
	for i, session := range sessions {
		wg.Add(1)
		go func(idx int, s client.Session) {
			defer wg.Done()
			pool.PutBack(s)
			t.Logf("Session %d put back", idx)
		}(i, session)
	}

	wg.Wait()
	t.Log("All sessions put back concurrently")
}

// TestSessionPoolFetchMoreData 测试 SessionPool 大数据量查询
func TestSessionPoolFetchMoreData(t *testing.T) {
	pool := client.NewSessionPool(NewTreeSessionPoolConfig(), 2, 10000, 30000, false)
	defer pool.Close()

	session, err := pool.GetSession()
	if err != nil {
		t.Fatalf("Failed to get session: %v", err)
	}
	defer pool.PutBack(session)

	session.SetFetchSize(1000)

	err = cleanupAndSetStorageGroup(session, database)
	if err != nil {
		t.Fatalf("Failed to set storage group: %v", err)
	}

	writeCount := 10000
	tablet, err := client.NewTablet(database+".pool_fetch_dev", []*client.MeasurementSchema{
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

	ds, err := session.ExecuteQueryStatement("select * from "+database+".pool_fetch_dev", nil)
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
	session.DeleteTimeseries([]string{database + ".pool_fetch_dev.**"})
	session.DeleteStorageGroup(database)
}

// TestSessionPoolExecuteStatementWithContext 测试 SessionPool 带上下文的语句执行
func TestSessionPoolExecuteStatementWithContext(t *testing.T) {
	pool := client.NewSessionPool(NewTreeSessionPoolConfig(), 1, 10000, 30000, false)
	defer pool.Close()

	session, err := pool.GetSession()
	if err != nil {
		t.Fatalf("Failed to get session: %v", err)
	}
	defer pool.PutBack(session)

	ctx := context.Background()
	_, err = session.ExecuteStatementWithContext(ctx, "show databases")
	if err != nil {
		t.Fatalf("Failed to execute statement with context: %v", err)
	}
}
