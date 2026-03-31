package tree

/*
* 测试树模型其他操作
 */

import (
	"testing"

	"github.com/apache/iotdb-client-go/v2/client"
)

// TestSessionTimeZone 测试时区操作
func TestSessionTimeZone(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	// 获取时区
	timezone, err := session.GetTimeZone()
	if err != nil {
		t.Fatalf("Failed to get timezone: %v", err)
	}
	t.Logf("Current timezone: %s", timezone)

	// 设置时区
	err = session.SetTimeZone("UTC")
	if err != nil {
		t.Fatalf("Failed to set timezone: %v", err)
	}

	// 再次获取时区验证
	timezone, err = session.GetTimeZone()
	if err != nil {
		t.Fatalf("Failed to get timezone after setting: %v", err)
	}
	if timezone != "UTC" {
		t.Errorf("Expected timezone UTC, got %s", timezone)
	}
}

// TestSessionGetSessionId 测试获取 Session ID
func TestSessionGetSessionId(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close()

	sessionId := session.GetSessionId()
	if sessionId <= 0 {
		t.Errorf("Expected positive session ID, got %d", sessionId)
	}
	t.Logf("Session ID: %d", sessionId)
}

// TestSessionOpenWithDefaultConfig 测试使用默认配置打开 Session
func TestSessionOpenWithDefaultConfig(t *testing.T) {
	config := &client.Config{
		Host:     host,
		Port:     port,
		UserName: username,
		Password: password,
		// FetchSize, TimeZone, ConnectRetryMax 使用默认值
	}
	session := client.NewSession(config)
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session with default config: %v", err)
	}
	defer session.Close()

	t.Logf("Session opened successfully with default config")
}

// TestSessionOpenWithCompression 测试启用压缩打开 Session
func TestSessionOpenWithCompression(t *testing.T) {
	session := client.NewSession(NewTreeSessionConfig())
	err := session.Open(true, 10000) // 启用压缩
	if err != nil {
		t.Logf("Failed to open session with compression: %v (server may not support compression)", err)
		return
	}
	defer session.Close()
	t.Logf("Session opened with compression enabled")
}

// TestSessionVersionConfig 测试版本配置
func TestSessionVersionConfig(t *testing.T) {
	config := &client.Config{
		Host:     host,
		Port:     port,
		UserName: username,
		Password: password,
		Version:  client.V_1_0,
	}
	session := client.NewSession(config)
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session with version config: %v", err)
	}
	defer session.Close()
	t.Logf("Session opened with version V_1_0")
}

// TestSessionDatabaseConfig 测试数据库配置
func TestSessionDatabaseConfig(t *testing.T) {
	config := &client.Config{
		Host:     host,
		Port:     port,
		UserName: username,
		Password: password,
		Database: database,
	}
	session := client.NewSession(config)
	err := session.Open(false, 10000)
	if err != nil {
		t.Fatalf("Failed to open session with database config: %v", err)
	}
	defer session.Close()
	t.Logf("Session opened with database: %s", database)
}

// TestClusterSessionDatabaseConfig 测试集群 Session 数据库配置
func TestClusterSessionDatabaseConfig(t *testing.T) {
	clusterConfig := &client.ClusterConfig{
		NodeUrls: nodeUrls,
		UserName: username,
		Password: password,
		Database: database,
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

	t.Logf("Cluster session opened with database: %s", database)
}
