package tree

/*
* 树模型测试公共配置和辅助函数
 */

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"

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
	database = props["tree_database"]
}

// NewTreeSessionConfig 创建 Session 配置
func NewTreeSessionConfig() *client.Config {
	return &client.Config{
		Host:      host,
		Port:      port,
		UserName:  username,
		Password:  password,
		FetchSize: 1024,
		TimeZone:  "Asia/Shanghai",
	}
}

// NewTreeSessionPoolConfig 创建 SessionPool 配置
func NewTreeSessionPoolConfig() *client.PoolConfig {
	return &client.PoolConfig{
		NodeUrls:  nodeUrls,
		UserName:  username,
		Password:  password,
		FetchSize: 1024,
		TimeZone:  "Asia/Shanghai",
	}
}

// cleanupAndSetStorageGroup 清理并设置存储组
func cleanupAndSetStorageGroup(session client.Session, sg string) error {
	// 先尝试删除已有的存储组
	session.DeleteStorageGroup(sg)
	// 设置新的存储组
	return session.SetStorageGroup(sg)
}

// createTreeTablet 辅助函数：创建树模型 Tablet
func createTreeTablet(rowCount int) (*client.Tablet, error) {
	tablet, err := client.NewTablet(database+".tablet_dev", []*client.MeasurementSchema{
		{Measurement: "restart_count", DataType: client.INT32},
		{Measurement: "price", DataType: client.DOUBLE},
		{Measurement: "tick_count", DataType: client.INT64},
		{Measurement: "temperature", DataType: client.FLOAT},
		{Measurement: "description", DataType: client.TEXT},
		{Measurement: "status", DataType: client.BOOLEAN},
	}, rowCount)

	if err != nil {
		return nil, err
	}
	ts := rand.Int63() % 1000000000
	for row := 0; row < rowCount; row++ {
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
	return tablet, nil
}
