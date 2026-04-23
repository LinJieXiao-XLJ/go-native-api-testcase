# IoTDB Go 客户端测试用例

本项目用于测试 Apache IoTDB Go 客户端的测试程序

## 目录

- [环境](#环境)
- [安装](#安装)
  - [Windows](#windows)
  - [Linux (Ubuntu)](#linux-ubuntu)
- [使用](#使用)
  - [基础自动化测试](#基础自动化测试)
  - [代码覆盖率测试](#代码覆盖率测试)

---

## 项目结构

```
go-native-api-testcase/
├── conf/
│   └── config.properties      # 配置文件
├── test/
│   ├── table/
│   │   └── e2e_table_test.go  # Table API 测试用例
│   └── tree/
│       └── e2e_tree_test.go   # Tree API 测试用例
├── go.mod                     # Go 模块定义
├── go.sum                     # 依赖校验和
└── README.md                  # 项目文档
```

---

## 环境

### 必需环境

| 组件 | 版本要求 | 说明 |
|------|----------|------|
| Go | >= 1.21 | Go 语言运行环境 |
| IoTDB | >= 1.0.0 | Apache IoTDB 数据库服务 |

### 依赖库

| 依赖 | 版本 | 说明 |
|------|------|------|
| github.com/apache/iotdb-client-go/v2 | v2.0.4 | IoTDB Go 客户端库 |
| github.com/apache/thrift | v0.15.0 | Apache Thrift RPC 框架 |

### 配置

可在 `conf/config.properties` 中修改配置信息

---

## 安装

### Windows

#### 1. 安装 Go 环境

```powershell
# 使用官网安装包：访问 https://go.dev/dl/ 下载 Windows 安装包 (go1.21.x.windows-amd64.msi)

# 验证安装
go version
```

#### 2. 克隆项目

```powershell
# 克隆仓库
git clone https://github.com/LinJieXiao-XLJ/go-native-api-testcase.git
cd go-native-api-testcase
```

#### 3. 安装依赖

```powershell
# 下载依赖
go mod download

# 整理依赖
go mod tidy

# 验证依赖
go list -m all
```

### Linux (Ubuntu)

#### 1. 安装 Go 环境

```bash
# 方式一：使用 apt（版本可能较旧）
sudo apt update
sudo apt install -y golang-go

# 方式二：手动安装最新版本（推荐）
# 下载 Go 安装包
wget https://go.dev/dl/go1.21.6.linux-amd64.tar.gz

# 解压到 /usr/local
sudo tar -C /usr/local -xzf go1.21.6.linux-amd64.tar.gz

# 配置环境变量
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
echo 'export GOPATH=$HOME/go' >> ~/.bashrc
echo 'export PATH=$PATH:$GOPATH/bin' >> ~/.bashrc
source ~/.bashrc

# 验证安装
go version
```

#### 2. 克隆项目

```bash
# 克隆仓库
git clone https://github.com/LinJieXiao-XLJ/go-native-api-testcase.git
cd go-native-api-testcase
```

#### 3. 安装依赖

```bash
# 下载依赖
go mod download

# 整理依赖
go mod tidy

# 验证依赖
go list -m all
```

---

## 使用

### 配置修改

在运行测试前，请根据实际 IoTDB 环境修改配置文件 `conf/config.properties`：

### 基础自动化测试

#### 运行所有测试

```bash
# 运行所有测试用例（在根目录下执行）
go test -v ./test/...

# 运行指定模块的测试
go test -v ./test/table/
go test -v ./test/tree/
```

#### 运行单个测试

```bash
# 运行指定测试函数（go test 不能稳定地直接按单个 _test.go 文件执行；对应做法是按包运行，再用 -run 限定这个文件里的测）
go test -v ./test/table/ -run TestTableSessionBasic
```

### 代码覆盖率测试

#### 检查外部库中的

```bash
# 生成覆盖率数据
go test -cover ./test/... -coverpkg=github.com/apache/iotdb-client-go/v2/client -coverprofile=coverage.out
# 生成 HTML 报告（默认位于当前目录下）
go tool cover -html=coverage.out -o coverage.html
```

---

# 新增测试用例

## 基础用法

1. 测试文件命名规则：文件名必须是以`_test.go`为结尾，例如：table_session_test.go
2. 测试函数命名规则：单元测试必须以`Test为开头`，例如：TestTableSessionBasic

```go
package demo
import "testing"

// 单元测试
func TestXxx(t *testing.T) {
    // 测试逻辑
}

// 基准测试（性能）
func BenchmarkXxx(b *testing.B) {
}
```

## 进阶用法

1. 初始化 / 清理方式
   - 包级别初始化（整个包只执行一次）：`func TestMain(m *testing.M)`
    ```go
    package demo
   import (
    "os"
    "testing"
    )
    // 整个包只会执行一次
    func TestMain(m *testing.M) {
        // 测试前初始化

        // 运行所有测试
        code := m.Run()
    
        // 测试后清理

        // 退出
        os.Exit(code)
    }
    ```
   - 单个测试文件所有测试函数执行前执行：`func init()`
   ```go
   package demo
   func init() {
    // 初始化逻辑
    }
   ```
   - 单个文件里每个Test函数执行前自动初始化、执行后自动清理：`t.Cleanup`
   ```go
   package demo
   import "testing"
   func setup(t *testing.T) {
        // 初始化逻辑

        // 每个 Test 后：自动清理 
        t.Cleanup(func() {
            // 清理环境逻辑
        })
    }
   
    // ========== 测试函数 ==========
    func TestA(t *testing.T) {
        setup(t) // 自动初始化，在测试结束会自动执行清理逻辑
    }
   ```
