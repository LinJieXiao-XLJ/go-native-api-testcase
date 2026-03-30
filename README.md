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

### IoTDB 配置

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
# 运行指定测试函数
go test -v ./test/table/ -run TestTableSessionBasic
```

### 代码覆盖率测试

#### 生成 HTML 格式覆盖率报告

```bash
# 生成覆盖率数据
go test -cover ./test/... -coverprofile=coverage.out
# 生成 HTML 报告
go tool cover -html=coverage.out -o coverage.html
```

