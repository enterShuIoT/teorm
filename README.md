# teorm

`teorm` 是一个专为 **TDengine** 设计的轻量级 Go 语言 ORM 库，模仿了 GORM 的 API 风格。它旨在简化 TDengine 的超级表管理、自动子表创建和数据写入流程。

## 特性

*   **自动迁移 (AutoMigrate)**: 根据 Go 结构体自动创建超级表 (STable)。
*   **自动子表管理**: 写入数据时，根据 Tag 自动创建子表 (`USING ... TAGS ...`)。
*   **动态表名**: 支持根据每条数据的字段值动态计算目标子表名。
*   **高性能批量写入**: 自动将批量数据按目标子表分组，并行执行批量插入 SQL，大幅提升写入性能。
*   **链式查询**: 支持 `Find`, `Where`, `Order`, `Limit`, `Select` 等链式调用。
*   **Tag 映射**: 支持 `teorm:"primaryKey"`, `teorm:"tag"`, `teorm:"column:name"` 等标签。

## 安装

```bash
go get github.com/enterShuIoT/teorm
```

## 快速开始

### 1. 定义模型

```go
type Sensor struct {
    Ts          time.Time `teorm:"primaryKey;column:ts"` // 主键
    Temperature float64   `teorm:"column:current_temp"`  // 自定义列名
    Humidity    float64
    Location    string    `teorm:"tag"` // 标签列
    GroupId     int       `teorm:"tag"` // 标签列
}

// 定义超级表名称
func (Sensor) StableName() string {
    return "sensors_stable"
}

// 定义动态子表名规则 (可选)
func (s Sensor) TableName() string {
    return fmt.Sprintf("sensor_%s_%d", s.Location, s.GroupId)
}
```

### 2. 连接与使用

```go
package main

import (
    "time"
    "log"
    teorm "github.com/enterShuIoT/teorm"
)

func main() {
    // 连接 TDengine (RESTful 接口)
    dsn := "root:taosdata@http(127.0.0.1:6041)/test_db"
    db, err := teorm.Open(dsn)
    if err != nil {
        log.Fatal(err)
    }

    // 自动创建超级表
    db.AutoMigrate(&Sensor{})

    // 写入数据 (自动分发到不同子表)
    sensors := []Sensor{
        {Ts: time.Now(), Temperature: 25.5, Location: "room_a", GroupId: 1},
        {Ts: time.Now(), Temperature: 22.1, Location: "room_b", GroupId: 2},
    }
    db.Create(sensors)

    // 查询数据
    var results []Sensor
    db.Where("current_temp > ?", 20).Find(&results)
}
```

## 核心接口

### Tabler & Stabler

*   `StableName() string`: 返回超级表名称。如果未定义，默认使用结构体名的蛇形命名 (Snake Case)。
*   `TableName() string`: 返回子表名称。支持基于实例值的动态生成逻辑。

### 标签 (Tags)

*   `teorm:"primaryKey"`: 标记为主键 (TIMESTAMP)。
*   `teorm:"tag"`: 标记为 TAG 列。
*   `teorm:"column:name"`: 自定义数据库列名。
*   `teorm:"type:INT"`: 自定义数据类型 (可选，默认自动推断)。

## 注意事项

*   本库依赖 `github.com/taosdata/driver-go/v3`，默认使用 RESTful 接口 (6041 端口)。
*   批量写入时，会自动按 `TableName()` 返回值分组提交，确保高性能。

## License

MIT
