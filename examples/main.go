package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	teorm "github.com/enterShuIoT/teorm"
	_ "github.com/taosdata/driver-go/v3/taosRestful"
)

// 1. 定义模型
type Sensor struct {
	Ts          time.Time `teorm:"primaryKey;column:ts"` // 时间戳主键
	Temperature float64   `teorm:"column:current_temp"`  // 普通列
	Humidity    float64
	Location    string `teorm:"tag"` // 标签列
	GroupId     int    `teorm:"tag"` // 标签列
}

// 2. 定义超级表名称 (可选，默认使用结构体名蛇形命名)
func (Sensor) StableName() string {
	return "sensors_stable"
}

// 3. 定义动态表名逻辑 (可选)
// teorm 会根据每条数据调用此方法来决定写入哪个子表
func (s Sensor) TableName() string {
	if s.Location == "" {
		return "sensor_default"
	}
	// 例如：sensor_room_a_1
	return fmt.Sprintf("sensor_%s_%d", s.Location, s.GroupId)
}

func main() {
	// 0. 准备数据库 (仅用于示例，实际环境应已存在 DB)
	ensureDBExists()

	// 1. 连接 TDengine
	// 格式: user:pass@http(host:port)/dbname
	dsn := "root:taosdata@http(127.0.0.1:6041)/test_db"
	db, err := teorm.Open(dsn)
	if err != nil {
		log.Fatal(err)
	}

	// 2. 自动迁移 (创建超级表)
	fmt.Println("Migrating...")
	err = db.AutoMigrate(&Sensor{})
	if err != nil {
		log.Fatal("Migrate failed:", err)
	}
	fmt.Println("AutoMigrate success")

	// 3. 写入数据 (支持自动创建子表)
	
	// 3.1 单条写入
	fmt.Println("Creating single data...")
	sensor1 := Sensor{
		Ts:          time.Now(),
		Temperature: 25.5,
		Humidity:    60.2,
		Location:    "room_a",
		GroupId:     1,
	}
	// 将自动写入表 'sensor_room_a_1' (由 TableName 方法决定)
	db.Create(&sensor1)
	if db.Error != nil {
		log.Println("Create failed:", db.Error)
	}

	// 3.2 批量写入 (自动分组插入到不同子表)
	fmt.Println("Batch creating...")
	sensors := []Sensor{
		{
			Ts:          time.Now(),
			Temperature: 20.0,
			Location:    "room_a", // -> sensor_room_a_1
			GroupId:     1,
		},
		{
			Ts:          time.Now(),
			Temperature: 22.0,
			Location:    "room_b", // -> sensor_room_b_2
			GroupId:     2,
		},
	}
	db.Create(sensors)
	if db.Error != nil {
		log.Println("Batch create failed:", db.Error)
	}

	// 4. 查询数据
	fmt.Println("Querying...")
	var results []Sensor
	// 默认查询超级表 (获取所有子表数据)
	db.Where("current_temp > ?", 20).Order("ts DESC").Limit(10).Find(&results)
	
	fmt.Printf("Found %d sensors\n", len(results))
	for _, s := range results {
		fmt.Printf("  %v: %s-%d Temp:%.1f\n", s.Ts, s.Location, s.GroupId, s.Temperature)
	}
}

func ensureDBExists() {
	// 使用 sql 驱动直接连接，确保数据库存在
	db, err := sql.Open("taosRestful", "root:taosdata@http(127.0.0.1:6041)/")
	if err != nil {
		log.Println("Warning: failed to connect to root:", err)
		return
	}
	defer db.Close()
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS test_db")
	if err != nil {
		log.Println("Warning: failed to create DB:", err)
	}
}
