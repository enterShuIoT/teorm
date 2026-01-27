package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	teorm "github.com/enterShuIoT/teorm"
	_ "github.com/taosdata/driver-go/v3/taosRestful"
)

type SensorPtr struct {
	Ts          time.Time `teorm:"primaryKey;column:ts"`
	Temperature *float64  `teorm:"column:current_temp"` // Pointer
	Humidity    *float64  `teorm:"column:humidity"`     // Pointer
	Location    string    `teorm:"tag"`
	GroupId     int       `teorm:"tag"`
}

func (SensorPtr) StableName() string {
	return "sensors_ptr_stable"
}

func (s SensorPtr) TableName() string {
	return "sensor_ptr_default"
}

func main() {
	db := initDB()

	fmt.Println("Migrating...")
	err := db.AutoMigrate(&SensorPtr{})
	if err != nil {
		log.Fatal("Migrate failed:", err)
	}

	// 1. Insert Only Temperature
	fmt.Println("Inserting temperature only...")
	temp := 25.5
	s1 := SensorPtr{
		Ts:          time.Now(),
		Temperature: &temp,
		Humidity:    nil, // Should skip this column in INSERT
		Location:    "room_ptr",
		GroupId:     1,
	}
	db.Create(&s1)
	if db.Error != nil {
		log.Fatal("Create s1 failed:", db.Error)
	}
	
	// 2. Insert Only Humidity (Same subtable, different time)
	fmt.Println("Inserting humidity only...")
	time.Sleep(time.Second)
	hum := 60.2
	s2 := SensorPtr{
		Ts:          time.Now(),
		Temperature: nil, // Should skip this column
		Humidity:    &hum,
		Location:    "room_ptr",
		GroupId:     1,
	}
	db.Create(&s2)
	if db.Error != nil {
		log.Fatal("Create s2 failed:", db.Error)
	}
	
	// 3. Batch Insert with mixed nil fields
	fmt.Println("Batch inserting mixed fields...")
	time.Sleep(time.Second)
	
	t3 := 20.0
	h4 := 50.0
	
	batch := []SensorPtr{
		{
			Ts:          time.Now(),
			Temperature: &t3,
			Humidity:    nil, // Only Temp
			Location:    "room_ptr",
			GroupId:     1,
		},
		{
			Ts:          time.Now().Add(time.Second),
			Temperature: nil, // Only Humidity
			Humidity:    &h4,
			Location:    "room_ptr",
			GroupId:     1,
		},
	}
	
	// This should internally split into two INSERT statements because column signatures are different
	db.Create(batch)
	if db.Error != nil {
		log.Fatal("Batch create failed:", db.Error)
	}

	// Verify
	var results []SensorPtr
	db.Find(&results)
	fmt.Printf("Found %d records\n", len(results))
	for _, s := range results {
		tStr := "nil"
		if s.Temperature != nil {
			tStr = fmt.Sprintf("%.1f", *s.Temperature)
		}
		hStr := "nil"
		if s.Humidity != nil {
			hStr = fmt.Sprintf("%.1f", *s.Humidity)
		}
		fmt.Printf("  %v: T=%s H=%s\n", s.Ts, tStr, hStr)
	}
}

func initDB() *teorm.DB {
	// Clean setup
	sqlDB, err := sql.Open("taosRestful", "root:taosdata@http(127.0.0.1:6041)/")
	if err != nil {
		log.Fatal(err)
	}
	defer sqlDB.Close()
	sqlDB.Exec("CREATE DATABASE IF NOT EXISTS ptr_test")
	
	db, err := teorm.Open("root:taosdata@http(127.0.0.1:6041)/ptr_test")
	if err != nil {
		log.Fatal(err)
	}
	return db
}
