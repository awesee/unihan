package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const dbDSN = "root@tcp(127.0.0.1:3306)/unicode"

var (
	codes   = make([]string, 0)
	columns = make(map[string]int)
	data    = make(map[string]map[string]string)
)

var db = func() *sql.DB {
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		log.Fatalln(err)
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(10 * time.Minute)
	db.SetConnMaxIdleTime(30 * time.Second)
	if err := db.Ping(); err != nil {
		log.Fatalln(err)
	}
	return db
}()

func main() {
	createTable()
	names, err := filepath.Glob("*/Unihan_*.txt")
	check(err)
	for _, name := range names {
		f, err := os.Open(name)
		check(err)
		r := bufio.NewScanner(f)
		for r.Scan() {
			col := strings.Split(r.Text(), "\t")
			if len(col) < 2 || !strings.HasPrefix(col[0], "U+") {
				continue
			}
			code := strings.TrimSpace(col[0])
			key := strings.TrimSpace(col[1])
			value := strings.TrimSpace(col[2])
			if columns[key] <= len(value) {
				columns[key] = len(value)
			}
			if data[code] == nil {
				codes = append(codes, code)
				data[code] = map[string]string{key: value}
			} else {
				data[code][key] = value
			}
		}
	}
	for _, code := range codes {
		for key, value := range data[code] {
			fmt.Printf("%s\t%s\t%s\n", code, key, value)
			updateValue(code, key, value)
		}
	}
}

func check(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}

func createDatabase() {
	query := "CREATE DATABASE IF NOT EXISTS `unicode` CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_unicode_ci'"
	if _, err := db.Exec(query); err != nil {
		log.Println(err)
	}
}

func createTable() {
	queries := []string{
		"DROP TABLE IF EXISTS `unihan`",
		"CREATE TABLE `unihan` (\n  `id` int unsigned NOT NULL AUTO_INCREMENT,\n  `code` varchar(7) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,\n  `char` varchar(7) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',\n  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,\n  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `idx_code` (`code`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
	}
	for i, query := range queries {
		if _, err := db.Exec(query); err != nil {
			log.Println(i, err)
		}
	}
}

func addColumn(name string) {
	l := columns[name]
	if l < 0 {
		return
	}
	l += 7 - l%8
	columns[name] = -1
	query := fmt.Sprintf("ALTER TABLE `unicode`.`unihan` \nADD COLUMN `%s` varchar(%d) NOT NULL DEFAULT '' AFTER `char`", name, l)
	if _, err := db.Exec(query); err != nil {
		log.Println(err)
	}
}

func updateValue(code, key, value string) {
	code = strings.TrimSpace(code)
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	addColumn(key)
	query := fmt.Sprintf("UPDATE `unihan` SET `%s` = ? WHERE `code` = ?", key)
	result, err := db.Exec(query, value, code)
	check(err)
	affected, err := result.RowsAffected()
	check(err)
	if affected > 0 {
		return
	}
	query = fmt.Sprintf("INSERT INTO `unihan` (`code`, `char`, `%s`) VALUES (?, ?, ?)", key)
	i, err := strconv.ParseInt(strings.TrimPrefix(code, "U+"), 16, 64)
	check(err)
	_, err = db.Exec(query, code, fmt.Sprintf("%c", i), value)
	check(err)
}
