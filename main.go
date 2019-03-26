package main

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"
	"unicode/utf8"
)

var cols []string
var tableName string
var insertStatement string
var driver string

func main() {
	args := os.Args[1:]
	if len(args) != 1 {
		log.Fatal("Usage: loadDB {optionsfile}\n\twhere optionsfile if a json file containing the optins to load the data. (see example)")
	}
	options := loadOptions(args[0])
	driver = options.Driver
	var valsTemp bytes.Buffer
	var insertTemp bytes.Buffer
	var createTemp bytes.Buffer
	dlm := ""
	insertTemp.WriteString("INSERT INTO ")
	insertTemp.WriteString(options.TableName)
	insertTemp.WriteString(" (\n")

	createTemp.WriteString("create table ")
	createTemp.WriteString(options.TableName)
	createTemp.WriteString(" (\n")

	for i, _ := range options.TableData {
		var t string
		if driver == "postgres" {
			t = fmt.Sprintf("$%d", i+1)
		} else {
			t = "?"
		}
		valsTemp.WriteString(dlm)
		valsTemp.WriteString(t)
		dlm = ","

		insertTemp.WriteString("\t")
		insertTemp.WriteString(options.TableData[i].ColumnName)
		if i < len(options.TableData)-1 {
			insertTemp.WriteString(",")
		} else {
			insertTemp.WriteString(") VALUES (")
			insertTemp.WriteString(valsTemp.String())
			insertTemp.WriteString(")")
		}
		insertTemp.WriteString("\n ")

		createTemp.WriteString("\t")
		createTemp.WriteString(options.TableData[i].ColumnName)

		cols = append(cols, options.TableData[i].ColumnName)
		createTemp.WriteString("\t")
		createTemp.WriteString(options.TableData[i].ColumnDef)
		if i < len(options.TableData)-1 {
			createTemp.WriteString(",")
		}
		createTemp.WriteString("\n")
	}
	insertStatement = insertTemp.String()
	fmt.Println(insertStatement)
	createTemp.WriteString(")\n")
	createStmt := createTemp.String()
	//cols=options.TableData
	tableName = options.TableName
	bulk := options.Bulk
	var ssl string
	if options.SSLDisable {
		ssl = "sslmode=disable"
	} else {
		ssl = " "
	}
	records, err := parseCSV(options.Filename, options.ColumnDelimeter, options.LineEnd)
	if err != nil {
		log.Fatal(err)
	}
	var connStr string
	if driver == "postgres" {
		connStr = fmt.Sprintf("user=%s password=%s dbname=%s %s host=%s port=%d", options.User, options.Password, options.DB, ssl, options.Host, options.Port)
		//fmt.Println(connStr)
	} else {
		if options.Driver == "mysql" {
			connStr = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", options.User, options.Password, options.Host, options.Port, options.DB)
			fmt.Println(connStr)
		}
		//os.Exit(8)
	}

	db, pgerr := sql.Open(options.Driver, connStr)
	if pgerr != nil {
		fmt.Println("1")
		log.Fatal(pgerr)
	}
	if options.DeleteTable {
		DoSQL(db, fmt.Sprintf("DROP TABLE IF EXISTS %s", options.TableName))
	}
	if options.CreateTable {
		cerr := DoSQL(db, createStmt)
		if cerr != nil {
			fmt.Println("2")
			log.Fatal(cerr)
		}
	} else {
		DoSQL(db, fmt.Sprintf("truncate %s RESTART IDENTITY", options.TableName))
	}

	if bulk {
		bulkInsertRecords(db, records)
	} else {
		insertRecords(db, records, insertStatement)
	}

}

func bulkInsertRecords(db *sql.DB, records [][]string) {
	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := txn.Prepare(pq.CopyIn(tableName, cols...))
	if err != nil {
		log.Fatal(err)
	}

	for _, s := range records {
		var anything []interface{}
		for _, val := range s {
			anything = append(anything, val)
		}
		_, err = stmt.Exec(anything...)
		if err != nil {
			log.Fatal(err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}
}

func insertRecords(db *sql.DB, records [][]string, insertStatement string) {
	for _, s := range records {
		var anything []interface{}
		for _, val := range s {
			anything = append(anything, val)
		}
		if driver == "postgres" {
			eerr := db.QueryRow(insertStatement, anything)
			if eerr != nil {
				fmt.Println("3")
				fmt.Println(eerr.Scan())
				log.Fatal(eerr)
			}
		} else {
			fmt.Println("Try mysql")
			_, merr := db.Exec(insertStatement, "a", "a", "a", "a", "a", "a", 1, "a", time.Now(), 3)
			if merr != nil {
				log.Fatal(merr.Error()) // proper error handling instead of panic in your app
			}
		}
	}

}

func DoSQL(db *sql.DB, s string) error {
	stmt, r := db.Prepare(s)
	if r == nil {
		stmt.Exec()
		return nil
	} else {
		fmt.Println(r.Error())
		return r
	}
}

func parseCSV(fn string, dlm string, eol string) ([][]string, error) {
	if eol == "" {
		eol = "\r"
	}
	if dlm == "" {
		dlm = ","
	}
	b, err := ioutil.ReadFile(fn) // just pass the file name
	if err != nil {
		return nil, err
	}
	if !utf8.Valid(b) {
		fmt.Println("There is invalid data")
		return nil, nil
	}
	r := csv.NewReader(strings.NewReader(string(b)))
	r.Comma = []rune(dlm)[0]
	records, err := r.ReadAll()
	return records, err
}
