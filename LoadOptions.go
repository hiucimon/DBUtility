package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

type Options struct {
	User string `json:User`
	Password string `json:Password`
	DB string `json:DB`
	SSLDisable bool `json:SSLDisable`
	Host string `json:Host`
	Port int `json:Port`
	Driver string `json:Driver`
	Filename string `json:Filename`
	ColumnDelimeter string `json:ColumnDelimeter`
	LineEnd string `json:LineEnd`
	Bulk bool `json:Bulk`
	TableName string `json:TableName`
	DeleteTable bool `json:DeleteTable`
	CreateTable bool `json:CreateTable`
	TableData []Column `json:Column`
}

type Column struct {
	ColumnName string `json:ColumnName`
	ColumnDef string `json:ColumnDef`
}

func loadOptions(fn string) Options{
	jsonFile, err := os.Open(fn)
	// if we os.Open returns an error then handle it
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Successfully Opened",fn)
	// defer the closing of our jsonFile so that we can parse it later on
	defer jsonFile.Close()
	byteValue, _ := ioutil.ReadAll(jsonFile)
	var options Options
	uerr:=json.Unmarshal(byteValue, &options)
	if uerr!=nil {
		log.Fatal("Fatal error parsing options:",uerr)
	}
	return options
	//fmt.Println(uerr,options)
}
