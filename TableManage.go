package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
)

func Recreate() {
	dropStatement:=`DROP TABLE IF EXISTS public.aa_user_entitlement`
	createStatement:=`create table  public.aa_user_entitlement
      (
         eid varchar(128),
         application_name varchar(128),
         entitlement_name varchar(1000),
         entitlement_display_value varchar(450),
         entitlement_formal_value varchar(1000),
         login_id varchar(255),
         granted_by_role numeric(18),
         entitlement_type varchar(255),
         etl_create_date date,
         edw_publn_id numeric(18)
      )
   `
	connStr:="user=joe password=password dbname=test sslmode=disable host=localhost port=5432"
	db,pgerr:=sql.Open("postgres",connStr)
	if pgerr!=nil {
		log.Fatal(pgerr)
	}
	DoSQL2(db,dropStatement)
	DoSQL2(db,createStatement)
	fmt.Println("DB Opened OK:",db)
}

//func CreateTable(tab string, cols []string,types []string) {
//	c:=strings.Join(cols,",")
//	createStatement:=fmt.Sprintf("create table %s",tab,cols)` public.aa_user_entitlement
//      (
//         eid ,
//         application_name
//         entitlement_name
//         entitlement_display_value
//         entitlement_formal_value
//         login_id
//         granted_by_role
//         entitlement_type
//         etl_create_date
//         edw_publn_id
//      )varchar(128),varchar(128),varchar(1000),varchar(450),varchar(1000),varchar(255),numeric(18),varchar(255),date,numeric(18)
//   `
//
//}