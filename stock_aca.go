/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package main

import (
        "errors"
        "fmt"
        "time"
        "encoding/json"
        "strconv"
//      "github.com/op/go-logging"
        "github.com/hyperledger/fabric/core/chaincode/shim"
//      "github.com/hyperledger/fabric/core/chaincode/shim/crypto/attr"
)

// var myLogger = logging.MustGetLogger("trading")

// TradeChaincode example simple Asset Management Chaincode implementation
// with access control enforcement at chaincode level.
// Look here for more information on how to implement access control at chaincode level:
// https://github.com/openblockchain/obc-docs/blob/master/tech/application-ACL.md
// An product_code is represented by a string
type TradeChaincode struct {
}

// 在庫マスタ、取引テーブルを作成する
//
func (t *TradeChaincode) Init(stub *shim.ChaincodeStub, function string, args []string) ([]byte, error) {
//      myLogger.Info("[TradeChaincode] Init")

        var assigner string

        if len(args) != 0 {
                return nil, errors.New("Incorrect number of arguments. Expecting 0")
        }
        
        callerRole, err := stub.ReadCertAttribute("role")
        if err != nil {
                fmt.Printf("Error reading attribute [%v] \n", err)
                return nil, fmt.Errorf("Failed fetching caller role. Error was [%v]", err)
        }
        
        caller := string(callerRole[:])
        assigner = "admin"

        if caller != assigner {
                fmt.Printf("Caller is not assigner - caller %v assigner %v\n", caller, assigner)
                return nil, fmt.Errorf("The caller does not have the rights to invoke assign. Expected role [%v], caller role [%v]", assigner, caller)
        }

        // Create 在庫マスタを作成する
        // テーブル名：stock
        err_stock := stub.CreateTable("stock", []*shim.ColumnDefinition{
                &shim.ColumnDefinition{"key_system_code", shim.ColumnDefinition_STRING, true},
                &shim.ColumnDefinition{"key_product_code", shim.ColumnDefinition_STRING, true},
                &shim.ColumnDefinition{"value_current_stock", shim.ColumnDefinition_INT64, false},
                &shim.ColumnDefinition{"value_allocate_stock", shim.ColumnDefinition_INT64, false},
                &shim.ColumnDefinition{"value_backorder_stock", shim.ColumnDefinition_INT64, false},
        })
        if err_stock != nil {
                return nil, errors.New("Failed creating stock table.")
        }

        // Create 取引テーブルを作成する
        // テーブル名：order
        err_order := stub.CreateTable("order", []*shim.ColumnDefinition{
                &shim.ColumnDefinition{"key_system_code", shim.ColumnDefinition_STRING, true},
                &shim.ColumnDefinition{"key_UUID", shim.ColumnDefinition_STRING, true},
                &shim.ColumnDefinition{"key_orderer_code", shim.ColumnDefinition_STRING, true},
                &shim.ColumnDefinition{"key_accepter_code", shim.ColumnDefinition_STRING, true},
                &shim.ColumnDefinition{"key_product_code", shim.ColumnDefinition_STRING, true},
                &shim.ColumnDefinition{"value_order_amount", shim.ColumnDefinition_INT64, false},
                &shim.ColumnDefinition{"value_order_status", shim.ColumnDefinition_STRING, false},
                &shim.ColumnDefinition{"value_last_updated_by", shim.ColumnDefinition_STRING, false},
                &shim.ColumnDefinition{"value_last_updated_datetime", shim.ColumnDefinition_STRING, false},
        })


        if err_order != nil {
                return nil, errors.New("Failed creating order table.")
        }

//      myLogger.Info("テーブル登録完了")

        // Set the role of the users that are allowed to assign assets
        // The metadata will contain the role of the users that are allowed to assign assets
        assignerRole, err := stub.GetCallerMetadata()
        fmt.Printf("Assiger role is %v\n", string(assignerRole))

        if err != nil {
                return nil, fmt.Errorf("Failed getting metadata, [%v]", err)
        }

        if len(assignerRole) == 0 {
                return nil, errors.New("Invalid assigner role. Empty.")
        }

        stub.PutState("assignerRole", assignerRole)

        return nil, nil

}

//  在庫マスタにデータを登録する
//   引数：商品コード,在庫数

func (t *TradeChaincode) register(stub *shim.ChaincodeStub, args []string) ([]byte, error) {
        if len(args) != 4 {
                return nil, errors.New("Incorrect number of arguments. Expecting 4")
        }

        var value_current_stock, value_allocate_stock, value_backorder_stock int64
        var key_product_code string
        key_product_code    = args[0]
        value_current_stock, err1   := strconv.ParseInt(args[1], 10, 64)
        value_allocate_stock, err2  := strconv.ParseInt(args[2], 10, 64)
        value_backorder_stock, err3 := strconv.ParseInt(args[3], 10, 64)
        if err1 != nil {
                return nil, errors.New("Expecting integer value for asset holding")
        } else if err2 != nil {
                return nil, errors.New("Expecting integer value for asset holding")
        } else if err3 != nil {
                return nil, errors.New("Expecting integer value for asset holding")
        }
        key_system_code := "stock_management"

        var assigner string
        
        callerRole, err := stub.ReadCertAttribute("role")
        if err != nil {
                fmt.Printf("Error reading attribute [%v] \n", err)
                return nil, fmt.Errorf("Failed fetching caller role. Error was [%v]", err)
        }
        
        caller := string(callerRole[:])
        assigner = "admin"

        if caller != assigner {
                fmt.Printf("Caller is not assigner - caller %v assigner %v\n", caller, assigner)
                return nil, fmt.Errorf("The caller does not have the rights to invoke assign. Expected role [%v], caller role [%v]", assigner, caller)
        }

        // 在庫マスタを登録する
        ok, err := stub.InsertRow("stock", shim.Row{
                Columns: []*shim.Column{
                        &shim.Column{Value: &shim.Column_String_{String_: key_system_code}},
                        &shim.Column{Value: &shim.Column_String_{String_: key_product_code}},
                        &shim.Column{Value: &shim.Column_Int64{Int64: value_current_stock}},
                        &shim.Column{Value: &shim.Column_Int64{Int64: value_allocate_stock}},
                        &shim.Column{Value: &shim.Column_Int64{Int64: value_backorder_stock}},
                },
        })

        if err != nil {
                return nil, errors.New("Failed inserting row.")
        }

        if !ok {
                return nil, errors.New("insertRow operation failed. Row with given key already exists")
        }

        return nil, nil
}


//  在庫マスタのデータを更新する
//   引数：商品コード,在庫追加数

func (t *TradeChaincode) update(stub *shim.ChaincodeStub, args []string) ([]byte, error) {
        if len(args) != 2 {
                return nil, errors.New("Incorrect number of arguments. Expecting 2")
        }

        var stock_add, current_stock, allocate_stock, backorder_stock int64
        var product_code string
        product_code    = args[0]
        stock_add, err := strconv.ParseInt(args[1], 10, 64)
        if err != nil {
                return nil, errors.New("Expecting integer value for asset holding")
        }
        
        callerRole, err := stub.ReadCertAttribute("role")
        if err != nil {
                fmt.Printf("Error reading attribute [%v] \n", err)
                return nil, fmt.Errorf("Failed fetching caller role. Error was [%v]", err)
        }
        
        caller := string(callerRole[:])
        assigner = "admin"

        if caller != assigner {
                fmt.Printf("Caller is not assigner - caller %v assigner %v\n", caller, assigner)
                return nil, fmt.Errorf("The caller does not have the rights to invoke assign. Expected role [%v], caller role [%v]", assigner, caller)
        }
        
        key_system_code := "stock_management"

        var stock_columns []shim.Column
        s_col1 := shim.Column{Value: &shim.Column_String_{String_: key_system_code}}
        s_col2 := shim.Column{Value: &shim.Column_String_{String_: product_code}}
        stock_columns = append(stock_columns, s_col1)
        stock_columns = append(stock_columns, s_col2)

//      myLogger.Debug("s_col1の内容を確認する。 [%s] ", s_col1)
//      myLogger.Debug("s_col2の内容を確認する。 [%s] ", s_col2)

        row, err := stub.GetRow("stock", stock_columns)

        if err != nil {
                jsonResp := "{\"Error\":\"Failed retrieveing product_code " + product_code + ". Error " + err.Error() + ". \"}"
                return nil, errors.New(jsonResp)
        }

//      current_stock, err2 := strconv.ParseInt(string(row.Columns[1].GetBytes()), 10, 64)
//      if err2 != nil {
//              return nil, errors.New("data not found")
//      }
        current_stock   = row.Columns[2].GetInt64()
        allocate_stock  = row.Columns[3].GetInt64()
        backorder_stock = row.Columns[4].GetInt64()


        if current_stock == 0 {
                return nil, fmt.Errorf("Invalid 在庫数. 0")
        }

        // 現在庫に引数．在庫追加数を加算
        current_stock = current_stock + stock_add

        var columns []*shim.Column
        col1 := shim.Column{Value: &shim.Column_String_{String_: key_system_code}}
        col2 := shim.Column{Value: &shim.Column_String_{String_: product_code}}
        col3 := shim.Column{Value: &shim.Column_Int64{Int64: current_stock}}
        col4 := shim.Column{Value: &shim.Column_Int64{Int64: allocate_stock}}
        col5 := shim.Column{Value: &shim.Column_Int64{Int64: backorder_stock}}
//      col2 := shim.Column{Value: &shim.Column_Int64{Int64: []byte(strconv.Itoa(current_stock))}}

        columns = append(columns, &col1)
        columns = append(columns, &col2)
        columns = append(columns, &col3)
        columns = append(columns, &col4)
        columns = append(columns, &col5)
        new_row := shim.Row{Columns: columns}

        ok, err := stub.ReplaceRow("stock", new_row)

        if err != nil {
                return nil, errors.New("Failed inserting row.")
        }

        if !ok {
                return nil, errors.New("ReplaceRow operation failed. Row with given key does not exist")
        }

        return nil, nil
}

// Query callback representing the query of a chaincode
func (t *TradeChaincode) Query(stub *shim.ChaincodeStub, function string, args []string) ([]byte, error) {
        switch function {

                case "stock_search":

                        var err error

                        if len(args) != 1 {
                                return nil, errors.New("Incorrect number of arguments. Expecting name of an search key to query")
                        }

                        search_key := args[0]

                        callerRole, err := stub.ReadCertAttribute("role")
                        if err != nil {
                                fmt.Printf("Error reading attribute [%v] \n", err)
                                return nil, fmt.Errorf("Failed fetching caller role. Error was [%v]", err)
                        }

                        caller := string(callerRole[:])
                        if caller == "admin" || caller == "trading_company" || caller == "warehouse" {

                                var columns []shim.Column
                                col1 := shim.Column{Value: &shim.Column_String_{String_: search_key}}
                                columns = append(columns, col1)

                                rowChannel, err := stub.GetRows("stock", columns)
                                if err != nil {
                                        jsonResp := "{\"Error\":\"Failed retrieveing search_key " + search_key + ". Error " + err.Error() + ". \"}"
                                        return nil, errors.New(jsonResp)
                                }

                                // 構造体の定義
                                type res_stock_search struct {
                                        Key_product_code string // 商品コード
                                        Value_current_stock int64 // 現在庫数
                                        Value_allocate_stock int64 // 引当数
                                        Value_backorder_stock int64 // 発注残数
                                }

                                var rows []shim.Row
                                var myResponces []res_stock_search

                                for {
                                        select {
                                        case row, ok := <-rowChannel:
                                                if !ok {
                                                        rowChannel = nil
                                                } else {
                                                        // 文字列型の場合.GetString_()を使う
                                                        var myRes res_stock_search
                                                        myRes.Key_product_code      = row.Columns[1].GetString_()
                                                        myRes.Value_current_stock   = row.Columns[2].GetInt64()
                                                        myRes.Value_allocate_stock  = row.Columns[3].GetInt64()
                                                        myRes.Value_backorder_stock = row.Columns[4].GetInt64()

                                                        myResponces = append(myResponces, myRes)

                                                        rows = append(rows, row)
                                                }
                                        }
                                        if rowChannel == nil {
                                                break
                                        }
                                }

                                //JSON型にする。
                                jsonRows, err := json.Marshal(myResponces)
                                if err != nil {
                                        return nil, fmt.Errorf("stock_search operation failed. Error marshaling JSON: %s", err)
                                }

                                return jsonRows, nil
                               
                        } else {
                                fmt.Printf("Caller is not assigner - caller %v assigner %v\n", caller, assigner)
                                return nil, fmt.Errorf("The caller does not have the rights to invoke assign. Expected role [%v], caller role [%v]", assigner, caller)
                        }

                case "order_search":

                        var err error

                        if len(args) != 1 {
                                return nil, errors.New("Incorrect number of arguments. Expecting name of an search key to query")
                        }

                        search_key := args[0]

                        callerRole, err := stub.ReadCertAttribute("role")
                        if err != nil {
                                fmt.Printf("Error reading attribute [%v] \n", err)
                                return nil, fmt.Errorf("Failed fetching caller role. Error was [%v]", err)
                        }

                        caller := string(callerRole[:])
                        if caller == "admin" || caller == "trading_company" || caller == "orderer" {

                                var columns []shim.Column
                                col1 := shim.Column{Value: &shim.Column_String_{String_: search_key}}
                                columns = append(columns, col1)

                                rowChannel, err := stub.GetRows("order", columns)
                                if err != nil {
                                        jsonResp := "{\"Error\":\"Failed retrieveing search_key " + search_key + ". Error " + err.Error() + ". \"}"
                                        return nil, errors.New(jsonResp)
                                }

                                // 構造体の定義
                                type res_order_search struct {
                                        UUID string // 番号
                                        Key_orderer_code string // 注文者
                                        Key_accepter_code string // 受注者
                                        Key_product_code string // 商品コード
                                        Value_order_amount int64 // 取引数
                                        Value_order_status string // 取引状態
                                        Value_last_updated_by string // 最終更新者
                                        Value_last_updated_datetime string // 最終更新日時
                                }

                                var myResponces []res_order_search

                                for {
                                        select {
                                        case row, ok := <-rowChannel:
                                                if !ok {
                                                        rowChannel = nil
                                                } else {
                                                        // 文字列型の場合.GetString_()を使う
                                                        var myRes res_order_search
                                                        myRes.UUID    = row.Columns[1].GetString_()
                                                        myRes.Key_orderer_code  = row.Columns[2].GetString_()
                                                        myRes.Key_accepter_code = row.Columns[3].GetString_()
                                                        myRes.Key_product_code = row.Columns[4].GetString_()
                                                        myRes.Value_order_amount = row.Columns[5].GetInt64()
                                                        myRes.Value_order_status = row.Columns[6].GetString_()
                                                        myRes.Value_last_updated_by = row.Columns[7].GetString_()
                                                        myRes.Value_last_updated_datetime = row.Columns[8].GetString_()

                                                        myResponces = append(myResponces, myRes)

                                                }
                                        }
                                        if rowChannel == nil {
                                                break
                                        }
                                }

                                //JSON型にする。
                                jsonRows, err := json.Marshal(myResponces)
                                if err != nil {
                                        return nil, fmt.Errorf("order_search operation failed. Error marshaling JSON: %s", err)
                                }

                                return jsonRows, nil

                        } else {
                                fmt.Printf("Caller is not assigner - caller %v assigner %v\n", caller, assigner)
                                return nil, fmt.Errorf("The caller does not have the rights to invoke assign. Expected role [%v], caller role [%v]", assigner, caller)
                        }

                case "order_row_search":

                        var err error

                        if len(args) != 2 {
                                return nil, errors.New("Incorrect number of arguments. Expecting name of an search key to query")
                        }

                        search_key := args[0]
                        search_UUID := args[1]

                        var columns []shim.Column
                        col1 := shim.Column{Value: &shim.Column_String_{String_: search_key}}
                        col2 := shim.Column{Value: &shim.Column_String_{String_: search_UUID}}
                        columns = append(columns, col1)
                        columns = append(columns, col2)

                        rowChannel, err := stub.GetRows("order", columns)
                        if err != nil {
                                jsonResp := "{\"Error\":\"Failed retrieveing search_key " + search_key + ". Error " + err.Error() + ". \"}"
                                return nil, errors.New(jsonResp)
                        }

                        // 構造体の定義
                        type res_order_search struct {
                                UUID string // 番号
                                Key_orderer_code string // 注文者
                                Key_accepter_code string // 受注者
                                Key_product_code string // 商品コード
                                Value_order_amount int64 // 取引数
                                Value_order_status string // 取引状態
                                Value_last_updated_by string // 最終更新者
                                Value_last_updated_datetime string // 最終更新日時
                        }

                        var myResponces []res_order_search

                        for {
                                select {
                                case row, ok := <-rowChannel:
                                        if !ok {
                                                rowChannel = nil
                                        } else {
                                                // 文字列型の場合.GetString_()を使う
                                                var myRes res_order_search
                                                myRes.UUID    = row.Columns[1].GetString_()
                                                myRes.Key_orderer_code  = row.Columns[2].GetString_()
                                                myRes.Key_accepter_code = row.Columns[3].GetString_()
                                                myRes.Key_product_code = row.Columns[4].GetString_()
                                                myRes.Value_order_amount = row.Columns[5].GetInt64()
                                                myRes.Value_order_status = row.Columns[6].GetString_()
                                                myRes.Value_last_updated_by = row.Columns[7].GetString_()
                                                myRes.Value_last_updated_datetime = row.Columns[8].GetString_()

                                                myResponces = append(myResponces, myRes)

                                        }
                                }
                                if rowChannel == nil {
                                        break
                                }
                        }

                        //JSON型にする。
                        jsonRows, err := json.Marshal(myResponces)
                        if err != nil {
                                return nil, fmt.Errorf("order_search operation failed. Error marshaling JSON: %s", err)
                        }

                        return jsonRows, nil


        default:
                        return nil, errors.New("Unsupported operation")
        }
}

func (t *TradeChaincode) order_entry(stub *shim.ChaincodeStub, args []string) ([]byte, error) {
        if len(args) != 5 {
                return nil, errors.New("引数は5個指定してください。")
        }
        
        var assigner string
        
        callerRole, err := stub.ReadCertAttribute("role")
        if err != nil {
                fmt.Printf("Error reading attribute [%v] \n", err)
                return nil, fmt.Errorf("Failed fetching caller role. Error was [%v]", err)
        }
        
        caller := string(callerRole[:])
        if caller != "admin" && caller != "orderer" {
                fmt.Printf("Caller is not assigner - caller %v assigner %v\n", caller, assigner)
                return nil, fmt.Errorf("The caller does not have the rights to invoke assign. Expected role [%v], caller role [%v]", assigner, caller)
        }

        var value_order_amount int64
        var value_order_status, value_last_updated_datetime, assigner string

        key_orderer_code        := args[0]
        key_accepter_code       := args[1]
        key_product_code    := args[2]
        value_order_amount,err := strconv.ParseInt(args[3], 10, 64)
        if err != nil {
                return nil, errors.New("取引数は数字を指定してください。")
        }
        value_last_updated_by   := args[4]
        value_order_status = "ACCEPTED"
        if (value_order_amount < 1) {
                return nil, fmt.Errorf("取引数は1以上を指定してください。取引数 [%d] ", value_order_amount )
        }

        // Recover the role that is allowed to make assignments
//        assignerRole, err := stub.GetState("assignerRole")
        if err != nil {
                fmt.Printf("Error getting role [%v] \n", err)
                return nil, errors.New("Failed fetching assigner role")
        }

        callerRole, err := stub.ReadCertAttribute("role")
        if err != nil {
                fmt.Printf("Error reading attribute [%v] \n", err)
                return nil, fmt.Errorf("Failed fetching caller role. Error was [%v]", err)
        }

        caller := string(callerRole[:])
//        assigner := string(assignerRole[:])
        assigner = "order_entry"

        if caller != assigner {
                fmt.Printf("Caller is not assigner - caller %v assigner %v\n", caller, assigner)
                return nil, fmt.Errorf("The caller does not have the rights to invoke assign. Expected role [%v], caller role [%v]", assigner, caller)
        }

        // 取引履歴を登録する。

        // Valueの内容をJSON形式で登録する
        const layout2 = "2006-01-02 15:04:05"
        value_last_updated_datetime = time.Now().Format(layout2)

        type order_table struct {
                key_orderer_code  string
                key_accepter_code string
                key_product_code string
                value_order_amount int64
                value_order_status string
                value_last_updated_by string
                value_last_updated_datetime string
        }

        UUID := stub.UUID
        key_system_code := "stock_management"

        _, err = stub.InsertRow("order", shim.Row{
                Columns: []*shim.Column{
                        &shim.Column{Value: &shim.Column_String_{String_: key_system_code}},              // args[0]
                        &shim.Column{Value: &shim.Column_String_{String_: UUID}},                         // args[1]
                        &shim.Column{Value: &shim.Column_String_{String_: key_orderer_code}},             // args[2]
                        &shim.Column{Value: &shim.Column_String_{String_: key_accepter_code}},            // args[3]
                        &shim.Column{Value: &shim.Column_String_{String_: key_product_code}},             // args[4]
                        &shim.Column{Value: &shim.Column_Int64{Int64: value_order_amount}},               // args[5]
                        &shim.Column{Value: &shim.Column_String_{String_: value_order_status}},           // args[6]
                        &shim.Column{Value: &shim.Column_String_{String_: value_last_updated_by}},        // args[7]
                        &shim.Column{Value: &shim.Column_String_{String_: value_last_updated_datetime}},  // args[8]
                },
        })

        if err != nil {
//              myLogger.Debug("error[%s]",err.Error())
                return nil, errors.New("Failed inserting row.")
        }

        return nil, nil
}

func (t *TradeChaincode) allocate_entry(stub *shim.ChaincodeStub, args []string) ([]byte, error) {

        if len(args) != 2 {
                return nil, errors.New("引数は2個指定してください。")
        }
        
        var assigner string
        
        callerRole, err := stub.ReadCertAttribute("role")
        if err != nil {
                fmt.Printf("Error reading attribute [%v] \n", err)
                return nil, fmt.Errorf("Failed fetching caller role. Error was [%v]", err)
        }
        
        caller := string(callerRole[:])
        if caller != "admin" && caller != "trading_company" {
                fmt.Printf("Caller is not assigner - caller %v assigner %v\n", caller, assigner)
                return nil, fmt.Errorf("The caller does not have the rights to invoke assign. Expected role [%v], caller role [%v]", assigner, caller)
        }

        var value_order_amount, value_current_stock, value_allocate_stock int64
        var UUID, key_orderer_code, key_accepter_code, key_product_code, value_order_status, value_last_updated_by, value_last_updated_datetime string
        UUID = args[0]
        value_last_updated_by = args[1]
        key_system_code := "stock_management"

        var order_columns []shim.Column
        o_col1 := shim.Column{Value: &shim.Column_String_{String_: key_system_code}}
        o_col2 := shim.Column{Value: &shim.Column_String_{String_: UUID}}
        order_columns = append(order_columns, o_col1)
        order_columns = append(order_columns, o_col2)

        rowChannel, err := stub.GetRows("order", order_columns)
        if err != nil {
                return nil, errors.New("GetRowでエラー")
        }

        for {
                select {
                case row, ok := <-rowChannel:
                        if !ok {
                                rowChannel = nil
                        } else {
                                key_orderer_code   = row.Columns[2].GetString_()
                                key_accepter_code  = row.Columns[3].GetString_()
                                key_product_code   = row.Columns[4].GetString_()
                                value_order_amount = row.Columns[5].GetInt64()
                                value_order_status = row.Columns[6].GetString_()
                        }
                }
                if rowChannel == nil {
                        break
                }
        }

//      if key_product_code == nil {
//              return nil, errors.New("商品コードが取得できません")
//      } else if value_order_amount == nil {
//              return nil, errors.New("取引数が取得できません")
//      } else if value_order_status != "ACCEPTED" {
//              return nil, errors.New("取引状態がACCEPTEDではありません")
//      }

        var stock_columns []shim.Column
        s_col1 := shim.Column{Value: &shim.Column_String_{String_: key_system_code}}
        s_col2 := shim.Column{Value: &shim.Column_String_{String_: key_product_code}}
        stock_columns = append(stock_columns, s_col1)
        stock_columns = append(stock_columns, s_col2)

        s_row, err := stub.GetRow("stock", stock_columns)

        if err != nil {
                return nil, errors.New("該当の商品コードが在庫マスタにありません")
        }
        value_current_stock = s_row.Columns[2].GetInt64()
        value_allocate_stock = s_row.Columns[3].GetInt64()
//      if value_current_stock == nil {
//              return nil, errors.New("現在庫数が取得できません")
//      } else if value_allocate_stock = nil {
//              return nil, errors.New("引当数が取得できません")
//      }

        //現在庫数 － 引当数 ＞ 取引数 のチェック
        if value_current_stock - value_allocate_stock < value_order_amount {
                return nil, errors.New("現在庫数が足りません")
        }

        // 在庫マスタ更新
        value_allocate_stock = value_allocate_stock + value_order_amount

        var s_columns []*shim.Column
        upd_s_col1 := shim.Column{Value: &shim.Column_String_{String_: key_system_code}}
        upd_s_col2 := shim.Column{Value: &shim.Column_String_{String_: key_product_code}}
        upd_s_col3 := shim.Column{Value: &shim.Column_Int64{Int64: value_current_stock}}
        upd_s_col4 := shim.Column{Value: &shim.Column_Int64{Int64: value_allocate_stock}}
        upd_s_col5 := shim.Column{Value: &shim.Column_Int64{Int64: s_row.Columns[4].GetInt64()}}

        s_columns = append(s_columns, &upd_s_col1)
        s_columns = append(s_columns, &upd_s_col2)
        s_columns = append(s_columns, &upd_s_col3)
        s_columns = append(s_columns, &upd_s_col4)
        s_columns = append(s_columns, &upd_s_col5)
        new_s_row := shim.Row{Columns: s_columns}

        ok, err := stub.ReplaceRow("stock", new_s_row)

        if err != nil {
                return nil, errors.New("Failed inserting row.")
        }

        if !ok {
                return nil, errors.New("ReplaceRow operation failed. Row with given key does not exist")
        }

//      myLogger.Info("在庫引当完了")

        // 取引データ更新
        value_order_status = "ALLOCATED"
        const layout2 = "2006-01-02 15:04:05"
        value_last_updated_datetime = time.Now().Format(layout2)

        var o_columns []*shim.Column
        upd_o_col1 := shim.Column{Value: &shim.Column_String_{String_: key_system_code}}
        upd_o_col2 := shim.Column{Value: &shim.Column_String_{String_: UUID}}
        upd_o_col3 := shim.Column{Value: &shim.Column_String_{String_: key_orderer_code}}
        upd_o_col4 := shim.Column{Value: &shim.Column_String_{String_: key_accepter_code}}
        upd_o_col5 := shim.Column{Value: &shim.Column_String_{String_: key_product_code}}
        upd_o_col6 := shim.Column{Value: &shim.Column_Int64{Int64: value_order_amount}}
        upd_o_col7 := shim.Column{Value: &shim.Column_String_{String_: value_order_status}}
        upd_o_col8 := shim.Column{Value: &shim.Column_String_{String_: value_last_updated_by}}
        upd_o_col9 := shim.Column{Value: &shim.Column_String_{String_: value_last_updated_datetime}}

        o_columns = append(o_columns, &upd_o_col1)
        o_columns = append(o_columns, &upd_o_col2)
        o_columns = append(o_columns, &upd_o_col3)
        o_columns = append(o_columns, &upd_o_col4)
        o_columns = append(o_columns, &upd_o_col5)
        o_columns = append(o_columns, &upd_o_col6)
        o_columns = append(o_columns, &upd_o_col7)
        o_columns = append(o_columns, &upd_o_col8)
        o_columns = append(o_columns, &upd_o_col9)
        new_o_row := shim.Row{Columns: o_columns}

        ok2, err := stub.ReplaceRow("order", new_o_row)

        if err != nil {
                return nil, errors.New("Failed inserting row.")
        }

        if !ok2 {
                return nil, errors.New("ReplaceRow operation failed. Row with given key does not exist")
        }

        return nil, nil
}

func (t *TradeChaincode) shipment_entry(stub *shim.ChaincodeStub, args []string) ([]byte, error) {

        if len(args) != 2 {
                return nil, errors.New("引数は2個指定してください。")
        }
        
        var assigner string
        
        callerRole, err := stub.ReadCertAttribute("role")
        if err != nil {
                fmt.Printf("Error reading attribute [%v] \n", err)
                return nil, fmt.Errorf("Failed fetching caller role. Error was [%v]", err)
        }
        
        caller := string(callerRole[:])
        if caller != "admin" && caller != "warehouse" {
                fmt.Printf("Caller is not assigner - caller %v assigner %v\n", caller, assigner)
                return nil, fmt.Errorf("The caller does not have the rights to invoke assign. Expected role [%v], caller role [%v]", assigner, caller)
        }

        var value_order_amount, value_current_stock, value_allocate_stock int64
        var UUID, key_orderer_code, key_accepter_code, key_product_code, value_order_status, value_last_updated_by, value_last_updated_datetime string
        UUID = args[0]
        value_last_updated_by = args[1]
        key_system_code := "stock_management"

        var order_columns []shim.Column
        o_col1 := shim.Column{Value: &shim.Column_String_{String_: key_system_code}}
        o_col2 := shim.Column{Value: &shim.Column_String_{String_: UUID}}
        order_columns = append(order_columns, o_col1)
        order_columns = append(order_columns, o_col2)

        rowChannel, err := stub.GetRows("order", order_columns)
        if err != nil {
                return nil, errors.New("GetRowでエラー")
        }

        for {
                select {
                case row, ok := <-rowChannel:
                        if !ok {
                                rowChannel = nil
                        } else {
                                key_orderer_code   = row.Columns[2].GetString_()
                                key_accepter_code  = row.Columns[3].GetString_()
                                key_product_code   = row.Columns[4].GetString_()
                                value_order_amount = row.Columns[5].GetInt64()
                                value_order_status = row.Columns[6].GetString_()
                        }
                }
                if rowChannel == nil {
                        break
                }
        }

        var stock_columns []shim.Column
        s_col1 := shim.Column{Value: &shim.Column_String_{String_: key_system_code}}
        s_col2 := shim.Column{Value: &shim.Column_String_{String_: key_product_code}}
        stock_columns = append(stock_columns, s_col1)
        stock_columns = append(stock_columns, s_col2)

        s_row, err := stub.GetRow("stock", stock_columns)

        if err != nil {
                return nil, errors.New("該当の商品コードが在庫マスタにありません")
        }
        value_current_stock = s_row.Columns[2].GetInt64()
        value_allocate_stock = s_row.Columns[3].GetInt64()

        //現在庫数 ＞ 取引数 のチェック
        if value_current_stock < value_order_amount {
                return nil, errors.New("現在庫数が足りません")
        }

        // 在庫マスタ更新
        value_current_stock = value_current_stock - value_order_amount
        value_allocate_stock = value_allocate_stock - value_order_amount

        var s_columns []*shim.Column
        upd_s_col1 := shim.Column{Value: &shim.Column_String_{String_: key_system_code}}
        upd_s_col2 := shim.Column{Value: &shim.Column_String_{String_: key_product_code}}
        upd_s_col3 := shim.Column{Value: &shim.Column_Int64{Int64: value_current_stock}}
        upd_s_col4 := shim.Column{Value: &shim.Column_Int64{Int64: value_allocate_stock}}
        upd_s_col5 := shim.Column{Value: &shim.Column_Int64{Int64: s_row.Columns[4].GetInt64()}}

        s_columns = append(s_columns, &upd_s_col1)
        s_columns = append(s_columns, &upd_s_col2)
        s_columns = append(s_columns, &upd_s_col3)
        s_columns = append(s_columns, &upd_s_col4)
        s_columns = append(s_columns, &upd_s_col5)
        new_s_row := shim.Row{Columns: s_columns}

        ok, err := stub.ReplaceRow("stock", new_s_row)

        if err != nil {
                return nil, errors.New("Failed inserting row.")
        }

        if !ok {
                return nil, errors.New("ReplaceRow operation failed. Row with given key does not exist")
        }

//      myLogger.Info("出荷完了")

        // 取引データ更新
        value_order_status = "SHIPPED"
        const layout2 = "2006-01-02 15:04:05"
        value_last_updated_datetime = time.Now().Format(layout2)

        var o_columns []*shim.Column
        upd_o_col1 := shim.Column{Value: &shim.Column_String_{String_: key_system_code}}
        upd_o_col2 := shim.Column{Value: &shim.Column_String_{String_: UUID}}
        upd_o_col3 := shim.Column{Value: &shim.Column_String_{String_: key_orderer_code}}
        upd_o_col4 := shim.Column{Value: &shim.Column_String_{String_: key_accepter_code}}
        upd_o_col5 := shim.Column{Value: &shim.Column_String_{String_: key_product_code}}
        upd_o_col6 := shim.Column{Value: &shim.Column_Int64{Int64: value_order_amount}}
        upd_o_col7 := shim.Column{Value: &shim.Column_String_{String_: value_order_status}}
        upd_o_col8 := shim.Column{Value: &shim.Column_String_{String_: value_last_updated_by}}
        upd_o_col9 := shim.Column{Value: &shim.Column_String_{String_: value_last_updated_datetime}}

        o_columns = append(o_columns, &upd_o_col1)
        o_columns = append(o_columns, &upd_o_col2)
        o_columns = append(o_columns, &upd_o_col3)
        o_columns = append(o_columns, &upd_o_col4)
        o_columns = append(o_columns, &upd_o_col5)
        o_columns = append(o_columns, &upd_o_col6)
        o_columns = append(o_columns, &upd_o_col7)
        o_columns = append(o_columns, &upd_o_col8)
        o_columns = append(o_columns, &upd_o_col9)
        new_o_row := shim.Row{Columns: o_columns}

        ok2, err := stub.ReplaceRow("order", new_o_row)

        if err != nil {
                return nil, errors.New("Failed inserting row.")
        }

        if !ok2 {
                return nil, errors.New("ReplaceRow operation failed. Row with given key does not exist")
        }

        return nil, nil
}

// Run callback representing the invocation of a chaincode
func (t *TradeChaincode) Invoke(stub *shim.ChaincodeStub, function string, args []string) ([]byte, error) {

        // Handle different functions
        if function == "register" {
                // 在庫マスタの登録
                return t.register(stub, args)
        } else if function == "update" {
                // 在庫マスタの現在庫数更新
                return t.update(stub, args)
        } else if function == "order_entry" {
                // 商品注文取引
                return t.order_entry(stub, args)
        } else if function == "allocate_entry" {
                // 在庫引当取引
                return t.allocate_entry(stub, args)
        } else if function == "shipment_entry" {
                // 倉庫出荷取引
                return t.shipment_entry(stub, args)
        }

        return nil, errors.New("Received unknown function invocation")
}

func main() {
        err := shim.Start(new(TradeChaincode))
        if err != nil {
                fmt.Printf("Error starting TradeChaincode: %s", err)
        }
}