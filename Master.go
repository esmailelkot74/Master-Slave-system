package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	db                 *sql.DB
	slaveAddresses            = []string{"http://localhost:8084", "http://localhost:8085"}
	isMaster           bool   = true
	masterAddress      string = "http://localhost:8083"
	electionInProgress bool
	slaveConnections   sync.Map
	replicationQueue   = make(chan ReplicationTask, 1000)
)

type ReplicationTask struct {
	Operation string
	Data      map[string]interface{}
}

func allowCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
}

func clearScreen() {
	if runtime.GOOS == "windows" {
		cmd := exec.Command("cmd", "/c", "cls")
		cmd.Stdout = os.Stdout
		cmd.Run()
	} else {
		fmt.Print("\033[H\033[2J")
	}
}

func printDashboard() {
	clearScreen()
	fmt.Println("╔════════════════════════════════════════════════════════════╗")
	fmt.Println("║                    MASTER SERVER DASHBOARD                  ║")
	fmt.Println("╠════════════════════════════════════════════════════════════╣")
	fmt.Println("║ Status: Running on port 8083                               ║")
	fmt.Println("║                                                            ║")
	fmt.Println("║ Connected Slaves:                                          ║")
	slaveConnections.Range(func(key, value interface{}) bool {
		addr := key.(string)
		status := value.(bool)
		statusStr := "❌ Offline"
		if status {
			statusStr = "✅ Online"
		}
		fmt.Printf("║   - %s: %s\n", addr, statusStr)
		return true
	})
	fmt.Println("║                                                            ║")
	fmt.Println("║ Available Commands:                                        ║")
	fmt.Println("║   1. Create Database                                       ║")
	fmt.Println("║   2. Drop Database                                         ║")
	fmt.Println("║   3. Create Table                                          ║")
	fmt.Println("║   4. Insert Record                                         ║")
	fmt.Println("║   5. Select Records                                        ║")
	fmt.Println("║   6. Update Record                                         ║")
	fmt.Println("║   7. Delete Record                                         ║")
	fmt.Println("║   8. Show Replication Status                               ║")
	fmt.Println("║   9. Refresh Dashboard                                     ║")
	fmt.Println("║   0. Exit                                                  ║")
	fmt.Println("╚════════════════════════════════════════════════════════════╝")
	fmt.Print("\nEnter command number: ")
}

func main() {
	var err error
	db, err = sql.Open("mysql", "root:k7l15981@tcp(127.0.0.1:3306)/")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}

	// Start replication worker
	go replicationWorker()

	// Start slave health check
	startSlaveHealthCheck()

	// Start HTTP server in a goroutine
	go func() {
		http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
			allowCORS(w)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("pong"))
		})

		http.HandleFunc("/register-slave", func(w http.ResponseWriter, r *http.Request) {
			allowCORS(w)
			slaveAddr := r.URL.Query().Get("address")
			if slaveAddr != "" {
				slaveConnections.Store(slaveAddr, true)
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]string{"status": "registered"})
			}
		})

		http.HandleFunc("/createdb", func(w http.ResponseWriter, r *http.Request) {
			allowCORS(w)
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusOK)
				return
			}
			createDB(w, r)
		})

		http.HandleFunc("/dropdb", func(w http.ResponseWriter, r *http.Request) {
			allowCORS(w)
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusOK)
				return
			}
			dropDB(w, r)
		})

		http.HandleFunc("/createtable", func(w http.ResponseWriter, r *http.Request) {
			allowCORS(w)
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusOK)
				return
			}
			createTable(w, r)
		})

		http.HandleFunc("/insert", func(w http.ResponseWriter, r *http.Request) {
			allowCORS(w)
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusOK)
				return
			}
			insertRecord(w, r)
		})

		http.HandleFunc("/select", func(w http.ResponseWriter, r *http.Request) {
			allowCORS(w)
			selectRecords(w, r)
		})

		http.HandleFunc("/update", func(w http.ResponseWriter, r *http.Request) {
			allowCORS(w)
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusOK)
				return
			}
			updateRecord(w, r)
		})

		http.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
			allowCORS(w)
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusOK)
				return
			}
			deleteRecord(w, r)
		})

		fmt.Println("Master server running on port 8083...")
		log.Fatal(http.ListenAndServe(":8083", nil))
	}()

	// Start dashboard
	for {
		printDashboard()
		var choice string
		fmt.Scanln(&choice)

		switch choice {
		case "1":
			fmt.Print("Enter database name: ")
			var dbname string
			fmt.Scanln(&dbname)
			_, err := db.Exec("CREATE DATABASE IF NOT EXISTS " + dbname)
			if err != nil {
				fmt.Println("Error creating database:", err)
			} else {
				fmt.Println("Database created successfully")
				replicationQueue <- ReplicationTask{
					Operation: "createdb",
					Data: map[string]interface{}{
						"dbname": dbname,
					},
				}
			}
		case "2":
			fmt.Print("Enter database name: ")
			var dbname string
			fmt.Scanln(&dbname)
			_, err := db.Exec("DROP DATABASE IF EXISTS " + dbname)
			if err != nil {
				fmt.Println("Error dropping database:", err)
			} else {
				fmt.Println("Database dropped successfully")
				replicationQueue <- ReplicationTask{
					Operation: "dropdb",
					Data: map[string]interface{}{
						"dbname": dbname,
					},
				}
			}
		case "3":
			fmt.Print("Enter database name: ")
			var dbname string
			fmt.Scanln(&dbname)
			fmt.Print("Enter table name: ")
			var table string
			fmt.Scanln(&table)
			fmt.Print("Enter schema (e.g., id INT, name VARCHAR(255)): ")
			var schema string
			fmt.Scanln(&schema)

			query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (%s)", dbname, table, schema)
			_, err := db.Exec(query)
			if err != nil {
				fmt.Println("Error creating table:", err)
			} else {
				fmt.Println("Table created successfully")
				replicationQueue <- ReplicationTask{
					Operation: "createtable",
					Data: map[string]interface{}{
						"dbname": dbname,
						"table":  table,
						"schema": schema,
					},
				}
			}
		case "4":
			fmt.Print("Enter database name: ")
			var dbname string
			fmt.Scanln(&dbname)
			fmt.Print("Enter table name: ")
			var table string
			fmt.Scanln(&table)
			fmt.Print("Enter values (e.g., 1, 'John'): ")
			var values string
			fmt.Scanln(&values)

			query := fmt.Sprintf("INSERT INTO %s.%s VALUES (%s)", dbname, table, values)
			_, err := db.Exec(query)
			if err != nil {
				fmt.Println("Error inserting record:", err)
			} else {
				fmt.Println("Record inserted successfully")
				replicationQueue <- ReplicationTask{
					Operation: "insert",
					Data: map[string]interface{}{
						"dbname": dbname,
						"table":  table,
						"values": values,
					},
				}
			}
		case "5":
			fmt.Print("Enter database name: ")
			var dbname string
			fmt.Scanln(&dbname)
			fmt.Print("Enter table name: ")
			var table string
			fmt.Scanln(&table)

			query := fmt.Sprintf("SELECT * FROM %s.%s", dbname, table)
			rows, err := db.Query(query)
			if err != nil {
				fmt.Println("Error selecting records:", err)
				continue
			}
			defer rows.Close()

			cols, err := rows.Columns()
			if err != nil {
				fmt.Println("Error getting columns:", err)
				continue
			}

			// Print column headers
			for _, col := range cols {
				fmt.Printf("%-15s", col)
			}
			fmt.Println("\n" + strings.Repeat("-", len(cols)*15))

			// Print rows
			for rows.Next() {
				columns := make([]interface{}, len(cols))
				columnPointers := make([]interface{}, len(cols))
				for i := range columns {
					columnPointers[i] = &columns[i]
				}

				if err := rows.Scan(columnPointers...); err != nil {
					fmt.Println("Error scanning row:", err)
					continue
				}

				for i := range cols {
					val := columnPointers[i].(*interface{})
					fmt.Printf("%-15v", *val)
				}
				fmt.Println()
			}
		case "6":
			fmt.Print("Enter database name: ")
			var dbname string
			fmt.Scanln(&dbname)
			fmt.Print("Enter table name: ")
			var table string
			fmt.Scanln(&table)
			fmt.Print("Enter SET clause (e.g., name='John'): ")
			var set string
			fmt.Scanln(&set)
			fmt.Print("Enter WHERE clause (e.g., id=1): ")
			var where string
			fmt.Scanln(&where)

			query := fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s", dbname, table, set, where)
			_, err := db.Exec(query)
			if err != nil {
				fmt.Println("Error updating record:", err)
			} else {
				fmt.Println("Record updated successfully")
				replicationQueue <- ReplicationTask{
					Operation: "update",
					Data: map[string]interface{}{
						"dbname": dbname,
						"table":  table,
						"set":    set,
						"where":  where,
					},
				}
			}
		case "7":
			fmt.Print("Enter database name: ")
			var dbname string
			fmt.Scanln(&dbname)
			fmt.Print("Enter table name: ")
			var table string
			fmt.Scanln(&table)
			fmt.Print("Enter WHERE clause (e.g., id=1): ")
			var where string
			fmt.Scanln(&where)

			query := fmt.Sprintf("DELETE FROM %s.%s WHERE %s", dbname, table, where)
			_, err := db.Exec(query)
			if err != nil {
				fmt.Println("Error deleting record:", err)
			} else {
				fmt.Println("Record deleted successfully")
				replicationQueue <- ReplicationTask{
					Operation: "delete",
					Data: map[string]interface{}{
						"dbname": dbname,
						"table":  table,
						"where":  where,
					},
				}
			}
		case "8":
			fmt.Println("\nReplication Status:")
			fmt.Println("------------------")
			slaveConnections.Range(func(key, value interface{}) bool {
				addr := key.(string)
				status := value.(bool)
				statusStr := "Offline"
				if status {
					statusStr = "Online"
				}
				fmt.Printf("Slave %s: %s\n", addr, statusStr)
				return true
			})
		case "9":
			continue
		case "0":
			fmt.Println("Exiting...")
			return
		default:
			fmt.Println("Invalid choice. Press Enter to continue...")
			fmt.Scanln()
		}

		fmt.Println("\nPress Enter to continue...")
		fmt.Scanln()
	}
}

func replicationWorker() {
	for task := range replicationQueue {
		slaveConnections.Range(func(key, value interface{}) bool {
			addr := key.(string)
			status := value.(bool)
			if status {
				// Check if slave is still alive before replicating
				resp, err := http.Get(addr + "/ping")
				if err != nil || resp.StatusCode != http.StatusOK {
					slaveConnections.Store(addr, false)
					return true
				}
				resp.Body.Close()
				go replicateToSlave(addr, task)
			}
			return true
		})
	}
}

func replicateToSlave(slaveAddr string, task ReplicationTask) {
	client := &http.Client{Timeout: 5 * time.Second}

	switch task.Operation {
	case "createdb":
		_, err := client.Get(fmt.Sprintf("%s/replicate/db?name=%s", slaveAddr, task.Data["dbname"]))
		if err != nil {
			slaveConnections.Store(slaveAddr, false)
		}
	case "dropdb":
		_, err := client.Get(fmt.Sprintf("%s/replicate/dropdb?name=%s", slaveAddr, task.Data["dbname"]))
		if err != nil {
			slaveConnections.Store(slaveAddr, false)
		}
	case "createtable":
		_, err := client.Get(fmt.Sprintf("%s/replicate/table?dbname=%s&table=%s&schema=%s",
			slaveAddr, task.Data["dbname"], task.Data["table"], url.QueryEscape(task.Data["schema"].(string))))
		if err != nil {
			slaveConnections.Store(slaveAddr, false)
		}
	case "insert":
		jsonData, _ := json.Marshal(task.Data)
		_, err := client.Post(slaveAddr+"/replicate/insert", "application/json", strings.NewReader(string(jsonData)))
		if err != nil {
			slaveConnections.Store(slaveAddr, false)
		}
	case "update":
		jsonData, _ := json.Marshal(task.Data)
		_, err := client.Post(slaveAddr+"/replicate/update", "application/json", strings.NewReader(string(jsonData)))
		if err != nil {
			slaveConnections.Store(slaveAddr, false)
		}
	case "delete":
		jsonData, _ := json.Marshal(task.Data)
		_, err := client.Post(slaveAddr+"/replicate/delete", "application/json", strings.NewReader(string(jsonData)))
		if err != nil {
			slaveConnections.Store(slaveAddr, false)
		}
	}
}

func createDB(w http.ResponseWriter, r *http.Request) {
	dbname := r.URL.Query().Get("name")
	if dbname == "" {
		http.Error(w, "Database name is required", http.StatusBadRequest)
		return
	}

	_, err := db.Exec("CREATE DATABASE IF NOT EXISTS " + dbname)
	if err != nil {
		http.Error(w, "Failed to create database: "+err.Error(), http.StatusInternalServerError)
		return
	}

	replicationQueue <- ReplicationTask{
		Operation: "createdb",
		Data: map[string]interface{}{
			"dbname": dbname,
		},
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Database created successfully"})
}

func dropDB(w http.ResponseWriter, r *http.Request) {
	dbname := r.URL.Query().Get("name")
	if dbname == "" {
		http.Error(w, "Database name is required", http.StatusBadRequest)
		return
	}

	_, err := db.Exec("DROP DATABASE IF EXISTS " + dbname)
	if err != nil {
		http.Error(w, "Failed to drop database: "+err.Error(), http.StatusInternalServerError)
		return
	}

	replicationQueue <- ReplicationTask{
		Operation: "dropdb",
		Data: map[string]interface{}{
			"dbname": dbname,
		},
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Database dropped successfully"})
}

func createTable(w http.ResponseWriter, r *http.Request) {
	dbname := r.URL.Query().Get("dbname")
	table := r.URL.Query().Get("table")
	schema := r.URL.Query().Get("schema")

	if dbname == "" || table == "" || schema == "" {
		http.Error(w, "All parameters (dbname, table, schema) are required", http.StatusBadRequest)
		return
	}

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (%s)", dbname, table, schema)
	_, err := db.Exec(query)
	if err != nil {
		http.Error(w, "Failed to create table: "+err.Error(), http.StatusInternalServerError)
		return
	}

	replicationQueue <- ReplicationTask{
		Operation: "createtable",
		Data: map[string]interface{}{
			"dbname": dbname,
			"table":  table,
			"schema": schema,
		},
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Table created successfully"})
}

func insertRecord(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DBName string `json:"dbname"`
		Table  string `json:"table"`
		Values string `json:"values"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.DBName == "" || req.Table == "" || req.Values == "" {
		http.Error(w, "All fields (dbname, table, values) are required", http.StatusBadRequest)
		return
	}

	query := fmt.Sprintf("INSERT INTO %s.%s VALUES (%s)", req.DBName, req.Table, req.Values)
	_, err := db.Exec(query)
	if err != nil {
		http.Error(w, "Failed to insert record: "+err.Error(), http.StatusInternalServerError)
		return
	}

	replicationQueue <- ReplicationTask{
		Operation: "insert",
		Data: map[string]interface{}{
			"dbname": req.DBName,
			"table":  req.Table,
			"values": req.Values,
		},
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Record inserted successfully"})
}

func selectRecords(w http.ResponseWriter, r *http.Request) {
	dbname := r.URL.Query().Get("dbname")
	table := r.URL.Query().Get("table")

	if dbname == "" || table == "" {
		http.Error(w, "Both dbname and table parameters are required", http.StatusBadRequest)
		return
	}

	query := fmt.Sprintf("SELECT * FROM %s.%s", dbname, table)
	rows, err := db.Query(query)
	if err != nil {
		http.Error(w, "Failed to query records: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		http.Error(w, "Failed to get columns: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Get column types
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		http.Error(w, "Failed to get column types: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var results []map[string]interface{}
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			http.Error(w, "Failed to scan row: "+err.Error(), http.StatusInternalServerError)
			return
		}

		row := make(map[string]interface{})
		for i, col := range cols {
			val := columnPointers[i].(*interface{})
			if *val == nil {
				row[col] = nil
				continue
			}

			switch colTypes[i].DatabaseTypeName() {
			case "INT", "BIGINT", "TINYINT", "SMALLINT", "MEDIUMINT":
				if b, ok := (*val).([]byte); ok {
					row[col] = string(b)
				} else {
					row[col] = *val
				}
			case "DECIMAL", "FLOAT", "DOUBLE":
				if b, ok := (*val).([]byte); ok {
					row[col] = string(b)
				} else {
					row[col] = *val
				}
			case "DATETIME", "TIMESTAMP", "DATE", "TIME":
				if b, ok := (*val).([]byte); ok {
					row[col] = string(b)
				} else {
					row[col] = *val
				}
			case "VARCHAR", "CHAR", "TEXT", "LONGTEXT", "MEDIUMTEXT", "TINYTEXT":
				if b, ok := (*val).([]byte); ok {
					row[col] = string(b)
				} else {
					row[col] = *val
				}
			default:
				if b, ok := (*val).([]byte); ok {
					row[col] = string(b)
				} else {
					row[col] = *val
				}
			}
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		http.Error(w, "Error during rows iteration: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

func updateRecord(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DBName string `json:"dbname"`
		Table  string `json:"table"`
		Set    string `json:"set"`
		Where  string `json:"where"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.DBName == "" || req.Table == "" || req.Set == "" || req.Where == "" {
		http.Error(w, "All fields (dbname, table, set, where) are required", http.StatusBadRequest)
		return
	}

	query := fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s", req.DBName, req.Table, req.Set, req.Where)
	_, err := db.Exec(query)
	if err != nil {
		http.Error(w, "Failed to update record: "+err.Error(), http.StatusInternalServerError)
		return
	}

	replicationQueue <- ReplicationTask{
		Operation: "update",
		Data: map[string]interface{}{
			"dbname": req.DBName,
			"table":  req.Table,
			"set":    req.Set,
			"where":  req.Where,
		},
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Record updated successfully"})
}

func deleteRecord(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DBName string `json:"dbname"`
		Table  string `json:"table"`
		Where  string `json:"where"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.DBName == "" || req.Table == "" || req.Where == "" {
		http.Error(w, "All fields (dbname, table, where) are required", http.StatusBadRequest)
		return
	}

	query := fmt.Sprintf("DELETE FROM %s.%s WHERE %s", req.DBName, req.Table, req.Where)
	_, err := db.Exec(query)
	if err != nil {
		http.Error(w, "Failed to delete record: "+err.Error(), http.StatusInternalServerError)
		return
	}

	replicationQueue <- ReplicationTask{
		Operation: "delete",
		Data: map[string]interface{}{
			"dbname": req.DBName,
			"table":  req.Table,
			"where":  req.Where,
		},
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Record deleted successfully"})
}

func startElection() {
	if electionInProgress {
		return
	}
	electionInProgress = true

	log.Println("Starting master election...")
	time.Sleep(time.Second * 2)

	if strings.HasSuffix(masterAddress, "8083") {
		promoteToMaster()
	}
}

func promoteToMaster() {
	isMaster = true
	masterAddress = "http://localhost:8083"
	log.Println("This node has been promoted to master")
}

func checkMasterHealth() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if !isMaster {
			client := &http.Client{Timeout: 5 * time.Second}
			_, err := client.Get(masterAddress + "/ping")
			if err != nil {
				log.Printf("Master is down: %v", err)
				startElection()
			}
		}
	}
}

// Add periodic slave health check
func startSlaveHealthCheck() {
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for range ticker.C {
			slaveConnections.Range(func(key, value interface{}) bool {
				addr := key.(string)
				status := value.(bool)
				if status {
					resp, err := http.Get(addr + "/ping")
					if err != nil || resp.StatusCode != http.StatusOK {
						slaveConnections.Store(addr, false)
					} else {
						resp.Body.Close()
					}
				}
				return true
			})
		}
	}()
}
