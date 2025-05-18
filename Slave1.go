package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	db                 *sql.DB
	masterAddress      string = "http://localhost:8083"
	isMaster           bool
	electionInProgress bool
)

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
	fmt.Println("║                    SLAVE 1 SERVER DASHBOARD                 ║")
	fmt.Println("╠════════════════════════════════════════════════════════════╣")
	fmt.Println("║ Status: Running on port 8084                               ║")
	fmt.Println("║                                                            ║")
	fmt.Println("║ Master Status:                                             ║")
	resp, err := http.Get(masterAddress + "/ping")
	masterStatus := "❌ Offline"
	if err == nil && resp.StatusCode == 200 {
		masterStatus = "✅ Online"
	}
	fmt.Printf("║   - %s: %s\n", masterAddress, masterStatus)
	fmt.Println("║                                                            ║")
	fmt.Println("║ Role: Slave                                                ║")
	fmt.Println("║                                                            ║")
	fmt.Println("║ Available Commands:                                        ║")
	fmt.Println("║   1. Show Replication Status                               ║")
	fmt.Println("║   2. Show Databases                                        ║")
	fmt.Println("║   3. Show Tables in Database                               ║")
	fmt.Println("║   4. Show Records in Table                                 ║")
	fmt.Println("║   5. Insert Record                                         ║")
	fmt.Println("║   6. Update Record                                         ║")
	fmt.Println("║   7. Delete Record                                         ║")
	fmt.Println("║   8. Select Records                                        ║")
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

	// Register with master
	go func() {
		for {
			resp, err := http.Get(fmt.Sprintf("%s/register-slave?address=http://localhost:8084", masterAddress))
			if err == nil && resp.StatusCode == 200 {
				log.Println("Successfully registered with master")
				break
			}
			time.Sleep(time.Second)
		}
	}()

	// Start HTTP server in a goroutine
	go func() {
		defineBasicRoutes()
		fmt.Println("Slave server running on port 8084...")
		log.Fatal(http.ListenAndServe(":8084", nil))
	}()

	// Start dashboard
	for {
		printDashboard()
		var choice string
		fmt.Scanln(&choice)

		switch choice {
		case "1":
			fmt.Println("\nReplication Status:")
			fmt.Println("------------------")
			resp, err := http.Get(masterAddress + "/ping")
			if err != nil {
				fmt.Println("Master is offline")
			} else {
				defer resp.Body.Close()
				fmt.Println("Master is online")
			}
		case "2":
			rows, err := db.Query("SHOW DATABASES")
			if err != nil {
				fmt.Println("Error showing databases:", err)
			} else {
				defer rows.Close()
				fmt.Println("\nDatabases:")
				fmt.Println("----------")
				for rows.Next() {
					var dbname string
					if err := rows.Scan(&dbname); err != nil {
						fmt.Println("Error scanning database:", err)
						continue
					}
					fmt.Println("- " + dbname)
				}
			}
		case "3":
			fmt.Print("Enter database name: ")
			var dbname string
			fmt.Scanln(&dbname)

			rows, err := db.Query(fmt.Sprintf("SHOW TABLES FROM %s", dbname))
			if err != nil {
				fmt.Println("Error showing tables:", err)
			} else {
				defer rows.Close()
				fmt.Printf("\nTables in %s:\n", dbname)
				fmt.Println("----------------")
				for rows.Next() {
					var table string
					if err := rows.Scan(&table); err != nil {
						fmt.Println("Error scanning table:", err)
						continue
					}
					fmt.Println("- " + table)
				}
			}
		case "4":
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

			// Get column types
			colTypes, err := rows.ColumnTypes()
			if err != nil {
				fmt.Println("Error getting column types:", err)
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
					if *val == nil {
						fmt.Printf("%-15s", "NULL")
						continue
					}

					switch colTypes[i].DatabaseTypeName() {
					case "INT", "BIGINT", "TINYINT", "SMALLINT", "MEDIUMINT":
						if b, ok := (*val).([]byte); ok {
							fmt.Printf("%-15s", string(b))
						} else {
							fmt.Printf("%-15v", *val)
						}
					case "DECIMAL", "FLOAT", "DOUBLE":
						if b, ok := (*val).([]byte); ok {
							fmt.Printf("%-15s", string(b))
						} else {
							fmt.Printf("%-15v", *val)
						}
					case "DATETIME", "TIMESTAMP", "DATE", "TIME":
						if b, ok := (*val).([]byte); ok {
							fmt.Printf("%-15s", string(b))
						} else {
							fmt.Printf("%-15v", *val)
						}
					case "VARCHAR", "CHAR", "TEXT", "LONGTEXT", "MEDIUMTEXT", "TINYTEXT":
						if b, ok := (*val).([]byte); ok {
							fmt.Printf("%-15s", string(b))
						} else {
							fmt.Printf("%-15v", *val)
						}
					default:
						if b, ok := (*val).([]byte); ok {
							fmt.Printf("%-15s", string(b))
						} else {
							fmt.Printf("%-15v", *val)
						}
					}
				}
				fmt.Println()
			}
		case "5":
			fmt.Print("Enter database name: ")
			var dbname string
			fmt.Scanln(&dbname)
			fmt.Print("Enter table name: ")
			var table string
			fmt.Scanln(&table)
			fmt.Print("Enter values (e.g., 1, 'John'): ")
			var values string
			fmt.Scanln(&values)

			// Send request to master
			jsonData, _ := json.Marshal(map[string]string{
				"dbname": dbname,
				"table":  table,
				"values": values,
			})
			resp, err := http.Post(masterAddress+"/insert", "application/json", strings.NewReader(string(jsonData)))
			if err != nil {
				fmt.Println("Error sending request to master:", err)
				continue
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				fmt.Println("Record inserted successfully")
			} else {
				fmt.Println("Error inserting record")
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

			// Send request to master
			jsonData, _ := json.Marshal(map[string]string{
				"dbname": dbname,
				"table":  table,
				"set":    set,
				"where":  where,
			})
			resp, err := http.Post(masterAddress+"/update", "application/json", strings.NewReader(string(jsonData)))
			if err != nil {
				fmt.Println("Error sending request to master:", err)
				continue
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				fmt.Println("Record updated successfully")
			} else {
				fmt.Println("Error updating record")
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

			// Send request to master
			jsonData, _ := json.Marshal(map[string]string{
				"dbname": dbname,
				"table":  table,
				"where":  where,
			})
			resp, err := http.Post(masterAddress+"/delete", "application/json", strings.NewReader(string(jsonData)))
			if err != nil {
				fmt.Println("Error sending request to master:", err)
				continue
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				fmt.Println("Record deleted successfully")
			} else {
				fmt.Println("Error deleting record")
			}
		case "8":
			fmt.Print("Enter database name: ")
			var dbname string
			fmt.Scanln(&dbname)
			fmt.Print("Enter table name: ")
			var table string
			fmt.Scanln(&table)

			// Send request to master
			resp, err := http.Get(fmt.Sprintf("%s/select?dbname=%s&table=%s", masterAddress, dbname, table))
			if err != nil {
				fmt.Println("Error sending request to master:", err)
				continue
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				var results []map[string]interface{}
				if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
					fmt.Println("Error decoding response:", err)
					continue
				}

				if len(results) > 0 {
					// Print headers
					headers := make([]string, 0)
					for k := range results[0] {
						headers = append(headers, k)
					}
					for _, h := range headers {
						fmt.Printf("%-15s", h)
					}
					fmt.Println("\n" + strings.Repeat("-", len(headers)*15))

					// Print rows
					for _, row := range results {
						for _, h := range headers {
							val := row[h]
							if val == nil {
								fmt.Printf("%-15s", "NULL")
								continue
							}
							fmt.Printf("%-15v", val)
						}
						fmt.Println()
					}
				} else {
					fmt.Println("No records found")
				}
			} else {
				fmt.Println("Error selecting records")
			}
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

func defineBasicRoutes() {
	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		allowCORS(w)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("pong"))
	})

	// Define replication routes
	http.HandleFunc("/replicate/db", func(w http.ResponseWriter, r *http.Request) {
		allowCORS(w)
		replicateDB(w, r)
	})

	http.HandleFunc("/replicate/dropdb", func(w http.ResponseWriter, r *http.Request) {
		allowCORS(w)
		replicateDropDB(w, r)
	})

	http.HandleFunc("/replicate/table", func(w http.ResponseWriter, r *http.Request) {
		allowCORS(w)
		replicateTable(w, r)
	})

	http.HandleFunc("/replicate/insert", func(w http.ResponseWriter, r *http.Request) {
		allowCORS(w)
		replicateInsert(w, r)
	})

	http.HandleFunc("/replicate/update", func(w http.ResponseWriter, r *http.Request) {
		allowCORS(w)
		replicateUpdate(w, r)
	})

	http.HandleFunc("/replicate/delete", func(w http.ResponseWriter, r *http.Request) {
		allowCORS(w)
		replicateDelete(w, r)
	})
}

func allowCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
}

func replicateDB(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "Database name is required", http.StatusBadRequest)
		return
	}

	_, err := db.Exec("CREATE DATABASE IF NOT EXISTS " + name)
	if err != nil {
		http.Error(w, "Failed to create database: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"message": "Database replicated successfully",
		"dbname":  name,
	})
}

func replicateDropDB(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "Database name is required", http.StatusBadRequest)
		return
	}

	_, err := db.Exec("DROP DATABASE IF EXISTS " + name)
	if err != nil {
		http.Error(w, "Failed to drop database: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"message": "Database dropped successfully",
		"dbname":  name,
	})
}

func replicateTable(w http.ResponseWriter, r *http.Request) {
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

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"message": "Table replicated successfully",
		"dbname":  dbname,
		"table":   table,
	})
}

func replicateInsert(w http.ResponseWriter, r *http.Request) {
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
	result, err := db.Exec(query)
	if err != nil {
		http.Error(w, "Failed to insert record: "+err.Error(), http.StatusInternalServerError)
		return
	}

	rowsAffected, _ := result.RowsAffected()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"message":      "Record inserted successfully",
		"rowsAffected": rowsAffected,
	})
}

func replicateUpdate(w http.ResponseWriter, r *http.Request) {
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
	result, err := db.Exec(query)
	if err != nil {
		http.Error(w, "Failed to update record: "+err.Error(), http.StatusInternalServerError)
		return
	}

	rowsAffected, _ := result.RowsAffected()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"message":      "Record updated successfully",
		"rowsAffected": rowsAffected,
	})
}

func replicateDelete(w http.ResponseWriter, r *http.Request) {
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
	result, err := db.Exec(query)
	if err != nil {
		http.Error(w, "Failed to delete record: "+err.Error(), http.StatusInternalServerError)
		return
	}

	rowsAffected, _ := result.RowsAffected()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"message":      "Record deleted successfully",
		"rowsAffected": rowsAffected,
	})
}
