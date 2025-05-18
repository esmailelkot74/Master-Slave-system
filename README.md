Distributed Database System with Master-Slave Replication
Overview
This project implements a distributed database system in Go, featuring a master-slave architecture with replication. The system consists of one master node (running on port 8083) and two slave nodes (running on ports 8084 and 8085). The master node handles database operations (CREATE, DROP, INSERT, UPDATE, DELETE) and replicates them to the slaves. Slaves can perform read operations and forward write operations to the master.
Features

Master-Slave Replication: The master node propagates database operations to slave nodes via HTTP requests.
Interactive Dashboards: Each node (master and slaves) has a terminal-based dashboard for managing database operations.
Health Monitoring: The master periodically checks slave health, and slaves monitor the master's status.
CORS Support: HTTP endpoints support Cross-Origin Resource Sharing for flexibility.
MySQL Integration: Uses MySQL as the underlying database, accessed via the go-sql-driver/mysql package.
Basic Leader Election: Includes a simple mechanism for master election if the master node fails.

Prerequisites

Go: Version 1.16 or higher
MySQL: Version 5.7 or higher
Git: For cloning the repository

Setup Instructions

Clone the Repository:
git clone https://github.com/esmailelkot74/Master-Slave-system.git
cd your-repo-name


Install Dependencies:
go mod init distributed-db
go get github.com/go-sql-driver/mysql


Configure MySQL:

Ensure MySQL is running on localhost:3306.
Create a user with the credentials root:k7l15981 or update the connection string in the code.
No initial database is required; the system can create databases dynamically.


Run the Nodes:

Open three terminal windows.
In the first terminal, run the master node:go run master.go


In the second terminal, run slave 1:go run slave1.go


In the third terminal, run slave 2:go run slave2.go





Usage

Master Dashboard (Port 8083):

Access the dashboard in the terminal.
Choose options (1–9) to perform operations like creating databases, tables, or records.
Option 8 shows replication status of connected slaves.
Option 0 exits the program.


Slave Dashboards (Ports 8084, 8085):

Similar to the master but with read-heavy operations.
Write operations (INSERT, UPDATE, DELETE) are forwarded to the master.
Option 1 shows the master's status.


HTTP API:

The master exposes endpoints like /createdb, /insert, /select, etc.
Slaves expose replication endpoints like /replicate/db, /replicate/insert, etc.
Example: Create a database via HTTP:curl "http://localhost:8083/createdb?name=mydb"





Project Structure

master.go: Implements the master node, handling primary database operations and replication.
slave1.go: Implements the first slave node, handling read operations and replication.
slave2.go: Implements the second slave node, identical to slave1 but on a different port.

Notes

The system assumes all nodes run on localhost. Update slaveAddresses and masterAddress in the code for distributed setups.
The leader election mechanism is basic and assumes the master is on port 8083. Enhance it for production use.
Error handling is implemented but may need refinement for edge cases.

Contributing
Contributions are welcome! Please fork the repository, create a feature branch, and submit a pull request.
License
This project is licensed under the MIT License.
