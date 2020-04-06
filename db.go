//
// This package contains our SQLite DB interface.  It is a little ropy.
//

package main

import (
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

//
// The global DB handle.
//
var db *sql.DB
var db_type string

//
// PuppetRuns is the structure which is used to list a summary of puppet
// runs on the front-page.
//
type PuppetRuns struct {
	Fqdn     string
	State    string
	At       string
	Epoch   string
	Ago      string
	Runtime  string
	Branch   string
	Role     string
	BuiltAt  string
	BuiltAgo string
	Pinned   string
}

//
// PuppetReportSummary is the structure used to represent a series
// of puppet-runs against a particular node.
//
type PuppetReportSummary struct {
	ID        string
	Fqdn      string
	State     string
	At        string
	Ago       string
	Runtime   string
	Branch    string
	Role      string
	BuiltAt   string
	BuiltAgo  string
	Failed    int
	Changed   int
	Total     int
	YamlFile  string
}

//
// PuppetHistory is a simple structure used solely for the stacked-graph
// on the front-page of our site.
//
type PuppetHistory struct {
	Date      string
	Failed    string
	Changed   string
	Unchanged string
}

//
// PuppetState is used to return the number of nodes in a given state,
// and is used for the submission of metrics.
//
type PuppetState struct {
	State      string
	Count      int
	Percentage float64
}

//
// SetupDB opens our SQLite database, creating it if necessary.
//
func SetupDB(db_type_in string, path string) error {

	var err error

	if (strings.Compare(db_type_in, "sqlite3") != 0 && strings.Compare(db_type_in, "mysql") != 0) {
		return errors.New(strings.Join([]string{"Invalid db type, sqlite3 or mysql supported:", "db_type_in"}, " "))
	}

	db_type = db_type_in

	//
	// Return if the database already exists.
	//
	db, err = sql.Open(db_type, path)
	if err != nil {
		return err
	}

	sqlStmt := ""

	//
	// Create the table.
	//
	if strings.Compare(db_type_in, "sqlite3") == 0 {
		sqlStmt = `

	        PRAGMA automatic_index = ON;
	        PRAGMA cache_size = 32768;
	        PRAGMA journal_size_limit = 67110000;
	        PRAGMA locking_mode = NORMAL;
	        PRAGMA synchronous = NORMAL;
	        PRAGMA temp_store = MEMORY;
	        PRAGMA journal_mode = WAL;
	        PRAGMA wal_autocheckpoint = 16384;

	        CREATE TABLE IF NOT EXISTS reports (
	          id          INTEGER PRIMARY KEY AUTOINCREMENT,
	          host_id     INTEGER,
	          fqdn        text,
	          state       text,
	          yaml_file   text,
	          runtime     integer,
	          executed_at integer(4),
	          role        text,
	          branch      text,
	          build_time  integer(4),
	          total       integer,
	          skipped     integer,
	          failed      integer,
	          changed     integer
	        )	        
			`
		//
		// Create the table, if missing.
		//
		// Errors here are pretty unlikely.
		//
		_, err = db.Exec(sqlStmt)
		if err != nil {
			return err
		}

		sqlStmt = `

	        PRAGMA automatic_index = ON;
	        PRAGMA cache_size = 32768;
	        PRAGMA journal_size_limit = 67110000;
	        PRAGMA locking_mode = NORMAL;
	        PRAGMA synchronous = NORMAL;
	        PRAGMA temp_store = MEMORY;
	        PRAGMA journal_mode = WAL;
	        PRAGMA wal_autocheckpoint = 16384;

			CREATE TABLE IF NOT EXISTS hosts (
	          host_id     INTEGER PRIMARY KEY AUTOINCREMENT,
	          fqdn        text,
	          role        text,
	          branch      text,
	          build_time  integer(4),
	          state       text,
	          last_seen   integer(4),
	          runtime     integer,
	          pinned      integer,
	          UNIQUE(fqdn)
	        )	        
			`
		//
		// Create the table, if missing.
		//
		// Errors here are pretty unlikely.
		//
		_, err = db.Exec(sqlStmt)
		if err != nil {
			return err
		}

		sqlStmt = `

	        PRAGMA automatic_index = ON;
	        PRAGMA cache_size = 32768;
	        PRAGMA journal_size_limit = 67110000;
	        PRAGMA locking_mode = NORMAL;
	        PRAGMA synchronous = NORMAL;
	        PRAGMA temp_store = MEMORY;
	        PRAGMA journal_mode = WAL;
	        PRAGMA wal_autocheckpoint = 16384;

			CREATE TABLE IF NOT EXISTS history (
	          id          INTEGER PRIMARY KEY AUTOINCREMENT,
	          date        text,
	          failed      integer,
	          changed     integer,
	          unchanged   integer,
	          UNIQUE(date)
	        )	        
			`
		//
		// Create the table, if missing.
		//
		// Errors here are pretty unlikely.
		//
		_, err = db.Exec(sqlStmt)
		if err != nil {
			return err
		}

	} else if strings.Compare(db_type_in, "mysql") == 0 {
		sqlStmt = `
			CREATE TABLE IF NOT EXISTS reports (
			  id int(6) unsigned NOT NULL AUTO_INCREMENT,
			  host_id int(6) unsigned NOT NULL,
			  fqdn varchar(255) DEFAULT NULL,
			  state varchar(255) DEFAULT NULL,
			  yaml_file varchar(255) DEFAULT NULL,
			  runtime int(11) DEFAULT NULL,
			  executed_at int(4) DEFAULT NULL,
			  role varchar(255) DEFAULT NULL,
			  branch varchar(255) DEFAULT NULL,
			  build_time int(4) DEFAULT NULL,
			  total int(11) DEFAULT NULL,
			  skipped int(11) DEFAULT NULL,
			  failed int(11) DEFAULT NULL,
			  changed int(11) DEFAULT NULL,
			  PRIMARY KEY (id),
			  KEY fqdn (fqdn)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
			`
		//
		// Create the table, if missing.
		//
		// Errors here are pretty unlikely.
		//
		_, err = db.Exec(sqlStmt)
		if err != nil {
			return err
		}

		sqlStmt = `
			CREATE TABLE IF NOT EXISTS hosts (
			  host_id int(6) unsigned NOT NULL AUTO_INCREMENT,
			  fqdn varchar(255) DEFAULT NULL,
			  role varchar(255) DEFAULT NULL,
			  branch varchar(255) DEFAULT NULL,
			  build_time int(4) DEFAULT NULL,
			  state varchar(255) DEFAULT NULL,
			  last_seen int(4) DEFAULT NULL,
			  runtime int(11) DEFAULT NULL,
			  pinned tinyint DEFAULT 0,
			  PRIMARY KEY (host_id),
			  UNIQUE KEY fqdn (fqdn)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
			`
		//
		// Create the table, if missing.
		//
		// Errors here are pretty unlikely.
		//
		_, err = db.Exec(sqlStmt)
		if err != nil {
			return err
		}

		sqlStmt = `
			CREATE TABLE IF NOT EXISTS history (
			  id        int(6) unsigned NOT NULL AUTO_INCREMENT,
			  date      varchar(255) DEFAULT NULL,
			  failed    int(11) DEFAULT 0,
			  changed   int(11) DEFAULT 0,
			  unchanged int(11) DEFAULT 0,
			  PRIMARY KEY (id),
			  UNIQUE KEY date (date)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
			`
		//
		// Create the table, if missing.
		//
		// Errors here are pretty unlikely.
		//
		_, err = db.Exec(sqlStmt)
		if err != nil {
			return err
		}

	} else {
		return errors.New("Invalid db type, sqlite3 or mysql supported")
	}



	return nil
}

//
// Add an entry to the database.
//
// The entry contains most of the interesting data from the parsed YAML.
//
// But note that it doesn't contain changed resources, etc.
//
//
func addDB(data PuppetReport, path string) error {

	//
	// Ensure we have a DB-handle
	//
	if db == nil {
		return errors.New("SetupDB not called")
	}

	host_id, err := getHostId(data.Fqdn)
	if err != nil {
		return err
	}

	if host_id == 0 {
		err = nil
		host_id, err = createHost(data.Fqdn)
		if err != nil {
			return err
		}
	}

	at := time.Now().Unix()

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	
	report_stmt, err := tx.Prepare("INSERT INTO reports(fqdn,host_id,state,yaml_file,executed_at,runtime, failed, changed, total, skipped, role, branch, build_time) values(?,?,?,?,?,?,?,?,?,?,?,?,?)")
	if err != nil {
		return err
	}
	defer report_stmt.Close()

	report_stmt.Exec(data.Fqdn,
		host_id,
		data.State,
		path,
		at,
		data.Runtime,
		data.Failed,
		data.Changed,
		data.Total,
		data.Skipped,
		data.Role,
		data.Branch,
		data.BuildTime)

	host_stmt, err := tx.Prepare("UPDATE hosts SET last_seen = ?, state = ?, runtime = ?, role = ?, branch = ? , build_time = ? WHERE host_id = ?")
	if err != nil {
		return err
	}
	defer host_stmt.Close()

	host_stmt.Exec(
		at,
		data.State,
		data.Runtime,
		data.Role,
		data.Branch,
		data.BuildTime,
		host_id)
	tx.Commit()

	updateHistory(at, data.State)

	return nil
}

//
// Get host id.
//
func getHostId(fqdn string) (int, error) {

	//
	// Ensure we have a DB-handle
	//
	if db == nil {
		return 0, errors.New("SetupDB not called")
	}

	var host_id int
	row := db.QueryRow("SELECT host_id FROM hosts WHERE fqdn = ?", fqdn)
	err := row.Scan(&host_id)

	if err == sql.ErrNoRows {
		return 0, nil
	}
	return host_id, err
}

//
// insert host.
//
func createHost(fqdn string) (int, error) {

	//
	// Ensure we have a DB-handle
	//
	if db == nil {
		return 0, errors.New("SetupDB not called")
	}

	_, err := db.Exec("INSERT INTO hosts(fqdn, state, last_seen, runtime, pinned, role, branch, build_time) VALUES (?, '', 0, 0, 0, '', '', 0)", fqdn)
	if err != nil {
		return 0, err
	}

	return getHostId(fqdn)
}

//
// update orphan hosts.
//
func updateOrphans() {

	//
	// Ensure we have a DB-handle
	//
	if db == nil {
		return
	}

	//
	// The threshold which marks the difference between
	// "current" and "orphaned"
	//
	// Here we set it to 4.5 days, which should be long
	// enough to cover any hosts that were powered-off over
	// a weekend.  (Friday + Saturday + Sunday + slack).
	//
	threshold := 3.5 * (24 * 60 * 60)

	_, err := db.Exec("UPDATE hosts SET state = 'orphaned' WHERE last_seen < ?", time.Now().Unix() - int64(threshold))
	if err != nil {
		return 
	}
}

//
// Purge Orphan hosts.
//
func purgeOrphans(days int) {

	//
	// Ensure we have a DB-handle
	//
	if db == nil {
		return
	}

	//
	// The threshold which marks the difference between
	// "current" and "orphaned"
	//
	// Here we set it to 4.5 days, which should be long
	// enough to cover any hosts that were powered-off over
	// a weekend.  (Friday + Saturday + Sunday + slack).
	//
	threshold := days * (24 * 60 * 60)

	_, err := db.Exec("DELETE FROM hosts WHERE last_seen < ? AND pinned = 0", time.Now().Unix() - int64(threshold))
	if err != nil {
		return 
	}
}

//
// Prune history.
//
func pruneHistory() {

	//
	// Ensure we have a DB-handle
	//
	if db == nil {
		return
	}

	history := 14

	var count int
	row := db.QueryRow("SELECT COUNT(*) FROM history")
	row.Scan(&count)

	if count <= history {
		return
	}

	purge := (count - history)



	sql_select := "SELECT id FROM history ORDER BY date LIMIT ?"

	//
	// Get the data.
	//
	stmt, err := db.Prepare(sql_select)
	if err != nil {
		return
	}

	rows, err := stmt.Query(purge)
	if err != nil {
		return
	}
	defer stmt.Close()
	defer rows.Close()

	ids := ""

	//
	// For each row in the result-set
	//
	for rows.Next() {
		var id int

		err := rows.Scan(&id)
		if err == nil {
			if strings.Compare(ids, "") == 0 {
				ids = strconv.Itoa(id)
			} else {
				ids = ids + ", " + strconv.Itoa(id)
			}
		}
	}

	sql_statment := "DELETE FROM history WHERE id IN (" + ids + ")"
	_, err = db.Exec(sql_statment)

	if err != nil {
		fmt.Printf("Error purging history: %s\n", err)		
	}
		
}

//
// update orphan hosts.
//
func updateHistory(date int64, state string) error {

	//
	// Ensure we have a DB-handle
	//
	if db == nil {
		return errors.New("SetupDB not called")
	}


	sql_lookup := ""
	if strings.Compare(db_type, "sqlite3") == 0 {
		sql_lookup = "SELECT id FROM history WHERE date = strftime('%Y/%m/%d', DATE(?, 'unixepoch'))"
	} else if strings.Compare(db_type, "mysql") == 0 {
		sql_lookup = "SELECT id FROM history WHERE date = from_unixtime(?, '%Y/%m/%d')"
	}

	id := 0

	row := db.QueryRow(sql_lookup, date)
	err := row.Scan(&id)

	sql_insert := ""
	if err == sql.ErrNoRows {
		if strings.Compare(db_type, "sqlite3") == 0 {
			sql_insert = "INSERT INTO history(date, failed, changed, unchanged) VALUES (strftime('%Y/%m/%d', DATE(?, 'unixepoch')), 0, 0, 0)"
		} else if strings.Compare(db_type, "mysql") == 0 {
			sql_insert = "INSERT INTO history(date, failed, changed, unchanged) VALUES (from_unixtime(?, '%Y/%m/%d'), 0, 0, 0)"
		}
		_, err := db.Exec(sql_insert, date)
		if err != nil {
			fmt.Printf("there - %s", err)
		}

		row := db.QueryRow(sql_lookup, date)
		err = row.Scan(&id)

		if err == sql.ErrNoRows {
			fmt.Printf("here - %s", err)
		}
	}


	failed := 0
	changed := 0
	unchanged := 0

	switch {
		case strings.Compare(state, "failed") == 0: 
			failed = 1
		case strings.Compare(state, "changed") == 0: 
			changed = 1
		case strings.Compare(state, "unchanged") == 0: 
			unchanged = 1
	}

	sql_update := "UPDATE history SET failed = failed + ?, changed = changed + ?, unchanged = unchanged + ? WHERE id = ?"

	_, err = db.Exec(sql_update, failed, changed, unchanged, id)
	if err != nil {
		return err
	}

	return nil
}

//
// Count the number of reports we have.
//
func countReports() (int, error) {

	//
	// Ensure we have a DB-handle
	//
	if db == nil {
		return 0, errors.New("SetupDB not called")
	}

	var count int
	row := db.QueryRow("SELECT COUNT(*) FROM reports")
	err := row.Scan(&count)
	return count, err
}

//
// Count the number of reports we have reaped.
//
func countUnchangedAndReapedReports() (int, error) {

	//
	// Ensure we have a DB-handle
	//
	if db == nil {
		return 0, errors.New("SetupDB not called")
	}

	var count int
	row := db.QueryRow("SELECT COUNT(*) FROM reports WHERE yaml_file='pruned'")
	err := row.Scan(&count)
	return count, err
}

//
// Return the contents of the YAML file which was associated
// with the given report-ID.
//
func getYAML(prefix string, id string) ([]byte, error) {

	//
	// Ensure we have a DB-handle
	//
	if db == nil {
		return nil, errors.New("SetupDB not called")
	}

	var path string
	row := db.QueryRow("SELECT yaml_file FROM reports WHERE id=?", id)
	err := row.Scan(&path)

	switch {
	case err == sql.ErrNoRows:
	case err != nil:
		return nil, errors.New("report not found")
	default:
	}

	//
	// Read the file content, first of all adding in the
	// prefix.
	//
	// (Because our reports are stored as relative paths
	// such as "$host/$time", rather than absolute paths
	// such as "reports/$host/$time".)
	//
	if len(path) > 0 {
		path = filepath.Join(prefix, path)
		content, err := ioutil.ReadFile(path)
		return content, err
	}
	return nil, errors.New("failed to find report with specified ID")
}

//
// Get the data which is shown on our index page
//
//  * The node-name.
//  * The status.
//  * The last-seen time.
//
func getIndexNodes() ([]PuppetRuns, error) {

	//
	// Our return-result.
	//
	var NodeList []PuppetRuns

	//
	// Ensure we have a DB-handle
	//
	if db == nil {
		return nil, errors.New("SetupDB not called")
	}

	sql := "SELECT fqdn, state, runtime, last_seen, branch, build_time, role, pinned FROM hosts;"
	
	//
	// Select the status - for nodes seen in the past 24 hours.
	//
	rows, err := db.Query(sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	//
	// For each row in the result-set
	//
	// Parse into a structure and add to the list.
	//
	for rows.Next() {
		var tmp PuppetRuns
		var at string
		var builtAt string
		var pinned int64

		err := rows.Scan(&tmp.Fqdn, &tmp.State, &tmp.Runtime, &at, &tmp.Branch, &builtAt , &tmp.Role, &pinned)
		if err != nil {
			return nil, err
		}

		tmp.Pinned = "No"
		if pinned == 1{
			tmp.Pinned = "Yes"
		}

		//
		// At this point `at` is a string containing seconds past
		// the epoch.
		//
		// We want to parse that into a string `At` which will
		// contain the literal time, and also the relative
		// time "Ago"
		//
		tmp.Epoch = at
		tmp.Ago = timeRelative(at)
		if strings.Compare(builtAt, "0") == 0 {
			tmp.BuiltAgo = "-"
			tmp.BuiltAt = "-"
		}else{
			tmp.BuiltAgo = timeRelative(builtAt)
			ib, _ := strconv.ParseInt(builtAt, 10, 64)
			tmp.BuiltAt = time.Unix(ib, 0).Format("2006-01-02 15:04:05")
		}

		//
		i, _ := strconv.ParseInt(at, 10, 64)
		tmp.At = time.Unix(i, 0).Format("2006-01-02 15:04:05")

		//
		// Add the new record.
		//
		NodeList = append(NodeList, tmp)

	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return NodeList, nil
}

//
// Return the state of our nodes.
//
func getStates() ([]PuppetState, error) {

	//
	// Get the nodes.
	//
	NodeList, err := getIndexNodes()
	if err != nil {
		return nil, err
	}

	//
	// Create a map to hold state.
	//
	states := make(map[string]int)

	//
	// Each known-state will default to being empty.
	//
	states["changed"] = 0
	states["unchanged"] = 0
	states["failed"] = 0
	states["orphaned"] = 0

	//
	// Count the nodes we encounter, such that we can
	// create a %-figure for each distinct-state.
	//
	var total int

	//
	// Count the states.
	//
	for _, o := range NodeList {
		states[o.State]++
		total++
	}

	//
	// Our return-result
	//
	var data []PuppetState

	//
	// Get the distinct keys/states in a sorted order.
	//
	var keys []string
	for name := range states {
		keys = append(keys, name)
	}
	sort.Strings(keys)

	//
	// Now for each key ..
	//
	for _, name := range keys {

		var tmp PuppetState
		tmp.State = name
		tmp.Count = states[name]
		tmp.Percentage = 0

		// Percentage has to be capped :)
		if total != 0 {
			c := float64(states[name])
			tmp.Percentage = (c / float64(total)) * 100
		}
		data = append(data, tmp)
	}

	return data, nil
}

//
// Get the summary-details of the runs against a given host
//
func getReports(fqdn string) ([]PuppetReportSummary, error) {

	//
	// Ensure we have a DB-handle
	//
	if db == nil {
		return nil, errors.New("SetupDB not called")
	}

	//
	// Select the status.
	//
	stmt, err := db.Prepare("SELECT id, fqdn, state, executed_at, runtime, failed, changed, total, yaml_file, branch, build_time, role FROM reports WHERE fqdn=? ORDER by executed_at DESC LIMIT 50")
	if err != nil {
		return nil, err
	}
	rows, err := stmt.Query(fqdn)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	defer rows.Close()

	//
	// We'll return a list of these hosts.
	//
	var NodeList []PuppetReportSummary

	//
	// For each row in the result-set
	//
	// Parse into a structure and add to the list.
	//
	for rows.Next() {
		var tmp PuppetReportSummary
		var at string
		var builtAt string
		err := rows.Scan(&tmp.ID, &tmp.Fqdn, &tmp.State, &at, &tmp.Runtime, &tmp.Failed, &tmp.Changed, &tmp.Total, &tmp.YamlFile, &tmp.Branch, &builtAt, &tmp.Role)
		if err != nil {
			return nil, err
		}

		//
		// At this point `at` is a string containing seconds past
		// the epoch.
		//
		// We want to parse that into a string `At` which will
		// contain the literal time, and also the relative
		// time "Ago"
		//
		tmp.Ago = timeRelative(at)
		if strings.Compare(builtAt, "0") == 0 {
			tmp.BuiltAgo = "-"
			tmp.BuiltAt = "-"
		}else{
			tmp.BuiltAgo = timeRelative(builtAt)
			ib, _ := strconv.ParseInt(builtAt, 10, 64)
			tmp.BuiltAt = time.Unix(ib, 0).Format("2006-01-02 15:04:05")
		}

		i, _ := strconv.ParseInt(at, 10, 64)
		tmp.At = time.Unix(i, 0).Format("2006-01-02 15:04:05")

		// Add the result of this fetch to our list.
		NodeList = append(NodeList, tmp)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	if len(NodeList) < 1 {
		return nil, errors.New("Failed to find reports for " + fqdn)

	}
	return NodeList, nil
}

//
// Get data for our stacked bar-graph
//
func getHistory() ([]PuppetHistory, error) {

	//
	// Ensure we have a DB-handle
	//
	if db == nil {
		return nil, errors.New("SetupDB not called")
	}

	//
	// Our result.
	//
	var res []PuppetHistory

	sql := "SELECT date, failed, changed, unchanged FROM history ORDER BY date"

	//
	// Get the data.
	//
	stmt, err := db.Prepare(sql)
	if err != nil {
		return nil, err
	}

	rows, err := stmt.Query()
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	defer rows.Close()

	//
	// For each row in the result-set
	//
	for rows.Next() {
		var d string
		var f int
		var c int
		var u int

		err := rows.Scan(&d, &f, &c, &u)
		if err != nil {
			return nil, errors.New("failed to scan SQL")
		}

		var x PuppetHistory
		x.Changed = strconv.Itoa(c)
		x.Unchanged = strconv.Itoa(u)
		x.Failed = strconv.Itoa(f)
		x.Date = d

		res = append(res, x)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return res, err

}

//
// Prune old reports
//
// We have to find the old reports, individually, so we can unlink the
// copy of the on-disk YAML, but once we've done that we can delete them
// as a group.
//
func pruneReports(prefix string, days int, verbose bool) error {

	//
	// Ensure we have a DB-handle
	//
	if db == nil {
		return errors.New("SetupDB not called")
	}

	//
	// Convert our query into something useful.
	//
	expire_time := days * (24 * 60 * 60)
	now := time.Now().Unix()

	//
	// Find things that are old.
	//
	sql := "SELECT id,yaml_file FROM reports WHERE ( ? - executed_at ) > ?"

	find, err := db.Prepare(sql)
	if err != nil {
		return err
	}

	//
	// Remove old reports, en mass.
	//
	sql = "DELETE FROM reports WHERE ( ( ? - executed_at ) > ? )"
	
	clean, err := db.Prepare(sql)
	if err != nil {
		return err
	}

	//
	// Find the old reports.
	//
	rows, err := find.Query(now, expire_time)
	if err != nil {
		return err
	}
	defer find.Close()
	defer clean.Close()
	defer rows.Close()

	//
	// For each row in the result-set
	//
	// Parse into "id" + "path", then remove the path from disk.
	//
	for rows.Next() {
		var id string
		var path string

		err = rows.Scan(&id, &path)
		if err == nil {

			//
			// Convert the path to a qualified one,
			// rather than one relative to our report-dir.
			//
			path = filepath.Join(prefix, path)
			if verbose {
				fmt.Printf("Removing ID:%s - %s\n", id, path)
			}

			//
			//  Remove the file from-disk
			//
			//  We won't care if this fails, it might have
			// been removed behind our back or failed to
			// be uploaded in the first place.
			//
			os.Remove(path)
		}
	}
	err = rows.Err()
	if err != nil {
		return err
	}

	//
	//  Now cleanup the old records
	//
	_, err = clean.Exec(now, expire_time)
	if err != nil {
		return err
	}

	return nil
}

//
// Prune reports from nodes which are unchanged.
//
// We have to find the old reports, individually, so we can unlink the
// copy of the on-disk YAML, but once we've done that we can delete them
// as a group.
//
func pruneUnchanged(prefix string, verbose bool) error {

	//
	// Ensure we have a DB-handle
	//
	if db == nil {
		return errors.New("SetupDB not called")
	}

	//
	// Find unchanged reports.
	//
	find, err := db.Prepare("SELECT id,yaml_file FROM reports WHERE state='unchanged'")
	if err != nil {
		return err
	}

	//
	// Prepare to update them all.
	//
	clean, err := db.Prepare("UPDATE reports SET yaml_file='pruned' WHERE state='unchanged'")
	if err != nil {
		return err
	}

	//
	// Find the reports.
	//
	rows, err := find.Query()
	if err != nil {
		return err
	}
	defer find.Close()
	defer clean.Close()
	defer rows.Close()

	//
	// For each row in the result-set
	//
	// Parse into "id" + "path", then remove the path from disk.
	//
	for rows.Next() {
		var id string
		var path string

		err = rows.Scan(&id, &path)
		if err == nil {

			//
			// Convert the path to a qualified one,
			// rather than one relative to our report-dir.
			//
			path = filepath.Join(prefix, path)
			if verbose {
				fmt.Printf("Removing ID:%s - %s\n", id, path)
			}

			//
			//  Remove the file from-disk
			//
			//  We won't care if this fails, it might have
			// been removed behind our back or failed to
			// be uploaded in the first place.
			//
			os.Remove(path)
		}
	}
	err = rows.Err()
	if err != nil {
		return err
	}

	//
	//  Now cleanup the old records
	//
	_, err = clean.Exec()
	if err != nil {
		return err
	}

	return nil
}

func pruneOrphaned(prefix string, verbose bool) error {

	NodeList, err := getIndexNodes()
	if err != nil {
		return err
	}

	for _, entry := range NodeList {

		if entry.State == "orphaned" {
			if verbose {
				fmt.Printf("Orphaned host: %s\n", entry.Fqdn)
			}

			//
			// Find all reports that refer to this host.
			//
			rows, err := db.Query("SELECT yaml_file FROM reports WHERE fqdn=?", entry.Fqdn)
			if err != nil {
				return err
			}
			defer rows.Close()

			for rows.Next() {
				var tmp string
				err = rows.Scan(&tmp)
				if err != nil {
					return err
				}

				//
				// Convert the path to a qualified one,
				// rather than one relative to our report-dir.
				//
				path := filepath.Join(prefix, tmp)
				if verbose {
					fmt.Printf("\tRemoving: %s\n", path)
				}

				//
				//  Remove the file from-disk
				//
				//  We won't care if this fails, it might have
				// been removed behind our back or failed to
				// be uploaded in the first place.
				//
				os.Remove(path)
			}

			//
			// Now remove the report-entries
			//
			clean, err := db.Prepare("DELETE FROM reports WHERE fqdn=?")
			if err != nil {
				return err
			}
			defer clean.Close()
			_, err = clean.Exec(entry.Fqdn)
			if err != nil {
				return err
			}

		}

	}

	return nil
}

