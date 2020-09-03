//
// Launch our HTTP-server for both consuming reports, and viewing them.
//

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"io/ioutil"
	"crypto/rand"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/subcommands"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/robfig/cron"
	_ "github.com/skx/golang-metrics"
)

//
// ReportPrefix is the path beneath which reports are stored.
//
var ReportPrefix = "reports"

var asyncUploadJobs = make(chan string)

//
// Exists is a utility method to determine whether a file/directory exists.
//
func Exists(name string) bool {
	_, err := os.Stat(name)
	return !os.IsNotExist(err)
}

//
// APIState is the handler for the HTTP end-point
//
//	 GET /api/state/$state
//
// This only will return plain-text by default, but JSON and XML are both
// possible via the `Accept:` header or `?accept=XX` parameter.
//
func APIState(res http.ResponseWriter, req *http.Request) {

	var (
		status int
		err    error
	)
	defer func() {
		if nil != err {
			http.Error(res, err.Error(), status)
		}
	}()

	//
	// Get the state the user is interested in.
	//
	vars := mux.Vars(req)
	state := vars["state"]

	//
	// Ensure we received a parameter.
	//
	if len(state) < 1 {
		status = http.StatusNotFound
		err = errors.New("missing 'state' parameter")
		return
	}

	//
	// Test the supplied state is valid.
	//
	switch state {
	case "changed":
	case "unchanged":
	case "failed":
	case "orphaned":
	default:
		err = errors.New("invalid state supplied")
		status = http.StatusInternalServerError
		return
	}

	//
	// Get the nodes.
	//
	NodeList, err := getIndexNodes()
	if err != nil {
		status = http.StatusInternalServerError
		return
	}

	//
	// The result
	//
	var result []string

	//
	// Add the hosts in the correct users' preferred state.
	//
	for _, o := range NodeList {
		if o.State == state {
			result = append(result, o.Fqdn)
		}
	}

	//
	// What kind of reply should we send?
	//
	// Accept either a "?accept=XXX" URL-parameter, or
	// the Accept HEADER in the HTTP request
	//
	accept := req.FormValue("accept")
	if len(accept) < 1 {
		accept = req.Header.Get("Accept")
	}

	switch accept {
	case "text/plain":
		res.Header().Set("Content-Type", "text/plain")

		for _, o := range result {
			fmt.Fprintf(res, "%s\n", o)
		}
	case "application/xml":
		x, err := xml.MarshalIndent(result, "", "  ")
		if err != nil {
			status = http.StatusInternalServerError
			return
		}

		res.Header().Set("Content-Type", "application/xml")
		res.Write(x)
	default:

		//
		// Convert the string-array to JSON, and return it.
		//
		res.Header().Set("Content-Type", "application/json")

		if len(result) > 0 {
			out, _ := json.Marshal(result)
			fmt.Fprintf(res, "%s", out)
		} else {
			fmt.Fprintf(res, "[]")
		}

	}

}

//
// RadiatorView is the handler for the HTTP end-point
//
//	 GET /radiator/
//
// It will respond in either HTML, JSON, or XML depending on the
// Accepts-header which is received.
//
func RadiatorView(res http.ResponseWriter, req *http.Request) {

	var (
		status int
		err    error
	)
	defer func() {
		if nil != err {
			http.Error(res, err.Error(), status)
		}
	}()

	// anonymous struct
	type Pagedata struct {
		States    []PuppetState
		Urlprefix string
	}

	//
	// Get the state of the nodes.
	//
	data, err := getStates()
	if err != nil {
		status = http.StatusInternalServerError
		return
	}

	//
	// Sum up our known-nodes.
	//
	total := 0
	for i := range data {
		total += data[i].Count
	}

	//
	// Add in the total count of nodes.
	//
	var tmp PuppetState
	tmp.State = "Total"
	tmp.Count = total
	tmp.Percentage = 0
	data = append(data, tmp)

	// genereic template args
	var x Pagedata
	x.States = data
	x.Urlprefix = templateArgs.urlprefix

	//
	// What kind of reply should we send?
	//
	// Accept either a "?accept=XXX" URL-parameter, or
	// the Accept HEADER in the HTTP request
	//
	accept := req.FormValue("accept")
	if len(accept) < 1 {
		accept = req.Header.Get("Accept")
	}

	switch accept {
	case "application/json":
		js, err := json.Marshal(data)

		if err != nil {
			status = http.StatusInternalServerError
			return
		}
		res.Header().Set("Content-Type", "application/json")
		res.Write(js)

	case "application/xml":
		x, err := xml.MarshalIndent(data, "", "  ")
		if err != nil {
			status = http.StatusInternalServerError
			return
		}

		res.Header().Set("Content-Type", "application/xml")
		res.Write(x)
	default:
		//
		// Load our template resource.
		//
		tmpl, err := getResource("data/radiator.template")
		if err != nil {
			fmt.Fprint(res, err.Error())
			return
		}

		//
		//  Load our template, from the resource.
		//
		src := string(tmpl)
		t := template.Must(template.New("tmpl").Parse(src))

		//
		// Execute the template into our buffer.
		//
		buf := &bytes.Buffer{}
		err = t.Execute(buf, x)

		//
		// If there were errors, then show them.
		if err != nil {
			fmt.Fprint(res, err.Error())
			return
		}

		//
		// Otherwise write the result.
		//
		buf.WriteTo(res)
	}
}

//
// ReportSubmissionHandler is the handler for the HTTP end-point:
//
//	POST /upload
//
// The input is read, and parsed as Yaml, and assuming that succeeds
// then the data is written beneath ./reports/$hostname/$timestamp
// and a summary-record is inserted into our SQLite database.
//
//
func ReportSubmissionHandler(res http.ResponseWriter, req *http.Request) {
	var (
		status int
		err    error
	)
	defer func() {
		if nil != err {
			http.Error(res, err.Error(), status)

			// Don't spam stdout when running test-cases.
			if flag.Lookup("test.v") == nil {
				fmt.Printf("Error: %s\n", err.Error())
			}
		}
	}()

	//
	// Ensure this was a POST-request
	//
	if req.Method != "POST" {
		err = errors.New("must be called via HTTP-POST")
		status = http.StatusInternalServerError
		return
	}

	//
	// Read the body of the request.
	//
	content, err := ioutil.ReadAll(req.Body)
	if err != nil {
		status = http.StatusInternalServerError
		return
	}

	//
	// Parse the YAML into something we can work with.
	//
	report, err := ParsePuppetReport(content)
	if err != nil {
		status = http.StatusInternalServerError
		return
	}

	//
	// Create a report directory for this host, unless it already exists.
	//
	dir := filepath.Join(ReportPrefix, report.Fqdn)
	if !Exists(dir) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			status = http.StatusInternalServerError
			return
		}
	}

	//
	// Does this report already exist?  This shouldn't happen
	// in a usual setup, but will happen if you're repeatedly
	// importing reports manually from a puppet-server.
	//
	// (Which is something you might do when testing the dashboard.)
	//
	path := filepath.Join(dir, report.Hash)

	if Exists(path) {
		fmt.Fprintf(res, "Ignoring duplicate submission")
		return
	}

	//
	// Create the new report-file, on-disk.
	//
	err = ioutil.WriteFile(path, content, 0644)
	if err != nil {
		status = http.StatusInternalServerError
		return
	}

	//
	// Record that report in our SQLite database
	//
	relativePath := filepath.Join(report.Fqdn, report.Hash)

	addDB(report, relativePath)

	//
	// Show something to the caller.
	//
	out := fmt.Sprintf("{\"host\":\"%s\"}", report.Fqdn)
	fmt.Fprint(res, string(out))

}

//
// AsyncReportSubmissionHandler is the handler for the HTTP end-point:
//
//	POST /async/upload
//
// The input is read, and parsed as Yaml, and assuming that succeeds
// then the data is written beneath ./reports/$hostname/$timestamp
// and a summary-record is inserted into our SQLite database.
//
// the work is handled by a queue of workers to respond back to puppet faster
//
//
func AsyncReportSubmissionHandler(res http.ResponseWriter, req *http.Request) {
	var (
		status int
		err    error
	)
	defer func() {
		if nil != err {
			http.Error(res, err.Error(), status)

			// Don't spam stdout when running test-cases.
			if flag.Lookup("test.v") == nil {
				fmt.Printf("Error: %s\n", err.Error())
			}
		}
	}()

	//
	// Ensure this was a POST-request
	//
	if req.Method != "POST" {
		err = errors.New("must be called via HTTP-POST")
		status = http.StatusInternalServerError
		return
	}

	//
	// Read the body of the request.
	//
	content, err := ioutil.ReadAll(req.Body)
	if err != nil {
		status = http.StatusInternalServerError
		return
	}


	b := make([]byte, 16)
	_, errRead := rand.Read(b)
	if errRead != nil {
	    fmt.Printf("Error: %s\n", errRead.Error())
	}
	uuid := fmt.Sprintf("%x-%x-%x-%x-%x",
	    b[0:4], b[4:6], b[6:8], b[8:10], b[10:])


	uuid = "puppet-summary-tmp-" + uuid

	path := filepath.Join("/tmp/", uuid)

	//
	// Create the temp file, on-disk.
	//
	err = ioutil.WriteFile(path, content, 0644)
	if err != nil {
		status = http.StatusInternalServerError
		return
	}

	asyncUploadJobs <- uuid

	//
	// Show something to the caller.
	//
	out := fmt.Sprintf("{\"job-queued\":\"%s\"}", uuid)
	fmt.Fprint(res, string(out))
}

func AsyncReportSubmissionWorker(id int, jobs <-chan string) {
    for j := range jobs {
        fmt.Println("worker", id, "started  job", j)
        AsyncReportSubmissionSaver(j)
        fmt.Println("worker", id, "finished job", j)
    }
}

func AsyncReportSubmissionSaver(uuid string){

	// read file uuid into content
	pathTemp := filepath.Join("/tmp/", uuid)

	//
	// Read the temp file.
	//
	content, err := ioutil.ReadFile(pathTemp)
    if err != nil {
		fmt.Printf("Failed to read temp file")
		return
	}

	//
	// Parse the YAML into something we can work with.
	//
	report, err := ParsePuppetReport(content)
	if err != nil {
		fmt.Printf("Failed to parse Yaml")
		return
	}

	//
	// Create a report directory for this host, unless it already exists.
	//
	dir := filepath.Join(ReportPrefix, report.Fqdn)
	if !Exists(dir) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			fmt.Printf("Failed to create host directory")
			return
		}
	}

	//
	// Does this report already exist?  This shouldn't happen
	// in a usual setup, but will happen if you're repeatedly
	// importing reports manually from a puppet-server.
	//
	// (Which is something you might do when testing the dashboard.)
	//
	path := filepath.Join(dir, report.Hash)

	if Exists(path) {
		fmt.Printf("Ignoring duplicate submission")
		return
	}

	//
	// Create the new report-file, on-disk.
	//
	err = ioutil.WriteFile(path, content, 0644)
	if err != nil {
		fmt.Printf("Error saving file")
		return
	}

	//
	// Record that report in our SQLite database
	//
	relativePath := filepath.Join(report.Fqdn, report.Hash)

	addDB(report, relativePath)

	e := os.Remove("/tmp/" + uuid) 
    if e != nil { 
        fmt.Printf("Error deleteing file: " + uuid) 
    } 

}

//
// SearchHandler is the handler for the HTTP end-point:
//
//	POST /search
//
// We perform a search for nodes matching a given pattern.  The comparison
// is a regular substring-match, rather than a regular expression.
//
func SearchHandler(res http.ResponseWriter, req *http.Request) {
	var (
		status int
		err    error
	)
	defer func() {
		if nil != err {
			http.Error(res, err.Error(), status)

			// Don't spam stdout when running test-cases.
			if flag.Lookup("test.v") == nil {
				fmt.Printf("Error: %s\n", err.Error())
			}
		}
	}()

	//
	// Ensure this was a POST-request
	//
	if req.Method != "POST" {
		err = errors.New("must be called via HTTP-POST")
		status = http.StatusInternalServerError
		return
	}

	//
	// Get the term from the form.
	//
	req.ParseForm()
	term := req.FormValue("term")

	//
	// Ensure we have a term.
	//
	if len(term) < 1 {
		err = errors.New("missing search term")
		status = http.StatusInternalServerError
		return
	}

	//
	// Annoying struct to allow us to populate our template
	// with both the matching nodes, and the term used for the search
	//
	type Pagedata struct {
		Nodes     []PuppetRuns
		Term      string
		Urlprefix string
	}

	//
	// Get all known nodes.
	//
	NodeList, err := getIndexNodes()
	if err != nil {
		status = http.StatusInternalServerError
		return
	}

	//
	// Populate this structure with the search-term
	//
	var x Pagedata
	x.Term = term
	x.Urlprefix = templateArgs.urlprefix

	//
	// Add in any nodes which match our term.
	//
	for _, o := range NodeList {
		if strings.Contains(o.Fqdn, term) {
			x.Nodes = append(x.Nodes, o)
		}
	}

	//
	// Load our template source.
	//
	tmpl, err := getResource("data/results.template")
	if err != nil {
		fmt.Fprint(res, err.Error())
		return
	}

	//
	//  Load our template, from the resource.
	//
	src := string(tmpl)
	t := template.Must(template.New("tmpl").Parse(src))

	//
	// Execute the template into our buffer.
	//
	buf := &bytes.Buffer{}
	err = t.Execute(buf, x)

	//
	// If there were errors, then show them.
	if err != nil {
		fmt.Fprint(res, err.Error())
		return
	}

	//
	// Otherwise write the result.
	//
	buf.WriteTo(res)
}

//
// ReportHandler is the handler for the HTTP end-point
//
//	 GET /report/NN
//
// It will respond in either HTML, JSON, or XML depending on the
// Accepts-header which is received.
//
func ReportHandler(res http.ResponseWriter, req *http.Request) {
	var (
		status int
		err    error
	)
	defer func() {
		if nil != err {
			http.Error(res, err.Error(), status)

			// Don't spam stdout when running test-cases.
			if flag.Lookup("test.v") == nil {
				fmt.Printf("Error: %s\n", err.Error())
			}
		}
	}()

	//
	// Get the node name we're going to show.
	//
	vars := mux.Vars(req)
	id := vars["id"]

	//
	// Ensure we received a parameter.
	//
	if len(id) < 1 {
		status = http.StatusNotFound
		err = errors.New("missing 'id' parameter")
		return
	}

	//
	// If the ID is non-numeric we're in trouble.
	//
	reg, _ := regexp.Compile("^([0-9]+)$")
	if !reg.MatchString(id) {
		status = http.StatusInternalServerError
		err = errors.New("the report ID must be numeric")
		return
	}

	//
	// Get the content.
	//
	content, err := getYAML(ReportPrefix, id)
	if err != nil {
		status = http.StatusInternalServerError
		return
	}

	// need generic struct
	type Pagedata struct {
		Report    PuppetReport
		Urlprefix string
	}

	//
	// Parse it
	//
	report, err := ParsePuppetReport(content)
	if err != nil {
		status = http.StatusInternalServerError
		return
	}

	var x Pagedata
	x.Report = report
	x.Urlprefix = templateArgs.urlprefix

	//
	// Accept either a "?accept=XXX" URL-parameter, or
	// the Accept HEADER in the HTTP request
	//
	accept := req.FormValue("accept")
	if len(accept) < 1 {
		accept = req.Header.Get("Accept")
	}

	switch accept {
	case "application/json":
		js, err := json.Marshal(report)

		if err != nil {
			status = http.StatusInternalServerError
			return
		}
		res.Header().Set("Content-Type", "application/json")
		res.Write(js)

	case "application/xml":
		x, err := xml.MarshalIndent(report, "", "  ")
		if err != nil {
			status = http.StatusInternalServerError
			return
		}

		res.Header().Set("Content-Type", "application/xml")
		res.Write(x)
	default:

		//
		// Load our template resource.
		//
		tmpl, err := getResource("data/report.template")
		if err != nil {
			fmt.Fprint(res, err.Error())
			return
		}

		//
		// Helper to allow a float to be truncated
		// to two/three digits.
		//
		funcMap := template.FuncMap{

			"truncate": func(s string) string {

				//
				// Parse as a float.
				//
				f, _ := strconv.ParseFloat(s, 64)

				//
				// Output to a truncated string
				//
				s = fmt.Sprintf("%.2f", f)
				return s
			},
		}

		//
		//  Load our template, from the resource.
		//
		src := string(tmpl)
		t := template.Must(template.New("tmpl").Funcs(funcMap).Parse(src))

		//
		// Execute the template into our buffer.
		//
		buf := &bytes.Buffer{}
		err = t.Execute(buf, x)

		//
		// If there were errors, then show them.
		if err != nil {
			fmt.Fprint(res, err.Error())
			return
		}

		//
		// Otherwise write the result.
		//
		buf.WriteTo(res)
	}
}

//
// NodeHandler is the handler for the HTTP end-point
//
//	 GET /node/$FQDN
//
// It will respond in either HTML, JSON, or XML depending on the
// Accepts-header which is received.
//
func NodeHandler(res http.ResponseWriter, req *http.Request) {
	var (
		status int
		err    error
	)
	defer func() {
		if nil != err {
			http.Error(res, err.Error(), status)

			// Don't spam stdout when running test-cases.
			if flag.Lookup("test.v") == nil {
				fmt.Printf("Error: %s\n", err.Error())
			}
		}
	}()

	//
	// Get the node name we're going to show.
	//
	vars := mux.Vars(req)
	fqdn := vars["fqdn"]

	//
	// Ensure we received a parameter.
	//
	if len(fqdn) < 1 {
		status = http.StatusNotFound
		err = errors.New("missing 'fqdn' parameter")
		return
	}

	//
	// Get the reports
	//
	reports, err := getReports(fqdn)

	//
	// Ensure that something was present.
	//
	if (reports == nil) || (len(reports) < 1) {
		status = http.StatusNotFound
		return
	}

	//
	// Handle error(s)
	//
	if err != nil {
		status = http.StatusInternalServerError
		return
	}

	//
	// Annoying struct to allow us to populate our template
	// with both the reports and the fqdn of the host.
	//
	type Pagedata struct {
		Fqdn      string
		Nodes     []PuppetReportSummary
		Urlprefix string
	}

	//
	// Populate this structure.
	//
	var x Pagedata
	x.Nodes = reports
	x.Fqdn = fqdn
	x.Urlprefix = templateArgs.urlprefix

	//
	// Accept either a "?accept=XXX" URL-parameter, or
	// the Accept HEADER in the HTTP request
	//
	accept := req.FormValue("accept")
	if len(accept) < 1 {
		accept = req.Header.Get("Accept")
	}

	switch accept {
	case "application/json":
		js, err := json.Marshal(reports)

		if err != nil {
			status = http.StatusInternalServerError
			return
		}
		res.Header().Set("Content-Type", "application/json")
		res.Write(js)

	case "application/xml":
		x, err := xml.MarshalIndent(reports, "", "  ")
		if err != nil {
			status = http.StatusInternalServerError
			return
		}

		res.Header().Set("Content-Type", "application/xml")
		res.Write(x)
	default:

		//
		// Load our template resource.
		//
		tmpl, err := getResource("data/node.template")
		if err != nil {
			fmt.Fprint(res, err.Error())
			return
		}

		funcMap := template.FuncMap{

			"incr": func(d int) string {

				//
				// Return the incremented string.
				//
				s := fmt.Sprintf("%d", (d + 1))
				return s
			},
		}

		//
		//  Load our template, from the resource.
		//
		src := string(tmpl)
		t := template.Must(template.New("tmpl").Funcs(funcMap).Parse(src))

		//
		// Execute the template into our buffer.
		//
		buf := &bytes.Buffer{}
		err = t.Execute(buf, x)

		//
		// If there were errors, then show them.
		if err != nil {
			fmt.Fprint(res, err.Error())
			return
		}

		//
		// Otherwise write the result.
		//
		buf.WriteTo(res)
	}
}

//
// IconHandler is the handler for the HTTP end-point
//
//	 GET /favicon.ico
//
// It will server an embedded binary resource.
//
func IconHandler(res http.ResponseWriter, req *http.Request) {

	serveStatic(res, req, "data/favicon.ico", "image/vnd.microsoft.icon")
}

// CSSPath is the handler for all the CSS files beneath /css.
func CSSPath(res http.ResponseWriter, req *http.Request) {

	//
	// Get the path we're going to serve.
	//
	vars := mux.Vars(req)
	path := vars["path"]

	//
	// Ensure we received a path.
	//
	if len(path) < 1 {
		res.WriteHeader(http.StatusNotFound)
		fmt.Fprint(res, "The request you made pointed to a missing resource")
		return
	}

	//
	// Serve it
	//
	serveStatic(res, req, "data/css/"+path, "text/css")
}

// serveStatic serves a static path, with the given MIME type.
func serveStatic(res http.ResponseWriter, req *http.Request, path string, mime string) {

	// Load the asset.
	data, err := getResource(path)
	if err != nil {
		res.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(res, "Error loading the resource you requested: %s", err.Error())
		return
	}

	res.Header().Set("Content-Type", mime)
	res.Write(data)
}

//
// JavascriptPath is the handler for all the javascript files beneath /js.
// It will serve an embedded javascript resource.
//
func JavascriptPath(res http.ResponseWriter, req *http.Request) {

	//
	// Get the path we're going to serve.
	//
	vars := mux.Vars(req)
	path := vars["path"]

	//
	// Ensure we received a path.
	//
	if len(path) < 1 {
		res.WriteHeader(http.StatusNotFound)
		fmt.Fprint(res, "The request you made pointed to a missing resource")
		return
	}

	//
	// Serve it
	//
	serveStatic(res, req, "data/js/"+path, "application/javascript")
}

//
// IndexHandler is the handler for the HTTP end-point
//
//	 GET /
//
// It will respond in either HTML, JSON, or XML depending on the
// Accepts-header which is received.
//
func IndexHandler(res http.ResponseWriter, req *http.Request) {
	var (
		status int
		err    error
	)
	defer func() {
		if nil != err {
			http.Error(res, err.Error(), status)

			// Don't spam stdout when running test-cases.
			if flag.Lookup("test.v") == nil {
				fmt.Printf("Error: %s\n", err.Error())
			}
		}
	}()

	//
	// Annoying struct to allow us to populate our template
	// with both the nodes in the list, and the graph-data
	//
	type Pagedata struct {
		Graph     []PuppetHistory
		Nodes     []PuppetRuns
		Urlprefix string
	}

	//
	// Get the nodes to show on our front-page
	//
	NodeList, err := getIndexNodes()
	if err != nil {
		status = http.StatusInternalServerError
		return
	}

	//
	// Get the graph-data
	//
	graphs, err := getHistory()
	if err != nil {
		status = http.StatusInternalServerError
		return
	}

	//
	// Populate this structure.
	//
	var x Pagedata
	x.Graph = graphs
	x.Nodes = NodeList
	x.Urlprefix = templateArgs.urlprefix

	//
	// Accept either a "?accept=XXX" URL-parameter, or
	// the Accept HEADER in the HTTP request
	//
	accept := req.FormValue("accept")
	if len(accept) < 1 {
		accept = req.Header.Get("Accept")
	}

	switch accept {
	case "application/json":
		js, err := json.Marshal(NodeList)

		if err != nil {
			status = http.StatusInternalServerError
			return
		}
		res.Header().Set("Content-Type", "application/json")
		res.Write(js)

	case "application/xml":
		x, err := xml.MarshalIndent(NodeList, "", "  ")
		if err != nil {
			status = http.StatusInternalServerError
			return
		}

		res.Header().Set("Content-Type", "application/xml")
		res.Write(x)
	default:

		//
		// Load our template source.
		//
		tmpl, err := getResource("data/index.template")
		if err != nil {
			fmt.Fprint(res, err.Error())
			return
		}

		//
		//  Load our template, from the resource.
		//
		src := string(tmpl)
		t := template.Must(template.New("tmpl").Parse(src))

		//
		// Execute the template into our buffer.
		//
		buf := &bytes.Buffer{}
		err = t.Execute(buf, x)

		//
		// If there were errors, then show them.
		if err != nil {
			fmt.Fprint(res, err.Error())
			return
		}

		//
		// Otherwise write the result.
		//
		buf.WriteTo(res)
	}
}

//
//  Entry-point.
//
func serve(settings serveCmd) {
	templateArgs.urlprefix = settings.urlprefix

	//
	// Preserve our prefix
	//
	ReportPrefix = settings.prefix

	//
	// Create a new router and our route-mappings.
	//
	router := mux.NewRouter()

	//
	// API end-points
	//
	router.HandleFunc("/api/state/{state}/", APIState).Methods("GET")
	router.HandleFunc("/api/state/{state}", APIState).Methods("GET")

	//
	//
	//
	router.HandleFunc("/radiator/", RadiatorView).Methods("GET")
	router.HandleFunc("/radiator", RadiatorView).Methods("GET")

	//
	// Upload a new report.
	//
	router.HandleFunc("/upload/", ReportSubmissionHandler).Methods("POST")
	router.HandleFunc("/upload", ReportSubmissionHandler).Methods("POST")

	//
	// Upload a new report.
	//
	router.HandleFunc("/async/upload/", AsyncReportSubmissionHandler).Methods("POST")
	router.HandleFunc("/async/upload", AsyncReportSubmissionHandler).Methods("POST")

	//
	// Search nodes.
	//
	router.HandleFunc("/search/", SearchHandler).Methods("POST")
	router.HandleFunc("/search", SearchHandler).Methods("POST")

	//
	// Show the recent state of a node.
	//
	router.HandleFunc("/node/{fqdn}/", NodeHandler).Methods("GET")
	router.HandleFunc("/node/{fqdn}", NodeHandler).Methods("GET")

	//
	// Show "everything" about a given run.
	//
	router.HandleFunc("/report/{id}/", ReportHandler).Methods("GET")
	router.HandleFunc("/report/{id}", ReportHandler).Methods("GET")

	//
	// Handle a display of all known nodes, and their last state.
	//
	router.HandleFunc("/", IndexHandler).Methods("GET")

	//
	// Static-Files
	//
	router.HandleFunc("/favicon.ico", IconHandler).Methods("GET")
	router.HandleFunc("/js/{path}", JavascriptPath).Methods("GET")
	router.HandleFunc("/css/{path}", CSSPath).Methods("GET")

	//
	// Bind the router.
	//
	http.Handle("/", router)

	//
	// Show where we'll bind
	//
	bind := fmt.Sprintf("%s:%d", settings.bindHost, settings.bindPort)
	fmt.Printf("Launching the server on http://%s\n", bind)

	//
	// Wire up logging.
	//
	loggedRouter := handlers.LoggingHandler(os.Stdout, router)

	//
	// We want to make sure we handle timeouts effectively by using
	// a non-default http-server
	//
	srv := &http.Server{
		Addr:         bind,
		Handler:      loggedRouter,
		ReadTimeout:  time.Duration(settings.readTimeout) * time.Second,
		WriteTimeout: time.Duration(settings.writeTimeout) * time.Second,
	}

	//
	// Launch the server.
	//
	err := srv.ListenAndServe()
	if err != nil {
		fmt.Printf("\nError: %s\n", err.Error())
	}
}

//
// The options set by our command-line flags.
//
type serveCmd struct {
	autoPrune    bool
	bindHost     string
	bindPort     int
	readTimeout  int
	writeTimeout int
	dbFile       string
	dbType       string
	prefix       string
	urlprefix    string
}

type templateOptions struct {
	urlprefix string
}

var templateArgs templateOptions

//
// Glue
//
func (*serveCmd) Name() string     { return "serve" }
func (*serveCmd) Synopsis() string { return "Launch the HTTP server." }
func (*serveCmd) Usage() string {
	return `serve [options]:
  Launch the HTTP server for receiving reports & viewing them
`
}

//
// Flag setup
//
func (p *serveCmd) SetFlags(f *flag.FlagSet) {
	f.IntVar(&p.bindPort, "port", 3001, "The port to bind upon.")
	f.IntVar(&p.readTimeout, "read-timeout", 5, "Timeout from when the connection is accepted to when the request body is fully read")
	f.IntVar(&p.writeTimeout, "write-timeout", 10, "Timeout from the end of the request header read to the end of the response write")
	f.BoolVar(&p.autoPrune, "auto-prune", false, "Prune reports automatically, once per week.")
	f.StringVar(&p.bindHost, "host", "127.0.0.1", "The IP to listen upon.")
	f.StringVar(&p.dbType, "db-type", "sqlite3", "The SQLite database to use.")
	f.StringVar(&p.dbFile, "db-file", "ps.db", "The SQLite database to use or DSN for mysql (`db_user:db_password@tcp(db_hostname:db_port)/db_name`)")
	f.StringVar(&p.prefix, "prefix", "./reports/", "The prefix to the local YAML hierarchy.")
	f.StringVar(&p.urlprefix, "urlprefix", "", "The URL prefix for serving behind a proxy.")
}

//
// Entry-point.
//
func (p *serveCmd) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {

	//
	// Setup the database, by opening a handle, and creating it if
	// missing.
	//
	SetupDB(p.dbType, p.dbFile)

	//
	// Create a cron scheduler
	//
	c := cron.New()

	//
	// If autoprune
	//
	if p.autoPrune {


		//
		//  Every seven days prune the reports.
		//
		c.AddFunc("@daily", func() {
			fmt.Printf("Automatically pruning old reports\n")
			pruneReports(p.prefix, 14, false)
		})

	}


	//
	//  Every hour update the orphan status.
	//
	c.AddFunc("@hourly", func() {
		fmt.Printf("Updating orphans\n")
		updateOrphans()
	})

	//
	//  Every day clean unpinned orphan hosts.
	//
	c.AddFunc("@daily", func() {
		fmt.Printf("Purging orphans\n")
		purgeOrphans(30)
	})

	//
	//  Every day purge history over 14 days.
	//
	c.AddFunc("@daily", func() {
		fmt.Printf("Purging history\n")
		pruneHistory()
	})

	//
	// Launch the cron-scheduler.
	//
	c.Start()


	
	for w := 1; w <= 4; w++ {
        go AsyncReportSubmissionWorker(w, asyncUploadJobs)
    }

	//
	// Start the server
	//
	serve(*p)

	//
	// All done.
	//
	return subcommands.ExitSuccess
}
