package main

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

type ReportSummary struct {
	FileName   string
	Date       string
	Passed     string
	Failed     string
	Skipped    string
	FailedTCs  []string
	FailReason string
}

func main() {
	dirs := []string{"../report_full", "../report_smoke"}

	outFile, err := os.Create("last7daysreport.txt")
	if err != nil {
		fmt.Println("Failed to create output file:", err)
		return
	}
	defer outFile.Close()

	writer := io.MultiWriter(os.Stdout, outFile)

	for _, dir := range dirs {
		reports, err := readLastNReports(dir, 7)
		if err != nil {
			fmt.Fprintln(writer, "Error:", err)
			continue
		}
		printTable(writer, dir, reports)
	}

	fmt.Println("\n📄 Report written to: last7daysreport.txt")
}

/* ------------------ CORE LOGIC ------------------ */

func readLastNReports(dir string, n int) ([]*ReportSummary, error) {
	files := []string{}

	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if strings.HasSuffix(d.Name(), ".html") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no HTML files found")
	}

	sort.Slice(files, func(i, j int) bool {
		return extractTime(files[i]).Before(extractTime(files[j]))
	})

	if len(files) > n {
		files = files[len(files)-n:]
	}

	var summaries []*ReportSummary
	for _, file := range files {
		s, err := parseHTML(file)
		if err == nil {
			summaries = append(summaries, s)
		}
	}
	return summaries, nil
}

func parseHTML(path string) (*ReportSummary, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	html := string(data)

	r := &ReportSummary{
		FileName: filepath.Base(path),
		Date:     extractDate(path),
		Passed:   extractCount(html, `class="passed">(\d+) Passed`),
		Failed:   extractCount(html, `class="failed">(\d+) Failed`),
		Skipped:  extractCount(html, `class="skipped">(\d+) Skipped`),
	}

	if r.Failed != "0" {
		r.FailedTCs = extractFailedTests(html)
		r.FailReason = extractFailureReason(html)
	}

	return r, nil
}

/* ------------------ PARSERS ------------------ */

func extractCount(html, pattern string) string {
	re := regexp.MustCompile(pattern)
	m := re.FindStringSubmatch(html)
	if len(m) > 1 {
		return m[1]
	}
	return "0"
}

func extractFailedTests(html string) []string {
	re := regexp.MustCompile(`<tr class="failed.*?">.*?<td class="col-name">(.*?)</td>`)
	matches := re.FindAllStringSubmatch(html, -1)

	var failed []string
	for _, m := range matches {
		if len(m) > 1 {
			failed = append(failed, stripHTML(m[1]))
		}
	}
	return failed
}

func extractFailureReason(html string) string {
	re := regexp.MustCompile(`<pre>(.*?)</pre>`)
	m := re.FindStringSubmatch(html)
	if len(m) > 1 {
		return stripHTML(m[1])
	}
	return "Reason not available"
}

func stripHTML(s string) string {
	re := regexp.MustCompile(`<.*?>`)
	return strings.TrimSpace(re.ReplaceAllString(s, ""))
}

/* ------------------ TIME ------------------ */

func extractDate(path string) string {
	t := extractTime(path)
	if t.IsZero() {
		return "unknown"
	}
	return t.Format("2006-01-02 15:04:05")
}

func extractTime(path string) time.Time {
	re := regexp.MustCompile(`_(\d{8})_(\d{6})`)
	m := re.FindStringSubmatch(path)
	if len(m) < 3 {
		return time.Time{}
	}
	t, err := time.Parse("20060102150405", m[1]+m[2])
	if err != nil {
		return time.Time{}
	}
	return t
}

/* ------------------ OUTPUT ------------------ */

func printTable(w io.Writer, dir string, reports []*ReportSummary) {
	if len(reports) == 0 {
		return
	}

	fmt.Fprintln(w, "\n========================================================================================")
	fmt.Fprintf(w, "DIRECTORY: %s\n", dir)
	fmt.Fprintln(w, "========================================================================================")
	fmt.Fprintf(w, "%-50s %-22s %-10s %-8s %-8s\n",
		"Log File Name", "Date", "Passed", "Failed", "Skipped")
	fmt.Fprintln(w, "----------------------------------------------------------------------------------------")

	for _, r := range reports {
		fmt.Fprintf(w, "%-50s %-22s %-10s %-8s %-8s\n",
			r.FileName, r.Date, r.Passed, r.Failed, r.Skipped)
	}

	for _, r := range reports {
		if r.Failed != "0" {
			// fmt.Fprintf(w, "\n❌ Failures in: %s\n", r.FileName)
			// for _, tc := range r.FailedTCs {
			// 	fmt.Fprintf(w, "  - %s\n", tc)
			// }
			// fmt.Fprintln(w, "Reason:")
			// fmt.Fprintln(w, r.FailReason)
		}
	}
}
