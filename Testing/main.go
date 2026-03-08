// package main

// import (
// 	"bytes"
// 	"crypto/tls"
// 	"crypto/x509"
// 	"encoding/base64"
// 	"encoding/json"
// 	"fmt"
// 	"io/ioutil"
// 	"net/http"
// 	"os"

// 	"github.com/xuri/excelize/v2"
// )

// type Hit struct {
// 	Source struct {
// 		Number       string `json:"number"`
// 		Created      string `json:"created"`
// 		PlannedStart string `json:"planned_start"`
// 		State        string `json:"state"`
// 	} `json:"_source"`
// }

// type ESResponse struct {
// 	Hits struct {
// 		Hits []Hit `json:"hits"`
// 	} `json:"hits"`
// }

// func main() {

// 	// ----------- LOAD ENV VARIABLES ----------------
// 	ELASTIC_USER := os.Getenv("ELASTIC_USER")
// 	ELASTIC_PASSWORD := os.Getenv("ELASTIC_PASSWORD")
// 	ELASTIC_SERVER_HOST := os.Getenv("ELASTIC_SERVER_HOST")
// 	ELASTIC_INDEX := os.Getenv("ELASTIC_INDEX")
// 	ELASTIC_CERTIFICATE := os.Getenv("ELASTIC_CERTIFICATE")

// 	if ELASTIC_USER == "" {
// 		fmt.Println("ERROR: Missing ELASTIC_USER environment variable")
// 		return
// 	}

// 	// Remove trailing slash from server host
// 	if ELASTIC_SERVER_HOST[len(ELASTIC_SERVER_HOST)-1] == '/' {
// 		ELASTIC_SERVER_HOST = ELASTIC_SERVER_HOST[:len(ELASTIC_SERVER_HOST)-1]
// 	}

// 	esURL := fmt.Sprintf("%s/%s/_search", ELASTIC_SERVER_HOST, ELASTIC_INDEX)

// 	// ----------- LOAD TLS CERT FROM BASE64 ----------------
// 	caBytes, err := base64.StdEncoding.DecodeString(ELASTIC_CERTIFICATE)
// 	if err != nil {
// 		fmt.Println("Failed to decode certificate:", err)
// 		return
// 	}

// 	caCertPool := x509.NewCertPool()
// 	if !caCertPool.AppendCertsFromPEM(caBytes) {
// 		fmt.Println("Failed to append CA certificate")
// 		return
// 	}

// 	transport := &http.Transport{
// 		TLSClientConfig: &tls.Config{
// 			RootCAs:            caCertPool,
// 			InsecureSkipVerify: false,
// 		},
// 	}
// 	client := &http.Client{Transport: transport}

// 	// ----------- ELASTICSEARCH QUERY ---------------------
// 	// Add: must_not -> state = "closed"
// 	query := map[string]interface{}{
// 		"track_total_hits": true,
// 		"_source": []string{
// 			"number",
// 			"created",
// 			"planned_start",
// 			"state",
// 		},
// 		"query": map[string]interface{}{
// 			"bool": map[string]interface{}{
// 				"must": []interface{}{
// 					map[string]interface{}{
// 						"exists": map[string]interface{}{
// 							"field": "analysis_result.error",
// 						},
// 					},
// 				},
// 				"must_not": []interface{}{
// 					map[string]interface{}{
// 						"term": map[string]interface{}{
// 							"state": "closed",
// 						},
// 					},
// 				},
// 				"filter": []interface{}{
// 					map[string]interface{}{
// 						"range": map[string]interface{}{
// 							"created": map[string]interface{}{
// 								"gte": "2025-11-15T00:00:00Z",
// 								"lte": "now",
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 		"sort": []interface{}{
// 			map[string]interface{}{
// 				"analyzed_at": map[string]interface{}{
// 					"order": "desc",
// 				},
// 			},
// 		},
// 		"from": 0,
// 		"size": 5000,
// 	}

// 	bodyBytes, _ := json.Marshal(query)

// 	req, _ := http.NewRequest("POST", esURL, bytes.NewBuffer(bodyBytes))
// 	req.Header.Set("Content-Type", "application/json")
// 	req.SetBasicAuth(ELASTIC_USER, ELASTIC_PASSWORD)

// 	resp, err := client.Do(req)
// 	if err != nil {
// 		fmt.Println("Failed to connect to Elasticsearch:", err)
// 		return
// 	}
// 	defer resp.Body.Close()

// 	respBody, _ := ioutil.ReadAll(resp.Body)
// 	if resp.StatusCode >= 400 {
// 		fmt.Println("Elasticsearch error:", string(respBody))
// 		return
// 	}

// 	var parsed ESResponse
// 	json.Unmarshal(respBody, &parsed)

// 	// ----------- WRITE TO EXCEL --------------------
// 	file := excelize.NewFile()
// 	sheet := file.GetSheetName(0)

// 	file.SetCellValue(sheet, "A1", "CR Number")
// 	file.SetCellValue(sheet, "B1", "Created")
// 	file.SetCellValue(sheet, "C1", "Planned Start")
// 	file.SetCellValue(sheet, "D1", "State")

// 	row := 2

// 	for _, hit := range parsed.Hits.Hits {
// 		file.SetCellValue(sheet, fmt.Sprintf("A%d", row), hit.Source.Number)
// 		file.SetCellValue(sheet, fmt.Sprintf("B%d", row), hit.Source.Created)
// 		file.SetCellValue(sheet, fmt.Sprintf("C%d", row), hit.Source.PlannedStart)
// 		file.SetCellValue(sheet, fmt.Sprintf("D%d", row), hit.Source.State)
// 		row++
// 	}

// 	err = file.SaveAs("cr_error_report_with_onlyclosedstate.xlsx")
// 	if err != nil {
// 		fmt.Println("Error writing excel:", err)
// 		return
// 	}

// 	fmt.Println("SUCCESS → Excel generated: cr_error_report_with_state.xlsx")
// }

// ================================== counting the regions , dc and tribe =======================================

package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
)

type Source struct {
	Regions      []string `json:"regions"`
	DC           []string `json:"dc"`
	Tribe        string   `json:"tribe"`
	PlannedStart string   `json:"planned_start"`
	Number       string   `json:"number"`
}

type Hit struct {
	Source Source `json:"_source"`
}

type ESResponse struct {
	Hits struct {
		Hits []Hit `json:"hits"`
	} `json:"hits"`
}

func main() {

	// -------- ENV VARS --------
	ELASTIC_USER := os.Getenv("ELASTIC_USER")
	ELASTIC_PASSWORD := os.Getenv("ELASTIC_PASSWORD")
	ELASTIC_SERVER_HOST := os.Getenv("ELASTIC_SERVER_HOST")
	ELASTIC_INDEX := os.Getenv("ELASTIC_INDEX")
	ELASTIC_CERTIFICATE := os.Getenv("ELASTIC_CERTIFICATE")

	if ELASTIC_SERVER_HOST[len(ELASTIC_SERVER_HOST)-1] == '/' {
		ELASTIC_SERVER_HOST = ELASTIC_SERVER_HOST[:len(ELASTIC_SERVER_HOST)-1]
	}

	esURL := fmt.Sprintf("%s/%s/_search", ELASTIC_SERVER_HOST, ELASTIC_INDEX)

	// -------- TLS CERT --------
	caBytes, err := base64.StdEncoding.DecodeString(ELASTIC_CERTIFICATE)
	if err != nil {
		fmt.Println("Certificate decode error:", err)
		return
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caBytes)

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs: caCertPool,
		},
	}
	client := &http.Client{Transport: transport}

	// -------- QUERY BODY --------
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					map[string]interface{}{
						"exists": map[string]interface{}{
							"field": "analysis_result",
						},
					},
				},
				"filter": []interface{}{
					map[string]interface{}{
						"range": map[string]interface{}{
							"created": map[string]interface{}{
								"gte": "now-24h",
								"lte": "now",
							},
						},
					},
				},
			},
		},
		"sort": []interface{}{
			map[string]interface{}{
				"analyzed_at": map[string]interface{}{
					"order": "desc",
				},
			},
		},
		"from": 0,
		"size": 100, // FETCH MORE DOCS
	}

	bodyBytes, _ := json.Marshal(query)

	req, _ := http.NewRequest("POST", esURL, bytes.NewBuffer(bodyBytes))
	req.SetBasicAuth(ELASTIC_USER, ELASTIC_PASSWORD)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("ES Query Error:", err)
		return
	}
	defer resp.Body.Close()

	respBytes, _ := ioutil.ReadAll(resp.Body)

	if resp.StatusCode >= 400 {
		fmt.Println("ERROR RESPONSE:", string(respBytes))
		return
	}

	var parsed ESResponse
	json.Unmarshal(respBytes, &parsed)

	// -------- GROUPING MAP --------
	summary := make(map[string]map[string]map[string]int)

	for _, hit := range parsed.Hits.Hits {

		src := hit.Source

		if len(src.Regions) == 0 || len(src.DC) == 0 || src.Tribe == "" || src.Number == "" {
			continue
		}

		region := src.Regions[0]
		dc := src.DC[0]
		tribe := src.Tribe

		// Initialize maps
		if _, ok := summary[region]; !ok {
			summary[region] = make(map[string]map[string]int)
		}
		if _, ok := summary[region][dc]; !ok {
			summary[region][dc] = make(map[string]int)
		}

		// COUNT CR's
		summary[region][dc][tribe]++
	}

	// ========== PRINT UI STYLE OUTPUT ==========
	fmt.Println("\n===== FINAL REGION → DC → TRIBE (CR COUNT) =====\n")

	for region, dcMap := range summary {

		// REGION TOTAL COUNT
		regionTotal := 0
		for _, tribeMap := range dcMap {
			for _, cnt := range tribeMap {
				regionTotal += cnt
			}
		}

		fmt.Printf("\nRegion: %s   (Total CRs: %d)\n", region, regionTotal)

		for dc, tribeMap := range dcMap {

			// DC TOTAL COUNT
			dcTotal := 0
			for _, cnt := range tribeMap {
				dcTotal += cnt
			}

			fmt.Printf("  Datacenter: %s   (CRs: %d)\n", dc, dcTotal)

			for tribe, cnt := range tribeMap {
				fmt.Printf("     - %s : %d CRs\n", tribe, cnt)
			}
		}
	}
}
