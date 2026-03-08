///================================ Extracting needed data frm all incident json =========================================
// package main

// import (
// 	"encoding/json"
// 	"fmt"
// 	"io/ioutil"
// 	"os"
// 	"path/filepath"
// )

// type Incident struct {
// 	Number           string `json:"number"`
// 	ShortDescription string `json:"short_description"`
// 	CreatedBy        string `json:"created_by"`
// 	Tribe            string `json:"tribe"`
// 	Severity         string `json:"severity"`
// 	Close_notes      string `json:"close_notes"`
// 	Status           string `json:"status"`
// 	Long_description string `json:"long_description"`
// 	Regions          string `json:"regions"`
// 	Service_names    string `json:"service_names"`
// }

// type Output struct {
// 	Number           string `json:"number"`
// 	ShortDescription string `json:"short_description"`
// 	CreatedBy        string `json:"created_by"`
// 	Tribe            string `json:"tribe"`
// 	Severity         string `json:"severity"`
// 	Close_notes      string `json:"close_notes"`
// 	Status           string `json:"status"`
// 	Long_description string `json:"long_description"`
// 	Regions          string `json:"regions"`
// 	Service_names    string `json:"service_names"`
// }

// func main() {
// 	inputDir := "Incidents" // <-- ensure folder name matches exactly
// 	outputFile := "output.json"

// 	var results []Output
// 	uniqueNumbers := make(map[string]bool) // for counting unique numbers

// 	// Walk through all files
// 	err := filepath.Walk(inputDir, func(path string, info os.FileInfo, err error) error {
// 		if err != nil {
// 			return err
// 		}

// 		// Only .json files
// 		if !info.IsDir() && filepath.Ext(path) == ".json" {

// 			// Read file
// 			data, readErr := ioutil.ReadFile(path)
// 			if readErr != nil {
// 				return readErr
// 			}

// 			// Parse JSON
// 			var inc Incident
// 			if unmarshalErr := json.Unmarshal(data, &inc); unmarshalErr != nil {
// 				return unmarshalErr
// 			}

// 			// Skip incidents created by TIP.SA
// 			if inc.CreatedBy == "TIP.SA" {
// 				fmt.Printf("Skipping file (created_by = TIP.SA): %s\n", path)
// 				return nil
// 			}

// 			// Add unique number tracking
// 			if !uniqueNumbers[inc.Number] {
// 				uniqueNumbers[inc.Number] = true
// 			}

// 			// Store result
// 			results = append(results, Output{
// 				Number:           inc.Number,
// 				ShortDescription: inc.ShortDescription,
// 				CreatedBy:        inc.CreatedBy,
// 				Tribe:            inc.Tribe,
// 				Severity:         inc.Severity,
// 				Close_notes:      inc.Close_notes,
// 				Status:           inc.Status,
// 				Long_description: inc.Long_description,
// 				Regions:          inc.Regions,
// 				Service_names:    inc.Service_names,
// 			})
// 		}
// 		return nil
// 	})

// 	if err != nil {
// 		fmt.Println("Error scanning directory:", err)
// 		return
// 	}

// 	// Convert to JSON
// 	outData, err := json.MarshalIndent(results, "", "  ")
// 	if err != nil {
// 		fmt.Println("Error marshalling:", err)
// 		return
// 	}

// 	// Write output.json
// 	err = ioutil.WriteFile(outputFile, outData, 0644)
// 	if err != nil {
// 		fmt.Println("Error writing output file:", err)
// 		return
// 	}

// 	fmt.Println("✔ Successfully written output to:", outputFile)
// 	fmt.Println("📌 Total JSON files processed:", len(results))
// 	fmt.Println("📌 Unique incident numbers found:", len(uniqueNumbers))
// }

// / /// ============================== connecting ollama 3.1 and generating needed data (with working prompt) ==============================================

// package main

// import (
// 	"bufio"
// 	"bytes"
// 	"encoding/json"
// 	"fmt"
// 	"net/http"
// 	"os"
// )

// type OllamaRequest struct {
// 	Model  string `json:"model"`
// 	Prompt string `json:"prompt"`
// }

// type OllamaStreamResponse struct {
// 	Response string `json:"response"`
// 	Done     bool   `json:"done"`
// }

// func main() {

// 	// STEP 1 — Read JSON file
// 	jsonBytes, err := os.ReadFile("./input11-20.json")
// 	if err != nil {
// 		panic(err)
// 	}

// 	// STEP 2 — Your prompt

// 	userPrompt := `
// You are an AI assistant that analyzes ServiceNow incident records.

// You will be provided with a JSON array of incident objects.
// Each incident contains the following fields:
// - number
// - short_description
// - created_by
// - tribe
// - severity
// - close_notes
// - status
// - long_description
// - regions
// - service_names

// TASK:
// For EACH incident in the input array, generate EXACTLY ONE output object.
// The final output MUST be a JSON array with the SAME number of elements as the input array.

// OUTPUT FORMAT:
// Each output object MUST have the following structure:

// {
//   "incidentid": "",
//   "Symptom": "",
//   "Incident": "",
//   "Root Cause": "",
//   "Fix": "",
//   "Service Impacted": ""
// }

// FIELD RULES:
// - "incidentid": You MUST copy the value EXACTLY from the input field "number".
//   Do NOT modify, reformat, renumber, normalize, or replace it.
//   If the input "number" is "INC10442527", the output "incidentid" MUST be "INC10442527".
// - "Symptom": Describe observable symptoms derived ONLY from that incident's
//   short_description or long_description. Do NOT use information from other incidents.
// - "Incident": Provide a concise summary using only fields from that incident.
// - "Root Cause": Mention the technical cause ONLY if it is explicitly stated in
//   short_description or close_notes. Otherwise, leave it empty.
// - "Fix": Mention remediation steps ONLY if explicitly stated in close_notes or
//   long_description. Otherwise, leave it empty.
// - "Service Impacted": Derive strictly from service_names or tribe fields only.

// STRICT RULES:
// 1. Do NOT assume, infer, or invent any information.
// 2. Do NOT reuse information across incidents.
// 3. If a value cannot be determined, use an empty string "".
// 4. Generate EXACTLY one output object per input incident.
// 5. Do NOT duplicate incidents.
// 6. Output ONLY the JSON array — no explanations, no markdown, no extra text.
// 7. Stop generation immediately after the closing ']'.
// 8. NEVER generate placeholder incident IDs (e.g., INC000001, INC123).
//    Always copy the exact "number" value from the input.
// 9. Do NOT introduce new entities, hostnames, ports, or events
//    that do not appear in the input fields.
// `

// 	finalPrompt := fmt.Sprintf(
// 		"Here is the incident JSON:\n%s\n\nInstruction: %s",
// 		string(jsonBytes),
// 		userPrompt,
// 	)

// 	// STEP 3 — Request payload
// 	reqBody := OllamaRequest{
// 		Model:  "llama3.1",
// 		Prompt: finalPrompt,
// 	}

// 	reqJSON, _ := json.Marshal(reqBody)

// 	// STEP 4 — Hit Ollama
// 	resp, err := http.Post(
// 		"http://localhost:11434/api/generate",
// 		"application/json",
// 		bytes.NewBuffer(reqJSON),
// 	)
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer resp.Body.Close()

// 	// STEP 5 — Read streaming response
// 	scanner := bufio.NewScanner(resp.Body)
// 	var finalOutput string

// 	for scanner.Scan() {
// 		line := scanner.Bytes()

// 		var chunk OllamaStreamResponse
// 		if err := json.Unmarshal(line, &chunk); err != nil {
// 			continue
// 		}

// 		finalOutput += chunk.Response
// 	}

// 	// STEP 6 — Write final combined text to file

// 	err = os.WriteFile("ollama_output11-202.json", []byte(finalOutput), 0644)
// 	if err != nil {
// 		panic(err)
// 	}

// 	fmt.Println("✔ Output written to ollama_output11-20.json")
// }

// // =========================================   Connecting to Neo4j      ===================
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type Incident struct {
	IncidentID      string `json:"incidentid"`
	Symptom         string `json:"Symptom"`
	Incident        string `json:"Incident"`
	RootCause       string `json:"Root Cause"`
	Fix             string `json:"Fix"`
	ServiceImpacted string `json:"Service Impacted"`
}

func main() {
	// ---------- CONFIG ----------
	neo4jURI := "bolt://localhost:7687"
	neo4jUser := "neo4j"
	neo4jPass := "password"
	jsonFile := "output.json"
	// ----------------------------

	// Read JSON file
	data, err := os.ReadFile(jsonFile)
	if err != nil {
		log.Fatalf("Failed to read JSON file: %v", err)
	}

	var incidents []Incident
	if err := json.Unmarshal(data, &incidents); err != nil {
		log.Fatalf("Failed to parse JSON: %v", err)
	}

	// Connect to Neo4j
	driver, err := neo4j.NewDriverWithContext(
		neo4jURI,
		neo4j.BasicAuth(neo4jUser, neo4jPass, ""),
	)
	if err != nil {
		log.Fatalf("Failed to create Neo4j driver: %v", err)
	}
	defer driver.Close(context.Background())

	session := driver.NewSession(context.Background(), neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})
	defer session.Close(context.Background())

	// Insert data
	for _, inc := range incidents {
		_, err := session.ExecuteWrite(context.Background(), func(tx neo4j.ManagedTransaction) (any, error) {
			query := `
MERGE (i:Incident {incidentid: $incidentid})
SET
  i.description = CASE
      WHEN $incident IS NOT NULL AND $incident <> "" THEN $incident
      ELSE i.description
  END,
  i.root_cause = CASE
      WHEN $rootCause IS NOT NULL AND $rootCause <> "" THEN $rootCause
      ELSE i.root_cause
  END,
  i.fix = CASE
      WHEN $fix IS NOT NULL AND $fix <> "" THEN $fix
      ELSE i.fix
  END

MERGE (s:Symptom {name: $symptom})
MERGE (svc:Service {name: $service})

MERGE (i)-[:HAS_SYMPTOM]->(s)
MERGE (i)-[:IMPACTS]->(svc)
`
			params := map[string]any{
				"incidentid": inc.IncidentID,
				"incident":   inc.Incident,
				"rootCause":  inc.RootCause,
				"fix":        inc.Fix,
				"symptom":    inc.Symptom,
				"service":    inc.ServiceImpacted,
			}
			_, err := tx.Run(context.Background(), query, params)
			return nil, err
		})

		if err != nil {
			log.Printf("Failed to insert incident %s: %v", inc.IncidentID, err)
		} else {
			fmt.Println("Inserted:", inc.IncidentID)
		}
	}

	fmt.Println("✅ All data loaded into Neo4j")
}

// =============================================================================================================
// package main

// import (
// 	"bytes"
// 	"encoding/json"
// 	"fmt"
// 	"io"
// 	"net/http"
// 	"os"
// )

// type OllamaRequest struct {
// 	Model  string `json:"model"`
// 	Prompt string `json:"prompt"`
// 	Stream bool   `json:"stream"`
// }

// type OllamaResponse struct {
// 	Response string `json:"response"`
// }

// // Extract only the JSON array from model output
// func extractJSONArray(text string) (string, error) {
// 	start := -1
// 	end := -1

// 	for i, ch := range text {
// 		if ch == '[' {
// 			start = i
// 			break
// 		}
// 	}

// 	for i := len(text) - 1; i >= 0; i-- {
// 		if text[i] == ']' {
// 			end = i + 1
// 			break
// 		}
// 	}

// 	if start == -1 || end == -1 || start >= end {
// 		return "", fmt.Errorf("no valid JSON array found")
// 	}

// 	return text[start:end], nil
// }

// func main() {

// 	// STEP 1 — Read input file
// 	inputBytes, err := os.ReadFile("./input1-10.json")
// 	if err != nil {
// 		panic(err)
// 	}

// 	var incidents []map[string]interface{}
// 	if err := json.Unmarshal(inputBytes, &incidents); err != nil {
// 		panic(err)
// 	}

// 	batchSize := 5
// 	var finalResults []map[string]interface{}

// 	// STEP 2 — Process in batches
// 	for i := 0; i < len(incidents); i += batchSize {

// 		end := i + batchSize
// 		if end > len(incidents) {
// 			end = len(incidents)
// 		}

// 		batch := incidents[i:end]
// 		batchJSON, _ := json.Marshal(batch)

// 		fmt.Printf("Processing batch %d to %d\n", i+1, end)

// 		// STEP 3 — Prompt
// 		userPrompt := `
// You are an AI assistant that analyzes ServiceNow incident records.

// You will be provided with a JSON array of incident objects.
// Each incident contains the following fields:
// - number
// - short_description
// - created_by
// - tribe
// - severity
// - close_notes
// - status
// - long_description
// - regions
// - service_names

// TASK:
// For EACH incident in the input array, generate EXACTLY ONE output object.
// The final output MUST be a JSON array with the SAME number of elements as the input array.

// OUTPUT FORMAT:
// Each output object MUST have the following structure:

// {
//   "incidentid": "",
//   "Symptom": "",
//   "Incident": "",
//   "Root Cause": "",
//   "Fix": "",
//   "Service Impacted": ""
// }

// FIELD RULES:
// - "incidentid": You MUST copy the value EXACTLY from the input field "number".
//   Do NOT modify, reformat, renumber, normalize, or replace it.
//   If the input "number" is "INC10442527", the output "incidentid" MUST be "INC10442527".
// - "Symptom": Describe observable symptoms derived ONLY from that incident's
//   short_description or long_description. Do NOT use information from other incidents.
// - "Incident": Provide a concise summary using only fields from that incident.
// - "Root Cause": Mention the technical cause ONLY if it is explicitly stated in
//   short_description or close_notes. Otherwise, leave it empty.
// - "Fix": Mention remediation steps ONLY if explicitly stated in close_notes or
//   long_description. Otherwise, leave it empty.
// - "Service Impacted": Derive strictly from service_names or tribe fields only.

// STRICT RULES:
// 1. Do NOT assume, infer, or invent any information.
// 2. Do NOT reuse information across incidents.
// 3. If a value cannot be determined, use an empty string "".
// 4. Generate EXACTLY one output object per input incident.
// 5. Do NOT duplicate incidents.
// 6. Output ONLY the JSON array — no explanations, no markdown, no extra text.
// 7. Stop generation immediately after the closing ']'.
// 8. NEVER generate placeholder incident IDs (e.g., INC000001, INC123).
//    Always copy the exact "number" value from the input.
// 9. Do NOT introduce new entities, hostnames, ports, or events
//    that do not appear in the input fields.
// `

// 		finalPrompt := fmt.Sprintf(
// 			"Here is the incident JSON:\n%s\n\nInstruction:\n%s",
// 			string(batchJSON),
// 			userPrompt,
// 		)

// 		// STEP 4 — Call Ollama
// 		reqBody := OllamaRequest{
// 			Model:  "llama3.1",
// 			Prompt: finalPrompt,
// 			Stream: false,
// 		}

// 		reqJSON, _ := json.Marshal(reqBody)

// 		resp, err := http.Post(
// 			"http://localhost:11434/api/generate",
// 			"application/json",
// 			bytes.NewBuffer(reqJSON),
// 		)
// 		if err != nil {
// 			panic(err)
// 		}

// 		body, _ := io.ReadAll(resp.Body)
// 		resp.Body.Close()

// 		var ollamaResp OllamaResponse
// 		if err := json.Unmarshal(body, &ollamaResp); err != nil {
// 			panic(err)
// 		}

// 		// STEP 5 — Extract JSON safely
// 		jsonOnly, err := extractJSONArray(ollamaResp.Response)
// 		if err != nil {
// 			fmt.Println("Raw model output:")
// 			fmt.Println(ollamaResp.Response)
// 			panic(err)
// 		}

// 		var batchResult []map[string]interface{}
// 		if err := json.Unmarshal([]byte(jsonOnly), &batchResult); err != nil {
// 			panic(err)
// 		}

// 		// STEP 6 — Validate count
// 		if len(batchResult) != len(batch) {
// 			panic("output count does not match input batch count")
// 		}

// 		// STEP 7 — Force correct incidentid
// 		for j := range batchResult {
// 			batchResult[j]["incidentid"] = batch[j]["number"]
// 		}

// 		finalResults = append(finalResults, batchResult...)
// 	}

// 	// STEP 8 — Write final output
// 	out, _ := json.MarshalIndent(finalResults, "", "  ")
// 	if err := os.WriteFile("output.json", out, 0644); err != nil {
// 		panic(err)
// 	}

// 	fmt.Println("✅ All batches processed successfully")
// }
