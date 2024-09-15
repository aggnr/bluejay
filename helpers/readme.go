//go:build ignoreme
// +build ignoreme

package main

import (
"fmt"
"io/ioutil"
"log"
"path/filepath"
"strings"
)

func main() {
	examplesDir := "examples"
	readmeFile := "README.md"

	// Read the examples directory
	files, err := ioutil.ReadDir(examplesDir)
	if err != nil {
		log.Fatalf("Failed to read examples directory: %v", err)
	}

	// Generate the list of example files with links
	var examplesList strings.Builder
	examplesList.WriteString("## Examples\n\nHere are some examples demonstrating how to use BlueJay:\n\n")
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".go") {
			exampleName := strings.TrimSuffix(file.Name(), ".go")
			exampleLink := filepath.Join(examplesDir, file.Name())
			examplesList.WriteString(fmt.Sprintf("- [%s](%s)\n", exampleName, exampleLink))
		}
	}

	// Read the existing README.md file
	readmeContent, err := ioutil.ReadFile(readmeFile)
	if err != nil {
		log.Fatalf("Failed to read README.md file: %v", err)
	}

	// Replace the existing examples section with the new list
	newReadmeContent := strings.Replace(string(readmeContent), "### More examples\nFor more examples, see the [examples](examples) directory.\n", examplesList.String(), 1)

	// Write the updated content back to the README.md file
	err = ioutil.WriteFile(readmeFile, []byte(newReadmeContent), 0644)
	if err != nil {
		log.Fatalf("Failed to write updated README.md file: %v", err)
	}

	fmt.Println("README.md updated successfully!")
}
