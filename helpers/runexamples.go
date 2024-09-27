package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
)

func listExampleFiles(dir string) ([]string, error) {
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(path) == ".go" {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

func runExample(example string) {
	fmt.Println("----------------------------")
	fmt.Println("Running example:", example)
	fmt.Println("----------------------------")

	cmd := exec.Command("go", "run", example)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		log.Fatalf("Failed to run example %s: %v", example, err)
	}
}

func main() {
	exampleDir := "examples"
	files, err := listExampleFiles(exampleDir)
	if err != nil {
		log.Fatalf("Failed to list files in %s: %v", exampleDir, err)
	}

	for _, file := range files {
		runExample(file)
	}
}