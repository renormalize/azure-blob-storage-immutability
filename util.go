package main

import (
	"fmt"
	"os"
)

func containerInfo() (string, string, string, error) {
	accountName, ok := os.LookupEnv("AZURE_ACCOUNT_NAME")
	if !ok {
		return "", "", "", fmt.Errorf("no account name was provided")
	}

	accountKey, ok := os.LookupEnv("AZURE_ACCOUNT_KEY")
	if !ok {
		return "", "", "", fmt.Errorf("no account key was provided")
	}

	containerName, ok := os.LookupEnv("AZURE_CONTAINER_NAME")
	if !ok {
		return "", "", "", fmt.Errorf("no container name was provided")
	}

	return accountName, accountKey, containerName, nil
}

func getOperations() (extend, delete bool) {
	for _, operation := range os.Args[1:] {
		switch operation {
		case "delete":
			delete = true
		case "extend":
			extend = true
		}
	}
	return
}
