package main

import (
	"fmt"
	"os"

	"github.com/renormalize/azure-blob-storage-immutability/azure"
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

func main() {
	fmt.Println("Immutable storage test")
	fmt.Println()

	// TODO: @renormalize this is a no-op for now
	getOperations()

	accountName, accountKey, containerName, err := containerInfo()
	if err != nil {
		fmt.Println("Starting the program failed because", err)
		return
	}

	containerClient, err := azure.NewAzureContainerClient(accountName, accountKey, containerName)
	if err != nil {
		fmt.Println("Could not create a client to the container of the given name", err)
		return
	}

	fmt.Println("The immutability properties are")
	hasImmutabilityPolicy, isImmutableStorageWithVersioningEnabled, err := containerClient.CheckImmutability()
	if err != nil {
		fmt.Println("Failed to check immutability properties because of", err)
	}

	fmt.Println("HasImmutabilityPolicy: ", hasImmutabilityPolicy)
	fmt.Println("HasImmutabilityPolicy: ", isImmutableStorageWithVersioningEnabled)
}
