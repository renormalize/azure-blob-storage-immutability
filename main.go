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

func main() {
	fmt.Println("Immutable storage test")
	fmt.Println()

	extend, delete := getOperations()

	accountName, accountKey, containerName, err := containerInfo()
	if err != nil {
		fmt.Println("Starting the program failed because", err)
		return
	}

	connection, err := createAzConnection(accountName, accountKey, containerName)
	if err != nil {
		fmt.Println("Could not create a connection", err)
		return
	}

	blobNames, err := connection.listBlobs()
	if err != nil {
		fmt.Println("error while listing the blob names:", err)
		return
	}

	fmt.Println("The blobs are:", blobNames)
	fmt.Println()

	if extend {
		fmt.Println("Extending blobs")
		for _, blobName := range blobNames {
			_, err := connection.setBlobImmutabilityDuration(blobName, 5)
			if err != nil {
				fmt.Println("error while extending the period:", err)
			}
		}
		fmt.Println()
	}

	if delete {
		fmt.Println("Deleting blobs")
		for _, blobName := range blobNames {
			_, err := connection.deleteObjects(blobName)
			if err != nil {
				fmt.Println("error while deleting the objects:", err)
			}
		}
		fmt.Println()
	}
}
