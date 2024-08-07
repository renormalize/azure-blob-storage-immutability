package main

import (
	"context"
	"log"
	"time"

	"github.com/renormalize/azure-blob-storage-immutability/container"
)

func main() {
	log.Println("Immutable storage test")

	extend, delete := getOperations()

	accountName, accountKey, containerName, err := containerInfo()
	if err != nil {
		log.Fatal("Starting the program failed because", err)
	}

	containerClient, err := container.NewAzureContainerClient(accountName, accountKey, containerName)
	if err != nil {
		log.Fatal("Could not create a client to the container of the given name", err)
	}

	log.Println("Listing all the blobs in the container", containerName)

	blobs, err := containerClient.ListBlobs(context.Background())
	if err != nil {
		log.Fatal("Listing the blobs failed with error: %w", err)
	}
	for _, blob := range blobs {
		log.Println(blob)
	}

	log.Println("The current immutability properties of the container are:")
	hasImmutabilityPolicy, isImmutableStorageWithVersioningEnabled, err := containerClient.CheckImmutability(context.Background())
	if err != nil {
		log.Fatal("Failed to check immutability properties because of", err)
	}

	log.Println("HasImmutabilityPolicy: ", hasImmutabilityPolicy)
	log.Println("IsImmutableStorageWithVersioningEnabled: ", isImmutableStorageWithVersioningEnabled)

	tags := map[string]string{
		"time:": time.Now().String(),
		"tag":   "set",
		"key":   "value",
		"test":  "ing",
	}

	log.Println("Setting the same tags on all blobs")
	for _, blob := range blobs {
		err := containerClient.NewBlockBlobClient(blob.Name).SetTags(context.Background(), tags)
		if err != nil {
			log.Printf("Failed to set tag on blob %v with error: %v\n", blob.Name, err)
		}
	}

	// Listing the blobs again
	log.Println("Listing the blobs after settings the tags in the container", containerName)
	blobs, err = containerClient.ListBlobs(context.Background())
	if err != nil {
		log.Fatal("Listing the blobs failed with error: %w", err)
	}
	for _, blob := range blobs {
		log.Println(blob)
	}

	extensionPeriod := 5

	if extend {
		log.Println("Extending the immutable period of the blobs")
		for _, blob := range blobs {
			log.Printf("Extending immutable period of blob %v by %d\n", blob.Name, extensionPeriod)
			expiryDate, err := containerClient.NewBlockBlobClient(blob.Name).
				SetImmutability(context.Background(), extensionPeriod)
			if err != nil {
				log.Printf("Failed to set immutability on blob %v with error: %v\n", blob.Name, err)
			} else {
				log.Printf("Blob %v expires on %v\n", blob.Name, *expiryDate)
			}
		}
	}

	if delete {
		log.Println("Deleting blobs")
		for _, blob := range blobs {
			err := containerClient.NewBlockBlobClient(blob.Name).Delete(context.Background())
			if err != nil {
				log.Printf("Failed to delete blob %v with error: %v\n", blob.Name, err)
			} else {
				log.Println("Deleted blob ", blob.Name)
			}
		}
	}
}
