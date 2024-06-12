package main

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

type azConnection struct {
	containerURL *azblob.ContainerURL
}

func (connection azConnection) listBlobs() ([]string, error) {
	var blobNames []string
	opts := azblob.ListBlobsSegmentOptions{}
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := connection.containerURL.ListBlobsFlatSegment(context.TODO(), marker, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to list the blobs, error: %v", err)
		}
		marker = listBlob.NextMarker

		// Process the blobs returned in this result segment
		for _, blob := range listBlob.Segment.BlobItems {
			blobNames = append(blobNames, blob.Name)
		}
	}
	return blobNames, nil
}

func (connection azConnection) setBlobImmutabilityDuration(blobName string, daysFromToday int) (*azblob.BlobSetImmutabilityPolicyResponse, error) {
	blobURL := connection.containerURL.NewBlobURL(blobName)
	extension := time.Now().AddDate(0, 0, daysFromToday)
	resp, err := blobURL.SetImmutabilityPolicy(context.Background(), extension, azblob.BlobImmutabilityPolicyModeNone, nil)
	if resp != nil {
		fmt.Println("Response: the immutability policy expires at: ", resp.ImmutabilityPolicyExpiry())
		fmt.Println("Response: the immutability policy expires at: ", resp.ImmutabilityPolicyMode())
	}
	return resp, err
}

func (connection azConnection) deleteObjects(blobName string) (*azblob.BlobDeleteResponse, error) {
	blobURL := connection.containerURL.NewBlobURL(blobName)
	resp, err := blobURL.Delete(context.Background(), azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{})
	if resp != nil {
		fmt.Println("The delete response code is: ", resp.Status())
		fmt.Println("The delete error code is: ", resp.ErrorCode())
	}
	return resp, err
}

func createAzConnection(accountName, accountKey, containerName string) (azConnection, error) {
	credentials, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		fmt.Println()
		return azConnection{}, fmt.Errorf("unable to create a credential object due to: %w", err)

	}

	pipeline := azblob.NewPipeline(credentials, azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			TryTimeout: 5 * time.Minute,
		},
	})

	blobURL, err := url.Parse(fmt.Sprintf("https://%s.%s", credentials.AccountName(), "blob.core.windows.net"))
	if err != nil {
		return azConnection{}, fmt.Errorf("unable to construct the blob url due to: %w", err)
	}

	serviceURL := azblob.NewServiceURL(*blobURL, pipeline)
	containerURL := serviceURL.NewContainerURL(containerName)

	return azConnection{
		containerURL: &containerURL,
	}, nil
}
