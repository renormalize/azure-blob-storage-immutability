package container

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/renormalize/azure-blob-storage-immutability/blob"
)

const azureBlobStorageDomain = "blob.core.windows.net"

// AzureContainerClient is the wrapper on top of the Azure Container Client
type AzureContainerClient struct {
	container.Client
}

// NewAzureContainerClient takes the account, account key, and container name and returns a containe client
func NewAzureContainerClient(accountName, accountKey, containerName string) (*AzureContainerClient, error) {
	containerURL := fmt.Sprintf("https://%s.%s/%s", accountName, azureBlobStorageDomain, containerName)

	cred, err := container.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create the shared key credential with error: %w", err)
	}

	client, err := container.NewClientWithSharedKeyCredential(containerURL, cred, &container.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Retry: policy.RetryOptions{
				TryTimeout: 10 * time.Second,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create a client with shared key credential with error: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// check if the container exists
	_, err = client.GetProperties(ctx, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.ContainerNotFound) {
			return nil, fmt.Errorf("failed to fetch properties of container %s becase it does not exist", containerName)
		}
		return nil, fmt.Errorf("failed to fetch the properties of the container with error: %w", err)
	}

	return &AzureContainerClient{
		Client: *client,
	}, nil
}

// CheckImmutability returns the immutability properties of the container
func (c *AzureContainerClient) CheckImmutability(ctx context.Context) (bool, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	resp, err := c.Client.GetProperties(ctx, nil)
	if err != nil {
		return false, false, fmt.Errorf("faild to fetch the properties of the container with error: %w", err)
	}

	return *resp.HasImmutabilityPolicy, *resp.IsImmutableStorageWithVersioningEnabled, nil
}

// SetImmutability sets the immutability policy on the container
func (c *AzureContainerClient) SetImmutability() {
	// TODO: @renormalize
	// this function requires more privileges than the current authentication allows for
	return
}

// ListBlobs lists all the blobs that are present in the container
func (c *AzureContainerClient) ListBlobs(ctx context.Context) ([]blob.Blob, error) {
	blobs := []blob.Blob{}

	pager := c.Client.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Include: container.ListBlobsInclude{
			Metadata:           true,
			Tags:               true,
			Versions:           true,
			ImmutabilityPolicy: true,
		},
	})
	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failure while trying to list blobs with error: %w", err)
		}

		for _, blobItem := range resp.Segment.BlobItems {
			// BlobTags might be nil, have to verify
			// blobtagset is a slice of pointers
			// TODO: wouldn't the tags be non-nil since ListBlobsInclude{} mentions Tags?
			var blobTags []*container.BlobTag
			if blobItem.BlobTags != nil {
				blobTags = blobItem.BlobTags.BlobTagSet
			}
			blobs = append(blobs, blob.Blob{
				Name:       *blobItem.Name,
				ExpiryTime: blobItem.Properties.ImmutabilityPolicyExpiresOn,
				BlobTags:   blobTags,
			})
		}
	}

	return blobs, nil
}

// NewBlockBlobClient creates a new BlockBlobClient
func (c *AzureContainerClient) NewBlockBlobClient(blobName string) *blob.BlockBlobClient {
	return &blob.BlockBlobClient{
		Name:   blobName,
		Client: *c.Client.NewBlockBlobClient(blobName),
	}
}
