package azure

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

const azureHostName = "blob.core.windows.net"

// AzureContainerClient is the wrapper on top of the Azure Container Client
type AzureContainerClient struct {
	container.Client
}

// NewAzureContainerClient takes the account, account key, and container name and returns a containe client
func NewAzureContainerClient(accountName, accountKey, containerName string) (*AzureContainerClient, error) {
	containerURL := fmt.Sprintf("https://%s.%s/%s", accountName, azureHostName, containerName)

	cred, err := container.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create the shared key credential with error: %w", err)
	}

	client, err := container.NewClientWithSharedKeyCredential(containerURL, cred, &container.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Retry: policy.RetryOptions{
				TryTimeout: 1 * time.Minute,
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

func (c *AzureContainerClient) CheckImmutability() (bool, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := c.Client.GetProperties(ctx, nil)
	if err != nil {
		return false, false, fmt.Errorf("faild to fetch the properties of the container with error: %w", err)
	}

	return *resp.HasImmutabilityPolicy, *resp.IsImmutableStorageWithVersioningEnabled, nil
}
