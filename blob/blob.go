package blob

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

// Blob holds all relevant information about a blob in a container.
type Blob struct {
	Name       string
	ExpiryTime *time.Time
	BlobTags   []*container.BlobTag
}

func (b Blob) String() string {
	s := "[" + b.Name

	expiryString := "<nil>"
	if b.ExpiryTime != nil {
		expiryString = (*b.ExpiryTime).String()
	}
	s += " " + expiryString

	tagString := ""
	if b.BlobTags != nil {
		for _, blobTag := range b.BlobTags {
			tagString += fmt.Sprintf("%q=%q ", *blobTag.Key, *blobTag.Value)
		}
	} else {
		tagString = "<nil>"
	}
	s += " " + tagString

	return s + "]"
}

// BlockBlobClient is the wrapper on top of the Azure Block Blob Client.
type BlockBlobClient struct {
	Name string
	blockblob.Client
}

// GetProperties fetches the properties of the blob.
func (b *BlockBlobClient) GetProperties() {
	// TODO: @renormalize
	return
}

// SetImmutability sets the immutability period for a blob in days.
func (b *BlockBlobClient) SetImmutability(ctx context.Context, daysFromToday int) (*time.Time, error) {
	expiryTime := time.Now().AddDate(0, 0, daysFromToday)
	resp, err := b.Client.SetImmutabilityPolicy(ctx, expiryTime, nil)
	return resp.ImmutabilityPolicyExpiry, err
}

// Delete deletes the blob.
func (b *BlockBlobClient) Delete(ctx context.Context) error {
	_, err := b.Client.Delete(ctx, nil)
	return err
}

func (b *BlockBlobClient) SetTags(ctx context.Context, tags map[string]string) error {
	_, err := b.Client.SetTags(ctx, tags, nil)
	return err
}
