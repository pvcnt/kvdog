package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/pvcnt/kvdog/internal/client"
	pb "github.com/pvcnt/kvdog/pkg/proto"

	"github.com/spf13/cobra"
)

func NewPutCmd(serverAddr *string) *cobra.Command {
	return &cobra.Command{
		Use:   "put <key> <value>",
		Short: "Put a key-value pair",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			key := args[0]
			value := args[1]

			c, err := client.New(*serverAddr)
			if err != nil {
				return err
			}
			defer c.Close()

			ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Second)
			defer cancel()

			_, err = c.PutItem(ctx, &pb.PutItemRequest{
				Key:   key,
				Value: []byte(value),
			})
			if err != nil {
				return fmt.Errorf("failed to put key: %w", err)
			}

			fmt.Printf("Successfully put %q\n", key)
			return nil
		},
	}
}
