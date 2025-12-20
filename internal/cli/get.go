package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/pvcnt/kvdog/internal/client"
	pb "github.com/pvcnt/kvdog/pkg/proto"
)

func NewGetCmd(serverAddr *string) *cobra.Command {
	return &cobra.Command{
		Use:   "get <key>",
		Short: "Get a value by key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			key := args[0]

			c, err := client.New(*serverAddr)
			if err != nil {
				return err
			}
			defer c.Close()

			ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Second)
			defer cancel()

			rsp, err := c.GetItem(ctx, &pb.GetItemRequest{Key: key})
			if err != nil {
				return fmt.Errorf("failed to get key %q: %w", key, err)
			}

			if !rsp.GetFound() {
				return fmt.Errorf("key %q not found", key)
			}

			fmt.Printf("%s\n", rsp.GetValue())
			return nil
		},
	}
}
