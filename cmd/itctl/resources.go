package main

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/urfave/cli/v2"
	"go.elara.ws/itd/api"
)

func resourcesLoad(c *cli.Context) error {
	return resLoadWithMode(c.Context, c.Args().Slice(), c.String("mode"))
}

func resLoad(ctx context.Context, args []string) error {
	return resLoadWithMode(ctx, args, "")
}

func resLoadWithMode(ctx context.Context, args []string, modeStr string) error {
	if len(args) == 0 {
		return cli.Exit("Command load requires one argument.", 1)
	}

	mode, err := api.ParseResourceLoadMode(modeStr)
	if err != nil {
		return err
	}

	path, err := filepath.Abs(args[0])
	if err != nil {
		return err
	}

	start := time.Now()

	progCh, err := client.FS().LoadResourcesWithMode(ctx, path, mode)
	if err != nil {
		return err
	}

	// Create progress bar templates
	rmTmpl := `Removing {{string . "filename"}}`
	chkTmpl := `Checking {{string . "filename"}}`
	upTmpl := `Uploading {{string . "filename"}} {{counters . }} B {{bar . "|" "-" (cycle .) " " "|"}} {{percent . }} {{rtime . "%s"}}`
	// Start full bar at 0 total
	bar := pb.ProgressBarTemplate(chkTmpl).Start(0)

	uploaded := make(map[string]bool)
	skipped := 0

	for evt := range progCh {
		if evt.Err != nil {
			bar.Finish()
			return evt.Err
		}

		switch evt.Operation {
		case api.ResourceRemove:
			bar.SetTemplateString(rmTmpl)
			bar.Set("filename", evt.Name)
			bar.SetTotal(0)
			bar.SetCurrent(0)
		case api.ResourceCheck:
			bar.SetTemplateString(chkTmpl)
			bar.Set("filename", evt.Name)
			bar.SetTotal(0)
			bar.SetCurrent(0)
		case api.ResourceSkip:
			skipped++
		default:
			uploaded[evt.Name] = true
			bar.SetTemplateString(upTmpl)
			bar.Set("filename", evt.Name)
			bar.SetTotal(evt.Total)
			bar.SetCurrent(evt.Sent)
		}
	}

	bar.Finish()
	fmt.Printf("Resources loaded in %s (%d uploaded, %d unchanged).\n", time.Since(start).Round(time.Millisecond), len(uploaded), skipped)

	return nil
}
