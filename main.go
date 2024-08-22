package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"os"
	"path/filepath"
)

var bucket = flag.String("bucket", "", "s3 bucket with exported qldb data")
var completedManifestKey = flag.String("completedManifestKey", "", "key of the qldb completed manifest")
var region = flag.String("region", os.Getenv("AWS_REGION"), "AWS region")

func checkRequiredFlags() {
	if *bucket == "" {
		fmt.Fprintln(os.Stderr, "-bucket is required")
		flag.Usage()
		os.Exit(1)
	}
	if *region == "" {
		fmt.Fprintln(os.Stderr, "-region is required")
		flag.Usage()
		os.Exit(1)
	}
	if *completedManifestKey == "" {
		fmt.Fprintln(os.Stderr, "-completedManifestKey is required")
		flag.Usage()
		os.Exit(1)
	}
}

func main() {
	flag.Parse()

	checkRequiredFlags()

	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		panic(err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.Region = *region
	})

	cwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	dir := filepath.Join(cwd, "vehicle-registration")

	dlr := NewS3Downloader(dir, *bucket, client)

	completedManifestLocalPath, err := dlr.Download(ctx, *completedManifestKey)
	if err != nil {
		panic(err)
	}

	dataKeys, err := ExtractKeysFromCompletedManifest(completedManifestLocalPath)
	if err != nil {
		panic(err)
	}

	localDataKeys := make([]string, len(dataKeys))
	for idx, key := range dataKeys {
		lk, err := dlr.Download(ctx, key)
		if err != nil {
			panic(err)
		}
		localDataKeys[idx] = lk
	}

	outFile := filepath.Join(cwd, "vehicle-registration-replay.sql")

	mr := NewQldbToDoltReplayWriter(localDataKeys, outFile)
	err = mr.WriteReplay(ctx)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("QLDB journal replay Dolt SQL file written to", outFile)
	}
}
