package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"os"
	"path/filepath"
)

type S3Downloader interface {
	Download(ctx context.Context, key string) (string, error)
}

type s3Downloader struct {
	bucket string
	dir    string
	client *s3.Client
}

var _ S3Downloader = &s3Downloader{}

func NewS3Downloader(dir, bucket string, client *s3.Client) *s3Downloader {
	return &s3Downloader{
		dir:    dir,
		bucket: bucket,
		client: client,
	}
}

func (d *s3Downloader) writeLocal(ctx context.Context, key string, rc io.ReadCloser) (lp string, err error) {
	defer func() {
		rerr := rc.Close()
		if err == nil {
			err = rerr
		}
	}()
	lp = filepath.Join(d.dir, key)
	if err = os.MkdirAll(filepath.Dir(lp), os.ModePerm); err != nil {
		return
	}
	var f *os.File
	f, err = os.Create(lp)
	if err != nil {
		return
	}
	defer func() {
		rerr := f.Close()
		if err == nil {
			err = rerr
		}
	}()
	_, err = io.Copy(f, rc)
	return
}

func (d *s3Downloader) Download(ctx context.Context, key string) (string, error) {
	err := os.MkdirAll(d.dir, os.ModePerm)
	if err != nil {
		return "", err
	}
	out, err := d.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", err
	}
	return d.writeLocal(ctx, key, out.Body)
}
