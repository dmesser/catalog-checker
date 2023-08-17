/*
Copyright © 2023 Daniel Messer
*/
package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/containers/image/v5/docker"
	"github.com/containers/image/v5/types"
	"github.com/spf13/cobra"
	"github.com/tidwall/gjson"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

type ImageData struct {
	Schema        string `json:"schema"`
	Name          string `json:"name"`
	Package       string `json:"package"`
	Image         string `json:"image"`
	RelatedImages []struct {
		Name  string `json:"name"`
		Image string `json:"image"`
	} `json:"relatedImages"`
}

type ImageType string

const (
	BundleImage  ImageType = "bundle"
	RelatedImage ImageType = "related"
)

type ParsedImageRef struct {
	BundleName string    `json:"name"`
	Package    string    `json:"package"`
	Image      string    `json:"image"`
	Type       ImageType `json:"type"`
}

type ImageReferenceFailure string

const (
	ManifestNotFound ImageReferenceFailure = "manifest-not-found"
	InvalidReference ImageReferenceFailure = "invalid-reference"
	AuthError        ImageReferenceFailure = "auth-error"
)

type FailedImageRef struct {
	ImageRef ParsedImageRef
	Reason   ImageReferenceFailure
}

type CheckImageContext struct {
	ImageRef ParsedImageRef
	Sys      *types.SystemContext
	Bar      *mpb.Bar
	Progress *mpb.Progress
	Sem      chan bool
	Wg       *sync.WaitGroup
}

var (
	catalogFile   string
	authFile      string
	parallelCount int
	csvFile       string
	showErrors    bool
	rootCmd       = &cobra.Command{
		Use:   "catalog-checker",
		Short: "Detect dead image references in an operator catalog",
		Run:   runChecker,
	}
)

var (
	failedImageRefs []FailedImageRef
)

func readFileWithProgress(filename string, progress *mpb.Progress) ([]byte, error) {
	// Open the file
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Get the file size
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fileInfo.Size()

	// Add the progress bar
	bar := progress.AddBar(fileSize,
		mpb.PrependDecorators(
			decor.Name("Reading catalog file", decor.WCSyncSpaceR),
			decor.CountersKibiByte("% .2f / % .2f", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(
			decor.EwmaETA(decor.ET_STYLE_MMSS, 60),
			decor.Name(" "),
			decor.AverageSpeed(decor.SizeB1000(0), "% .2f"),
		),
	)

	// Create a buffer to store the content
	var content bytes.Buffer

	// Read in chunks of 4096 bytes
	chunkSize := 4096
	buffer := make([]byte, chunkSize)

	for {
		bytesRead, err := file.Read(buffer)
		if err != nil {
			break
		}

		content.Write(buffer[:bytesRead])
		bar.IncrBy(bytesRead)
	}

	return content.Bytes(), nil
}

func runChecker(cmd *cobra.Command, args []string) {
	p := mpb.New(mpb.WithWidth(64), mpb.WithOutput(os.Stderr))

	fmt.Printf("Checking catalog file %s ...\n", catalogFile)

	// Read and parse the JSON file
	content, err := readFileWithProgress(catalogFile, p)
	if err != nil {
		panic(err)
	}

	// Use the containers/image library to check image
	sys, err := setupAuth()
	if err != nil {
		panic(err)
	}

	var imageRefs []ParsedImageRef

	// Extract image references from the catalog file
	gjson.GetBytes([]byte(content), "[@this].#(schema==\"olm.bundle\")#").ForEach(func(key, value gjson.Result) bool {
		name := value.Get("name").String()
		packageName := value.Get("package").String()
		image := value.Get("image").String()
		relatedImages := value.Get("relatedImages.#.image").Array()

		if image != "" {
			imageRefs = append(imageRefs, ParsedImageRef{
				Image:      image,
				Type:       BundleImage,
				BundleName: name,
				Package:    packageName,
			})
		}

		for _, relatedImage := range relatedImages {
			imageRefs = append(imageRefs, ParsedImageRef{
				Image:      relatedImage.String(),
				Type:       RelatedImage,
				BundleName: name,
				Package:    packageName,
			})
		}

		return true // keep iterating
	})

	if parallelCount <= 0 {
		parallelCount = runtime.NumCPU()
	}

	// add the progress bar
	totalJobs := len(imageRefs)
	bar := p.AddBar(int64(totalJobs),
		mpb.PrependDecorators(
			decor.Name("Checking images", decor.WCSyncSpaceR),
			decor.CountersNoUnit("%d / %d", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(
			decor.Percentage(decor.WC{}),
			decor.Name(" [ETA: "),
			decor.EwmaETA(decor.ET_STYLE_GO, 100),
			decor.Name(" ] "),
		),
	)

	sem := make(chan bool, parallelCount)
	var wg sync.WaitGroup
	for _, img := range imageRefs {
		wg.Add(1)
		ctx := CheckImageContext{
			ImageRef: img,
			Sys:      sys,
			Bar:      bar,
			Progress: p,
			Sem:      sem,
			Wg:       &wg,
		}
		go checkImage(ctx)
	}
	wg.Wait()

	if csvFile != "" {
		f, err := os.Create(csvFile)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		_, err = f.WriteString("package,bundlename,image,type,failure\n")
		if err != nil {
			panic(err)
		}

		// Sort the failed image references
		sort.Slice(failedImageRefs, func(i, j int) bool {
			return func() bool {
				if failedImageRefs[i].ImageRef.Package != failedImageRefs[j].ImageRef.Package {
					return failedImageRefs[i].ImageRef.Package < failedImageRefs[j].ImageRef.Package
				}
				if failedImageRefs[i].ImageRef.BundleName != failedImageRefs[j].ImageRef.BundleName {
					return failedImageRefs[i].ImageRef.BundleName < failedImageRefs[j].ImageRef.BundleName
				}
				if failedImageRefs[i].ImageRef.Image != failedImageRefs[j].ImageRef.Image {
					return failedImageRefs[i].ImageRef.Image < failedImageRefs[j].ImageRef.Image
				}
				return failedImageRefs[i].ImageRef.Type < failedImageRefs[j].ImageRef.Type
			}()
		})

		for _, img := range failedImageRefs {
			var imageRef = img.ImageRef
			_, err = f.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s\n", imageRef.Package, imageRef.BundleName, imageRef.Image, imageRef.Type, img.Reason))
			if err != nil {
				panic(err)
			}
		}
	}
}

func setupAuth() (*types.SystemContext, error) {
	sys := &types.SystemContext{}
	if authFile != "" {
		_, err := os.Stat(authFile)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, fmt.Errorf("auth file %s does not exist", authFile)
			} else {
				return nil, fmt.Errorf("failed to read auth file %s: %v", authFile, err)
			}
		}
		sys.AuthFilePath = authFile
	}
	return sys, nil
}

func checkImage(ctx CheckImageContext) {
	ctx.Sem <- true
	start := time.Now()

	defer func() {
		ctx.Bar.EwmaIncrement(time.Duration(time.Since(start).Nanoseconds() / int64(parallelCount)))
		ctx.Progress.Wait()
		ctx.Wg.Done()
	}()

	ref, err := docker.ParseReference("//" + ctx.ImageRef.Image)
	if err != nil {
		captureError(ctx.ImageRef, InvalidReference, fmt.Sprintf("Failed to parse image reference: %v", err), ctx.Progress)
		<-ctx.Sem
		return
	}

	src, err := ref.NewImageSource(context.Background(), ctx.Sys)
	if err != nil {
		var reason ImageReferenceFailure = InvalidReference

		if strings.Contains(err.Error(), "unauthorized") || strings.Contains(err.Error(), "denied") {
			reason = AuthError
		} else if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "unknown") {
			reason = ManifestNotFound
		}

		captureError(ctx.ImageRef, reason, fmt.Sprintf("Cannot create image resource: %v", err), ctx.Progress)
		<-ctx.Sem
		return
	}

	_, _, err = src.GetManifest(context.Background(), nil)
	if err != nil {
		captureError(ctx.ImageRef, ManifestNotFound, fmt.Sprintf("Cannot get manifest: %v", err), ctx.Progress)

		if src != nil {
			src.Close()
		}
		<-ctx.Sem
		return
	}

	src.Close()
	<-ctx.Sem
}

func captureError(imageRef ParsedImageRef, failure ImageReferenceFailure, errorMessage string, p *mpb.Progress) {

	failedImageRefs = append(failedImageRefs, FailedImageRef{imageRef, failure})

	if showErrors {
		if imageRef.Type == BundleImage {
			fmt.Fprintf(p,
				"Error processing operator bundle image %s from operator bundle '%s' in package '%s' cannot be found: %s\n",
				imageRef.Image, imageRef.BundleName, imageRef.Package, errorMessage,
			)
		} else {
			fmt.Fprintf(p,
				"Error processing related image %s from operator bundle '%s' in package '%s' cannot be found: %s\n",
				imageRef.Image, imageRef.BundleName, imageRef.Package, errorMessage,
			)
		}
	}
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&catalogFile, "catalog", "c", "", "Path to the file-based catalog JSON file")
	rootCmd.MarkPersistentFlagRequired("catalog")
	rootCmd.PersistentFlags().BoolVar(&showErrors, "show-errors", false, "Whether to output errors on ")
	rootCmd.PersistentFlags().StringVar(&csvFile, "csv", "", "Path to a CSV file to write the results to")
	rootCmd.PersistentFlags().StringVarP(&authFile, "auth", "a", "", "Path to Docker auth file")
	rootCmd.PersistentFlags().IntVarP(&parallelCount, "parallel", "p", 0, "Number of parallel checks")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
