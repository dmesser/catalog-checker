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
	failedImageRefs []ParsedImageRef
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
	p := mpb.New(mpb.WithWidth(64))

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

		_, err = f.WriteString("package,bundlename,image,type\n")
		if err != nil {
			panic(err)
		}

		// Sort the failed image references
		sort.Slice(failedImageRefs, func(i, j int) bool {
			return func() bool {
				if failedImageRefs[i].Package != failedImageRefs[j].Package {
					return failedImageRefs[i].Package < failedImageRefs[j].Package
				}
				if failedImageRefs[i].BundleName != failedImageRefs[j].BundleName {
					return failedImageRefs[i].BundleName < failedImageRefs[j].BundleName
				}
				if failedImageRefs[i].Image != failedImageRefs[j].Image {
					return failedImageRefs[i].Image < failedImageRefs[j].Image
				}
				return failedImageRefs[i].Type < failedImageRefs[j].Type
			}()
		})

		for _, img := range failedImageRefs {
			_, err = f.WriteString(fmt.Sprintf("%s,%s,%s,%s\n", img.Package, img.BundleName, img.Image, img.Type))
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
		captureError(ctx.ImageRef, fmt.Sprintf("Failed to parse image reference: %v", err))
		<-ctx.Sem
		return
	}

	src, err := ref.NewImageSource(context.Background(), ctx.Sys)
	if err != nil {
		captureError(ctx.ImageRef, fmt.Sprintf("Cannot create image resource: %v", err))
		<-ctx.Sem
		return
	}

	_, _, err = src.GetManifest(context.Background(), nil)
	if err != nil {
		captureError(ctx.ImageRef, fmt.Sprintf("Cannot get manifest: %v", err))

		if src != nil {
			src.Close()
		}
		<-ctx.Sem
		return
	}

	src.Close()
	<-ctx.Sem
}

func captureError(imageRef ParsedImageRef, errorMessage string) {

	failedImageRefs = append(failedImageRefs, imageRef)

	if showErrors {
		if imageRef.Type == BundleImage {
			fmt.Fprintf(os.Stderr,
				"Operator bundle image %s from operator bundle '%s' in package '%s' cannot be found: %s\n",
				imageRef.Image, imageRef.BundleName, imageRef.Package, errorMessage,
			)
		} else {
			fmt.Fprintf(os.Stderr,
				"Related image %s from operator bundle '%s' in package '%s' cannot be found: %s\n",
				imageRef.Image, imageRef.BundleName, imageRef.Package, errorMessage,
			)
		}
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&catalogFile, "catalog", "", "Path to the file-based catalog JSON file")
	rootCmd.MarkPersistentFlagRequired("catalog")
	rootCmd.PersistentFlags().BoolVar(&showErrors, "show-errors", false, "Whether to output errors on ")
	rootCmd.PersistentFlags().StringVar(&csvFile, "csv", "", "Path to a CSV file to write the results to")
	rootCmd.PersistentFlags().StringVar(&authFile, "auth", "", "Path to Docker auth file")
	rootCmd.PersistentFlags().IntVar(&parallelCount, "parallel", 0, "Number of parallel checks")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
