package slicer

import (
	"bytes"
	"fmt"
	"image"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestSliceRealImage(t *testing.T) {
	path := os.Getenv("TEST_IMAGE_PATH")
	if path == "" {
		t.Skip("set TEST_IMAGE_PATH to run this test")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	t.Logf("Input: %s (%d bytes)", path, len(data))

	slices, err := SliceImage(data, DefaultOptions())
	if err != nil {
		t.Fatalf("SliceImage: %v", err)
	}

	t.Logf("Produced %d slices:", len(slices))
	for _, s := range slices {
		t.Logf("  slice %d: y=[%d, %d) height=%d  (%d bytes JPEG)",
			s.Index, s.Y0, s.Y1, s.Y1-s.Y0, len(s.ImageData))
	}

	// Optionally write slices to /tmp for visual inspection.
	for _, s := range slices {
		outPath := fmt.Sprintf("/tmp/slice_%03d.jpg", s.Index)
		os.WriteFile(outPath, s.ImageData, 0644)
		t.Logf("  wrote %s", outPath)
	}
}

// TestSliceBatch processes all images in TEST_IMAGE_DIR and writes a diagnostic
// report + sliced output to /tmp/slicer-batch/.
//
// Usage:
//
//	TEST_IMAGE_DIR=~/src/cloudline/sample_logbooks/N188MC go test ./internal/slicer/ -run TestSliceBatch -v -count=1
func TestSliceBatch(t *testing.T) {
	dir := os.Getenv("TEST_IMAGE_DIR")
	if dir == "" {
		t.Skip("set TEST_IMAGE_DIR to run this test")
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}

	var files []string
	for _, e := range entries {
		ext := strings.ToLower(filepath.Ext(e.Name()))
		if ext == ".heic" || ext == ".jpg" || ext == ".jpeg" || ext == ".png" || ext == ".tiff" {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(files)
	t.Logf("Found %d image files", len(files))

	outDir := "/tmp/slicer-batch"
	os.RemoveAll(outDir)
	os.MkdirAll(outDir, 0755)

	opts := DefaultOptions()

	var summary []string
	totalSlices := 0
	singleSliceCount := 0

	for _, path := range files {
		name := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
		data, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("read %s: %v", path, err)
			continue
		}

		slices, err := SliceImage(data, opts)
		if err != nil {
			t.Errorf("slice %s: %v", name, err)
			summary = append(summary, fmt.Sprintf("%-20s ERROR: %v", name, err))
			continue
		}

		totalSlices += len(slices)
		if len(slices) <= 1 {
			singleSliceCount++
		}

		line := fmt.Sprintf("%-20s %2d slices", name, len(slices))
		for _, s := range slices {
			line += fmt.Sprintf("  [%d-%d h=%d]", s.Y0, s.Y1, s.Y1-s.Y0)
		}
		summary = append(summary, line)

		// Write slices
		imgDir := filepath.Join(outDir, name)
		os.MkdirAll(imgDir, 0755)
		for _, s := range slices {
			outPath := filepath.Join(imgDir, fmt.Sprintf("slice_%03d.jpg", s.Index))
			os.WriteFile(outPath, s.ImageData, 0644)
		}
	}

	t.Logf("\n=== BATCH SUMMARY (opts: threshold=%d dilation=%d minGap=%d minSlice=%d) ===",
		opts.DarknessThreshold, opts.DilationRadius, opts.MinGapHeight, opts.MinSliceHeight)
	for _, line := range summary {
		t.Logf("  %s", line)
	}
	t.Logf("  ---")
	t.Logf("  Total: %d images, %d slices, %d unsplit (%.0f%%)",
		len(files), totalSlices, singleSliceCount,
		float64(singleSliceCount)/float64(len(files))*100)
}

// TestSliceProfileDiag dumps the raw and dilated projection profile for a single
// image so we can understand why slicing isn't finding gaps.
//
// Usage:
//
//	TEST_IMAGE_PATH=~/src/cloudline/sample_logbooks/N188MC/IMG_8915.HEIC go test ./internal/slicer/ -run TestSliceProfileDiag -v -count=1
func TestSliceProfileDiag(t *testing.T) {
	path := os.Getenv("TEST_IMAGE_PATH")
	if path == "" {
		t.Skip("set TEST_IMAGE_PATH to run this test")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	img, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		// Try conversion (HEIC)
		converted, convErr := convertToJPEG(data)
		if convErr != nil {
			t.Fatalf("decode failed and conversion also failed: %v / %v", err, convErr)
		}
		img, _, err = image.Decode(bytes.NewReader(converted))
		if err != nil {
			t.Fatalf("decode converted: %v", err)
		}
	}

	bounds := img.Bounds()
	width := bounds.Dx()
	height := bounds.Dy()
	t.Logf("Image: %dx%d", width, height)

	opts := DefaultOptions()
	opts = scaleToHeight(opts, height) // match SliceImage behavior
	profile := projectionProfile(img, bounds, opts.DarknessThreshold)

	// Apply the same processing as SliceImage: noise floor then smoothing
	noiseFloor := width * 7 / 100
	for i, v := range profile {
		if v > noiseFloor {
			profile[i] = v - noiseFloor
		} else {
			profile[i] = 0
		}
	}
	smoothed := smoothProfile(profile, opts.DilationRadius)
	contentThreshold := width / 100

	// Find min/max of smoothed profile
	maxSmoothed := 0
	for _, v := range smoothed {
		if v > maxSmoothed {
			maxSmoothed = v
		}
	}
	t.Logf("Noise floor: %d (7%% of %d), smoothing radius: %d (scaled), content threshold: %d",
		noiseFloor, width, opts.DilationRadius, contentThreshold)
	t.Logf("Smoothed profile max: %d", maxSmoothed)

	// Show smoothed profile as a condensed histogram (every 20 rows)
	// Mark rows below content threshold with a different character
	t.Logf("\nSmoothed profile (every 20 rows, threshold=%d shown as |):", contentThreshold)
	for y := 0; y < height; y += 20 {
		sum := 0
		count := 0
		for dy := 0; dy < 20 && y+dy < height; dy++ {
			sum += smoothed[y+dy]
			count++
		}
		avg := sum / count
		barLen := 0
		if maxSmoothed > 0 {
			barLen = avg * 60 / maxSmoothed
		}
		marker := " "
		if avg <= contentThreshold {
			marker = "."
		}
		bar := strings.Repeat("█", barLen) + strings.Repeat("░", 60-barLen)
		t.Logf("  y=%4d: %s %4d %s", y, bar, avg, marker)
	}

	// Show the regions that would be found
	regions := findRegions(smoothed, height, opts.MinGapHeight, contentThreshold)
	t.Logf("\nRegions found (before absorb): %d", len(regions))
	for i, r := range regions {
		t.Logf("  region %d: y=[%d, %d) height=%d", i, r[0], r[1], r[1]-r[0])
	}
	if len(regions) > 1 {
		for i := 1; i < len(regions); i++ {
			gap := regions[i][0] - regions[i-1][1]
			t.Logf("  gap %d→%d: %d rows", i-1, i, gap)
		}
	}

	// Show after absorb
	minEntryHeight := height / 8
	absorbed := absorbSmallRegions(regions, minEntryHeight)
	t.Logf("\nRegions after absorb (minEntryHeight=%d): %d", minEntryHeight, len(absorbed))
	for i, r := range absorbed {
		t.Logf("  region %d: y=[%d, %d) height=%d", i, r[0], r[1], r[1]-r[0])
	}
	if len(absorbed) > 1 {
		for i := 1; i < len(absorbed); i++ {
			gap := absorbed[i][0] - absorbed[i-1][1]
			t.Logf("  gap %d→%d: %d rows", i-1, i, gap)
		}
	}
}

// TestSlicePDFBatch splits PDFs into page images via mutool (matching Lambda
// behavior), then runs the slicer on each page. Reports slice counts per page.
//
// Usage:
//
//	TEST_PDF_DIR=~/src/cloudline/sample_logbooks/N1953G go test ./internal/slicer/ -run TestSlicePDFBatch -v -count=1 -timeout=30m
//	TEST_PDF_DIR=~/src/cloudline/sample_logbooks/N69ZA go test ./internal/slicer/ -run TestSlicePDFBatch -v -count=1 -timeout=30m
func TestSlicePDFBatch(t *testing.T) {
	dir := os.Getenv("TEST_PDF_DIR")
	if dir == "" {
		t.Skip("set TEST_PDF_DIR to run this test")
	}

	mutoolPath, err := exec.LookPath("mutool")
	if err != nil {
		t.Skip("mutool not found in PATH")
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}

	var pdfs []string
	for _, e := range entries {
		if strings.ToLower(filepath.Ext(e.Name())) == ".pdf" {
			pdfs = append(pdfs, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(pdfs)
	t.Logf("Found %d PDF files in %s", len(pdfs), dir)

	outDir := "/tmp/slicer-pdf-batch"
	os.RemoveAll(outDir)
	os.MkdirAll(outDir, 0755)

	opts := DefaultOptions()
	totalPages := 0
	totalSlices := 0
	singleSlicePages := 0
	multiSlicePages := 0

	var summary []string

	for _, pdfPath := range pdfs {
		pdfName := strings.TrimSuffix(filepath.Base(pdfPath), filepath.Ext(pdfPath))
		t.Logf("\n--- Processing: %s ---", pdfName)

		// Split PDF into page images using mutool (matches Lambda pipeline at 200 DPI).
		// Homebrew mutool doesn't support JPEG output, so we use PNG locally.
		// The Lambda uses a custom mutool build with JPEG support.
		pdfOutDir := filepath.Join(outDir, pdfName)
		os.MkdirAll(pdfOutDir, 0755)
		pagePattern := filepath.Join(pdfOutDir, "page-%04d.png")

		cmd := exec.Command(mutoolPath, "draw", "-q", "-o", pagePattern, "-r", "200", pdfPath)
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Errorf("mutool failed for %s: %v\n%s", pdfName, err, output)
			continue
		}

		pages, err := filepath.Glob(filepath.Join(pdfOutDir, "page-*.png"))
		if err != nil || len(pages) == 0 {
			t.Errorf("no pages generated for %s", pdfName)
			continue
		}
		sort.Strings(pages)
		t.Logf("  Generated %d pages", len(pages))

		pdfSliceTotal := 0
		pdfSingle := 0

		for _, pagePath := range pages {
			pageNum := filepath.Base(pagePath)
			data, err := os.ReadFile(pagePath)
			if err != nil {
				t.Errorf("read %s: %v", pagePath, err)
				continue
			}

			slices, err := SliceImage(data, opts)
			if err != nil {
				t.Errorf("slice %s: %v", pagePath, err)
				continue
			}

			totalPages++
			totalSlices += len(slices)
			pdfSliceTotal += len(slices)

			if len(slices) <= 1 {
				singleSlicePages++
				pdfSingle++
			} else {
				multiSlicePages++
			}

			// Write slices for visual inspection
			sliceDir := filepath.Join(pdfOutDir, strings.TrimSuffix(pageNum, filepath.Ext(pageNum)))
			os.MkdirAll(sliceDir, 0755)
			for _, s := range slices {
				outPath := filepath.Join(sliceDir, fmt.Sprintf("slice_%03d.jpg", s.Index))
				os.WriteFile(outPath, s.ImageData, 0644)
			}

			if len(slices) > 1 {
				detail := fmt.Sprintf("    %s: %d slices", pageNum, len(slices))
				for _, s := range slices {
					detail += fmt.Sprintf("  [%d-%d h=%d]", s.Y0, s.Y1, s.Y1-s.Y0)
				}
				t.Logf("%s", detail)
			}
		}

		line := fmt.Sprintf("%-50s %3d pages  %3d slices  %3d unsplit (%.0f%%)",
			pdfName, len(pages), pdfSliceTotal, pdfSingle,
			float64(pdfSingle)/float64(len(pages))*100)
		summary = append(summary, line)
		t.Logf("  Summary: %d pages, %d slices, %d unsplit", len(pages), pdfSliceTotal, pdfSingle)
	}

	t.Logf("\n\n=== PDF BATCH SUMMARY ===")
	for _, line := range summary {
		t.Logf("  %s", line)
	}
	t.Logf("  ---")
	t.Logf("  Total: %d pages across %d PDFs, %d slices, %d unsplit (%.0f%%), %d split (%.0f%%)",
		totalPages, len(pdfs), totalSlices,
		singleSlicePages, float64(singleSlicePages)/float64(totalPages)*100,
		multiSlicePages, float64(multiSlicePages)/float64(totalPages)*100)
}
