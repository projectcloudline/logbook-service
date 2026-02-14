package slicer

import (
	"fmt"
	"os"
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
