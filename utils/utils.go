package utils

import (
	"io"
	"net/http"
	"os"
	"sync"
)

//DownloadFile download files to specific path
func DownloadFile(filepath string, url string, wg *sync.WaitGroup) (err error) {
	defer wg.Done()
	// Create the file
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Writer the body to file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	return nil
}
