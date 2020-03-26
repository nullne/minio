package madmin

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
)

type MaintenanceStatus struct {
	StartTime time.Time                   `json:"startTime"`
	Volumes   map[string]FileVolumeStatus `json:"volumes"`
	Status    string                      `json:"status"`
	Error     string                      `json:"error"`
}

type FileVolumeStatus struct {
	Status string `json:"status"`
	Log    string `json:"log"`
}

func (adm *AdminClient) StartDiskMaintenance(rate float64, timeRange string, drives, buckets []string) error {
	path := fmt.Sprintf("/v1/maintenance/start")
	queryVals := make(url.Values)
	queryVals.Set("rate", fmt.Sprint(rate))
	queryVals.Set("time-range", timeRange)
	for _, b := range buckets {
		queryVals.Add("buckets", b)
	}
	for _, b := range drives {
		queryVals.Add("drives", b)
	}
	return adm.maintainDisk(path, queryVals)
}

func (adm *AdminClient) FinishDiskMaintenance() error {
	path := fmt.Sprintf("/v1/maintenance/finish")
	return adm.maintainDisk(path, make(url.Values))
}

func (adm *AdminClient) maintainDisk(path string, queryVals url.Values) error {
	resp, err := adm.executeMethod("POST", requestData{
		relPath:     path,
		queryValues: queryVals,
	})
	defer closeResponse(resp)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusNoContent {
		return nil
	}

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var errResp ErrorResponse
	err = json.Unmarshal(respBytes, &errResp)
	if err != nil {
		// Unknown structure return error anyways.
		return err
	}
	return errResp
}

func (adm *AdminClient) GetDiskMaintenanceStatus() (result MaintenanceStatus, err error) {
	path := fmt.Sprintf("/v1/maintenance/status")
	queryVals := make(url.Values)
	resp, err := adm.executeMethod("POST", requestData{
		relPath:     path,
		queryValues: queryVals,
	})
	defer closeResponse(resp)
	if err != nil {
		return result, err
	}
	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return result, err
	}
	if err := json.Unmarshal(respBytes, &result); err != nil {
		// May be the server responded with error after success
		// message, handle it separately here.
		var errResp ErrorResponse
		err = json.Unmarshal(respBytes, &errResp)
		if err != nil {
			// Unknown structure return error anyways.
			return result, err
		}
		return result, errResp
	}
	return result, nil
}
