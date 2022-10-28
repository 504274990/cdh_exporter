package collector

import (
	"bytes"
	"encoding/json"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"sync"
	"time"
)

type metricInfo struct {
	Desc *prometheus.Desc
}

func newServiceTimeseriesMetric(metricName string, helpString string) *metricInfo {
	return &metricInfo{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName("", "cdh", metricName),
			helpString,
			[]string{"entity_name", "service_type", "category", "hostname", "role_type", "service_name"},
			nil,
		),
	}
}

type serviceTimeseriesExporter struct {
	serverMetrics map[string]metricInfo
	sMutex        sync.Mutex
}

func NewServiceTimeseriesExporter() *serviceTimeseriesExporter {
	timeseriesSchemaUrl := "http://" + *cdhAddress + "/api/" + *apiVersion + "/timeseries/schema"
	var s interface{}
	err := json.Unmarshal(cdhResponse(timeseriesSchemaUrl, "GET", nil, *userAccount), &s)

	if err != nil {
		level.Error(logger).Log("msg", err)
	}

	m := s.(map[string]interface{})
	var timeSeriesSchemaDataList = m["items"].([]interface{})
	sMetrics := make(map[string]metricInfo)

	for _, timeSeriesSchemaData := range timeSeriesSchemaDataList {
		timeSeriesSchemaData := timeSeriesSchemaData.(map[string]interface{})
		metricName := timeSeriesSchemaData["name"].(string)
		description := timeSeriesSchemaData["description"].(string)
		sMetrics[metricName] = *newServiceTimeseriesMetric(metricName, description)
	}

	return &serviceTimeseriesExporter{
		serverMetrics: sMetrics,
	}
}

func (s *serviceTimeseriesExporter) Describe(ch chan<- *prometheus.Desc) {
	for _, sMetric := range s.serverMetrics {
		ch <- sMetric.Desc
	}
}

func (s *serviceTimeseriesExporter) Collect(ch chan<- prometheus.Metric) {
	s.sMutex.Lock()
	defer s.sMutex.Unlock()

	serviceTimeseriesUrl := "http://" + *cdhAddress + "/api/" + *apiVersion + "/timeseries"
	serviceReqBody := bytes.NewReader(assembleBody("SERVICE"))
	roleTimeseriesUrl := "http://" + *cdhAddress + "/api/" + *apiVersion + "/timeseries"
	roleReqBody := bytes.NewReader(assembleBody("ROLE"))

	var t interface{}
	serviceErr := json.Unmarshal(cdhResponse(serviceTimeseriesUrl, "POST", serviceReqBody, *userAccount), &t)

	if serviceErr != nil {
		level.Error(logger).Log("msg", serviceErr)
	}

	serviceMap := t.(map[string]interface{})
	var serviceTimeSeriesList = serviceMap["items"].([]interface{})

	for _, serviceTimeSeriesDatas := range serviceTimeSeriesList {
		serviceTimeSeriesDatas := serviceTimeSeriesDatas.(map[string]interface{})
		var serviceTimeSeriesData = serviceTimeSeriesDatas["timeSeries"].([]interface{})

		for _, serviceTimeSeries := range serviceTimeSeriesData {
			serviceTimeSeries := serviceTimeSeries.(map[string]interface{})
			var metadata = serviceTimeSeries["metadata"].(map[string]interface{})
			var data = serviceTimeSeries["data"].([]interface{})

			if len(data) == 0 {
				continue
			} else {
				var metricValue = data[len(data)-1].(map[string]interface{})
				metricName := metadata["metricName"].(string)
				metricDesc, ok := s.serverMetrics[metricName]
				var attributes = metadata["attributes"].(map[string]interface{})

				if ok {
					//var unitNumerators = metadata["unitNumerators"].([]interface{})
					ch <- prometheus.MustNewConstMetric(
						metricDesc.Desc,
						prometheus.GaugeValue,
						metricValue["value"].(float64),
						attributes["entityName"].(string),
						attributes["serviceType"].(string),
						attributes["category"].(string),
						"",
						"",
						attributes["serviceName"].(string),
					)
				} else {
					level.Error(logger).Log("msg", metricName+" metric not exist")
				}
			}
		}
	}

	var r interface{}
	roleErr := json.Unmarshal(cdhResponse(roleTimeseriesUrl, "POST", roleReqBody, *userAccount), &r)

	if roleErr != nil {
		level.Error(logger).Log("msg", roleErr)
	}

	roleMap := r.(map[string]interface{})
	var roleTimeSeriesList = roleMap["items"].([]interface{})

	for _, roleTimeSeriesDatas := range roleTimeSeriesList {
		roleTimeSeriesDatas := roleTimeSeriesDatas.(map[string]interface{})
		var roleTimeSeriesData = roleTimeSeriesDatas["timeSeries"].([]interface{})

		for _, serviceTimeSeries := range roleTimeSeriesData {
			roleTimeSeries := serviceTimeSeries.(map[string]interface{})
			var metadata = roleTimeSeries["metadata"].(map[string]interface{})
			var data = roleTimeSeries["data"].([]interface{})

			if len(data) == 0 {
				continue
			} else {
				var metricValue = data[len(data)-1].(map[string]interface{})
				metricName := metadata["metricName"].(string)
				metricDesc, ok := s.serverMetrics[metricName]
				var attributes = metadata["attributes"].(map[string]interface{})

				if ok {
					ch <- prometheus.MustNewConstMetric(
						metricDesc.Desc,
						prometheus.GaugeValue,
						metricValue["value"].(float64),
						attributes["entityName"].(string),
						attributes["serviceType"].(string),
						attributes["category"].(string),
						attributes["hostname"].(string),
						attributes["roleType"].(string),
						attributes["serviceName"].(string),
					)
				} else {
					level.Error(logger).Log("msg", metricName+" metric not exist")
				}
			}
		}
	}
}

func assembleBody(category string) []byte {

	type ReqBody struct {
		TsQuery string `json:"query"`
		From    string `json:"from"`
	}

	reqBody := ReqBody{
		TsQuery: "SELECT * WHERE category = " + category,
		From:    time.Now().Add(-2*time.Minute).UTC().Format(time.RFC3339),
	}

	reqBodyJson, err := json.Marshal(reqBody)

	if err != nil {
		level.Error(logger).Log("msg", err)
	}
	
	return reqBodyJson
}
