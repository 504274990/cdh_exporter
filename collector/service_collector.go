package collector

import (
	"bytes"
	"encoding/json"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/promlog"
	"gopkg.in/alecthomas/kingpin.v2"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
)

var (
	logger       = promlog.New(&promlog.Config{})
	cdhAddress   = kingpin.Flag("cdh.address", "Address to listen on for web interface.").Default("1.1.1.1:17180").String()
	cdhCompenent = kingpin.Flag("cdh.compenent", "Components to be monitored.").Default("hbase", "hdfs", "zookeeper", "yarn").Strings()
	userAccount  = kingpin.Flag("user.account", "CDH account password, encrypted with Base64").Default("Basic XXXXXXXXXXXXXXXXXXX").String()
	apiVersion   = kingpin.Flag("api.version", "CDH api version").Default("v33").String()
	clusterName  = kingpin.Flag("cluster.name", "CDH cluster name").Default("Cluster 1").String()
)

type serviceCollector struct {
	servieState            *prometheus.Desc
	serviceStateSummary    *prometheus.Desc
	servieRoleState        *prometheus.Desc
	servieRoleStateSummary *prometheus.Desc
	sMutex                 sync.Mutex
}

func NewServiceCollector() *serviceCollector {
	return &serviceCollector{
		servieState: prometheus.NewDesc(
			"cdh_service_state",
			"cdh component status, GOOD: 0, DISABLED: 1, HISTORY_NOT_AVAILABLE: 2, NOT_AVAILABLE: 3, CONCERNING: 4, BAD: 5, UNKNOW: 6",
			[]string{"service_type", "health_check_name", "explanation"},
			nil,
		),
		serviceStateSummary: prometheus.NewDesc(
			"cdh_service_state_summary",
			"cdh service summary status, GOOD: 0, DISABLED: 1, HISTORY_NOT_AVAILABLE: 2, NOT_AVAILABLE: 3, CONCERNING: 4, BAD: 5, UNKNOW: 6",
			[]string{"service_type"},
			nil,
		),
		servieRoleState: prometheus.NewDesc(
			"cdh_service_role_state",
			"cdh component role status, GOOD: 0, DISABLED: 1, HISTORY_NOT_AVAILABLE: 2, NOT_AVAILABLE: 3, CONCERNING: 4, BAD: 5, UNKNOW: 6",
			[]string{"role_type", "cluster_name", "host_name", "service_type", "health_check_name", "explanation", "role_name"},
			nil,
		),
		servieRoleStateSummary: prometheus.NewDesc(
			"cdh_service_role_state_summary",
			"cdh service role summary status, GOOD: 0, DISABLED: 1, HISTORY_NOT_AVAILABLE: 2, NOT_AVAILABLE: 3, CONCERNING: 4, BAD: 5, UNKNOW: 6",
			[]string{"role_type", "cluster_name", "host_name", "role_name", "service_type"},
			nil,
		),
	}
}

func (s *serviceCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- s.servieState
	ch <- s.serviceStateSummary
	ch <- s.servieRoleState
	ch <- s.servieRoleStateSummary
}

func (s *serviceCollector) Collect(ch chan<- prometheus.Metric) {
	s.sMutex.Lock()
	defer s.sMutex.Unlock()
	clusterNameUrl := url.PathEscape(*clusterName)

	for _, cdhSvc := range *cdhCompenent {
		svcUrl := assembleSvcUrl(*cdhAddress, cdhSvc, *apiVersion, clusterNameUrl)
		var f interface{}
		err := json.Unmarshal(cdhResponse(svcUrl, "GET", nil, *userAccount), &f)
		if err != nil {
			level.Error(logger).Log("msg", err)
		}
		m := f.(map[string]interface{})
		var nameHealthList = m["healthChecks"].([]interface{})

		ch <- prometheus.MustNewConstMetric(
			s.serviceStateSummary,
			prometheus.GaugeValue,
			checkStatus(m["healthSummary"].(string)),
			m["type"].(string),
		)

		for _, nameHealthData := range nameHealthList {
			nameHealthData := nameHealthData.(map[string]interface{})
			ch <- prometheus.MustNewConstMetric(
				s.servieState,
				prometheus.GaugeValue,
				checkStatus(nameHealthData["summary"].(string)),
				m["type"].(string),
				nameHealthData["name"].(string),
				nameHealthData["explanation"].(string),
			)
		}
	}

	for _, cdhSvc := range *cdhCompenent {
		svcRoleUrl := assembleSvcUrl(*cdhAddress, cdhSvc+"/roles", *apiVersion, clusterNameUrl)
		var r interface{}
		err := json.Unmarshal(cdhResponse(svcRoleUrl, "GET", nil, *userAccount), &r)
		if err != nil {
			level.Error(logger).Log("msg", err)
		}
		m := r.(map[string]interface{})
		var roleList = m["items"].([]interface{})

		for _, roleData := range roleList {
			roleData := roleData.(map[string]interface{})
			var serviceRefData = roleData["serviceRef"].(map[string]interface{})
			hostRefData := roleData["hostRef"].(map[string]interface{})
			var roleHealthList = roleData["healthChecks"].([]interface{})

			ch <- prometheus.MustNewConstMetric(
				s.servieRoleStateSummary,
				prometheus.GaugeValue,
				checkStatus(roleData["healthSummary"].(string)),
				roleData["type"].(string),
				serviceRefData["clusterName"].(string),
				hostRefData["hostname"].(string),
				roleData["name"].(string),
				serviceRefData["serviceType"].(string),
			)

			for _, roleHealthData := range roleHealthList {
				roleHealthData := roleHealthData.(map[string]interface{})

				ch <- prometheus.MustNewConstMetric(
					s.servieRoleState,
					prometheus.GaugeValue,
					checkStatus(roleHealthData["summary"].(string)),
					roleData["type"].(string),
					serviceRefData["clusterName"].(string),
					hostRefData["hostname"].(string),
					serviceRefData["serviceType"].(string),
					roleHealthData["name"].(string),
					roleHealthData["explanation"].(string),
					roleData["name"].(string),
				)
			}
		}

	}
}

func checkStatus(status string) float64 {
	switch status {
	case "GOOD":
		return 0
	case "DISABLED":
		return 1
	case "HISTORY_NOT_AVAILABLE":
		return 2
	case "NOT_AVAILABLE":
		return 3
	case "CONCERNING":
		return 4
	case "BAD":
		return 5
	default:
		level.Error(logger).Log("msg", "Get an unknown service summary")
		return 6
	}
}

func assembleSvcUrl(url string, service string, apiVersion string, clustername string) string {
	var buf bytes.Buffer
	buf.WriteString("http://")
	buf.WriteString(url)
	buf.WriteString("/api/")
	buf.WriteString(apiVersion)
	buf.WriteString("/clusters/")
	buf.WriteString(clustername)
	buf.WriteString("/services/")
	buf.WriteString(service)
	buf.WriteString("?view=FULL_WITH_HEALTH_CHECK_EXPLANATION")
	return buf.String()
}

func cdhResponse(url string, method string, reqBody io.Reader, account string) []byte {
	client := &http.Client{}

	if method == "POST" {
		req, err := http.NewRequest(method, url, reqBody)
		if err != nil {
			level.Error(logger).Log("msg", err)
		}
		req.Header.Add("content-type", "application/json;charset=utf-8")
		req.Header.Add("Accept-Language", "zh-CN,zh;q=0.9")
		req.Header.Add("Authorization", account)
		res, err := client.Do(req)
		if err != nil {
			level.Error(logger).Log("msg", err)
		}
		defer res.Body.Close()
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			level.Error(logger).Log("msg", err)
		}
		return body
	}

	if method == "GET" {
		req, err := http.NewRequest(method, url, nil)
		if err != nil {
			level.Error(logger).Log("msg", err)
		}
		req.Header.Add("content-type", "application/json;charset=utf-8")
		req.Header.Add("Accept-Language", "zh-CN,zh;q=0.9")
		req.Header.Add("Authorization", account)
		res, err := client.Do(req)
		if err != nil {
			level.Error(logger).Log("msg", err)
		}
		defer res.Body.Close()
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			level.Error(logger).Log("msg", err)
		}
		return body
	}

	return nil
}
