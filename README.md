# cdh_exporter
通过采集 cdh 的监控指标并暴露为 prometheus 格式，便于在 grafana 上配置面板及纳入告警系统

本工具适合 cdh 管理的大数据集群，可以通过此 exporter 直接采集 Cloudera Manager 的指标并暴露为 prometheus 格式，便于纳入自有的基于 prometheus 的监控告警集群。区别于通过 jmx 采集监控指标，此 exporter 只需要在 k8s 部署一个 pod 就可以采集指标，而不是通过 jmx ，需要在每台大数据集群节点部署 jmx 采集服务，更加便于管理。
