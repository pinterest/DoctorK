**Cluster Info API**

Basic APIs to get Cluster Info:

```
List Cluster:  curl -XGET http://localhost:8080/api/cluster
```


**Maintenance Mode API**

Allows users to disable DoctorKafka for a cluster so that manual maintenance operations can be performed on it without any interference from Dr. Kafka.

GET will get the current status of maintenance mode.
PUT will place the cluster in maintenance mode.
DELETE will remove the cluster from maintenance mode.


```
curl -XGET http://localhost:8080/api/cluster/<clustername>/admin/maintenance
curl -XPUT http://localhost:8080/api/cluster/<clustername>/admin/maintenance
curl -XDELETE http://localhost:8080/api/cluster/<clustername>/admin/maintenance
```