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

**API Security**

Dr. Kafka allows plugable API request authorization and follows the Role Based Access Control (RBAC) model. Authorization is performed by populating role-mapping in [DrKafkaSecurityContext](https://github.com/pinterest/doctorkafka/tree/master/drkafka/src/main/java/com/pinterest/doctorkafka/security/DrKafkaSecurityContext.java) by creating an implementation of AuthorizationFilter e.g. [SampleAuthorizationFilter](https://github.com/pinterest/doctorkafka/tree/master/drkafka/src/main/java/com/pinterest/doctorkafka/security/SampleAuthorizationFilter.java)

Here's the flow sequence:
1. DoctorKafkaMain checks if an authorization filter has been specified via `doctorkafka.authorization.filter.class` configuration and creates an instance of `DrKafkaAuthorizationFilter`
2. This instance is then configured (invoke `configure(DoctorKafkaConfig config)`) and registered with Jersey

All authorization filters must implement [DrKafkaAuthorizationFilter](https://github.com/pinterest/doctorkafka/tree/master/drkafka/src/main/java/com/pinterest/doctorkafka/security/DrKafkaAuthorizationFilter.java) which has two methods that need to be implemented:

- `configure(DoctorKafkaConfig config)`
- `filter(ContainerRequestContext requestContext)`  

`configure(DoctorKafkaConfig config)` provides DoctorKafkaConfig to allow authorizer to configure, `DoctorKafkaConfig.getDrKafkaAdminGroups()` returns the list of groups that need to be mapped to `drkafka_admin` role  

`filter(ContainerRequestContext requestContext)` should implement the logic to extract and populate PRINCIPAL & ROLE information which is needed to create a new instance of [DrKafkaSecurityContext](https://github.com/pinterest/doctorkafka/tree/master/drkafka/src/main/java/com/pinterest/doctorkafka/security/DrKafkaSecurityContext.java). Jersey then uses this information to restricted access to methods for users who are not in the `drkafka_admin` role. Here's the flow:

(Authentication) -> (Populates user & group info headers) -> (YourDrKafkaAuthoriziationFilter) -> (extract User and Group info) -> (Map groups to roles) -> (Create SecurityContext) -> (Inject SecurityContext back in session)

Note: We currently don't ship authentication mechanisms with Dr.Kafka since authentication requirements are environment/company specific. For plugable authentication, please refer to https://www.dropwizard.io/1.3.8/docs/manual/auth.html You may also use an authentication proxy.