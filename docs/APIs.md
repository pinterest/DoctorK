**Cluster Info API**

Basic APIs to get Cluster Info:

```
List Cluster:  curl -XGET http://localhost:8080/api/cluster
```


**Maintenance Mode API**

Allows users to disable DoctorK for a cluster so that manual maintenance operations can be performed on it without any interference from Dr. Kafka.

GET will get the current status of maintenance mode.
PUT will place the cluster in maintenance mode.
DELETE will remove the cluster from maintenance mode.


```
curl -XGET http://localhost:8080/api/cluster/<clustername>/admin/maintenance
curl -XPUT http://localhost:8080/api/cluster/<clustername>/admin/maintenance
curl -XDELETE http://localhost:8080/api/cluster/<clustername>/admin/maintenance
```

**API Security**

Dr. Kafka allows plugable API request authorization and follows the Role Based Access Control (RBAC) model. Authorization is performed by populating role-mapping in [DoctorKSecurityContext](https://github.com/pinterest/doctorkafka/tree/master/doctork/src/main/java/com/pinterest/doctork/security/DoctorKSecurityContext.java) by creating an implementation of AuthorizationFilter e.g. [SampleAuthorizationFilter](https://github.com/pinterest/doctorkafka/tree/master/doctork/src/main/java/com/pinterest/doctork/security/SampleAuthorizationFilter.java)

Here's the flow sequence:
1. DoctorKMain checks if an authorization filter has been specified via `doctork.authorization.filter.class` configuration and creates an instance of `DoctorKAuthorizationFilter`
2. This instance is then configured (invoke `configure(DoctorKConfig config)`) and registered with Jersey

All authorization filters must implement [DoctorKAuthorizationFilter](https://github.com/pinterest/doctorkafka/tree/master/doctork/src/main/java/com/pinterest/doctork/security/DoctorKAuthorizationFilter.java) which has two methods that need to be implemented:

- `configure(DoctorKConfig config)`
- `filter(ContainerRequestContext requestContext)`  

`configure(DoctorKConfig config)` provides DoctorKConfig to allow authorizer to configure, `DoctorKConfig.getDoctorKAdminGroups()` returns the list of groups that need to be mapped to `doctork_admin` role  

`filter(ContainerRequestContext requestContext)` should implement the logic to extract and populate PRINCIPAL & ROLE information which is needed to create a new instance of [DoctorKSecurityContext](https://github.com/pinterest/doctorkafka/tree/master/doctork/src/main/java/com/pinterest/doctork/security/DoctorKSecurityContext.java). Jersey then uses this information to restricted access to methods for users who are not in the `doctork_admin` role. Here's the flow:

(Authentication) -> (Populates user & group info headers) -> (YourDoctorKAuthoriziationFilter) -> (extract User and Group info) -> (Map groups to roles) -> (Create SecurityContext) -> (Inject SecurityContext back in session)

Note: We currently don't ship authentication mechanisms with Dr.Kafka since authentication requirements are environment/company specific. For plugable authentication, please refer to https://www.dropwizard.io/1.3.8/docs/manual/auth.html You may also use an authentication proxy.