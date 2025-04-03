# Transmogrifier

Transmogrifier translates jsonl and yaml streams. You can customize the output objects' structure as well as output json, jsonl, jsonp, yaml, or csv.

Example config:

```yaml
# match-rule: all
match-rule: drop-no-match

common-output:
- timestamp: timestamp
- project: resource.labels.project_id
- log-name:
    src: logName
    regex: projects/.*?/logs/(.*)
    value: $1
- resource-type: resource.type
- resource-labels: resource.labels
- labels: labels

specific-outputs:
- field: logName
  eq: projects/myprojname/logs/cloudaudit.googleapis.com/data_access
  and:
  - field: resource.type
    eq: k8s_cluster
  output:
  - dataset-id: resource.labels.dataset_id
- field: logName
  matches: .*?cloudaudit.googleapis.com/.*
  output:
  - principal: protoPayload.authenticationInfo.principalEmail
  - method-name: protoPayload.methodName
  - caller-ip: protoPayload.requestMetadata.callerIp
  - test-custom:
      mn: protoPayload.methodName 
      rn: protoPayload.methodName 
```
