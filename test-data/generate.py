import json
import os
import random

def generate_record(i):
    # Mix of match types
    is_audit = i % 2 == 0
    is_k8s = i % 4 == 0
    
    project = "myprojname" if is_k8s else f"project-{i % 10}"
    log_name = (
        "projects/myprojname/logs/cloudaudit.googleapis.com/data_access"
        if is_audit
        else f"projects/{project}/logs/syslog"
    )
    
    rec = {
        "timestamp": f"2026-05-26T18:{i % 60:02d}:{i % 60:02d}Z",
        "resource": {
            "labels": {
                "project_id": project,
                "dataset_id": f"dataset-{i % 50}"
            },
            "type": "k8s_cluster" if is_k8s else "gce_instance"
        },
        "logName": log_name,
        "labels": {
            "env": "production" if i % 3 == 0 else "staging",
            "version": "v1.2.3"
        }
    }
    
    if is_audit:
        rec["protoPayload"] = {
            "authenticationInfo": {
                "principalEmail": f"user-{i % 100}@google.com"
            },
            "methodName": "google.cloud.bigquery.v2.JobService.InsertJob" if i % 2 == 0 else "google.cloud.storage.v1.Objects.Insert",
            "requestMetadata": {
                "callerIp": f"66.249.80.{i % 255}"
            }
        }
        
    return rec

def main():
    os.makedirs("test-data", exist_ok=True)
    
    # 1. Generate 200,000 JSONL records
    jsonl_path = "test-data/large_audit.jsonl"
    print(f"Generating 200,000 records to {jsonl_path}...")
    with open(jsonl_path, "w") as f:
        for i in range(200_000):
            rec = generate_record(i)
            f.write(json.dumps(rec) + "\n")
            
    # 2. Generate 200,000 YAML records as a stream
    yaml_path = "test-data/large_audit.yaml"
    print(f"Generating 200,000 YAML documents to {yaml_path}...")
    with open(yaml_path, "w") as f:
        for i in range(200_000):

            rec = generate_record(i)
            # Simple YAML dump manually to avoid dependencies
            f.write(f"timestamp: {rec['timestamp']}\n")
            f.write("resource:\n")
            f.write("  labels:\n")
            f.write(f"    project_id: {rec['resource']['labels']['project_id']}\n")
            f.write(f"    dataset_id: {rec['resource']['labels']['dataset_id']}\n")
            f.write(f"  type: {rec['resource']['type']}\n")
            f.write(f"logName: {rec['logName']}\n")
            f.write("labels:\n")
            f.write(f"  env: {rec['labels']['env']}\n")
            f.write(f"  version: {rec['labels']['version']}\n")
            if "protoPayload" in rec:
                f.write("protoPayload:\n")
                f.write("  authenticationInfo:\n")
                f.write(f"    principalEmail: {rec['protoPayload']['authenticationInfo']['principalEmail']}\n")
                f.write(f"  methodName: {rec['protoPayload']['methodName']}\n")
                f.write("  requestMetadata:\n")
                f.write(f"    callerIp: {rec['protoPayload']['requestMetadata']['callerIp']}\n")
            f.write("---\n")
            
    print("Done!")

if __name__ == "__main__":
    main()
