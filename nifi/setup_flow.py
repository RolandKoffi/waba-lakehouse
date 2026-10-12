import os
import sys
import time

import requests

API = os.environ.get("NIFI_URL", "http://nifi:8080/nifi-api")
S3 = {
    "Region": os.environ.get("AWS_REGION", "us-east-1"),
    "Access Key": os.environ["MINIO_ROOT_USER"],
    "Secret Key": os.environ["MINIO_ROOT_PASSWORD"],
    "Endpoint Override URL": os.environ.get("S3_ENDPOINT", "http://minio:9000"),
    "use-path-style-access": "true",
}
BUCKET = os.environ.get("RAW_BUCKET", "raw-landing")
KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
FLOW_NAME = "waba-raw-to-kafka"

TOPIC = (
    "${filename:substringAfterLast('/'):startsWith('bank_txn'):ifElse('raw-bank-transactions',"
    "${filename:substringAfterLast('/'):startsWith('insurance_ops'):ifElse('raw-insurance-operations',"
    "${filename:substringAfterLast('/'):startsWith('mobile_money'):ifElse('raw-mobile-money-payments',"
    "'raw-loan-repayments')})})}"
)


def call(method, path, **kwargs):
    r = requests.request(method, f"{API}{path}", timeout=30, **kwargs)
    if r.status_code >= 400:
        sys.exit(f"{method} {path} -> {r.status_code}: {r.text}")
    return r.json() if r.text else {}


def wait_for_nifi():
    for _ in range(90):
        try:
            if requests.get(f"{API}/flow/about", timeout=5).ok:
                return
        except requests.RequestException:
            pass
        print("attente de NiFi...")
        time.sleep(10)
    sys.exit("NiFi indisponible")


def create_group(root):
    existing = call("GET", f"/process-groups/{root}/process-groups")["processGroups"]
    for pg in existing:
        if pg["component"]["name"] == FLOW_NAME:
            return None
    body = {"revision": {"version": 0}, "component": {"name": FLOW_NAME, "position": {"x": 0, "y": 0}}}
    return call("POST", f"/process-groups/{root}/process-groups", json=body)["id"]


def supported(descriptors, props):
    kept = {k: v for k, v in props.items() if k in descriptors or descriptors.get("__dynamic__")}
    for k in set(props) - set(kept):
        print(f"propriété ignorée (non supportée par cette version) : {k}")
    return kept


def controller_service(pg, ctype, name, props):
    body = {"revision": {"version": 0}, "component": {"type": ctype, "name": name}}
    cs = call("POST", f"/process-groups/{pg}/controller-services", json=body)
    descriptors = cs["component"]["descriptors"]
    body = {"revision": cs["revision"],
            "component": {"id": cs["id"], "properties": supported(descriptors, props)}}
    cs = call("PUT", f"/controller-services/{cs['id']}", json=body)
    call("PUT", f"/controller-services/{cs['id']}/run-status",
         json={"revision": cs["revision"], "state": "ENABLED"})
    return cs["id"]


def processor(pg, ptype, name, x, y, props, auto_terminate=(), schedule="0 sec", dynamic=False):
    body = {"revision": {"version": 0},
            "component": {"type": ptype, "name": name, "position": {"x": x, "y": y}}}
    p = call("POST", f"/process-groups/{pg}/processors", json=body)
    descriptors = dict(p["component"]["config"]["descriptors"])
    if dynamic:
        descriptors["__dynamic__"] = True
    config = {"properties": supported(descriptors, props), "schedulingPeriod": schedule,
              "autoTerminatedRelationships": list(auto_terminate), "penaltyDuration": "10 sec"}
    p = call("PUT", f"/processors/{p['id']}",
             json={"revision": p["revision"], "component": {"id": p["id"], "config": config}})
    return p["id"]


def connect(pg, source, dest, relationships, objects=10000, size="1 GB"):
    body = {"revision": {"version": 0}, "component": {
        "source": {"id": source, "groupId": pg, "type": "PROCESSOR"},
        "destination": {"id": dest, "groupId": pg, "type": "PROCESSOR"},
        "selectedRelationships": relationships,
        "backPressureObjectThreshold": objects,
        "backPressureDataSizeThreshold": size,
        "flowFileExpiration": "0 sec",
    }}
    call("POST", f"/process-groups/{pg}/connections", json=body)


def main():
    wait_for_nifi()
    root = call("GET", "/flow/process-groups/root")["processGroupFlow"]["id"]
    pg = create_group(root)
    if pg is None:
        print("flux déjà présent, rien à faire")
        return

    csv_reader = controller_service(pg, "org.apache.nifi.csv.CSVReader", "CSV reader", {
        "schema-access-strategy": "csv-header-derived",
        "Skip Header Line": "true",
    })
    json_reader = controller_service(pg, "org.apache.nifi.json.JsonTreeReader", "JSON reader", {})
    json_writer = controller_service(pg, "org.apache.nifi.json.JsonRecordSetWriter", "JSON writer (1 objet/ligne)", {
        "output-grouping": "output-oneline",
    })

    list_s3 = processor(pg, "org.apache.nifi.processors.aws.s3.ListS3", "ListS3 raw-landing", 0, 0,
                        {**S3, "Bucket": BUCKET}, schedule="5 sec")
    route = processor(pg, "org.apache.nifi.processors.standard.RouteOnAttribute", "Ignorer les référentiels",
                      0, 200, {"transactions": "${filename:startsWith('referentials/'):not()}"},
                      auto_terminate=["unmatched"], dynamic=True)
    fetch = processor(pg, "org.apache.nifi.processors.aws.s3.FetchS3Object", "FetchS3Object", 0, 400,
                      {**S3, "Bucket": "${s3.bucket}", "Object Key": "${filename}"},
                      auto_terminate=["permission.denied", "not.found"])
    attrs = processor(pg, "org.apache.nifi.processors.attributes.UpdateAttribute", "Topic + pays + source", 0, 600, {
        "kafka.topic": TOPIC,
        "country": "${filename:substringBefore('/')}",
        "source.file": f"s3://{BUCKET}/${{filename}}",
    }, dynamic=True)
    enrich = processor(pg, "org.apache.nifi.processors.standard.UpdateRecord", "Ajout ingestion_timestamp/source_file",
                       0, 800, {
                           "record-reader": csv_reader,
                           "record-writer": json_writer,
                           "replacement-value-strategy": "literal-value",
                           "/ingestion_timestamp": "${now():format(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\", 'GMT')}",
                           "/source_file": "${source.file}",
                       }, dynamic=True)
    publish = processor(pg, "org.apache.nifi.processors.kafka.pubsub.PublishKafkaRecord_2_6", "Publish raw-*",
                        0, 1000, {
                            "bootstrap.servers": KAFKA,
                            "topic": "${kafka.topic}",
                            "record-reader": json_reader,
                            "record-writer": json_writer,
                            "use-transactions": "false",
                            "acks": "all",
                            "message-key-field": "country_code",
                            "compression.type": "snappy",
                            "max.request.size": "1 MB",
                        }, auto_terminate=["success"])
    dlq = processor(pg, "org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6", "Publish DLQ (fichier illisible)",
                    400, 800, {
                        "bootstrap.servers": KAFKA,
                        "topic": "dlq-financial-events",
                        "use-transactions": "false",
                        "acks": "all",
                    }, auto_terminate=["success"])

    connect(pg, list_s3, route, ["success"])
    connect(pg, route, fetch, ["transactions"])
    connect(pg, fetch, attrs, ["success"])
    connect(pg, fetch, fetch, ["failure"], objects=1000)
    connect(pg, attrs, enrich, ["success"], objects=200, size="500 MB")
    connect(pg, enrich, publish, ["success"], objects=50, size="200 MB")
    connect(pg, enrich, dlq, ["failure"], objects=100)
    connect(pg, publish, publish, ["failure"], objects=50, size="200 MB")
    connect(pg, dlq, dlq, ["failure"], objects=100)

    status = call("GET", f"/process-groups/{pg}/processors")["processors"]
    for p in status:
        errors = p["component"].get("validationErrors")
        if errors:
            print(f"{p['component']['name']} invalide : {errors}")
    call("PUT", f"/flow/process-groups/{pg}", json={"id": pg, "state": "RUNNING"})
    print("flux NiFi démarré")


if __name__ == "__main__":
    main()
