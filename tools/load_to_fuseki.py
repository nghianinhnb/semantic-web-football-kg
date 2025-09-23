#!/usr/bin/env python3
import argparse
import os
import sys
import time
from urllib.parse import quote

import re
import requests
from tqdm import tqdm


def ensure_dataset(base_url: str, dataset: str, admin_user: str, admin_pass: str) -> None:
    # Try HEAD to check
    r = requests.head(f"{base_url}/{dataset}")
    if r.status_code == 200:
        return
    # Create dataset via Fuseki admin (works for stain/jena-fuseki)
    create_url = f"{base_url}/$/datasets"
    data = {
        "dbName": dataset,
        "dbType": "mem"
    }
    auth = (admin_user, admin_pass) if admin_user or admin_pass else None
    resp = requests.post(create_url, data=data, auth=auth)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Cannot create dataset {dataset}: {resp.status_code} {resp.text}")


PREFIX_HEADER = (
    b"@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
    b"@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n"
    b"@prefix owl: <http://www.w3.org/2002/07/owl#> .\n"
    b"@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
    b"@prefix foaf: <http://xmlns.com/foaf/0.1/> .\n"
    b"@prefix schema: <http://schema.org/> .\n"
    b"@prefix kg: <https://kg-football.vn/ontology#> .\n"
    b"@prefix res: <https://kg-football.vn/resource/> .\n\n"
)


def ensure_prefixes(content: bytes) -> bytes:
    # naive check: if '@prefix res:' not present, prepend common header
    lower = content.lower()
    if b"@prefix res" in lower:
        return content
    return PREFIX_HEADER + content


def load_ttl_file(base_url: str, dataset: str, ttl_path: str, graph_uri: str, admin_user: str | None = None, admin_pass: str | None = None) -> None:
    update_url = f"{base_url}/{dataset}/data?graph={quote(graph_uri, safe=':/#?=&') }"
    with open(ttl_path, "rb") as f:
        data_bytes = ensure_prefixes(f.read())
        # Replace prefixed res:LOCAL with full IRI if LOCAL contains invalid chars like '/'
        # Work on text level
        text = data_bytes.decode("utf-8")
        pattern = re.compile(r"\bres:([^\s;,.()\]\">]+)")
        def repl(m: re.Match) -> str:
            local = m.group(1)
            # If local contains '/', or other characters that may break PN_LOCAL, expand to full IRI
            if "/" in local or "#" in local:
                return f"<https://kg-football.vn/resource/{local}>"
            # Otherwise keep as is
            return m.group(0)
        text = pattern.sub(repl, text)
        data_bytes = text.encode("utf-8")
        headers = {"Content-Type": "text/turtle"}
        auth = (admin_user, admin_pass) if admin_user or admin_pass else None
        resp = requests.post(update_url, data=data_bytes, headers=headers, auth=auth)
        if resp.status_code not in (200, 201, 204):
            raise RuntimeError(f"Failed to load {ttl_path} into {graph_uri}: {resp.status_code} {resp.text}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Load TTL files into Fuseki as named graphs")
    parser.add_argument("--fuseki", default="http://localhost:3030", help="Fuseki base URL")
    parser.add_argument("--dataset", default="football", help="Dataset name")
    parser.add_argument("--ttl_dir", default=os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "silver", "ttl")), help="Directory of TTL files")
    parser.add_argument("--graph_base", default="http://kg.local/silver/", help="Base URI for named graphs")
    parser.add_argument("--admin_user", default="admin", help="Fuseki admin user")
    parser.add_argument("--admin_pass", default="admin", help="Fuseki admin password")
    args = parser.parse_args()

    ttl_dir = os.path.abspath(args.ttl_dir)
    if not os.path.isdir(ttl_dir):
        print(f"TTL directory not found: {ttl_dir}", file=sys.stderr)
        sys.exit(1)

    # Create dataset if missing
    ensure_dataset(args.fuseki, args.dataset, args.admin_user, args.admin_pass)

    files = [f for f in os.listdir(ttl_dir) if f.endswith('.ttl')]
    files.sort()
    errors: list[tuple[str, str]] = []
    ok_count = 0
    for filename in tqdm(files, desc="Loading TTL into Fuseki"):
        path = os.path.join(ttl_dir, filename)
        name_no_ext = os.path.splitext(filename)[0]
        graph_uri = f"{args.graph_base}{name_no_ext}"
        try:
            load_ttl_file(args.fuseki, args.dataset, path, graph_uri, args.admin_user, args.admin_pass)
            ok_count += 1
        except Exception as ex:  # noqa: BLE001
            errors.append((filename, str(ex)))
        # brief sleep to avoid overwhelming endpoint
        time.sleep(0.02)

    report_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "linking", "load_report.txt"))
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as rf:
        rf.write(f"Loaded OK: {ok_count}\n")
        rf.write(f"Failed: {len(errors)}\n")
        for fname, msg in errors:
            rf.write(f"- {fname}: {msg}\n")
    print(f"Done. OK={ok_count}, Failed={len(errors)}. Report: {report_path}")


if __name__ == "__main__":
    main()


