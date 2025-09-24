#!/usr/bin/env python3
import os
import sys
import time

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


def load_ttl_file(base_url: str, dataset: str, ttl_path: str, admin_user: str | None = None, admin_pass: str | None = None) -> None:
    # Always load into default graph
    update_url = f"{base_url}/{dataset}/data"
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
            raise RuntimeError(f"Failed to load {ttl_path} into default graph: {resp.status_code} {resp.text}")


def main() -> None:
    # Fixed configuration: always load to default graph of local dataset "football"
    base_url = "http://localhost:3030"
    dataset = "football"
    admin_user = "admin"
    admin_pass = "admin"
    
    # Load ontology first, then data
    ontology_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "ontology"))
    ttl_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "silver", "ttl"))

    # Create dataset if missing
    ensure_dataset(base_url, dataset, admin_user, admin_pass)

    all_errors: list[tuple[str, str]] = []
    total_ok = 0

    # Step 1: Load ontology files first
    if os.path.isdir(ontology_dir):
        print("Loading ontology files...")
        ontology_files = [f for f in os.listdir(ontology_dir) if f.endswith('.ttl')]
        ontology_files.sort()
        
        for filename in tqdm(ontology_files, desc="Loading Ontology"):
            path = os.path.join(ontology_dir, filename)
            try:
                load_ttl_file(base_url, dataset, path, admin_user, admin_pass)
                total_ok += 1
            except Exception as ex:  # noqa: BLE001
                all_errors.append((f"ontology/{filename}", str(ex)))
            time.sleep(0.02)
    else:
        print(f"Warning: Ontology directory not found: {ontology_dir}")

    # Step 2: Load data files
    if os.path.isdir(ttl_dir):
        print("Loading data files...")
        data_files = [f for f in os.listdir(ttl_dir) if f.endswith('.ttl')]
        data_files.sort()
        
        for filename in tqdm(data_files, desc="Loading Data"):
            path = os.path.join(ttl_dir, filename)
            try:
                load_ttl_file(base_url, dataset, path, admin_user, admin_pass)
                total_ok += 1
            except Exception as ex:  # noqa: BLE001
                all_errors.append((f"data/{filename}", str(ex)))
            time.sleep(0.02)
    else:
        print(f"Warning: Data directory not found: {ttl_dir}")

    # Generate report
    report_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "linking", "load_report.txt"))
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as rf:
        rf.write(f"Loaded OK: {total_ok}\n")
        rf.write(f"Failed: {len(all_errors)}\n")
        for fname, msg in all_errors:
            rf.write(f"- {fname}: {msg}\n")
    
    print(f"Done. OK={total_ok}, Failed={len(all_errors)}. Report: {report_path}")


if __name__ == "__main__":
    main()


