#!/usr/bin/env python3
"""
Script để xử lý file alignment_players.ttl (external linking),
lọc các cặp entity có measure >= 0.96 và < 1.0,
và tạo các triple owl:sameAs để đẩy lên Fuseki.
"""

import os
import sys
import xml.etree.ElementTree as ET
from typing import List, Tuple
import requests
from tqdm import tqdm


def parse_alignment_ttl(file_path: str, threshold: float = 0.96) -> List[Tuple[str, str, float]]:
    """
    Parse file alignment_players.ttl và trả về danh sách các cặp entity có measure >= threshold và < 1.0.
    
    Args:
        file_path: Đường dẫn đến file alignment_players.ttl
        threshold: Ngưỡng measure tối thiểu (mặc định 0.96)
    
    Returns:
        List các tuple (entity1, entity2, measure)
    """
    print(f"Đang đọc file: {file_path}")

    tree = ET.parse(file_path)
    root = tree.getroot()

    namespaces = {
        'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
        'align': 'http://knowledgeweb.semanticweb.org/heterogeneity/alignment#'
    }

    matches: List[Tuple[str, str, float]] = []

    cells = root.findall('.//align:Cell', namespaces)
    print(f"Tìm thấy {len(cells)} cells trong file")

    for cell in tqdm(cells, desc="Đang xử lý cells"):
        try:
            entity1_elem = cell.find('align:entity1', namespaces)
            entity2_elem = cell.find('align:entity2', namespaces)
            measure_elem = cell.find('align:measure', namespaces)

            if entity1_elem is None or entity2_elem is None or measure_elem is None:
                continue

            entity1 = entity1_elem.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
            entity2 = entity2_elem.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
            measure_text = (measure_elem.text or '').strip()
            if not measure_text:
                continue
            measure = float(measure_text)

            if measure >= threshold:
                matches.append((entity1, entity2, measure))
        except Exception as ex:  # noqa: BLE001
            print(f"Bỏ qua 1 cell do lỗi: {ex}")
            continue

    print(f"Tìm thấy {len(matches)} cặp entity có measure >= {threshold}")
    return matches


def generate_owl_sameas_ttl(matches: List[Tuple[str, str, float]], threshold: float) -> str:
    """Tạo nội dung TTL với các triple owl:sameAs từ danh sách matches."""
    header = (
        "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
        "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n"
        "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n"
        "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
        "@prefix foaf: <http://xmlns.com/foaf/0.1/> .\n"
        "@prefix schema: <http://schema.org/> .\n"
        "@prefix kg: <https://kg-football.vn/ontology#> .\n"
        "@prefix res: <https://kg-football.vn/resource/> .\n\n"
        f"# Các triple owl:sameAs từ external linking với measure >= {threshold}\n\n"
    )

    lines = [header]
    for entity1, entity2, measure in matches:
        lines.append(f"# Measure: {measure}")
        lines.append(f"<{entity1}> owl:sameAs <{entity2}> .")
        lines.append(f"<{entity2}> owl:sameAs <{entity1}> .\n")

    return "\n".join(lines)


def ensure_dataset(base_url: str, dataset: str, admin_user: str, admin_pass: str) -> None:
    r = requests.head(f"{base_url}/{dataset}")
    if r.status_code == 200:
        return
    create_url = f"{base_url}/$/datasets"
    data = {"dbName": dataset, "dbType": "mem"}
    auth = (admin_user, admin_pass) if admin_user or admin_pass else None
    resp = requests.post(create_url, data=data, auth=auth)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Cannot create dataset {dataset}: {resp.status_code} {resp.text}")


def load_ttl_to_fuseki(base_url: str, dataset: str, ttl_content: str, admin_user: str | None = None, admin_pass: str | None = None) -> None:
    update_url = f"{base_url}/{dataset}/data"
    data_bytes = ttl_content.encode("utf-8")
    headers = {"Content-Type": "text/turtle"}
    auth = (admin_user, admin_pass) if admin_user or admin_pass else None
    resp = requests.post(update_url, data=data_bytes, headers=headers, auth=auth)
    if resp.status_code not in (200, 201, 204):
        raise RuntimeError(f"Failed to load TTL content: {resp.status_code} {resp.text}")


def main() -> None:
    base_url = "http://localhost:3030"
    dataset = "football"
    admin_user = "admin"
    admin_pass = "admin"
    threshold = 0.7

    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.abspath(os.path.join(script_dir, "..", "..", "linking", "links", "external_linking.ttl"))

    if not os.path.exists(input_path):
        print(f"Lỗi: Không tìm thấy file {input_path}")
        sys.exit(1)

    print("Bước 1: Đang parse file alignment_players.ttl...")
    matches = parse_alignment_ttl(input_path, threshold)

    if not matches:
        print(f"Không tìm thấy cặp entity nào có measure >= {threshold} và < 1.0")
        return

    print("Bước 2: Đang tạo nội dung TTL...")
    ttl_content = generate_owl_sameas_ttl(matches, threshold)

    temp_ttl_path = os.path.join(script_dir, "temp_external_sameas.ttl")
    with open(temp_ttl_path, "w", encoding="utf-8") as f:
        f.write(ttl_content)
    print(f"Đã lưu nội dung TTL vào: {temp_ttl_path}")

    print("Bước 3: Đang đẩy dữ liệu lên Fuseki...")
    ensure_dataset(base_url, dataset, admin_user, admin_pass)
    load_ttl_to_fuseki(base_url, dataset, ttl_content, admin_user, admin_pass)

    print(f"✅ Hoàn thành! Đã đẩy {len(matches)} cặp owl:sameAs (external) lên Fuseki")
    print(f"Dataset: {base_url}/{dataset}")

    os.remove(temp_ttl_path)
    print("Đã xóa file tạm thời")


if __name__ == "__main__":
    main()
