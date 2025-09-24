#!/usr/bin/env python3
"""
Script để xử lý file internal_linking.ttl, lọc các cặp entity có measure >= 0.95
và tạo các triple owl:sameAs để đẩy lên Fuseki.
"""

import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from typing import List, Tuple
import requests
from tqdm import tqdm


def parse_internal_linking_ttl(file_path: str, threshold: float = 0.95) -> List[Tuple[str, str, float]]:
    """
    Parse file internal_linking.ttl và trả về danh sách các cặp entity có measure >= threshold.
    
    Args:
        file_path: Đường dẫn đến file internal_linking.ttl
        threshold: Ngưỡng measure tối thiểu (mặc định 0.95)
    
    Returns:
        List các tuple (entity1, entity2, measure)
    """
    print(f"Đang đọc file: {file_path}")
    
    # Parse XML
    tree = ET.parse(file_path)
    root = tree.getroot()
    
    # Namespace mapping
    namespaces = {
        'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
        'align': 'http://knowledgeweb.semanticweb.org/heterogeneity/alignment#'
    }
    
    matches = []
    
    # Tìm tất cả các Cell elements
    cells = root.findall('.//align:Cell', namespaces)
    print(f"Tìm thấy {len(cells)} cells trong file")
    
    for cell in tqdm(cells, desc="Đang xử lý cells"):
        try:
            # Lấy entity1 và entity2
            entity1_elem = cell.find('align:entity1', namespaces)
            entity2_elem = cell.find('align:entity2', namespaces)
            measure_elem = cell.find('align:measure', namespaces)
            
            if entity1_elem is not None and entity2_elem is not None and measure_elem is not None:
                entity1 = entity1_elem.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
                entity2 = entity2_elem.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
                measure = float(measure_elem.text)
                
                # Chỉ lấy các cặp có measure >= threshold và < 1.0 (loại bỏ self-links)
                if measure >= threshold and measure < 1.0 and entity1 != entity2:
                    matches.append((entity1, entity2, measure))
        
        except (ValueError, AttributeError) as e:
            print(f"Lỗi khi xử lý cell: {e}")
            continue
    
    print(f"Tìm thấy {len(matches)} cặp entity có measure >= {threshold}")
    return matches


def generate_owl_sameas_ttl(matches: List[Tuple[str, str, float]]) -> str:
    """
    Tạo nội dung TTL với các triple owl:sameAs từ danh sách matches.
    
    Args:
        matches: List các tuple (entity1, entity2, measure)
    
    Returns:
        Nội dung TTL dưới dạng string
    """
    ttl_content = """@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix schema: <http://schema.org/> .
@prefix kg: <https://kg-football.vn/ontology#> .
@prefix res: <https://kg-football.vn/resource/> .

# Các triple owl:sameAs từ internal linking với measure >= 0.95

"""
    
    for entity1, entity2, measure in matches:
        # Tạo comment với measure
        ttl_content += f"# Measure: {measure}\n"
        # Tạo triple owl:sameAs
        ttl_content += f"<{entity1}> owl:sameAs <{entity2}> .\n"
        ttl_content += f"<{entity2}> owl:sameAs <{entity1}> .\n\n"
    
    return ttl_content


def ensure_dataset(base_url: str, dataset: str, admin_user: str, admin_pass: str) -> None:
    """Đảm bảo dataset tồn tại trong Fuseki."""
    # Try HEAD to check
    r = requests.head(f"{base_url}/{dataset}")
    if r.status_code == 200:
        return
    # Create dataset via Fuseki admin
    create_url = f"{base_url}/$/datasets"
    data = {
        "dbName": dataset,
        "dbType": "mem"
    }
    auth = (admin_user, admin_pass) if admin_user or admin_pass else None
    resp = requests.post(create_url, data=data, auth=auth)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Cannot create dataset {dataset}: {resp.status_code} {resp.text}")


def load_ttl_to_fuseki(base_url: str, dataset: str, ttl_content: str, admin_user: str | None = None, admin_pass: str | None = None) -> None:
    """Đẩy nội dung TTL lên Fuseki."""
    update_url = f"{base_url}/{dataset}/data"
    data_bytes = ttl_content.encode("utf-8")
    headers = {"Content-Type": "text/turtle"}
    auth = (admin_user, admin_pass) if admin_user or admin_pass else None
    resp = requests.post(update_url, data=data_bytes, headers=headers, auth=auth)
    if resp.status_code not in (200, 201, 204):
        raise RuntimeError(f"Failed to load TTL content: {resp.status_code} {resp.text}")


def main():
    """Hàm chính."""
    # Cấu hình
    base_url = "http://localhost:3030"
    dataset = "football"
    admin_user = "admin"
    admin_pass = "admin"
    threshold = 0.95
    
    # Đường dẫn file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    internal_linking_path = os.path.join(script_dir, "..", "..", "linking", "links", "internal_linking.ttl")
    internal_linking_path = os.path.abspath(internal_linking_path)
    
    if not os.path.exists(internal_linking_path):
        print(f"Lỗi: Không tìm thấy file {internal_linking_path}")
        sys.exit(1)
    
    try:
        # Bước 1: Parse file và lọc các cặp entity
        print("Bước 1: Đang parse file internal_linking.ttl...")
        matches = parse_internal_linking_ttl(internal_linking_path, threshold)
        
        if not matches:
            print(f"Không tìm thấy cặp entity nào có measure >= {threshold}")
            return
        
        # Bước 2: Tạo nội dung TTL
        print("Bước 2: Đang tạo nội dung TTL...")
        ttl_content = generate_owl_sameas_ttl(matches)
        
        # Lưu file TTL tạm thời
        temp_ttl_path = os.path.join(script_dir, "temp_owl_sameas.ttl")
        with open(temp_ttl_path, "w", encoding="utf-8") as f:
            f.write(ttl_content)
        print(f"Đã lưu nội dung TTL vào: {temp_ttl_path}")
        
        # Bước 3: Đẩy lên Fuseki
        print("Bước 3: Đang đẩy dữ liệu lên Fuseki...")
        ensure_dataset(base_url, dataset, admin_user, admin_pass)
        load_ttl_to_fuseki(base_url, dataset, ttl_content, admin_user, admin_pass)
        
        print(f"✅ Hoàn thành! Đã đẩy {len(matches)} cặp owl:sameAs lên Fuseki")
        print(f"Dataset: {base_url}/{dataset}")
        
        # Xóa file tạm
        os.remove(temp_ttl_path)
        print("Đã xóa file tạm thời")
        
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
