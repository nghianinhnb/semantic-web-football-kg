#!/usr/bin/env python3
"""
Script duy nhất để:
1. Sửa lỗi syntax TTL (bỏ markdown, sửa triples chưa hoàn thiện)
2. Tạo file additional.ttl chứa các định nghĩa còn thiếu
3. Xóa các TTL trống (chỉ có prefix/comment, không có triple)
"""

import os
import re
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, List, Set, Tuple
import argparse
from tqdm import tqdm

def clean_markdown_and_fix_syntax(content: str) -> str:
    """Loại bỏ markdown và sửa lỗi syntax."""
    # Loại bỏ markdown notation
    content = re.sub(r'^```turtle\s*\n', '', content, flags=re.MULTILINE)
    content = re.sub(r'\n```\s*$', '', content, flags=re.MULTILINE)
    content = re.sub(r'^```\s*$', '', content, flags=re.MULTILINE)
    
    lines = content.split('\n')
    fixed_lines = []
    
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            fixed_lines.append(line)
            continue
            
        # Nếu dòng kết thúc bằng ; hoặc . thì OK
        if line.endswith(';') or line.endswith('.'):
            fixed_lines.append(line)
            continue
            
        # Sửa triples chưa hoàn thiện
        if 'kg:' in line or 'res:' in line or 'a ' in line:
            # Nếu dòng có subject và predicate nhưng thiếu object
            if re.search(r'kg:\w+\s+kg:\w+\s*$', line):
                line += ' "UNKNOWN" .'
            elif re.search(r'res:\w+\s+kg:\w+\s*$', line):
                line += ' "UNKNOWN" .'
            elif re.search(r'res:\w+\s+a\s*$', line):
                line += ' kg:Thing .'
            elif re.search(r'kg:\w+\s+res:\w+\s*$', line):
                line += ' .'
        
        fixed_lines.append(line)
    
    return '\n'.join(fixed_lines)

def extract_ontology_definitions(ontology_dir: str) -> Set[str]:
    """Trích xuất tất cả các terms đã được định nghĩa trong ontology."""
    defined_terms = set()
    
    for file_path in Path(ontology_dir).glob("*.ttl"):
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Loại bỏ markdown notation
        content = re.sub(r'^```turtle\s*\n', '', content, flags=re.MULTILINE)
        content = re.sub(r'\n```\s*$', '', content, flags=re.MULTILINE)
        content = re.sub(r'^```\s*$', '', content, flags=re.MULTILINE)

        # Mọi subject bắt đầu bằng kg: ở đầu dòng được coi là "đã định nghĩa"
        # Không phụ thuộc vào việc có type a owl:... hay không
        for match in re.finditer(r'(?m)^(kg:(\w+))\s', content):
            defined_terms.add(match.group(2))
    
    return defined_terms

def extract_ttl_usage(ttl_dir: str) -> Counter:
    """Trích xuất tất cả các terms được sử dụng trong TTL."""
    terms_used = Counter()
    
    ttl_files = list(Path(ttl_dir).glob("*.ttl"))
    
    for file_path in tqdm(ttl_files, desc="Phân tích TTL files"):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Loại bỏ markdown notation
            content = re.sub(r'^```turtle\s*\n', '', content, flags=re.MULTILINE)
            content = re.sub(r'\n```\s*$', '', content, flags=re.MULTILINE)
            content = re.sub(r'^```\s*$', '', content, flags=re.MULTILINE)
                
            # Tìm tất cả các kg: terms được sử dụng
            term_pattern = r'kg:(\w+)'
            for match in re.finditer(term_pattern, content):
                terms_used[match.group(1)] += 1
                
        except Exception as e:
            print(f"Lỗi đọc file {file_path}: {e}")
            
    return terms_used

# Bỏ phân loại property/class: chỉ cần có định nghĩa tối thiểu là đủ

def is_effectively_empty_ttl(content: str) -> bool:
    """Kiểm tra file TTL có 'trống' không: có thể có prefix/BASE/PREFIX, comment, dòng trắng
    nhưng KHÔNG có bất kỳ triple nào (câu TTL kết thúc bằng dấu chấm).
    """
    # Loại bỏ markdown notation
    content = re.sub(r'^```turtle\s*\n', '', content, flags=re.MULTILINE)
    content = re.sub(r'\n```\s*$', '', content, flags=re.MULTILINE)
    content = re.sub(r'^```\s*$', '', content, flags=re.MULTILINE)

    for raw_line in content.split('\n'):
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith('#'):
            continue
        if line.startswith('@prefix') or line.startswith('@base'):
            continue
        # SPARQL-style prefix/base directives also hợp lệ trong Turtle
        if line.upper().startswith('PREFIX ') or line.upper().startswith('BASE '):
            continue
        # Nếu bất kỳ dòng nội dung (không phải directive/comment) kết thúc bằng '.' => có triple
        if line.endswith('.'):
            return False
    return True

def delete_empty_ttl_files(ttl_dir: str) -> Dict[str, int]:
    """Xóa các TTL 'trống' (chỉ có prefix/comment/dòng trắng, không có triple)."""
    results = {"deleted": 0, "total": 0}
    ttl_files = list(Path(ttl_dir).glob("*.ttl"))
    results["total"] = len(ttl_files)

    for file_path in tqdm(ttl_files, desc="Xóa TTL trống"):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            if is_effectively_empty_ttl(content):
                os.remove(file_path)
                results["deleted"] += 1
        except Exception as e:
            print(f"Lỗi xóa file {file_path}: {e}")

    return results

def fix_ttl_files(ttl_dir: str) -> Dict[str, int]:
    """Sửa lỗi syntax trong tất cả files TTL."""
    results = {"fixed": 0, "total": 0}
    
    ttl_files = list(Path(ttl_dir).glob("*.ttl"))
    results["total"] = len(ttl_files)
    
    for file_path in tqdm(ttl_files, desc="Sửa lỗi TTL files"):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                original_content = f.read()
            
            fixed_content = clean_markdown_and_fix_syntax(original_content)
            
            if fixed_content != original_content:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(fixed_content)
                results["fixed"] += 1
                
        except Exception as e:
            print(f"Lỗi sửa file {file_path}: {e}")
    
    return results

def create_additional_ontology(defined_terms: Set[str], 
                             terms_used: Counter,
                             output_file: str,
                             min_usage: int = 3) -> None:
    """Tạo file additional.ttl chứa các định nghĩa còn thiếu và phân tích unused terms."""
    
    # Tìm terms còn thiếu
    missing_terms = []
    for term_name, count in terms_used.items():
        if term_name not in defined_terms and count >= min_usage:
            missing_terms.append((term_name, count))
    
    # Tìm terms đã định nghĩa nhưng không sử dụng
    unused_terms = []
    for term_name in defined_terms:
        if term_name not in terms_used:
            unused_terms.append(term_name)
    
    # Sắp xếp theo tần suất sử dụng
    missing_terms.sort(key=lambda x: x[1], reverse=True)
    unused_terms.sort()
    
    # Tạo nội dung file additional.ttl
    content = []
    content.append("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .")
    content.append("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .")
    content.append("@prefix owl: <http://www.w3.org/2002/07/owl#> .")
    content.append("@prefix kg: <https://kg-football.vn/ontology#> .")
    content.append("")
    content.append("# === CÁC TERMS ĐƯỢC TỰ ĐỘNG BỔ SUNG ===")
    content.append("# Dựa trên phân tích dữ liệu TTL")
    content.append("")
    
    # Thêm tất cả missing terms
    if missing_terms:
        content.append("# Missing Terms (được sử dụng nhưng chưa định nghĩa)")
        for term_name, usage_count in missing_terms:
            content.append(f"kg:{term_name} rdfs:label \"{term_name}\"@en , \"{term_name}\"@vi ;")
            content.append(f"  rdfs:comment \"Used {usage_count} times in data\"@en .")
            content.append("")
    
    # Thêm thông tin về unused terms
    if unused_terms:
        content.append("# Unused Terms (đã định nghĩa nhưng không sử dụng)")
        content.append("# Có thể xem xét mapping hoặc xóa bỏ")
        for term_name in unused_terms:
            content.append(f"# kg:{term_name} - Đã định nghĩa nhưng không sử dụng")
        content.append("")
    
    # Ghi file
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(content))
    
    print(f"Đã tạo file {output_file}")
    print(f"Thêm {len(missing_terms)} terms còn thiếu")
    print(f"Phát hiện {len(unused_terms)} terms không sử dụng")
    
    # In thống kê
    print(f"\n=== THỐNG KÊ ===")
    print(f"Terms đã định nghĩa: {len(defined_terms)}")
    print(f"Terms được sử dụng: {len(terms_used)}")
    print(f"Terms còn thiếu: {len(missing_terms)}")
    print(f"Terms không sử dụng: {len(unused_terms)}")
    
    return missing_terms, unused_terms

def main():
    parser = argparse.ArgumentParser(description="Sửa lỗi TTL và tạo additional ontology")
    parser.add_argument("--ttl-dir", default="silver/ttl", help="Thư mục TTL files")
    parser.add_argument("--ontology-dir", default="ontology", help="Thư mục ontology")
    parser.add_argument("--output", default="additional.ttl", help="File additional ontology")
    parser.add_argument("--min-usage", type=int, default=3, help="Tần suất sử dụng tối thiểu")
    parser.add_argument("--skip-fix", action="store_true", help="Bỏ qua việc sửa TTL files")
    parser.add_argument("--keep-empty", action="store_true", help="Giữ lại TTL trống, không xóa")
    
    args = parser.parse_args()
    
    if not args.skip_fix:
        print("=== BƯỚC 1: SỬA LỖI SYNTAX TTL ===")
        fix_results = fix_ttl_files(args.ttl_dir)
        print(f"Đã sửa {fix_results['fixed']}/{fix_results['total']} files")
        print("")

    if not args.keep_empty:
        print("=== BƯỚC 1.5: XÓA TTL TRỐNG ===")
        del_results = delete_empty_ttl_files(args.ttl_dir)
        print(f"Đã xóa {del_results['deleted']}/{del_results['total']} files (TTL trống)")
        print("")
    
    print("=== BƯỚC 2: TẠO ADDITIONAL ONTOLOGY ===")
    print("Đang phân tích ontology hiện có...")
    defined_terms = extract_ontology_definitions(args.ontology_dir)
    
    print("Đang phân tích TTL usage...")
    terms_used = extract_ttl_usage(args.ttl_dir)
    
    print("Tạo file additional.ttl...")
    missing_terms, unused_terms = create_additional_ontology(defined_terms, terms_used, args.output, args.min_usage)
    
    print("\n=== HOÀN TẤT ===")
    print(f"File additional ontology: {args.output}")

if __name__ == "__main__":
    main()
