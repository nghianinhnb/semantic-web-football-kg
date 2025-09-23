#!/usr/bin/env python3
"""
Script duy nhất để:
1. Sửa lỗi syntax TTL (bỏ markdown, sửa triples chưa hoàn thiện)
2. Xóa các TTL trống (chỉ có prefix/comment, không có triple)
3. Ghi danh sách terms còn thiếu vào scripts/silver/missing_terms.txt
"""

import os
import re
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, List, Set, Tuple, Optional
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
        # Cắt bỏ comment nội dòng (Turtle: '#' bắt đầu comment)
        if '#' in line:
            line = line.split('#', 1)[0]
        line = line.strip()
        # Bỏ dòng rỗng sau khi cắt comment
        if not line:
            continue
        # Bỏ toàn bộ dòng prefix/base (bao gồm SPARQL-style)
        if line.startswith('@prefix') or line.startswith('@base'):
            continue
        if line.upper().startswith('PREFIX ') or line.upper().startswith('BASE '):
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
            # Lưu đầy đủ với prefix kg:
            defined_terms.add(match.group(1))
    
    return defined_terms

def extract_ttl_usage(ttl_dir: str) -> Tuple[Counter, Dict[str, Set[str]]]:
    """Trích xuất tất cả các terms được sử dụng trong TTL và mapping term->set(files)."""
    terms_used = Counter()
    term_files: Dict[str, Set[str]] = defaultdict(set)
    
    ttl_files = list(Path(ttl_dir).glob("*.ttl"))
    
    for file_path in tqdm(ttl_files, desc="Phân tích TTL files"):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Loại bỏ markdown notation
            content = re.sub(r'^```turtle\s*\n', '', content, flags=re.MULTILINE)
            content = re.sub(r'\n```\s*$', '', content, flags=re.MULTILINE)
            content = re.sub(r'^```\s*$', '', content, flags=re.MULTILINE)
                
            # Loại bỏ mọi literal trong dấu nháy đôi để tránh đếm trong text
            content_no_strings = re.sub(r'"[^"\\]*(?:\\.[^"\\]*)*"', '""', content)

            # Tìm tất cả các kg:terms (không tính prefix/comment, đã loại ở bước fix syntax)
            term_pattern = r'\bkg:(\w+)\b'
            for match in re.finditer(term_pattern, content_no_strings):
                full_term = f"kg:{match.group(1)}"
                terms_used[full_term] += 1
                term_files[full_term].add(str(Path(file_path).resolve()))
                
        except Exception as e:
            print(f"Lỗi đọc file {file_path}: {e}")
            
    return terms_used, term_files

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

def find_missing_term(defined_terms: Set[str], 
                             terms_used: Counter,
                             output_file: str,
                             min_usage: int = 3,
                             term_files: Optional[Dict[str, Set[str]]] = None) -> None:
    missing_terms = []
    for term_name, count in terms_used.items():
        if term_name not in defined_terms and count >= min_usage:
            missing_terms.append((term_name, count))
    
    if output_file.lower().endswith('.txt'):
        # term_name đã bao gồm kg:
        lines = [term_name for term_name, _ in missing_terms]
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
        print(f"Đã tạo file {output_file}")
        print(f"Thêm {len(missing_terms)} terms còn thiếu")
        print(f"\n=== THỐNG KÊ ===")
        print(f"Terms đã định nghĩa: {len(defined_terms)}")
        print(f"Terms được sử dụng: {len(terms_used)}")
        print(f"Terms còn thiếu: {len(missing_terms)}")

    # In thống kê
    print(f"\n=== THỐNG KÊ ===")
    print(f"Terms đã định nghĩa: {len(defined_terms)}")
    print(f"Terms được sử dụng: {len(terms_used)}")
    print(f"Terms còn thiếu: {len(missing_terms)}")
    
    return missing_terms

def main():
    # Cấu hình cố định
    ttl_dir = "silver/ttl"
    ontology_dir = "ontology"
    output_txt = "scripts/silver/missing_terms.txt"
    min_usage = 1

    print("=== BƯỚC 1: SỬA LỖI SYNTAX TTL ===")
    fix_results = fix_ttl_files(ttl_dir)
    print(f"Đã sửa {fix_results['fixed']}/{fix_results['total']} files")
    print("")

    print("=== BƯỚC 1.5: XÓA TTL TRỐNG ===")
    del_results = delete_empty_ttl_files(ttl_dir)
    print(f"Đã xóa {del_results['deleted']}/{del_results['total']} files (TTL trống)")
    print("")

    print("=== BƯỚC 2: PHÂN TÍCH TERMS VÀ GHI TXT ===")
    print("Đang phân tích ontology hiện có...")
    defined_terms = extract_ontology_definitions(ontology_dir)
    
    print("Đang phân tích TTL usage...")
    terms_used, term_files = extract_ttl_usage(ttl_dir)
    
    print("Ghi danh sách terms còn thiếu...")
    find_missing_term(defined_terms, terms_used, output_txt, min_usage, term_files)
    
    print("\n=== HOÀN TẤT ===")
    print(f"File danh sách terms còn thiếu: {output_txt}")

if __name__ == "__main__":
    main()

