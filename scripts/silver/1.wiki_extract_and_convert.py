#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script tổng hợp để:
1. Trích xuất dữ liệu từ các file JSON Wikipedia
2. Chuyển đổi wiki content thành markdown format
3. Bỏ internal links, giữ lại external links
"""

import json
import os
import glob
import re
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
import mwparserfromhell

class WikiExtractAndConvert:
    def __init__(self, input_dir: str = "bronze/wiki_raw"):
        """
        Khởi tạo extractor và converter
        
        Args:
            input_dir: Thư mục chứa các file JSON Wikipedia
        """
        self.input_dir = Path(input_dir)
        
    def extract_from_file(self, file_path: str) -> Optional[Dict]:
        """
        Trích xuất dữ liệu từ một file JSON
        
        Args:
            file_path: Đường dẫn đến file JSON
            
        Returns:
            Dict chứa title, content, touched, canonicalurl hoặc None nếu lỗi
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Kiểm tra cấu trúc dữ liệu
            if 'query' not in data or 'pages' not in data['query']:
                print(f"File {file_path} không có cấu trúc hợp lệ")
                return None
            
            pages = data['query']['pages']
            
            # Xử lý trường hợp pages là list hoặc dict
            if isinstance(pages, list):
                if not pages:
                    print(f"File {file_path} có pages rỗng")
                    return None
                page = pages[0]  # Lấy page đầu tiên
            elif isinstance(pages, dict):
                if not pages:
                    print(f"File {file_path} không có pages")
                    return None
                page = list(pages.values())[0]  # Lấy page đầu tiên
            else:
                print(f"File {file_path} có cấu trúc pages không hợp lệ")
                return None
            
            result = {
                'title': page.get('title', ''),
                'content': '',
                'touched': page.get('touched', ''),
                'canonicalurl': page.get('canonicalurl', ''),
                'pageid': page.get('pageid', '')
            }
            
            # Kiểm tra nếu page bị missing
            if page.get('missing', False):
                return result
            
            # Lấy content từ revision cuối cùng
            if 'revisions' in page and page['revisions']:
                revisions = page['revisions']
                if revisions:
                    last_revision = revisions[-1]  # Revision cuối cùng
                    if 'slots' in last_revision and 'main' in last_revision['slots']:
                        result['content'] = last_revision['slots']['main'].get('content', '')
            
            return result
            
        except json.JSONDecodeError as e:
            print(f"Lỗi JSON trong file {file_path}: {e}")
            return None
        except Exception as e:
            print(f"Lỗi khi đọc file {file_path}: {e}")
            return None
    
    def convert_wiki_to_markdown(self, text: str) -> str:
        """Chuyển đổi wiki text thành markdown format"""
        if not text:
            return ""
        
        try:
            parsed = mwparserfromhell.parse(text)
            
            # Xử lý từng node
            result_parts = []
            
            for node in parsed.nodes:
                if isinstance(node, mwparserfromhell.nodes.Text):
                    result_parts.append(str(node))
                
                elif isinstance(node, mwparserfromhell.nodes.Wikilink):
                    # Bỏ internal links, chỉ giữ text
                    display = str(node.text) if node.text else str(node.title)
                    result_parts.append(display)
                
                elif isinstance(node, mwparserfromhell.nodes.ExternalLink):
                    # Giữ external links dạng markdown
                    url = str(node.url)
                    display = str(node.title) if hasattr(node, 'title') and node.title else url
                    result_parts.append(f"[{display}]({url})")
                
                elif isinstance(node, mwparserfromhell.nodes.Template):
                    # Xử lý template (infobox, etc.)
                    template_name = str(node.name).strip()
                    if template_name.lower() in ['infobox', 'infobox football biography', 'infobox football player']:
                        # Chuyển infobox thành markdown table
                        result_parts.append(self._convert_infobox_to_markdown(node))
                    else:
                        # Template khác - chỉ lấy text
                        result_parts.append(str(node).strip())
                
                elif isinstance(node, mwparserfromhell.nodes.Tag):
                    # Xử lý HTML tags
                    if node.tag in ['ref', 'sup', 'sub']:
                        # Bỏ qua reference và superscript/subscript
                        continue
                    elif node.tag == 'table':
                        # Chuyển table thành markdown
                        result_parts.append(self._convert_table_to_markdown(node))
                    else:
                        # Giữ text trong tag
                        result_parts.append(str(node.contents).strip())
                
                else:
                    # Các node khác - lấy text
                    result_parts.append(str(node).strip())
            
            # Ghép lại và làm sạch
            result = ''.join(result_parts)
            result = self._clean_markdown(result)
            
            return result
        
        except Exception as e:
            print(f"Lỗi khi convert wiki text: {e}")
            return self._fallback_convert(text)
    
    def _convert_infobox_to_markdown(self, template_node) -> str:
        """Chuyển đổi infobox thành markdown table"""
        if not template_node.params:
            return ""
        
        markdown_parts = ["\n### Thông tin cơ bản\n"]
        
        for param in template_node.params:
            param_name = str(param.name).strip()
            param_value = str(param.value).strip()
            
            if param_name and param_value:
                # Làm sạch param_value
                param_value = self._clean_text(param_value)
                markdown_parts.append(f"**{param_name}**: {param_value}\n")
        
        markdown_parts.append("\n")
        return ''.join(markdown_parts)
    
    def _convert_table_to_markdown(self, table_node) -> str:
        """Chuyển đổi wiki table thành markdown table"""
        try:
            # Đơn giản hóa - chỉ lấy text từ table
            table_text = str(table_node.contents)
            # Loại bỏ các markup phức tạp
            table_text = re.sub(r'[|!]+', '|', table_text)
            table_text = re.sub(r'[=]{2,}', '', table_text)
            table_text = re.sub(r'[#*:;]+', '', table_text)
            return f"\n{table_text}\n"
        except:
            return ""
    
    def _clean_text(self, text: str) -> str:
        """Làm sạch text"""
        # Loại bỏ wiki markup
        text = re.sub(r'\[\[([^|\]]+)\|([^\]]+)\]\]', r'\2', text)
        text = re.sub(r'\[\[([^\]]+)\]\]', r'\1', text)
        text = re.sub(r'\{\{[^}]*\}\}', '', text)
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'[=]{2,}', '', text)
        text = re.sub(r'[#*:;]+', '', text)
        return text.strip()
    
    def _clean_markdown(self, text: str) -> str:
        """Làm sạch markdown text"""
        # Chuẩn hóa xuống dòng
        text = re.sub(r'\n\s*\n\s*\n', '\n\n', text)
        
        # Loại bỏ các ký tự đặc biệt wiki
        text = re.sub(r'[=]{2,}', '', text)
        text = re.sub(r'[#*:;]+', '', text)
        
        # Loại bỏ các reference
        text = re.sub(r'<ref[^>]*>.*?</ref>', '', text, flags=re.DOTALL)
        text = re.sub(r'<ref[^>]*/>', '', text)
        
        # Loại bỏ các file/image links
        text = re.sub(r'\[\[(File|Tập tin):[^\]]+\]\]', '', text)
        
        return text.strip()
    
    def _fallback_convert(self, text: str) -> str:
        """Fallback method nếu mwparserfromhell lỗi"""
        # Loại bỏ template phức tạp
        text = re.sub(r'\{\{[^}]*\}\}', '', text)
        
        # Bỏ wiki links nhưng giữ text
        text = re.sub(r'\[\[([^|\]]+)\|([^\]]+)\]\]', r'\2', text)
        text = re.sub(r'\[\[([^\]]+)\]\]', r'\1', text)
        
        # Giữ external links
        text = re.sub(r'\[(https?://[^\s\]]+)\s+([^\]]+)\]', r'[\2](\1)', text)
        
        # Loại bỏ các thẻ HTML không cần thiết
        text = re.sub(r'<ref[^>]*>.*?</ref>', '', text, flags=re.DOTALL)
        text = re.sub(r'<ref[^>]*/>', '', text)
        
        # Loại bỏ các ký tự đặc biệt
        text = re.sub(r'[=]{2,}', '', text)
        text = re.sub(r'[#*:;]+', '', text)
        
        return text.strip()
    
    def extract_and_convert_from_directory(self, pattern: str = "*.json") -> List[Dict]:
        """
        Trích xuất dữ liệu và chuyển đổi từ tất cả file JSON trong thư mục
        
        Args:
            pattern: Pattern để tìm file (mặc định: *.json)
            
        Returns:
            List các Dict chứa dữ liệu đã chuyển đổi
        """
        if not self.input_dir.exists():
            print(f"Thư mục {self.input_dir} không tồn tại")
            return []
        
        # Tìm tất cả file JSON
        json_files = list(self.input_dir.glob(pattern))
        print(f"Tìm thấy {len(json_files)} file JSON trong {self.input_dir}")
        
        results = []
        for i, file_path in enumerate(json_files):
            if i % 100 == 0:
                print(f"Đã xử lý {i}/{len(json_files)} files...")
            
            # Trích xuất dữ liệu
            data = self.extract_from_file(str(file_path))
            if data and data['content']:
                # Chuyển đổi content thành markdown
                markdown_content = self.convert_wiki_to_markdown(data['content'])
                
                # Tạo entry mới với markdown content
                markdown_entry = {
                    "title": data.get('title', ''),
                    "pageid": data.get('pageid', ''),
                    "canonicalurl": data.get('canonicalurl', ''),
                    "touched": data.get('touched', ''),
                    "content": markdown_content
                }
                
                results.append(markdown_entry)
        
        return results
    
    def save_to_json(self, data: List[Dict], output_file: str):
        """
        Lưu dữ liệu đã chuyển đổi vào file JSON
        
        Args:
            data: List dữ liệu đã chuyển đổi
            output_file: Tên file output
        """
        try:
            # Tạo thư mục nếu chưa tồn tại
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"Đã lưu {len(data)} records vào {output_file}")
        except Exception as e:
            print(f"Lỗi khi lưu file {output_file}: {e}")
    
    def print_summary(self, data: List[Dict]):
        """
        In tóm tắt dữ liệu đã chuyển đổi
        
        Args:
            data: List dữ liệu đã chuyển đổi
        """
        if not data:
            print("Không có dữ liệu để hiển thị")
            return
        
        print(f"\n=== TÓM TẮT DỮ LIỆU ===")
        print(f"Tổng số file đã xử lý: {len(data)}")
        
        # Thống kê về content
        with_content = [d for d in data if d['content']]
        print(f"File có content: {len(with_content)}")
        
        # Thống kê về độ dài content
        if with_content:
            lengths = [len(d['content']) for d in with_content]
            print(f"Độ dài content trung bình: {sum(lengths)/len(lengths):.0f} ký tự")
            print(f"Độ dài content min: {min(lengths)} ký tự")
            print(f"Độ dài content max: {max(lengths)} ký tự")
        
        # Hiển thị một vài ví dụ
        print(f"\n=== VÍ DỤ DỮ LIỆU ===")
        for i, item in enumerate(data[:3]):  # Hiển thị 3 item đầu
            print(f"\n{i+1}. Title: {item['title']}")
            print(f"   Content length: {len(item['content'])} ký tự")
            print(f"   Touched: {item['touched']}")
            print(f"   URL: {item['canonicalurl']}")
            if item['content']:
                # Hiển thị 100 ký tự đầu của content
                preview = item['content'][:100].replace('\n', ' ')
                print(f"   Content preview: {preview}...")

def main():
    """Hàm main để chạy script"""
    # Khởi tạo extractor và converter
    processor = WikiExtractAndConvert("bronze/wiki_raw")
    
    # Trích xuất và chuyển đổi dữ liệu từ tất cả file JSON
    print("Bắt đầu trích xuất và chuyển đổi dữ liệu...")
    data = processor.extract_and_convert_from_directory("*.json")
    
    if data:
        # In tóm tắt
        processor.print_summary(data)
        
        # Lưu vào file JSON
        processor.save_to_json(data, "silver/extracted_wiki/wiki_markdown_data.json")
        
        print(f"\nHoàn thành! Đã trích xuất và chuyển đổi {len(data)} file.")
    else:
        print("Không tìm thấy dữ liệu nào để trích xuất.")

if __name__ == "__main__":
    main()
