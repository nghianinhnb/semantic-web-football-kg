#!/usr/bin/env python3
"""
Đọc danh sách terms thiếu từ scripts/silver/missing_terms.txt, duyệt các TTL trong silver/ttl và loại bỏ:
- Nếu term xuất hiện làm Subject (S): xóa cả statement/triple (toàn bộ khối kết thúc bằng '.')
- Nếu term xuất hiện làm Predicate (P): xóa cả predicate đó cùng toàn bộ object-list của nó
- Nếu term xuất hiện làm Object (O): chỉ xóa object khớp; nếu predicate không còn object nào sau khi xóa, xóa luôn predicate

Lưu ý: Script dùng heuristic đơn giản dựa trên tách statement theo dấu '.' kết thúc dòng.
Giữ nguyên @prefix, comment và dòng trắng.
"""

import os
from pathlib import Path
from typing import List, Tuple, Set
import re
from tqdm import tqdm

MISSING_TERMS_FILE = Path(__file__).parent / 'missing_terms.txt'
TTL_DIR = Path(__file__).resolve().parents[2] / 'silver' / 'ttl'


def read_missing_terms(path: Path) -> Tuple[Set[str], Set[str]]:
    """Đọc file terms, trả về (full_set, local_set).
    full_set chứa nguyên chuỗi (có thể gồm 'kg:'), local_set chỉ là local name sau 'kg:'.
    Bỏ qua dòng rỗng/comment.
    """
    full: Set[str] = set()
    local: Set[str] = set()
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            term = line.strip()
            if not term or term.startswith('#'):
                continue
            full.add(term)
            if term.startswith('kg:') and len(term) > 3:
                local.add(term[3:])
            else:
                local.add(term)
    return full, local


def split_statements(content: str) -> List[str]:
    """Chia nội dung thành các statements kết thúc bằng '.' theo dòng (heuristic).
    Tích lũy dòng cho đến khi gặp một dòng kết thúc bằng '.' (bỏ khoảng trắng).
    """
    stmts: List[str] = []
    acc: List[str] = []
    for raw_line in content.split('\n'):
        line = raw_line.rstrip()
        acc.append(line)
        if line.endswith('.'):
            stmts.append('\n'.join(acc).strip())
            acc = []
    if acc:
        # phần còn lại (không kết thúc bằng '.') để nguyên dạng
        stmts.append('\n'.join(acc).strip())
    return stmts


def first_token(text: str) -> str:
    text = text.lstrip()
    # Bỏ comment đầu dòng nếu có
    if text.startswith('#'):
        return '#'
    # token đầu tiên đến khoảng trắng/; hoặc cuối chuỗi
    m = re.match(r"([^\s;]+)", text)
    return m.group(1) if m else ''


def split_outside_quotes(s: str, sep: str) -> List[str]:
    parts: List[str] = []
    buf: List[str] = []
    in_str = False
    escape = False
    for ch in s:
        if in_str:
            if escape:
                escape = False
            elif ch == '\\':
                escape = True
            elif ch == '"':
                in_str = False
            buf.append(ch)
            continue
        else:
            if ch == '"':
                in_str = True
                buf.append(ch)
                continue
            if ch == sep:
                parts.append(''.join(buf).strip())
                buf = []
                continue
            buf.append(ch)
    if buf:
        parts.append(''.join(buf).strip())
    return parts


def token_is_missing(token: str, missing_full: Set[str], missing_local: Set[str]) -> bool:
    # loại bỏ kết thúc ; hoặc . nếu dính kèm và cặp < > của IRI rút gọn
    token = token.strip().rstrip(';.')
    if token.startswith('<') and token.endswith('>'):
        token = token[1:-1]
    if token in missing_full:
        return True
    if token.startswith('kg:'):
        local = token[3:]
        return local in missing_local
    return False


def strip_angle_brackets(token: str) -> str:
    token = token.strip()
    if token.startswith('<') and token.endswith('>'):
        return token[1:-1]
    return token


def process_statement(stmt: str, missing_full: Set[str], missing_local: Set[str]) -> str:
    # Giữ nguyên @prefix, comment-only, hoặc rỗng
    stripped = stmt.strip()
    if not stripped:
        return stmt
    if stripped.startswith('@prefix') or stripped.startswith('@base'):
        return stmt
    if stripped.startswith('#'):
        return stmt

    # Lấy subject
    subj = first_token(stripped)
    if not subj:
        return ''
    if token_is_missing(subj, missing_full, missing_local):
        # Xóa cả statement nếu Subject là term thiếu
        return ''

    # Cắt bỏ subject khỏi phần còn lại
    rest = stripped[len(subj):].strip()
    if not rest:
        return ''
    # Bỏ dấu '.' kết thúc để xử lý
    if rest.endswith('.'):
        rest = rest[:-1].rstrip()

    # Tách theo ';' thành các predicate-segments
    pred_segments = split_outside_quotes(rest, ';')
    new_segments: List[str] = []
    for seg in pred_segments:
        if not seg:
            continue
        seg = seg.strip()
        # Lấy predicate token đầu tiên
        pred = first_token(seg)
        if not pred:
            continue
        # Chuẩn hóa predicate có thể ở dạng <kg:...>
        pred_norm = strip_angle_brackets(pred)
        if token_is_missing(pred_norm, missing_full, missing_local):
            # Bỏ cả predicate nếu pred là term thiếu
            continue
        # phần sau predicate là object-list (có thể cách nhau ',')
        obj_part = seg[len(pred):].strip()
        # Xóa dấu trailing ';' hoặc ',' nếu còn
        obj_part = obj_part.rstrip(';,')
        # Tách object theo ',' (ngoài dấu ")
        objects = split_outside_quotes(obj_part, ',')
        kept_objs: List[str] = []
        for obj in objects:
            raw = obj.strip()
            tok = first_token(raw)
            # Chuẩn hóa token object (có thể là <kg:...> hoặc kg:...)
            tok_norm = strip_angle_brackets(tok)
            if tok_norm and token_is_missing(tok_norm, missing_full, missing_local):
                continue
            kept_objs.append(obj.strip())
        if not kept_objs:
            # Không còn object -> bỏ predicate
            continue
        # Ghép lại segment
        seg_text = f"{pred} " + ' , '.join(kept_objs)
        new_segments.append(seg_text)

    if not new_segments:
        return ''

    # Ghép lại statement
    # Định dạng: subject + mỗi predicate xuống dòng với thụt 2 spaces
    stmt_lines = [f"{subj} {new_segments[0]} "]
    for seg in new_segments[1:]:
        stmt_lines.append(f"  ; {seg} ")
    stmt_lines[-1] = stmt_lines[-1].rstrip() + ' .'
    return '\n'.join(stmt_lines)


def process_file(path: Path, missing_full: Set[str], missing_local: Set[str]) -> Tuple[bool, str]:
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Bảo toàn phần @prefix đầu file và comments rời rạc
    stmts = split_statements(content)
    new_stmts: List[str] = []
    for stmt in stmts:
        new_stmt = process_statement(stmt, missing_full, missing_local)
        if new_stmt is None:
            new_stmt = ''
        if new_stmt.strip():
            new_stmts.append(new_stmt)

    new_content = '\n'.join(new_stmts).rstrip() + '\n'
    changed = new_content != content
    return changed, new_content


def main():
    if not MISSING_TERMS_FILE.exists():
        raise FileNotFoundError(f"Không tìm thấy file missing terms: {MISSING_TERMS_FILE}")
    if not TTL_DIR.exists():
        raise FileNotFoundError(f"Không tìm thấy thư mục TTL: {TTL_DIR}")

    missing_full, missing_local = read_missing_terms(MISSING_TERMS_FILE)

    ttl_files = sorted(TTL_DIR.glob('*.ttl'))
    changed_count = 0

    for fp in tqdm(ttl_files, desc='Loại bỏ terms thiếu trong TTL'):
        try:
            changed, new_content = process_file(fp, missing_full, missing_local)
            if changed:
                with open(fp, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                changed_count += 1
        except Exception as e:
            print(f"Lỗi xử lý {fp}: {e}")

    print(f"Hoàn tất. Đã cập nhật {changed_count}/{len(ttl_files)} files.")


if __name__ == '__main__':
    main()


