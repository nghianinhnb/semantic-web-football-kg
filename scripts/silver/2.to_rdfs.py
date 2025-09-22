from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig
import json
import os
import re
from pathlib import Path
from tqdm.auto import tqdm

model_name = "Qwen/Qwen3-14B"

# cấu hình 4-bit
bnb_config = BitsAndBytesConfig(
    load_in_4bit=True,
    bnb_4bit_compute_dtype="bfloat16",
    bnb_4bit_use_double_quant=True,
    bnb_4bit_quant_type="nf4"
)

# load tokenizer và model
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForCausalLM.from_pretrained(
    model_name,
    quantization_config=bnb_config,
    device_map="auto",
)

# IO paths
INPUT_JSON = "/home/nghianv/workspace/semantic-web-football-kg/silver/extracted_wiki/wiki_markdown_data.json"
OUTPUT_DIR = "/home/nghianv/workspace/semantic-web-football-kg/silver/ttl"


def slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[\s/]+", "_", value)
    value = re.sub(r"[^a-z0-9_\-]+", "", value)
    value = value.strip("_-")
    return value or "untitled"


def build_prompt(title: str, content: str) -> str:
    return f"""
Bạn là chuyên gia trích xuất tri thức từ văn bản tiếng Việt.

Nhiệm vụ: Từ đoạn văn sau về "{title}", trích xuất các triples (Subject-Predicate-Object) và xuất ra đúng Turtle (.ttl). 

ĐOẠN VĂN:
{content or 'Không có nội dung'}

ONTOLOGY ĐẦY ĐỦ:
@prefix kg: <https://kg-football.vn/ontology#> .
@prefix res: <https://kg-football.vn/resource/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix schema: <http://schema.org/> .

CÁC CLASS CHÍNH:
# Core Classes
- kg:Player
- kg:Team
- kg:Club
- kg:Match
- kg:Position

# Position Hierarchy
- kg:Goalkeeper - rdfs:subClassOf kg:Position
- kg:Defender - rdfs:subClassOf kg:Position
- kg:Midfielder - rdfs:subClassOf kg:Position
- kg:Forward - rdfs:subClassOf kg:Position

# People
- kg:Coach
- kg:AssistantCoach - rdfs:subClassOf kg:Coach
- kg:Referee
- kg:Owner

# Geographic
- kg:Place - rdfs:subClassOf schema:Place
- kg:City - rdfs:subClassOf kg:Place
- kg:Country - rdfs:subClassOf kg:Place

# Stadium & Venue
- kg:Stadium

# Competition
- kg:Competition
- kg:Season
- kg:Organization

# Other
- kg:Footedness - rdfs:subClassOf schema:Enumeration
- kg:LeftFooted - rdfs:subClassOf kg:Footedness
- kg:RightFooted - rdfs:subClassOf kg:Footedness
- kg:BothFooted - rdfs:subClassOf kg:Footedness
- kg:AgeGroup - rdfs:subClassOf schema:Enumeration

CÁC PROPERTY CHÍNH:
# Player Properties
- kg:playsFor - domain: kg:Player, range: kg:Team
- kg:primaryPosition - domain: kg:Player, range: kg:Position
- kg:birthDate - domain: kg:Player, range: xsd:date
- kg:birthPlace - domain: kg:Player, range: kg:City
- kg:nationality - domain: kg:Player, range: kg:Country
- kg:height - domain: kg:Player, range: xsd:decimal
- kg:weight - domain: kg:Player, range: xsd:decimal
- kg:shirtNumber - domain: kg:Player, range: xsd:positiveInteger
- kg:preferredFoot - domain: kg:Player, range: kg:Footedness

# Team Properties
- kg:teamName - domain: kg:Team, range: xsd:string
- kg:teamAbbreviation - domain: kg:Team, range: xsd:string
- kg:hasCoach - domain: kg:Team, range: kg:Coach
- kg:manages - domain: kg:Coach, range: kg:Team

# Club Properties
- kg:foundedDate - domain: kg:Club, range: xsd:date
- kg:homeStadium - domain: kg:Club, range: kg:Stadium
- kg:isHomeOf - domain: kg:Stadium, range: kg:Club
- kg:owns - domain: kg:Owner, range: kg:Club
- kg:hasOwner - domain: kg:Club, range: kg:Owner

# Stadium Properties
- kg:capacity - domain: kg:Stadium, range: xsd:positiveInteger

# Match Properties
- kg:homeTeam - domain: kg:Match, range: kg:Team
- kg:awayTeam - domain: kg:Match, range: kg:Team
- kg:venue - domain: kg:Match, range: kg:Stadium
- kg:kickoffTime - domain: kg:Match, range: xsd:dateTime
- kg:inCompetition - domain: kg:Match, range: kg:Competition
- kg:inSeason - domain: kg:Match, range: kg:Season
- kg:referee - domain: kg:Match, range: kg:Referee
- kg:referees - domain: kg:Referee, range: kg:Match

# Competition Properties
- kg:competitionName - domain: kg:Competition, range: xsd:string
- kg:organizes - domain: kg:Organization, range: kg:Competition
- kg:organizedBy - domain: kg:Competition, range: kg:Organization
- kg:heldIn - domain: kg:Competition, range: kg:Country
- kg:participantCount - domain: kg:Competition, range: xsd:positiveInteger

# Season Properties
- kg:hasSeason - domain: kg:Competition, range: kg:Season
- kg:seasonOf - domain: kg:Season, range: kg:Competition
- kg:participatesIn - domain: kg:Team, range: kg:Season

# Geographic Properties
- kg:locatedIn - domain: kg:Place, range: kg:Place

QUY TẮC TRÍCH XUẤT:
1. Tạo resource URI theo pattern:
   - res:player/Ten_Cau_Thu
   - res:team/Ten_Doi
   - res:club/Ten_CLB
   - res:city/Ten_Thanh_Pho
   - res:country/Ten_Quoc_Gia
   - res:stadium/Ten_San
   - res:position/Vi_Tri
   - res:competition/Ten_Giai
   - res:season/Mua_Giai
   - res:match/Trận_Đấu

1. Sử dụng đúng class và property từ ontology trên
2. Giá trị ngày tháng dùng format xsd:date
3. Giá trị số dùng xsd:decimal hoặc xsd:positiveInteger
4. Giá trị chuỗi dùng xsd:string
5. Giá trị boolean dùng xsd:boolean
6. Chỉ dùng các Classes và Properties trong namespace kg và các thông tin nêu trên, không bịa ra thêm.
7. Chỉ khai báo prefix nếu có sử dụng trong triples.
8. Chỉ xuất Turtle, không giải thích thêm.
"""


def main() -> None:
    # Prepare IO
    input_path = Path(INPUT_JSON)
    output_dir = Path(OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load items
    with open(input_path, "r", encoding="utf-8") as f:
        items = json.load(f)
        if not isinstance(items, list):
            raise ValueError("Input JSON must be an array of items")

    total = len(items)
    print(f"Bắt đầu chuyển đổi {total} items sang TTL...")

    for item in tqdm(items[236:], total=total-236, desc="Converting to TTL", unit="item"):
        title = item.get("title", "").strip() or "Untitled"
        content = item.get("content", "").strip()

        prompt = build_prompt(title, content)

        messages = [
            {"role": "user", "content": prompt}
        ]
        text = tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True,
            enable_thinking=False
        )
        model_inputs = tokenizer([text], return_tensors="pt").to(model.device)

        generated_ids = model.generate(
            **model_inputs,
            max_new_tokens=4096,
            use_cache=True,
            repetition_penalty=1.1
        )
        output_ids = generated_ids[0][len(model_inputs.input_ids[0]):].tolist()
        ttl_content = tokenizer.decode(output_ids, skip_special_tokens=True).strip("\n")

        # Save per-item TTL
        filename = slugify(title)[:200] + ".ttl"
        out_path = output_dir / filename
        with open(out_path, "w", encoding="utf-8") as out_f:
            out_f.write(ttl_content + "\n")

    print("Hoàn thành chuyển đổi TTL.")


if __name__ == "__main__":
    main()
