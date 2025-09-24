from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse, JSONResponse, HTMLResponse
from pydantic import BaseModel
from rdflib import Graph
from rdflib.plugins.stores.sparqlstore import SPARQLStore
import os
import io
import csv
from typing import List, Dict, Any, Optional

BASE_URI = "https://kg-football.vn/"
DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../gold/ttl"))
ONTOLOGY_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../ontology/core.ttl"))

FUSEKI_QUERY_ENDPOINT = os.getenv("FUSEKI_QUERY_ENDPOINT", "").strip()

app = FastAPI(title="KG Football Dereferenceable API")


def load_graph() -> Graph:
	g = Graph()
	if os.path.exists(ONTOLOGY_FILE):
		g.parse(ONTOLOGY_FILE, format="turtle")
	# load gold ttl snippets
	if os.path.isdir(DATA_DIR):
		for fn in os.listdir(DATA_DIR):
			if fn.endswith(".ttl"):
				g.parse(os.path.join(DATA_DIR, fn), format="turtle")
	return g


def remote_graph() -> Graph:
	if not FUSEKI_QUERY_ENDPOINT:
		raise RuntimeError("FUSEKI_QUERY_ENDPOINT is not set")
	store = SPARQLStore(FUSEKI_QUERY_ENDPOINT)
	return Graph(store=store)


def is_construct_query(q: str) -> bool:
	return q.lstrip().upper().startswith("CONSTRUCT") or q.lstrip().upper().startswith("DESCRIBE")


def is_select_query(q: str) -> bool:
	return q.lstrip().upper().startswith("SELECT")


def is_ask_query(q: str) -> bool:
	return q.lstrip().upper().startswith("ASK")


def negotiate(request: Request) -> str:
	accept = request.headers.get("accept", "")
	if "text/turtle" in accept:
		return "turtle"
	if "application/ld+json" in accept or "application/json" in accept:
		return "json-ld"
	return "html"


@app.get("/resource/{path:path}")
async def deref_resource(path: str, request: Request):
	iri = f"{BASE_URI}resource/{path}"
	fmt = negotiate(request)
	g = None
	if FUSEKI_QUERY_ENDPOINT:
		try:
			g = remote_graph()
		except Exception:
			g = load_graph()
	else:
		g = load_graph()
	q = f"""
	CONSTRUCT {{ <{iri}> ?p ?o . ?o ?p2 ?o2 }}
	WHERE {{
	  OPTIONAL {{ <{iri}> ?p ?o . OPTIONAL {{ ?o ?p2 ?o2 }} }}
	}}
	"""
	try:
		res = g.query(q)
	except Exception:
		# Fallback local graph nếu remote lỗi
		g_local = load_graph()
		res = g_local.query(q)
	cg = res.graph if hasattr(res, "graph") else Graph()
	# Nếu không có triple, thử deref như một thuật ngữ ontology (Class/Property)
	if len(cg) == 0:
		onto_iri = f"{BASE_URI}ontology#{path}"
		q2 = f"""
		CONSTRUCT {{ <{onto_iri}> ?p ?o . ?o ?p2 ?o2 }}
		WHERE {{
		  OPTIONAL {{ <{onto_iri}> ?p ?o . OPTIONAL {{ ?o ?p2 ?o2 }} }}
		}}
		"""
		try:
			res2 = g.query(q2)
		except Exception:
			g_local2 = load_graph()
			res2 = g_local2.query(q2)
		cg = res2.graph if hasattr(res2, "graph") else Graph()
		if len(cg) == 0:
			raise HTTPException(status_code=404, detail="Resource not found")

	if fmt == "turtle":
		return PlainTextResponse(cg.serialize(format="turtle"), media_type="text/turtle")
	if fmt == "json-ld":
		return JSONResponse(content=cg.serialize(format="json-ld", indent=2).decode("utf-8"))

	# HTML view minimal
	triples = "".join([f"<tr><td>{s}</td><td>{p}</td><td>{o}</td></tr>" for s,p,o in cg])
	html = f"""
	<html><head><title>{iri}</title></head>
	<body>
	  <h1>{iri}</h1>
	  <p>Content negotiation: text/turtle, application/ld+json, text/html</p>
	  <table border=\"1\"><thead><tr><th>S</th><th>P</th><th>O</th></tr></thead>
	  <tbody>{triples}</tbody></table>
	</body></html>
	"""
	return HTMLResponse(content=html)


@app.get("/page/resource/{path:path}")
async def human_page(path: str):
	iri_res = f"{BASE_URI}resource/{path}"
	iri_onto = f"{BASE_URI}ontology#{path}"
	# Query dữ liệu để hiển thị label, types, outgoing/incoming
	g = load_graph() if not FUSEKI_QUERY_ENDPOINT else remote_graph()
	def try_query(q: str):
		try:
			return g.query(q)
		except Exception:
			g2 = load_graph()
			return g2.query(q)

	q_labels = f"""
	SELECT ?s ?label WHERE {{ VALUES ?s {{ <{iri_res}> <{iri_onto}> }} OPTIONAL {{ ?s <http://www.w3.org/2000/01/rdf-schema#label> ?label }} }}
	"""
	q_types = f"""
	SELECT ?s ?type WHERE {{ VALUES ?s {{ <{iri_res}> <{iri_onto}> }} OPTIONAL {{ ?s a ?type }} }}
	"""
	q_out = f"""
	SELECT ?p ?o WHERE {{ VALUES ?s {{ <{iri_res}> <{iri_onto}> }} ?s ?p ?o }} LIMIT 200
	"""
	q_in = f"""
	SELECT ?s2 ?p WHERE {{ VALUES ?s {{ <{iri_res}> <{iri_onto}> }} ?s2 ?p ?s }} LIMIT 200
	"""

	labels_res = try_query(q_labels)
	types_res = try_query(q_types)
	out_res = try_query(q_out)
	in_res = try_query(q_in)

	labels = sorted({str(r[1]) for r in labels_res if r[1] is not None})
	types = sorted({str(r[1]) for r in types_res if r[1] is not None})
	out_rows = "".join([f"<tr><td>{str(r[0])}</td><td>{str(r[1])}</td></tr>" for r in out_res])
	in_rows = "".join([f"<tr><td>{str(r[0])}</td><td>{str(r[1])}</td></tr>" for r in in_res])

	labels_html = "".join([f'<span class="badge">{l}</span>' for l in labels]) or '<span class="small">Không có</span>'
	types_html = "".join([f'<div class="small">{t}</div>' for t in types]) or '<span class="small">Không có</span>'
	out_rows_html = out_rows or '<tr><td colspan="2" class="small">Không có</td></tr>'
	in_rows_html = in_rows or '<tr><td colspan="2" class="small">Không có</td></tr>'

	html = f"""
	<!DOCTYPE html>
	<html>
	<head>
	  <meta charset=\"utf-8\" />
	  <title>{path} – KG Football</title>
	  <style>
		body {{ font-family: system-ui, sans-serif; margin: 20px; }}
		h1 {{ margin: 0 0 8px 0; }}
		.sub {{ color:#666; margin-bottom: 16px; }}
		.grid {{ display:grid; grid-template-columns: 1fr 1fr; gap:16px; }}
		.card {{ border:1px solid #ddd; border-radius:10px; padding:12px; background:#fff; }}
		table {{ width:100%; border-collapse: collapse; }}
		th, td {{ text-align:left; border-bottom: 1px solid #eee; padding:6px; font-size: 14px; }}
		.badge {{ display:inline-block; padding:2px 6px; background:#e8eefc; border:1px solid #c0d1ff; border-radius:6px; font-size:12px; margin-right: 6px; }}
		.small {{ color:#666; font-size: 12px; }}
	  </style>
	</head>
	<body>
	  <h1>{path}</h1>
	  <div class=\"sub\">IRI tài nguyên: <a href=\"{iri_res}\">{iri_res}</a> • Thuật ngữ ontology: <a href=\"{iri_onto}\">{iri_onto}</a></div>
	  <div class=\"grid\">
		<div class=\"card\">
		  <h3>Nhãn (label)</h3>
		  <div>{labels_html}</div>
		  <h3>Kiểu (rdf:type)</h3>
		  <div>{types_html}</div>
		</div>
		<div class=\"card\">
		  <h3>Liên kết tham khảo</h3>
		  <div class=\"small\">Dữ liệu: <code>/resource/{path}</code> (HTML/Turtle/JSON-LD qua content negotiation)</div>
		  <div class=\"small\">Trang người đọc: <code>/page/resource/{path}</code></div>
		</div>
		<div class=\"card\" style=\"grid-column: 1 / span 1;\">
		  <h3>Outgoing</h3>
		  <table><thead><tr><th>Predicate</th><th>Object</th></tr></thead><tbody>{out_rows_html}</tbody></table>
		</div>
		<div class=\"card\" style=\"grid-column: 2 / span 1;\">
		  <h3>Incoming</h3>
		  <table><thead><tr><th>Subject</th><th>Predicate</th></tr></thead><tbody>{in_rows_html}</tbody></table>
		</div>
	  </div>
	</body>
	</html>
	"""
	return HTMLResponse(content=html)


class SparqlRequest(BaseModel):
	query: str
	format: Optional[str] = "json"  # one of: json, csv, text


def serialize_select_json(result) -> Dict[str, Any]:
	vars: List[str] = [str(v) for v in result.vars]
	rows: List[Dict[str, Any]] = []
	for binding in result:
		row: Dict[str, Any] = {}
		for i, var in enumerate(vars):
			val = binding[i]
			if val is None:
				row[var] = None
			else:
				row[var] = {
					"value": str(val),
					"type": getattr(val, "datatype", None) and "literal" or "uri" if getattr(val, "n3", None) else "literal",
				}
		rows.append(row)
	return {"head": {"vars": vars}, "results": {"bindings": rows}}


def serialize_select_csv(result) -> str:
	vars: List[str] = [str(v) for v in result.vars]
	buf = io.StringIO()
	w = csv.writer(buf)
	w.writerow(vars)
	for binding in result:
		row = []
		for i, _ in enumerate(vars):
			val = binding[i]
			row.append("" if val is None else str(val))
		w.writerow(row)
	return buf.getvalue()


def serialize_select_text(result) -> str:
	vars: List[str] = [str(v) for v in result.vars]
	lines = ["\t".join(vars)]
	for binding in result:
		row = []
		for i, _ in enumerate(vars):
			val = binding[i]
			row.append("" if val is None else str(val))
		lines.append("\t".join(row))
	return "\n".join(lines)


@app.post("/sparql/run")
async def sparql_run(req: SparqlRequest):
	q = req.query
	fmt = (req.format or "json").lower()

	graph = remote_graph() if FUSEKI_QUERY_ENDPOINT else load_graph()
	res = graph.query(q)

	if is_select_query(q):
		if fmt == "json":
			return JSONResponse(content=serialize_select_json(res))
		if fmt == "csv":
			return PlainTextResponse(content=serialize_select_csv(res), media_type="text/csv")
		# default text
		return PlainTextResponse(content=serialize_select_text(res), media_type="text/plain")

	if is_construct_query(q):
		cg = res.graph if hasattr(res, "graph") else Graph()
		if fmt == "json":
			return JSONResponse(content=cg.serialize(format="json-ld", indent=2).decode("utf-8"))
		# text -> turtle
		return PlainTextResponse(cg.serialize(format="turtle"), media_type="text/turtle")

	if is_ask_query(q):
		val = bool(res.askAnswer) if hasattr(res, "askAnswer") else bool(list(res))
		if fmt == "json":
			return JSONResponse(content={"boolean": val})
		return PlainTextResponse(content=str(val).lower(), media_type="text/plain")

	# Fallback
	return JSONResponse(content={"message": "Unsupported or empty result"})


@app.get("/app/semantic")
async def semantic_app():
	html = """
	<!DOCTYPE html>
	<html>
	<head>
	  <meta charset=\"utf-8\" />
	  <title>Semantic Stack Builder</title>
	  <style>
		body { font-family: system-ui, sans-serif; margin: 20px; }
		.grid { display:grid; grid-template-columns: 260px 1fr; gap:16px; }
		.toolbox { border:1px solid #ddd; border-radius:10px; background:#fafafa; padding:10px; }
		.stack { display:grid; grid-template-columns: 1fr; gap:12px; }
		.layer { border:1px dashed #9aa; border-radius:10px; padding:12px; background:#fff; min-height:120px; }
		.h3 { margin:0 0 6px 0; font-weight:600; }
		.chip { display:inline-block; padding:6px 8px; margin:6px 6px 0 0; border:1px solid #d0d0d0; border-radius:999px; background:#f6f8fa; cursor:grab; }
		.item { padding:6px 8px; margin:6px 0; border:1px solid #e0e0e0; border-radius:8px; background:#fff; }
		.row { display:flex; gap:8px; align-items:center; }
		label { font-size:12px; color:#333; }
		input[type=text], select { padding:6px 8px; border:1px solid #ccc; border-radius:6px; }
		pre { background:#0b1021; color:#e6edf3; padding:12px; border-radius:8px; max-height:280px; overflow:auto; }
		.btn { padding:8px 12px; border-radius:8px; border:1px solid #bbb; background:#fff; cursor:pointer; }
		.btn.primary { background:#175fe6; color:#fff; border-color:#175fe6; }
	  </style>
	</head>
	<body>
	  <h1>Semantic Stack</h1>
	  <div class=\"grid\">
		<div class=\"toolbox\">
		  <h3>Field</h3>
		  <div class=\"chip palette\" draggable=\"true\" data-kind=\"field\" data-name=\"foaf:name\">foaf:name</div>
		  <div class=\"chip palette\" draggable=\"true\" data-kind=\"field\" data-name=\"rdfs:label\">rdfs:label</div>
		  <div class=\"chip palette\" draggable=\"true\" data-kind=\"field\" data-name=\"schema:birthDate\">schema:birthDate</div>
		  <hr />
		  <h3>Transform</h3>
		  <div class=\"chip palette\" draggable=\"true\" data-kind=\"transform\" data-type=\"upper\">Upper</div>
		  <div class=\"chip palette\" draggable=\"true\" data-kind=\"transform\" data-type=\"lower\">Lower</div>
		  <div class=\"chip palette\" draggable=\"true\" data-kind=\"transform\" data-type=\"camel\">Camel</div>
		  <div class=\"chip palette\" draggable=\"true\" data-kind=\"transform\" data-type=\"snake\">Snake</div>
		  <div class=\"chip palette\" draggable=\"true\" data-kind=\"transform\" data-type=\"trim\">Trim</div>
		  <div class=\"chip palette\" draggable=\"true\" data-kind=\"transform\" data-type=\"regex\">Regex replace</div>
		  <div class=\"chip palette\" draggable=\"true\" data-kind=\"transform\" data-type=\"filter_contains\">Filter contains</div>
		  <hr />
		  <h3>Output</h3>
		  <div class=\"chip palette\" draggable=\"true\" data-kind=\"output\" data-type=\"json\">JSON</div>
		  <div class=\"chip palette\" draggable=\"true\" data-kind=\"output\" data-type=\"csv\">CSV</div>
		  <div class=\"chip palette\" draggable=\"true\" data-kind=\"output\" data-type=\"txt\">Text</div>
		</div>
		<div>
		  <div class=\"stack\">
			<div>
			  <div class=\"h3\">1) Select fields</div>
			  <div id=\"layer-select\" class=\"layer\">Thả các field vào đây…</div>
			</div>
			<div>
			  <div class=\"h3\">2) Transform</div>
			  <div id=\"layer-transform\" class=\"layer\">Thả các transform vào đây…</div>
			</div>
			<div>
			  <div class=\"h3\">3) Output</div>
			  <div id=\"layer-output\" class=\"layer\">Thả 1 kiểu output vào đây…</div>
			</div>
		  </div>
		  <div style=\"margin-top:12px\" class=\"row\">
			<label>Sample JSON</label>
			<input id=\"sample\" type=\"text\" value=\"{\"foaf:name\":\"Lionel Messi\",\"rdfs:label\":\"player_lionel_messi\",\"schema:birthDate\":\"1987-06-24\"}\" style=\"width:100%\" />
			<button class=\"btn primary\" id=\"run\">Run</button>
		  </div>
		  <pre id=\"result\"></pre>
		</div>
	  </div>
	  <script>
		// Drag & drop from palette to layers
		document.querySelectorAll('.palette').forEach(el=>{
		  el.addEventListener('dragstart', e=>{ e.dataTransfer.setData('text/plain', JSON.stringify({ kind: el.dataset.kind, type: el.dataset.type||null, name: el.dataset.name||null })); });
		});
		['layer-select','layer-transform','layer-output'].forEach(id=>{
		  const layer=document.getElementById(id);
		  layer.addEventListener('dragover', e=>e.preventDefault());
		  layer.addEventListener('drop', e=>{
			 e.preventDefault();
			 const data=JSON.parse(e.dataTransfer.getData('text/plain'));
			 layer.appendChild(renderItem(data));
		  });
		});

		function renderItem(data){
		  const div=document.createElement('div');
		  div.className='item';
		  if(data.kind==='field'){
			div.innerHTML=`<div class=\"row\"><strong>Field</strong><span>${data.name}</span></div>`;
			div.dataset.kind='field'; div.dataset.name=data.name;
		  } else if(data.kind==='transform'){
			if(data.type==='regex') div.innerHTML=`<div class=\"row\"><strong>Regex</strong><input placeholder=\"pattern\" data-role=\"pat\"/><input placeholder=\"replace\" data-role=\"rep\"/></div>`;
			else if(data.type==='filter_contains') div.innerHTML=`<div class=\"row\"><strong>Filter contains</strong><input placeholder=\"text\" data-role=\"contains\"/></div>`;
			else div.innerHTML=`<div class=\"row\"><strong>${data.type}</strong><span class=\"small\">no params</span></div>`;
			div.dataset.kind='transform'; div.dataset.type=data.type;
		  } else if(data.kind==='output'){
			div.innerHTML=`<div class=\"row\"><strong>Output</strong><span>${data.type.toUpperCase()}</span></div>`;
			div.dataset.kind='output'; div.dataset.type=data.type;
		  }
		  return div;
		}

		function camelCase(s){ return s.replace(/[-_ ]+(.)/g,(m,g)=>g.toUpperCase()).replace(/^(.)/, (m,g)=>g.toLowerCase()); }
		function snakeCase(s){ return s.replace(/([a-z])([A-Z])/g,'$1_$2').replace(/[ -]+/g,'_').toLowerCase(); }
		function applyTransforms(value, transforms){
		  let v=value==null?'' : String(value);
		  for(const t of transforms){
			if(t.type==='upper') v=v.toUpperCase();
			else if(t.type==='lower') v=v.toLowerCase();
			else if(t.type==='camel') v=camelCase(v);
			else if(t.type==='snake') v=snakeCase(v);
			else if(t.type==='trim') v=v.trim();
			else if(t.type==='regex') { try { v=v.replace(new RegExp(t.pat||'', 'g'), t.rep||''); } catch(e){} }
			else if(t.type==='filter_contains') { if(!v.includes(t.contains||'')) return null; }
		  }
		  return v;
		}

		document.getElementById('run').addEventListener('click', ()=>{
		  let record={};
		  try { record=JSON.parse(document.getElementById('sample').value); } catch(e){}
		  const fields=[...document.querySelectorAll('#layer-select .item')].filter(n=>n.dataset.kind==='field').map(n=>n.dataset.name);
		  const transforms=[...document.querySelectorAll('#layer-transform .item')].map(n=>({
			type:n.dataset.type||n.querySelector('strong')?.innerText?.toLowerCase(),
			pat:n.querySelector('[data-role=pat]')?.value||'',
			rep:n.querySelector('[data-role=rep]')?.value||'',
			contains:n.querySelector('[data-role=contains]')?.value||''
		  }));
		  const outputType=(document.querySelector('#layer-output .item')?.dataset.type)||'json';
		  const out={};
		  for(const f of fields){
			const raw=record[f]!==undefined? record[f] : '';
			const val=applyTransforms(raw, transforms);
			if(val!==null) out[f]=val; // filter_contains có thể loại bỏ
		  }
		  let rendered='';
		  if(outputType==='json') rendered=JSON.stringify(out, null, 2);
		  else if(outputType==='csv') { const headers=Object.keys(out); const values=headers.map(h=>out[h]); rendered=headers.join(',')+'\n'+values.join(','); }
		  else { rendered=Object.entries(out).map(([k,v])=>`${k}: ${v}`).join('\n'); }
		  document.getElementById('result').textContent=rendered;
		});
	  </script>
	</body>
	</html>
	"""
	return HTMLResponse(content=html)
