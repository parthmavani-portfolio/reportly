"""
================================================================================
FMCG ANNUAL REPORT DECODER  v4.0  —  Ultra-Lean (Free Tier Ready)
================================================================================
Optimized for 512MB RAM. Removed: matplotlib, sklearn, wordcloud, PIL.
Uses: pdfplumber, VADER, reportlab, pandas (lazy-loaded for Excel only).

Peak memory: ~250MB on a 400-page report.
Entry point: process_report(pdf_path, output_dir, label) → dict
================================================================================
"""
import re, os, gc, math, warnings
from collections import Counter, defaultdict
from datetime import datetime
import pdfplumber
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
    Table, TableStyle, PageBreak, HRFlowable)
from reportlab.graphics.shapes import Drawing, Rect, String, Line, Wedge
from reportlab.graphics import renderPDF

warnings.filterwarnings("ignore")

# ── Stopwords (built-in, no NLTK download needed) ──
STOP_WORDS = set("i me my myself we our ours ourselves you your yours yourself yourselves he him his himself she her hers herself it its itself they them their theirs themselves what which who whom this that these those am is are was were be been being have has had having do does did doing a an the and but if or because as until while of at by for with about against between through during before after above below to from up down in out on off over under again further then once here there when where why how all both each few more most other some such no nor not only own same so than too very can will just don should now would could also may much like well still even back make made many get got been one two three four five first new year years company business shall upon within without between shall said report board".split())

def sent_tokenize(text):
    return [s.strip() for s in re.split(r'(?<=[.!?])\s+(?=[A-Z])', text) if s.strip() and len(s.strip())>20]

# ═══ DICTIONARIES ═══
THEMES = {
    "Volume & Distribution":["volume","offtake","throughput","sell-through","velocity","distribution","reach","penetration","outlet","SKU","SKUs","channel","numeric distribution","weighted distribution","general trade","modern trade","e-commerce","quick commerce","rural","urban","tier-2","tier-3","distributor","stockist","coverage","fill rate","availability","out-of-stock"],
    "Raw Material & Commodity":["raw material","commodity","palm oil","crude","packaging","inflation","deflation","procurement","hedging","hedge","input cost","material cost","RM cost","agri","agriculture","crop","monsoon","supplier","sourcing","supply chain","price hike","cost pressure","material inflation","polyethylene","PET","glass","aluminium","tin"],
    "Brand & Innovation":["brand","innovation","new product","NPD","launch","re-launch","portfolio","premiumization","premium","naturals","organic","advertising","A&P","media","digital marketing","influencer","awareness","equity","category creation","market development","consumer insight","renovation","reformulation","pack","variant","market share","share of voice","category leader"],
    "Margin & Profitability":["gross margin","EBITDA","EBIT","operating margin","PAT margin","net margin","margin expansion","margin accretion","profitability","cost efficiency","operating leverage","savings","cost reduction","zero-based budgeting","ZBB","price realisation","mix improvement","premiumisation","cash flow","free cash flow","return on equity","ROCE","working capital","inventory","debtor days"],
    "Growth & Strategy":["growth","expand","expansion","opportunity","investment","acceleration","scale","footprint","market development","adjacency","diversification","acquisition","M&A","JV","partnership","strategic","long-term","roadmap","vision","digital","D2C","direct-to-consumer","omnichannel","emerging markets","export","international"],
    "Risk & Headwinds":["risk","uncertain","pressure","decline","slowdown","challenge","headwind","disruption","competition","competitive intensity","downtrading","downgrade","value-seeking","elasticity","regulatory","compliance","tax","GST","FDI","import duty","litigation","recall","reputational","climate","ESG","labour","attrition","talent","geopolitical"],
}
THEME_COLORS_HEX = {"Volume & Distribution":"2196F3","Raw Material & Commodity":"FF9800","Brand & Innovation":"9C27B0","Margin & Profitability":"4CAF50","Growth & Strategy":"00BCD4","Risk & Headwinds":"F44336"}

GUIDANCE_PATTERNS = {
    "Revenue Growth":{"kw":["revenue growth","top-line growth","sales growth","revenue target","turnover growth","gross revenue","net revenue","top line","revenue outlook","double-digit growth","growth target"],"fw":["expect","target","aim","project","anticipate","forecast","guidance","outlook","plan to","poised to","on track","going forward","looking ahead","next year","medium-term","long-term","aspire","goal","envisage"]},
    "Margin Expansion":{"kw":["margin expansion","margin improvement","EBITDA margin","operating margin","gross margin","net margin","margin target","profitability improvement","cost efficiency","margin recovery","basis points","operating leverage"],"fw":["expect","improve","expand","target","aim","outlook","going forward","trajectory","on track","plan","aspire","continue to","sustainable","medium-term"]},
    "Capex & Investment":{"kw":["capital expenditure","capex","investment","invest","capacity expansion","new plant","new facility","greenfield","brownfield","manufacturing capacity","capacity addition","expansion plan","capital allocation"],"fw":["plan","planned","propose","commit","allocate","upcoming","pipeline","project","commission","under construction","expected to","will be","set to"]},
    "Market Expansion":{"kw":["new market","market entry","geographic expansion","export","international","overseas","emerging market","new category","category entry","white space","adjacency","new segment","launch","D2C","e-commerce","quick commerce"],"fw":["plan","target","enter","foray","explore","opportunity","poised","aim","pipeline","roadmap","strategy"]},
}
PMI_COMPONENTS = {
    "New Orders":{"weight":0.30,"up":["order growth","strong demand","robust demand","demand growth","increasing orders","order book","healthy demand","buoyant demand","record orders","demand uptick","demand recovery","consumer demand","strong offtake","rising demand","growing demand","high demand"],"down":["order decline","weak demand","sluggish demand","muted demand","demand slowdown","falling orders","tepid demand","soft demand","demand contraction","declining orders","subdued demand","demand pressure"]},
    "Output":{"weight":0.25,"up":["production growth","capacity utilisation","output growth","increased production","higher output","production expansion","manufacturing growth","volume growth","capacity expansion","record production","ramp up","full capacity","operational efficiency"],"down":["production decline","lower output","capacity underutilisation","production cut","output decline","plant shutdown","production disruption","production slowdown","idle capacity"]},
    "Employment":{"weight":0.20,"up":["hiring","recruitment","talent acquisition","workforce expansion","new hires","headcount growth","job creation","skilling","upskilling","training","employee strength","human capital","talent development","workforce growth","employee engagement"],"down":["layoff","retrenchment","workforce reduction","job cuts","headcount reduction","attrition","employee turnover","rightsizing","manpower reduction","staff reduction","restructuring"]},
    "Supplier Delivery":{"weight":0.15,"up":["supply chain efficiency","supply chain resilience","supply normalisation","supply improvement","supply stability","vendor performance","smooth supply","stable supply","supply chain agility","procurement efficiency"],"down":["supply disruption","supply chain disruption","supply constraint","supply shortage","supply bottleneck","logistics challenge","raw material shortage","supply chain stress","supply chain risk","supply delay"]},
    "Inventories":{"weight":0.10,"up":["inventory build","inventory growth","stock build","higher inventory","inventory addition","inventory replenishment","buffer stock","adequate inventory","inventory optimisation"],"down":["inventory depletion","destocking","inventory reduction","stock out","out of stock","inventory decline","inventory pressure","lower inventory","excess inventory"]},
}
DEFAULT_SECTIONS = ["Letter to Shareholders","Chairman","Managing Director","Management Discussion","MD&A","Business Overview","Supply Chain","Research and Development","R&D","Board's Report"]

# ═══ CORE NLP (no sklearn) ═══
_vader = SentimentIntensityAnalyzer()
def get_sentiment(text):
    words=text.split(); scores=[]
    for i in range(0,len(words),500):
        scores.append(_vader.polarity_scores(" ".join(words[i:i+500]))["compound"])
    return sum(scores)/len(scores) if scores else 0.0

def sentiment_label(s): return "Positive" if s>0.05 else "Negative" if s<-0.05 else "Neutral"
def clean_text(t): return re.sub(r"\s{2,}"," ",re.sub(r"\n+"," ",re.sub(r"-\n","",t))).strip()

def extract_keywords(texts_dict, top_n=10):
    """Lightweight TF-IDF using pure Python. No sklearn needed."""
    docs = {n: re.findall(r"\b[a-zA-Z]{4,}\b", t.lower()) for n,t in texts_dict.items()}
    # Document frequency
    df = Counter()
    for words in docs.values():
        df.update(set(words))
    n_docs = max(len(docs), 1)
    result = {}
    for name, words in docs.items():
        filtered = [w for w in words if w not in STOP_WORDS]
        tf = Counter(filtered)
        # TF-IDF score
        scored = {}
        for word, count in tf.items():
            idf = math.log(n_docs / max(df.get(word, 1), 1)) + 1
            scored[word] = count * idf
        top = sorted(scored.items(), key=lambda x: -x[1])[:top_n]
        result[name] = [w for w, _ in top]
    return result

def count_theme(text, tw):
    t = text.lower(); return sum(t.count(w.lower()) for w in tw)

def extract_theme_sentences(text, tw, top_n=3):
    scored = []
    for s in sent_tokenize(clean_text(text)):
        sl = s.lower(); h = sum(sl.count(w.lower()) for w in tw)
        if h > 0: scored.append((h, s.strip()))
    scored.sort(key=lambda x: -x[0]); return [s for _, s in scored[:top_n]]

def dominant_theme(tc): return max(tc, key=tc.get)

def narrative_label(totals, avg):
    rm,brand,marg = totals["Raw Material & Commodity"],totals["Brand & Innovation"],totals["Margin & Profitability"]
    grow,risk = totals["Growth & Strategy"],totals["Risk & Headwinds"]
    if grow>risk*2 and avg>0.05: return ("Aggressive Growth Narrative","Management language is strongly growth-oriented with positive sentiment, signalling confidence in volume acceleration and market expansion.")
    if brand>rm and marg>rm and avg>0: return ("Brand-Led Premiumisation Narrative","Management emphasis on brand investment and margin improvement suggests a premiumisation strategy.")
    if rm>brand and rm>grow: return ("Commodity Stress Narrative","Raw material and input-cost language dominates, indicating focus on navigating inflationary headwinds.")
    if marg>grow and marg>rm: return ("Margin Recovery Narrative","Profitability and efficiency language leads, suggesting a margin-repair cycle.")
    if risk>grow*1.5: return ("Defensive / Stress Narrative","Risk language dominates, signalling caution around demand, competition, or regulatory uncertainty.")
    return ("Cautious Optimism / Balanced Narrative","Management tone is balanced across growth, margin, and risk themes.")

def extract_summary(text, n=4):
    sents = sent_tokenize(clean_text(text))
    if not sents: return "No text available."
    imp = ["growth","revenue","profit","margin","strategy","performance","market","brand","innovation","expansion","investment","digital","consumer","demand","volume","opportunity","future","outlook","expect","target","achieved","increased","improved","declined","challenge","dividend","acquisition","capacity","competitive"]
    bp = ["committee","pursuant","regulation","disclosure","resolution","aforesaid","herein","thereof"]
    scored = []
    for idx, s in enumerate(sents):
        wc = len(s.split())
        if wc < 8 or wc > 80: continue
        sl = s.lower(); sc = 0.0
        if idx < len(sents)*0.2: sc += 2.0
        if idx > len(sents)*0.9: sc += 1.0
        sc += sum(1 for w in imp if w in sl) / max(wc,1) * 10
        if re.search(r'\d+\.?\d*', s): sc += 1.0
        if '%' in s: sc += 2.0
        sc -= sum(1 for w in bp if w in sl) * 1.5
        scored.append((idx, sc, s))
    scored.sort(key=lambda x: -x[1])
    top = sorted(scored[:n], key=lambda x: x[0])
    return " ".join(t[2] for t in top) if top else "Could not generate summary."

def extract_guidance(text):
    sents = sent_tokenize(clean_text(text)); guidance = {}
    bp = {"committee","pursuant","regulation","disclosure","resolution","aforesaid","compliance"}
    for cat, pats in GUIDANCE_PATTERNS.items():
        found = []
        for s in sents:
            sl = s.lower()
            if any(b in sl for b in bp): continue
            if any(kw in sl for kw in pats["kw"]) and any(fm in sl for fm in pats["fw"]):
                kh = sum(1 for kw in pats["kw"] if kw in sl)
                fh = sum(1 for fm in pats["fw"] if fm in sl)
                hn = bool(re.search(r'\d+\.?\d*\s*%', s))
                found.append({"sentence":s.strip()[:400],"score":kh+fh+(5 if hn else 0),"has_numbers":hn})
        found.sort(key=lambda x:-x["score"]); guidance[cat]=found[:3]
    return guidance

def calculate_pmi(text):
    sents = sent_tokenize(clean_text(text)); comps = {}
    for nm, d in PMI_COMPONENTS.items():
        ic=dc=0; ie=[]; de=[]
        for s in sents:
            sl = s.lower()
            ih = sum(1 for kw in d["up"] if kw in sl)
            dh = sum(1 for kw in d["down"] if kw in sl)
            if ih > dh: ic += 1; (len(ie)<2 and ie.append(s.strip()[:200]))
            elif dh > ih: dc += 1; (len(de)<2 and de.append(s.strip()[:200]))
        rel = ic + dc; si = (ic/rel*100) if rel else 50.0
        comps[nm] = {"weight":d["weight"],"improved":ic,"deteriorated":dc,"sub_index":round(si,1),"imp_ev":ie,"det_ev":de}
    pmi = round(sum(c["sub_index"]*c["weight"] for c in comps.values()), 1)
    if pmi>65: interp="Strong Expansion — robust growth signals across orders, production, and employment."
    elif pmi>55: interp="Moderate Expansion — positive signals outweigh negatives, steady growth."
    elif pmi>50: interp="Mild Expansion — marginal positive signals."
    elif pmi>45: interp="Mild Contraction — slightly more negative signals."
    elif pmi>35: interp="Moderate Contraction — significant negative signals."
    else: interp="Sharp Contraction — strong negative language across most components."
    return {"pmi_score":pmi,"interpretation":interp,"components":comps}

def generate_outlook(rpt):
    avg = rpt["avg_sentiment"]
    tone = "strongly optimistic" if avg>0.5 else "moderately positive" if avg>0.2 else "cautiously optimistic" if avg>0.05 else "neutral" if avg>-0.05 else "cautious to negative"
    top2 = sorted(rpt["totals"].items(), key=lambda x:-x[1])[:2]
    grow,risk = rpt["totals"].get("Growth & Strategy",0), rpt["totals"].get("Risk & Headwinds",0)
    s = f"The overall management outlook is {tone} (avg. sentiment {avg:+.3f}). Classified as '{rpt['narrative']}'. Dominant themes: '{top2[0][0]}' ({top2[0][1]}) and '{top2[1][0]}' ({top2[1][1]}). "
    if grow>risk*1.5: s += f"Growth-to-risk ratio is {grow/max(risk,1):.1f}:1 — expansionary posture. "
    elif risk>grow: s += "Risk outweighs growth — defensive posture. "
    return s

# ═══ PDF SECTION EXTRACTOR ═══
def _find_pages(toc, names):
    found = {}
    for n in names:
        m = re.search(re.escape(n)+r"[\s.\-\u2013\u2014|]+(\d+)", toc, re.I)
        if m: found[n]=int(m.group(1)); continue
        m = re.search(r"(\d+)\s+"+re.escape(n), toc, re.I)
        if m: found[n]=int(m.group(1))
    return found

def extract_sections(pdf_path, target_sections=None):
    if target_sections is None: target_sections = DEFAULT_SECTIONS
    MAX_PG = 8
    with pdfplumber.open(pdf_path) as pdf:
        tp = len(pdf.pages)
        toc = ""
        for p in range(1, min(6, tp)):
            pt = pdf.pages[p].extract_text() or ""
            if any(n[:6].lower() in pt.lower() for n in target_sections): toc += pt+"\n"
        pm = _find_pages(toc, target_sections)
        if not pm:
            toc = "\n".join((pdf.pages[p].extract_text() or "") for p in range(min(15, tp)))
            pm = _find_pages(toc, target_sections)
        if not pm:
            for n in target_sections:
                m = re.search(n.split()[0]+r"[^0-9]*?(\d{1,3})", toc, re.I)
                if m: pm[n] = int(m.group(1))
        del toc; gc.collect()
        if not pm: return {}, {}
        ss = sorted(pm.items(), key=lambda x:x[1]); secs={}; pgs={}
        for i,(n,sp) in enumerate(ss):
            ep = min((ss[i+1][1]-1) if i+1<len(ss) else sp+MAX_PG, sp+MAX_PG, tp)
            txt = ""
            for j in range(sp-1, ep):
                if j < tp:
                    pt = pdf.pages[j].extract_text()
                    if pt: txt += pt+"\n"
            secs[n] = clean_text(txt); pgs[n] = (sp, ep)
            print(f"  ✓ '{n}' pp.{sp}–{ep} ({len(txt.split())} words)")
    gc.collect()
    return secs, pgs

# ═══ ANALYSIS ═══
def analyse_report(label, pdf_path):
    print(f"\n{'='*60}\n  ANALYSING: {label}\n{'='*60}")
    sections, pages = extract_sections(pdf_path)
    if not sections: print("  ⚠ No sections found."); return {}
    kw_map = extract_keywords(sections)
    all_text = " ".join(sections.values())
    results = {}
    for sn, text in sections.items():
        sc = get_sentiment(text)
        tc = {t: count_theme(text, w) for t, w in THEMES.items()}
        dom = dominant_theme(tc)
        vol,rm,brand = tc["Volume & Distribution"],tc["Raw Material & Commodity"],tc["Brand & Innovation"]
        marg,grow,risk = tc["Margin & Profitability"],tc["Growth & Strategy"],tc["Risk & Headwinds"]
        if risk>grow and risk>brand: ins="Risk and headwind language leads — management is cautious."
        elif brand>rm and marg>rm: ins="Brand investment + margin language suggests premiumisation focus."
        elif rm>brand and rm>grow: ins="Raw material/commodity mentions dominate — cost pressure is front of mind."
        elif grow>risk*2: ins="Strongly growth-oriented language with limited risk acknowledgement."
        elif vol>marg: ins="Volume and distribution emphasis — management prioritising top-line reach."
        else: ins="Balanced narrative across growth, margin, and risk themes."
        ts = {t: extract_theme_sentences(text, w) for t, w in THEMES.items() if tc[t]>0}
        results[sn] = {"sentiment":sc,"tone":sentiment_label(sc),"theme_counts":tc,"dominant_theme":dom,
                       "insight":ins,"keywords":kw_map.get(sn,[]),"theme_sentences":ts,
                       "summary":extract_summary(text,4),"pages":pages.get(sn,(0,0)),"word_count":len(text.split())}
        print(f"  ✓ {sn}  sent={sc:+.3f}  dom={dom}")
    del sections; gc.collect()
    totals = defaultdict(int)
    for r in results.values():
        for t,c in r["theme_counts"].items(): totals[t]+=c
    avg = sum(r["sentiment"] for r in results.values())/len(results)
    narr, expl = narrative_label(totals, avg)
    guid = extract_guidance(all_text)
    pmi = calculate_pmi(all_text)
    del all_text; gc.collect()
    rpt = {"label":label,"sections":results,"totals":dict(totals),"avg_sentiment":avg,
           "narrative":narr,"explanation":expl,"guidance":guid,"pmi":pmi}
    rpt["outlook"] = generate_outlook(rpt)
    print(f"\n  Narrative: {narr}\n  PMI: {pmi['pmi_score']}  ({pmi['interpretation'][:50]})")
    return rpt

# ═══ REPORTLAB INLINE CHARTS (no matplotlib) ═══
def _theme_bar_drawing(totals, width=460, height=160):
    """Horizontal bar chart using pure reportlab Drawing."""
    d = Drawing(width, height)
    names = list(THEMES.keys())
    vals = [totals.get(n,0) for n in names]
    max_val = max(vals) if vals else 1
    bar_h = 18; gap = 4; y_start = height - 20
    for i, (name, val) in enumerate(zip(names, vals)):
        y = y_start - i*(bar_h+gap)
        short = name.split("&")[0].strip()
        bar_w = (val/max_val) * (width - 180) if max_val else 0
        col = colors.HexColor("#"+THEME_COLORS_HEX.get(name,"999999"))
        d.add(Rect(170, y, bar_w, bar_h, fillColor=col, strokeColor=None))
        d.add(String(0, y+5, short, fontSize=8, fillColor=colors.HexColor("#333333")))
        d.add(String(175+bar_w+4, y+5, str(val), fontSize=8, fillColor=colors.HexColor("#333333")))
    return d

def _pmi_drawing(pmi, width=460, height=100):
    """PMI gauge using pure reportlab Drawing."""
    d = Drawing(width, height)
    score = pmi["pmi_score"]
    col = colors.HexColor("#4CAF50") if score>55 else colors.HexColor("#F44336") if score<45 else colors.HexColor("#FF9800")
    # Score display
    d.add(String(width/2-30, 65, f"{score:.1f}", fontSize=36, fontName="Helvetica-Bold", fillColor=col))
    label = "EXPANSION" if score>50 else "CONTRACTION"
    d.add(String(width/2-35, 42, "Text-Derived PMI", fontSize=11, fillColor=colors.HexColor("#666666")))
    d.add(String(width/2-25, 22, label, fontSize=12, fontName="Helvetica-Bold", fillColor=col))
    # Scale bar
    d.add(Rect(50, 8, width-100, 8, fillColor=colors.HexColor("#E0E0E0"), strokeColor=None))
    marker_x = 50 + (score/100)*(width-100)
    d.add(Rect(marker_x-3, 4, 6, 16, fillColor=col, strokeColor=None))
    d.add(Line(50+(50/100)*(width-100), 4, 50+(50/100)*(width-100), 20, strokeColor=colors.HexColor("#333333"), strokeWidth=1))
    return d

# ═══ PDF BUILDER ═══
def build_pdf(report, output_path):
    s = getSampleStyleSheet()
    nv = colors.HexColor("#1a237e")
    sT = ParagraphStyle("T",parent=s["Title"],fontSize=20,textColor=nv,spaceAfter=4)
    sST = ParagraphStyle("ST",parent=s["Normal"],fontSize=11,textColor=colors.HexColor("#455a64"),spaceAfter=10)
    sH1 = ParagraphStyle("H1",parent=s["Heading1"],fontSize=15,textColor=nv,spaceBefore=14,spaceAfter=6)
    sH2 = ParagraphStyle("H2",parent=s["Heading2"],fontSize=12,textColor=colors.HexColor("#283593"),spaceBefore=10,spaceAfter=4)
    sB = ParagraphStyle("B",parent=s["Normal"],fontSize=9,leading=13,alignment=TA_JUSTIFY,spaceAfter=5)
    sSm = ParagraphStyle("Sm",parent=s["Normal"],fontSize=8,leading=10,textColor=colors.HexColor("#616161"))
    sMet = ParagraphStyle("Mt",parent=s["Normal"],fontSize=9,textColor=colors.HexColor("#1b5e20"),spaceAfter=3)
    sFt = ParagraphStyle("Ft",parent=s["Normal"],fontSize=7,textColor=colors.HexColor("#9e9e9e"),alignment=TA_CENTER)
    hr = lambda: HRFlowable(width="100%",thickness=1,color=colors.HexColor("#bdbdbd"))
    W = A4[0]-40*mm; pmi = report["pmi"]
    def mktbl(data, cw):
        t=Table(data,colWidths=cw); t.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),nv),("TEXTCOLOR",(0,0),(-1,0),colors.white),("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,-1),8),("GRID",(0,0),(-1,-1),0.4,colors.HexColor("#bdbdbd")),("TOPPADDING",(0,0),(-1,-1),3),("BOTTOMPADDING",(0,0),(-1,-1),3),("LEFTPADDING",(0,0),(-1,-1),5),("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white,colors.HexColor("#f5f5f5")])])); return t

    doc = SimpleDocTemplate(output_path,pagesize=A4,leftMargin=20*mm,rightMargin=20*mm,topMargin=15*mm,bottomMargin=15*mm)
    story = []

    # PAGE 1: COVER
    story.append(HRFlowable(width="100%",thickness=2,color=nv)); story.append(Spacer(1,6))
    story.append(Paragraph("FMCG Annual Report Decoder",sT))
    story.append(Paragraph(f"Report: {report['label']}  |  Generated: {datetime.now().strftime('%d %B %Y')}",sST))
    story.append(hr()); story.append(Spacer(1,10))
    story.append(Paragraph("NARRATIVE CLASSIFICATION",sH1))
    nd=[["Narrative Type",report["narrative"]],["Avg Sentiment",f"{report['avg_sentiment']:+.3f}  ({sentiment_label(report['avg_sentiment'])})"],["Text-Derived PMI",f"{pmi['pmi_score']:.1f}  —  {'Expansion' if pmi['pmi_score']>50 else 'Contraction'}"]]
    t=Table(nd,colWidths=[120,W-130]); t.setStyle(TableStyle([("BACKGROUND",(0,0),(0,-1),colors.HexColor("#e8eaf6")),("FONTNAME",(0,0),(0,-1),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,-1),9),("VALIGN",(0,0),(-1,-1),"TOP"),("GRID",(0,0),(-1,-1),0.4,colors.HexColor("#bdbdbd")),("TOPPADDING",(0,0),(-1,-1),5),("BOTTOMPADDING",(0,0),(-1,-1),5),("LEFTPADDING",(0,0),(-1,-1),6)]))
    story.append(t); story.append(Spacer(1,8))
    story.append(Paragraph("MANAGEMENT OUTLOOK",sH2))
    story.append(Paragraph(report.get("outlook","N/A"),sB))
    story.append(Spacer(1,6))
    # Theme chart (inline reportlab drawing)
    story.append(Paragraph("THEME DISTRIBUTION",sH2))
    story.append(_theme_bar_drawing(report["totals"]))
    story.append(Spacer(1,6))
    tm = max(sum(report["totals"].values()),1)
    td = [["Theme","Mentions","Share"]]
    for tn in THEMES: c=report["totals"].get(tn,0); td.append([tn,str(c),f"{c/tm*100:.0f}%"])
    story.append(mktbl(td,[190,70,70]))

    # PAGE 2: THEME INSIGHTS
    story.append(PageBreak()); story.append(Paragraph("THEME INSIGHTS BY SECTION",sH1)); story.append(hr()); story.append(Spacer(1,6))
    for sn,d in report["sections"].items():
        story.append(Paragraph(f"{sn}  (Pages {d['pages'][0]}–{d['pages'][1]}, {d['word_count']} words)",sH2))
        story.append(Paragraph(f"<b>Sentiment:</b> {d['sentiment']:+.3f} ({d['tone']})  |  <b>Dominant:</b> {d['dominant_theme']}",sB))
        story.append(Paragraph(f"<b>Insight:</b> {d['insight']}",sB))
        story.append(Paragraph(f"<b>Keywords:</b> {', '.join(d['keywords'][:8])}",sSm)); story.append(Spacer(1,4))

    # PAGE 3: SUMMARIES
    story.append(PageBreak()); story.append(Paragraph("SECTION SUMMARIES",sH1)); story.append(hr()); story.append(Spacer(1,6))
    for sn,d in report["sections"].items():
        story.append(Paragraph(sn,sH2))
        sm = d.get("summary","")[:1000]
        story.append(Paragraph(sm,sB)); story.append(Spacer(1,4))

    # PAGE 4: GUIDANCE
    story.append(PageBreak()); story.append(Paragraph("MANAGEMENT GUIDANCE",sH1)); story.append(hr()); story.append(Spacer(1,6))
    for cat,items in report.get("guidance",{}).items():
        story.append(Paragraph(cat,sH2))
        if not items: story.append(Paragraph("No forward-looking statements identified.",sSm))
        else:
            for it in items:
                pfx = "★ " if it.get("has_numbers") else "• "
                story.append(Paragraph(f"{pfx}{it['sentence'][:350]}",sMet if it.get("has_numbers") else sB))
        story.append(Spacer(1,4))

    # PAGE 5: PMI
    story.append(PageBreak()); story.append(Paragraph("TEXT-DERIVED PMI",sH1)); story.append(hr()); story.append(Spacer(1,6))
    story.append(Paragraph("PMI is calculated by scanning sentences for improved/deteriorated signals across 5 components. Above 50 = expansion, below 50 = contraction.",sB))
    story.append(Spacer(1,4))
    # PMI gauge drawing
    story.append(_pmi_drawing(pmi))
    story.append(Spacer(1,8))
    # PMI table
    pt=[["Component","Weight","Improved","Deteriorated","Sub-Index","Signal"]]
    for cn,cd in pmi["components"].items():
        si=cd["sub_index"]; sig="Expansion" if si>55 else "Contraction" if si<45 else "Neutral"
        pt.append([cn,f"{cd['weight']*100:.0f}%",str(cd["improved"]),str(cd["deteriorated"]),f"{si:.1f}",sig])
    pt.append(["WEIGHTED PMI","100%","","",f"{pmi['pmi_score']:.1f}","EXPANSION" if pmi["pmi_score"]>50 else "CONTRACTION"])
    ptbl=Table(pt,colWidths=[100,50,60,70,60,75]); sty=TableStyle([("BACKGROUND",(0,0),(-1,0),nv),("TEXTCOLOR",(0,0),(-1,0),colors.white),("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,-1),8),("GRID",(0,0),(-1,-1),0.4,colors.HexColor("#bdbdbd")),("TOPPADDING",(0,0),(-1,-1),3),("BOTTOMPADDING",(0,0),(-1,-1),3),("LEFTPADDING",(0,0),(-1,-1),5),("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white,colors.HexColor("#f5f5f5")]),("BACKGROUND",(0,-1),(-1,-1),colors.HexColor("#e8eaf6")),("FONTNAME",(0,-1),(-1,-1),"Helvetica-Bold")])
    ptbl.setStyle(sty); story.append(ptbl)

    # REFERENCES
    story.append(PageBreak()); story.append(Paragraph("REFERENCES",sH1)); story.append(hr()); story.append(Spacer(1,6))
    rd=[["Section","Pages","Words","Analyses"]]
    for sn,d in report["sections"].items():
        rd.append([sn,f"pp.{d['pages'][0]}–{d['pages'][1]}",str(d["word_count"]),"Sentiment, Themes, Keywords, Summary, Guidance, PMI"])
    story.append(mktbl(rd,[110,60,45,W-225]))
    story.append(Spacer(1,12)); story.append(Paragraph("Methodology",sH2))
    story.append(Paragraph("<b>Sentiment:</b> VADER compound, 500-word chunks. <b>Themes:</b> 6 FMCG dictionaries. <b>Keywords:</b> TF-IDF (pure Python). <b>Summary:</b> Extractive scoring. <b>Guidance:</b> Keyword + temporal marker co-occurrence. <b>PMI:</b> 5-component weighted signal classification.",sB))
    story.append(Spacer(1,20)); story.append(hr())
    story.append(Paragraph(f"FMCG Annual Report Decoder v4.0  |  {datetime.now().strftime('%d %B %Y, %H:%M')}",sFt))
    doc.build(story); gc.collect()
    print(f"  ✓ PDF → {output_path}")

# ═══ EXCEL (lazy import pandas) ═══
def build_excel(report, output_path):
    import pandas as pd  # Lazy import — only loaded when building Excel
    label = report["label"]
    with pd.ExcelWriter(output_path, engine="openpyxl") as w:
        rows = []
        for sn,d in report["sections"].items():
            row = {"Section":sn,"Pages":f"{d['pages'][0]}-{d['pages'][1]}","Sentiment":round(d["sentiment"],4),"Tone":d["tone"],"Dominant Theme":d["dominant_theme"],"Insight":d["insight"],"Summary":d["summary"][:500],"Keywords":", ".join(d["keywords"][:10])}
            row.update({f"[{t}]":d["theme_counts"][t] for t in THEMES}); rows.append(row)
        pd.DataFrame(rows).to_excel(w,sheet_name="Summary",index=False)
        sr = []
        for sn,d in report["sections"].items():
            for th,ss in d["theme_sentences"].items():
                for i,st in enumerate(ss,1): sr.append({"Section":sn,"Theme":th,"Rank":i,"Sentence":st})
        if sr: pd.DataFrame(sr).to_excel(w,sheet_name="Key_Sentences",index=False)
        gr = []
        for cat,items in report.get("guidance",{}).items():
            for i,it in enumerate(items,1): gr.append({"Category":cat,"Rank":i,"Quantitative":it.get("has_numbers",False),"Statement":it["sentence"]})
        if gr: pd.DataFrame(gr).to_excel(w,sheet_name="Guidance",index=False)
        pr = []
        for cn,cd in report["pmi"]["components"].items():
            pr.append({"Component":cn,"Weight":f"{cd['weight']*100:.0f}%","Improved":cd["improved"],"Deteriorated":cd["deteriorated"],"Sub-Index":cd["sub_index"],"Evidence+":"; ".join(cd["imp_ev"])[:400],"Evidence-":"; ".join(cd["det_ev"])[:400]})
        pr.append({"Component":"WEIGHTED PMI","Sub-Index":report["pmi"]["pmi_score"],"Evidence+":report["pmi"]["interpretation"]})
        pd.DataFrame(pr).to_excel(w,sheet_name="PMI",index=False)
        pd.DataFrame([{"Report":label,"Narrative":report["narrative"],"Explanation":report["explanation"],"Outlook":report.get("outlook",""),"Sentiment":round(report["avg_sentiment"],4),"PMI":report["pmi"]["pmi_score"],**{f"[{t}]":report["totals"].get(t,0) for t in THEMES}}]).to_excel(w,sheet_name="Narrative",index=False)
    gc.collect()
    print(f"  ✓ Excel → {output_path}")

# ═══ PUBLIC API ═══
def process_report(pdf_path, output_dir=None, label=None):
    if output_dir is None: output_dir = os.path.dirname(pdf_path) or "."
    os.makedirs(output_dir, exist_ok=True)
    if label is None:
        label = re.sub(r'[^a-zA-Z0-9_\-]','_',os.path.splitext(os.path.basename(pdf_path))[0])[:30]
    print(f"\n{'='*60}\n  FMCG ANNUAL REPORT DECODER v4.0 (Ultra-Lean)\n{'='*60}")
    report = analyse_report(label, pdf_path)
    if not report: return {"error":"No sections could be extracted."}
    pdf_out = os.path.join(output_dir, f"FMCG_Dashboard_{label}.pdf")
    build_pdf(report, pdf_out)
    xlsx_out = os.path.join(output_dir, f"FMCG_Analysis_{label}.xlsx")
    build_excel(report, xlsx_out)
    gc.collect()
    print(f"\n{'='*60}\n  COMPLETE ✓\n  PDF:   {pdf_out}\n  Excel: {xlsx_out}\n{'='*60}\n")
    return {"pdf":pdf_out,"excel":xlsx_out,"charts":{},"report":report}

if __name__=="__main__":
    result = process_report("/home/claude/itc_fy25.pdf","/home/claude/output","ITC_FY25")
    if "error" in result: print(f"\n  ⚠ {result['error']}")
