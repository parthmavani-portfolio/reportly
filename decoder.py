"""
================================================================================
FMCG ANNUAL REPORT DECODER  v3.1  —  Production-Ready
================================================================================
Self-contained pipeline: PDF in → Dashboard PDF + Excel out.

Web-ready:  call  process_report(pdf_path, output_dir)  → dict of output paths.

Dependencies:
    pip install pdfplumber vaderSentiment scikit-learn pandas openpyxl \
               wordcloud matplotlib nltk reportlab Pillow
================================================================================
"""
import re, os, warnings, tempfile
from collections import Counter, defaultdict
from datetime import datetime
import pdfplumber, pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.patches import FancyBboxPatch
from wordcloud import WordCloud
from PIL import Image as PILImage
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from sklearn.feature_extraction.text import TfidfVectorizer
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
    Image as RLImage, Table, TableStyle, PageBreak, HRFlowable)

warnings.filterwarnings("ignore")

# ── NLTK stopwords with network-restricted fallback ──
try:
    import nltk
    _local_nltk = os.path.join(os.path.dirname(__file__), "nltk_data")
    if os.path.exists(_local_nltk):
        nltk.data.path.insert(0, _local_nltk)
    nltk.download("stopwords", quiet=True)
    from nltk.corpus import stopwords
    STOP_WORDS = set(stopwords.words("english"))
except Exception:
    STOP_WORDS = set("i me my myself we our ours ourselves you your yours yourself yourselves he him his himself she her hers herself it its itself they them their theirs themselves what which who whom this that these those am is are was were be been being have has had having do does did doing a an the and but if or because as until while of at by for with about against between through during before after above below to from up down in out on off over under again further then once here there when where why how all both each few more most other some such no nor not only own same so than too very s t can will just don should now".split())

# ── Sentence tokenizer (no NLTK punkt needed) ──
def sent_tokenize(text):
    return [s.strip() for s in re.split(r'(?<=[.!?])\s+(?=[A-Z])', text) if s.strip() and len(s.strip()) > 20]

# ═══════════════════════════════════════════════════════════════════
# DICTIONARIES
# ═══════════════════════════════════════════════════════════════════
THEMES = {
    "Volume & Distribution": ["volume","offtake","throughput","sell-through","velocity","distribution","reach","penetration","outlet","SKU","SKUs","channel","numeric distribution","weighted distribution","general trade","modern trade","e-commerce","quick commerce","rural","urban","tier-2","tier-3","distributor","stockist","coverage","fill rate","availability","out-of-stock"],
    "Raw Material & Commodity": ["raw material","commodity","palm oil","crude","packaging","inflation","deflation","procurement","hedging","hedge","input cost","material cost","RM cost","agri","agriculture","crop","monsoon","supplier","sourcing","supply chain","price hike","cost pressure","material inflation","polyethylene","PET","glass","aluminium","tin"],
    "Brand & Innovation": ["brand","innovation","new product","NPD","launch","re-launch","portfolio","premiumization","premium","naturals","organic","advertising","A&P","media","digital marketing","influencer","awareness","equity","category creation","market development","consumer insight","renovation","reformulation","pack","variant","market share","share of voice","category leader"],
    "Margin & Profitability": ["gross margin","EBITDA","EBIT","operating margin","PAT margin","net margin","margin expansion","margin accretion","profitability","cost efficiency","operating leverage","savings","cost reduction","zero-based budgeting","ZBB","price realisation","mix improvement","premiumisation","cash flow","free cash flow","return on equity","ROCE","working capital","inventory","debtor days"],
    "Growth & Strategy": ["growth","expand","expansion","opportunity","investment","acceleration","scale","footprint","market development","adjacency","diversification","acquisition","M&A","JV","partnership","strategic","long-term","roadmap","vision","digital","D2C","direct-to-consumer","omnichannel","emerging markets","export","international"],
    "Risk & Headwinds": ["risk","uncertain","pressure","decline","slowdown","challenge","headwind","disruption","competition","competitive intensity","downtrading","downgrade","value-seeking","elasticity","regulatory","compliance","tax","GST","FDI","import duty","litigation","recall","reputational","climate","ESG","labour","attrition","talent","geopolitical"],
}
THEME_COLORS = {"Volume & Distribution":"#2196F3","Raw Material & Commodity":"#FF9800","Brand & Innovation":"#9C27B0","Margin & Profitability":"#4CAF50","Growth & Strategy":"#00BCD4","Risk & Headwinds":"#F44336"}

GUIDANCE_PATTERNS = {
    "Revenue Growth": {"keywords":["revenue growth","top-line growth","sales growth","revenue target","turnover growth","gross revenue","net revenue","top line","revenue outlook","sales target","double-digit growth","single-digit growth","growth target"],"forward_markers":["expect","target","aim","project","anticipate","forecast","guidance","outlook","plan to","poised to","on track","going forward","looking ahead","next year","FY26","FY 26","medium-term","long-term","aspire","goal","envisage"]},
    "Margin Expansion": {"keywords":["margin expansion","margin improvement","margin accretion","EBITDA margin","operating margin","gross margin","net margin","margin target","profitability improvement","cost efficiency","margin recovery","margin trajectory","basis points","operating leverage"],"forward_markers":["expect","improve","expand","target","aim","outlook","going forward","trajectory","path to","on track","plan","aspire","continue to","sustainable","medium-term"]},
    "Capex & Investment": {"keywords":["capital expenditure","capex","investment","invest","capacity expansion","new plant","new facility","greenfield","brownfield","manufacturing capacity","capacity addition","expansion plan","capital allocation","outlay"],"forward_markers":["plan","planned","propose","commit","allocate","invest","upcoming","pipeline","project","commission","under construction","expected to","will be","set to"]},
    "Market Expansion": {"keywords":["new market","market entry","geographic expansion","export","international","overseas","emerging market","new geography","new category","category entry","white space","adjacency","new segment","launch","D2C","e-commerce","quick commerce"],"forward_markers":["plan","target","enter","foray","explore","opportunity","poised","aim","pipeline","roadmap","strategy"]},
}
PMI_COMPONENTS = {
    "New Orders":             {"weight":0.30,"improved":["order growth","strong demand","robust demand","demand growth","increasing orders","order book","healthy demand","buoyant demand","record orders","order momentum","demand uptick","demand recovery","consumer demand","strong offtake","rising demand","demand surge","accelerating demand","order inflow","growing demand","increased orders","high demand","strong orders"],"deteriorated":["order decline","weak demand","sluggish demand","muted demand","demand slowdown","falling orders","tepid demand","soft demand","demand contraction","order weakness","declining orders","lower demand","subdued demand","demand pressure","demand erosion"]},
    "Output (Production)":    {"weight":0.25,"improved":["production growth","capacity utilisation","output growth","increased production","production ramp","higher output","production expansion","manufacturing growth","volume growth","throughput increase","capacity expansion","record production","ramp up","production increase","higher capacity","full capacity","operational efficiency"],"deteriorated":["production decline","lower output","capacity underutilisation","production cut","output decline","reduced production","plant shutdown","production disruption","lower throughput","production slowdown","idle capacity"]},
    "Employment":             {"weight":0.20,"improved":["hiring","recruitment","talent acquisition","workforce expansion","new hires","employee addition","headcount growth","job creation","talent pipeline","skilling","upskilling","training","employee strength","human capital","talent development","people investment","workforce growth","employee engagement"],"deteriorated":["layoff","retrenchment","workforce reduction","job cuts","headcount reduction","attrition","employee turnover","rightsizing","manpower reduction","staff reduction","restructuring","voluntary retirement"]},
    "Supplier Delivery Times":{"weight":0.15,"improved":["supply chain efficiency","supply chain resilience","supply normalisation","supply improvement","supply stability","vendor performance","smooth supply","stable supply","supply chain strength","logistics efficiency","supply easing","supply chain agility","procurement efficiency"],"deteriorated":["supply disruption","supply chain disruption","supply constraint","supply shortage","supply bottleneck","logistics challenge","raw material shortage","supply chain stress","procurement challenge","supply chain risk","supply delay","vendor disruption","geopolitical disruption","logistics disruption"]},
    "Inventories":            {"weight":0.10,"improved":["inventory build","inventory growth","stock build","higher inventory","inventory addition","inventory investment","stocking up","inventory replenishment","buffer stock","adequate inventory","inventory optimisation"],"deteriorated":["inventory depletion","destocking","inventory reduction","stock out","out of stock","inventory decline","inventory pressure","lower inventory","excess inventory","inventory write-down"]},
}
DEFAULT_TARGET_SECTIONS = ["Letter to Shareholders","Chairman","Managing Director","Management Discussion","MD&A","Business Overview","Supply Chain","Research and Development","R&D","Board's Report"]

# ═══════════════════════════════════════════════════════════════════
# CORE NLP
# ═══════════════════════════════════════════════════════════════════
_vader = SentimentIntensityAnalyzer()

def get_sentiment(text):
    words = text.split(); scores = []
    for i in range(0, len(words), 500):
        scores.append(_vader.polarity_scores(" ".join(words[i:i+500]))["compound"])
    return sum(scores)/len(scores) if scores else 0.0

def sentiment_label(s):
    return "Positive" if s > 0.05 else "Negative" if s < -0.05 else "Neutral"

def clean_text(text):
    return re.sub(r"\s{2,}", " ", re.sub(r"\n+", " ", re.sub(r"-\n", "", text))).strip()

def extract_tfidf_keywords(stexts, top_n=15):
    names = list(stexts.keys()); corpus = [stexts[n] for n in names]
    vec = TfidfVectorizer(stop_words=list(STOP_WORDS), min_df=1, token_pattern=r"(?u)\b[a-zA-Z]{4,}\b", ngram_range=(1,2))
    try: mat = vec.fit_transform(corpus)
    except ValueError: return {n: [w for w,_ in Counter(w for w in re.findall(r"\b[a-zA-Z]{4,}\b", stexts[n].lower()) if w not in STOP_WORDS).most_common(top_n)] for n in names}
    feats = vec.get_feature_names_out(); out = {}
    for i, n in enumerate(names):
        row = mat[i].toarray()[0]; idx = row.argsort()[::-1][:top_n]
        out[n] = [feats[j] for j in idx if row[j] > 0]
    return out

def count_theme(text, tw):
    t = text.lower(); return sum(t.count(w.lower()) for w in tw)

def extract_theme_sentences(text, tw, top_n=3):
    scored = []
    for s in sent_tokenize(clean_text(text)):
        sl = s.lower(); h = sum(sl.count(w.lower()) for w in tw)
        if h > 0: scored.append((h, s.strip()))
    scored.sort(key=lambda x: -x[0]); return [s for _,s in scored[:top_n]]

def dominant_theme(tc): return max(tc, key=tc.get)

def narrative_label(totals, avg):
    rm,brand,marg,grow,risk = totals["Raw Material & Commodity"],totals["Brand & Innovation"],totals["Margin & Profitability"],totals["Growth & Strategy"],totals["Risk & Headwinds"]
    if grow > risk*2 and avg > 0.05: return ("Aggressive Growth Narrative","Management language is strongly growth-oriented with positive sentiment, signalling confidence in volume acceleration and market expansion.")
    if brand > rm and marg > rm and avg > 0: return ("Brand-Led Premiumisation Narrative","Management emphasis on brand investment and margin improvement suggests a premiumisation strategy, prioritising value over pure volume growth.")
    if rm > brand and rm > grow: return ("Commodity Stress Narrative","Raw material and input-cost language dominates, indicating management is primarily focused on navigating inflationary headwinds.")
    if marg > grow and marg > rm: return ("Margin Recovery Narrative","Profitability and efficiency language leads the report, suggesting management is in a margin-repair cycle after prior cost pressures.")
    if risk > grow*1.5: return ("Defensive / Stress Narrative","Risk language dominates, signalling management caution around demand, competition, or regulatory uncertainty.")
    return ("Cautious Optimism / Balanced Narrative","Management tone is balanced across growth, margin, and risk, typical of a steady-state FMCG business in a stable but competitive environment.")

def extract_summary(text, n=5):
    sents = sent_tokenize(clean_text(text))
    if not sents: return "No text available."
    imp = ["growth","revenue","profit","margin","strategy","performance","market","brand","innovation","expansion","investment","digital","consumer","demand","volume","segment","business","opportunity","future","outlook","expect","target","achieved","increased","improved","declined","challenge","dividend","acquisition","capacity","sustainability","competitive"]
    boiler = ["committee","pursuant","regulation","disclosure","resolution","aforesaid","herein","thereof"]
    scored = []; total = len(sents)
    for idx, s in enumerate(sents):
        wc = len(s.split())
        if wc < 8 or wc > 80: continue
        sl = s.lower(); sc = 0.0; pos = idx/max(total,1)
        if pos < 0.2: sc += 2.0
        if pos > 0.9: sc += 1.0
        sc += sum(1 for w in imp if w in sl)/max(wc,1)*10
        if re.search(r'\d+\.?\d*', s): sc += 1.0
        if '%' in s or 'percent' in sl: sc += 2.0
        sc -= sum(1 for w in boiler if w in sl)*1.5
        scored.append((idx, sc, s))
    scored.sort(key=lambda x: -x[1]); top = sorted(scored[:n], key=lambda x: x[0])
    return " ".join(t[2] for t in top) if top else "Could not generate summary."

def extract_guidance(text):
    sents = sent_tokenize(clean_text(text)); guidance = {}
    boiler = {"committee","pursuant","regulation","disclosure","resolution","aforesaid","compliance"}
    for cat, pats in GUIDANCE_PATTERNS.items():
        found = []
        for s in sents:
            sl = s.lower()
            if any(b in sl for b in boiler): continue
            has_kw = any(kw in sl for kw in pats["keywords"])
            has_fw = any(fm in sl for fm in pats["forward_markers"])
            if has_kw and has_fw:
                kh = sum(1 for kw in pats["keywords"] if kw in sl)
                fh = sum(1 for fm in pats["forward_markers"] if fm in sl)
                hn = bool(re.search(r'\d+\.?\d*\s*%', s))
                found.append({"sentence":s.strip(),"score":kh+fh+(5 if hn else 0),"has_numbers":hn})
        found.sort(key=lambda x:-x["score"]); guidance[cat] = found[:3]
    return guidance

def calculate_pmi(text):
    sents = sent_tokenize(clean_text(text)); comps = {}
    for nm, d in PMI_COMPONENTS.items():
        ic=dc=0; ie=[]; de=[]
        for s in sents:
            sl = s.lower()
            ih = sum(1 for kw in d["improved"] if kw in sl)
            dh = sum(1 for kw in d["deteriorated"] if kw in sl)
            if ih > dh: ic+=1; (len(ie)<2 and ie.append(s.strip()[:250]))
            elif dh > ih: dc+=1; (len(de)<2 and de.append(s.strip()[:250]))
        rel = ic+dc
        si = (ic/rel*100) if rel else 50.0
        comps[nm] = {"weight":d["weight"],"improved":ic,"deteriorated":dc,"pct_improved":round(ic/rel*100 if rel else 0,1),"pct_deteriorated":round(dc/rel*100 if rel else 0,1),"sub_index":round(si,1),"improved_evidence":ie,"deteriorated_evidence":de}
    pmi = round(sum(c["sub_index"]*c["weight"] for c in comps.values()),1)
    if pmi>65: interp="Strong Expansion — robust growth signals across orders, production, and employment."
    elif pmi>55: interp="Moderate Expansion — positive signals outweigh negatives, steady growth."
    elif pmi>50: interp="Mild Expansion — marginal positive signals; growth present but not dominant."
    elif pmi>45: interp="Mild Contraction — slightly more negative signals; caution evident."
    elif pmi>35: interp="Moderate Contraction — significant negative signals in demand or production."
    else: interp="Sharp Contraction — strong negative language across most components."
    return {"pmi_score":pmi,"interpretation":interp,"components":comps}

def generate_outlook(rpt):
    avg = rpt["avg_sentiment"]
    tone = "strongly optimistic" if avg>0.5 else "moderately positive" if avg>0.2 else "cautiously optimistic" if avg>0.05 else "neutral" if avg>-0.05 else "cautious to negative"
    top2 = sorted(rpt["totals"].items(), key=lambda x:-x[1])[:2]
    grow,risk,brand = rpt["totals"].get("Growth & Strategy",0),rpt["totals"].get("Risk & Headwinds",0),rpt["totals"].get("Brand & Innovation",0)
    s = f"The overall management outlook is {tone} (avg. sentiment {avg:+.3f}). Classified as '{rpt['narrative']}'. Dominant themes: '{top2[0][0]}' ({top2[0][1]}) and '{top2[1][0]}' ({top2[1][1]}). "
    if grow>risk*1.5: s += f"Growth-to-risk ratio is {grow/max(risk,1):.1f}:1 — expansionary posture. "
    elif risk>grow: s += "Risk outweighs growth language — defensive posture. "
    if brand>50: s += "Significant brand & innovation emphasis — premiumisation in play. "
    return s

# ═══════════════════════════════════════════════════════════════════
# PDF SECTION EXTRACTOR
# ═══════════════════════════════════════════════════════════════════
def _find_pages(toc, names):
    found = {}
    for n in names:
        m = re.search(re.escape(n)+r"[\s.\-\u2013\u2014|]+(\d+)", toc, re.I)
        if m: found[n]=int(m.group(1)); continue
        m = re.search(r"(\d+)\s+"+re.escape(n), toc, re.I)
        if m: found[n]=int(m.group(1))
    return found

def extract_sections(pdf_path, target_sections=None):
    if target_sections is None: target_sections = DEFAULT_TARGET_SECTIONS
    with pdfplumber.open(pdf_path) as pdf:
        tp = len(pdf.pages)
        toc = ""
        for p in range(1, min(6,tp)):
            pt = pdf.pages[p].extract_text() or ""
            if any(n[:6].lower() in pt.lower() for n in target_sections): toc += pt+"\n"
        pm = _find_pages(toc, target_sections)
        if not pm:
            toc = "\n".join((pdf.pages[p].extract_text() or "") for p in range(min(15,tp)))
            pm = _find_pages(toc, target_sections)
        if not pm:
            for n in target_sections:
                m = re.search(n.split()[0]+r"[^0-9]*?(\d{1,3})", toc, re.I)
                if m: pm[n] = int(m.group(1))
        if not pm: return {}, {}
        ss = sorted(pm.items(), key=lambda x:x[1]); secs={}; pgs={}
        for i,(n,sp) in enumerate(ss):
            ep = (ss[i+1][1]-1) if i+1<len(ss) else min(sp+20,tp)
            txt = ""
            for j in range(sp-1,ep):
                if j<tp:
                    pt=pdf.pages[j].extract_text()
                    if pt: txt+=pt+"\n"
            secs[n]=clean_text(txt); pgs[n]=(sp,ep)
    return secs, pgs

def extract_logo(pdf_path, out_dir):
    logo_path = os.path.join(out_dir, "company_logo.png")
    try:
        import subprocess
        tmp = os.path.join(out_dir, "_logo_tmp"); os.makedirs(tmp, exist_ok=True)
        subprocess.run(["pdfimages","-png","-f","1","-l","3",pdf_path,os.path.join(tmp,"img")], capture_output=True, timeout=30)
        cands = []
        for f in sorted(os.listdir(tmp)):
            fp = os.path.join(tmp, f)
            try:
                img = PILImage.open(fp); w,h = img.size
                if 80<w<1200 and 40<h<600: cands.append((w*h, fp))
            except: continue
        if cands:
            cands.sort(key=lambda x:-x[0])
            PILImage.open(cands[0][1]).save(logo_path); return logo_path
    except: pass
    return None

# ═══════════════════════════════════════════════════════════════════
# CHART GENERATORS
# ═══════════════════════════════════════════════════════════════════
def generate_dashboard_chart(report, out_dir):
    sections=report["sections"]; sn=list(sections.keys()); tn=list(THEMES.keys())
    short=[t.split("&")[0].strip() for t in tn]
    fig=plt.figure(figsize=(18,14))
    fig.suptitle(f"FMCG Annual Report Analysis  —  {report['label']}", fontsize=15, fontweight="bold", y=0.98)
    gs=gridspec.GridSpec(2,2,figure=fig,hspace=0.45,wspace=0.35)
    # Stacked bar
    ax1=fig.add_subplot(gs[0,0]); bottoms=[0]*len(sn)
    for t in tn:
        vals=[sections[s]["theme_counts"][t] for s in sn]
        ax1.bar(sn,vals,bottom=bottoms,color=THEME_COLORS[t],label=t.split("&")[0].strip())
        bottoms=[b+v for b,v in zip(bottoms,vals)]
    ax1.set_title("Theme Mentions by Section",fontweight="bold"); ax1.set_ylabel("Mentions")
    ax1.set_xticklabels(sn,rotation=30,ha="right",fontsize=7); ax1.legend(fontsize=6,loc="upper right")
    # Radar
    ax2=fig.add_subplot(gs[0,1],polar=True); totals=report["totals"]
    vr=[totals.get(t,0) for t in tn]+[totals.get(tn[0],0)]; N=len(tn)
    angles=[n/N*2*3.14159 for n in range(N)]+[0]
    ax2.plot(angles,vr,"o-",lw=2,color="#1976D2"); ax2.fill(angles,vr,alpha=0.2,color="#1976D2")
    ax2.set_xticks(angles[:-1]); ax2.set_xticklabels(short,fontsize=7)
    ax2.set_title("Aggregate Theme Profile",fontweight="bold",pad=20)
    # Sentiment
    ax3=fig.add_subplot(gs[1,0]); ss=[sections[s]["sentiment"] for s in sn]
    cs=["#4CAF50" if v>0.05 else "#F44336" if v<-0.05 else "#9E9E9E" for v in ss]
    bars=ax3.barh(sn,ss,color=cs); ax3.axvline(0,color="black",lw=0.8,ls="--")
    ax3.set_title("Sentiment by Section",fontweight="bold"); ax3.set_xlabel("VADER Score")
    for b,v in zip(bars,ss): ax3.text(v+0.005,b.get_y()+b.get_height()/2,f"{v:+.3f}",va="center",fontsize=7)
    # Pie
    ax4=fig.add_subplot(gs[1,1]); pv=[totals.get(t,0) for t in tn]
    ax4.pie(pv,labels=short,colors=[THEME_COLORS[t] for t in tn],autopct="%1.0f%%",startangle=90,textprops={"fontsize":7})
    ax4.set_title("Narrative Composition",fontweight="bold")
    path=os.path.join(out_dir,"dashboard.png"); plt.savefig(path,dpi=150,bbox_inches="tight"); plt.close(); return path

def generate_pmi_chart(pmi_data, out_dir):
    fig,axes=plt.subplots(1,2,figsize=(14,5.5))
    fig.suptitle("Text-Derived Purchase Manager Index (PMI)",fontsize=14,fontweight="bold")
    ax=axes[0]; comps=pmi_data["components"]; names=list(comps.keys())
    short=[n.split("(")[0].strip() for n in names]
    si=[comps[n]["sub_index"] for n in names]; wts=[comps[n]["weight"]*100 for n in names]
    cs=["#4CAF50" if v>55 else "#F44336" if v<45 else "#FF9800" for v in si]
    bars=ax.barh(short,si,color=cs,height=0.55,edgecolor="white",linewidth=0.5)
    ax.axvline(50,color="#333",lw=1.5,ls="--",label="Neutral (50)"); ax.set_xlim(0,105)
    ax.set_title("Sub-Index by Component",fontweight="bold",fontsize=11); ax.set_xlabel("Sub-Index (>50 = Expansion)")
    for b,v,w in zip(bars,si,wts): ax.text(v+1.5,b.get_y()+b.get_height()/2,f"{v:.0f}  (wt {w:.0f}%)",va="center",fontsize=9)
    ax.legend(fontsize=8)
    ax2=axes[1]; ax2.set_xlim(0,100); ax2.set_ylim(0,100); pmi=pmi_data["pmi_score"]
    col="#4CAF50" if pmi>55 else "#F44336" if pmi<45 else "#FF9800"
    ax2.add_patch(FancyBboxPatch((15,25),70,55,boxstyle="round,pad=5",facecolor=col,alpha=0.12,edgecolor=col,lw=2))
    ax2.text(50,60,f"{pmi:.1f}",fontsize=64,fontweight="bold",ha="center",va="center",color=col)
    ax2.text(50,38,"TEXT-DERIVED PMI",fontsize=13,ha="center",color="#333")
    ax2.text(50,28,"EXPANSION" if pmi>50 else "CONTRACTION",fontsize=12,ha="center",fontweight="bold",color=col)
    ax2.axis("off")
    plt.tight_layout(rect=[0,0,1,0.92]); path=os.path.join(out_dir,"pmi_chart.png")
    plt.savefig(path,dpi=150,bbox_inches="tight"); plt.close(); return path

def generate_wordclouds(report, out_dir):
    sections=report["sections"]; n=len(sections)
    fig,axes=plt.subplots(1,n,figsize=(7*n,4))
    if n==1: axes=[axes]
    for ax,(nm,d) in zip(axes,sections.items()):
        wc=WordCloud(width=700,height=350,background_color="white",colormap="Blues",stopwords=STOP_WORDS,collocations=True).generate(d["text"])
        ax.imshow(wc,interpolation="bilinear"); ax.axis("off")
        ax.set_title(f"{nm}\n({d['dominant_theme']})",fontsize=8,fontweight="bold",color=THEME_COLORS.get(d["dominant_theme"],"#333"))
    fig.suptitle(f"Word Clouds  —  {report['label']}",fontsize=12,fontweight="bold"); plt.tight_layout()
    path=os.path.join(out_dir,"wordclouds.png"); plt.savefig(path,dpi=130,bbox_inches="tight"); plt.close(); return path

# ═══════════════════════════════════════════════════════════════════
# FULL ANALYSIS PIPELINE
# ═══════════════════════════════════════════════════════════════════
def analyse_report(label, pdf_path):
    print(f"\n{'='*60}\n  ANALYSING: {label}\n{'='*60}")
    sections, pages = extract_sections(pdf_path)
    if not sections: print("  ⚠  No sections found."); return {}
    kw_map = extract_tfidf_keywords(sections); all_text = " ".join(sections.values()); results = {}
    for sn, text in sections.items():
        sc = get_sentiment(text); tc = {t:count_theme(text,w) for t,w in THEMES.items()}; dom = dominant_theme(tc)
        vol,rm,brand = tc["Volume & Distribution"],tc["Raw Material & Commodity"],tc["Brand & Innovation"]
        marg,grow,risk = tc["Margin & Profitability"],tc["Growth & Strategy"],tc["Risk & Headwinds"]
        if risk>grow and risk>brand: ins="Risk and headwind language leads — management is cautious."
        elif brand>rm and marg>rm: ins="Brand investment + margin language suggests premiumisation focus."
        elif rm>brand and rm>grow: ins="Raw material/commodity mentions dominate — cost pressure is front of mind."
        elif grow>risk*2: ins="Strongly growth-oriented language with limited risk acknowledgement."
        elif vol>marg: ins="Volume and distribution emphasis — management prioritising top-line reach."
        else: ins="Balanced narrative across growth, margin, and risk themes."
        ts = {t:extract_theme_sentences(text,w) for t,w in THEMES.items() if tc[t]>0}
        results[sn] = {"text":text,"sentiment":sc,"tone":sentiment_label(sc),"theme_counts":tc,"dominant_theme":dom,"insight":ins,"keywords":kw_map.get(sn,[]),"theme_sentences":ts,"summary":extract_summary(text,4),"pages":pages.get(sn,(0,0))}
        print(f"  ✓ {sn}  sent={sc:+.3f}  dom={dom}")
    totals = defaultdict(int)
    for r in results.values():
        for t,c in r["theme_counts"].items(): totals[t]+=c
    avg = sum(r["sentiment"] for r in results.values())/len(results)
    narr,expl = narrative_label(totals,avg); guid = extract_guidance(all_text); pmi = calculate_pmi(all_text)
    rpt = {"label":label,"sections":results,"totals":dict(totals),"avg_sentiment":avg,"narrative":narr,"explanation":expl,"guidance":guid,"pmi":pmi,"section_pages":pages}
    rpt["outlook"] = generate_outlook(rpt)
    print(f"\n  Narrative : {narr}\n  PMI       : {pmi['pmi_score']}  ({pmi['interpretation'][:60]})")
    return rpt

# ═══════════════════════════════════════════════════════════════════
# PDF DASHBOARD BUILDER
# ═══════════════════════════════════════════════════════════════════
def _sty():
    base=getSampleStyleSheet(); nv=colors.HexColor("#1a237e")
    return {
        "title":ParagraphStyle("T",parent=base["Title"],fontSize=20,textColor=nv,spaceAfter=4),
        "subtitle":ParagraphStyle("ST",parent=base["Normal"],fontSize=11,textColor=colors.HexColor("#455a64"),spaceAfter=10),
        "h1":ParagraphStyle("H1",parent=base["Heading1"],fontSize=15,textColor=nv,spaceBefore=14,spaceAfter=6),
        "h2":ParagraphStyle("H2",parent=base["Heading2"],fontSize=12,textColor=colors.HexColor("#283593"),spaceBefore=10,spaceAfter=4),
        "body":ParagraphStyle("B",parent=base["Normal"],fontSize=9,leading=13,alignment=TA_JUSTIFY,spaceAfter=5),
        "small":ParagraphStyle("S",parent=base["Normal"],fontSize=8,leading=10,textColor=colors.HexColor("#616161")),
        "metric":ParagraphStyle("M",parent=base["Normal"],fontSize=9,textColor=colors.HexColor("#1b5e20"),spaceAfter=3),
        "footer":ParagraphStyle("F",parent=base["Normal"],fontSize=7,textColor=colors.HexColor("#9e9e9e"),alignment=TA_CENTER),
    }
def _hr(): return HRFlowable(width="100%",thickness=1,color=colors.HexColor("#bdbdbd"))
def _ts(hdr="#1a237e"):
    return TableStyle([("BACKGROUND",(0,0),(-1,0),colors.HexColor(hdr)),("TEXTCOLOR",(0,0),(-1,0),colors.white),("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,-1),8),("GRID",(0,0),(-1,-1),0.4,colors.HexColor("#bdbdbd")),("TOPPADDING",(0,0),(-1,-1),3),("BOTTOMPADDING",(0,0),(-1,-1),3),("LEFTPADDING",(0,0),(-1,-1),5),("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white,colors.HexColor("#f5f5f5")])])

def build_pdf(report, logo_path, charts, output_path):
    s=_sty(); W=A4[0]-40*mm; pmi=report["pmi"]
    doc=SimpleDocTemplate(output_path,pagesize=A4,leftMargin=20*mm,rightMargin=20*mm,topMargin=15*mm,bottomMargin=15*mm)
    story=[]
    # === PAGE 1: COVER ===
    if logo_path and os.path.exists(logo_path): story.append(RLImage(logo_path,width=140,height=60)); story.append(Spacer(1,6))
    story.append(HRFlowable(width="100%",thickness=2,color=colors.HexColor("#1a237e"))); story.append(Spacer(1,6))
    story.append(Paragraph("FMCG Annual Report Decoder — Dashboard",s["title"]))
    story.append(Paragraph(f"Report: {report['label']}  |  Generated: {datetime.now().strftime('%d %B %Y')}",s["subtitle"]))
    story.append(_hr()); story.append(Spacer(1,10))
    story.append(Paragraph("NARRATIVE CLASSIFICATION",s["h1"]))
    nd=[["Narrative Type",report["narrative"]],["Avg Sentiment",f"{report['avg_sentiment']:+.3f}  ({sentiment_label(report['avg_sentiment'])})"],["Text-Derived PMI",f"{pmi['pmi_score']:.1f}  —  {'Expansion' if pmi['pmi_score']>50 else 'Contraction'}"]]
    t=Table(nd,colWidths=[120,W-130]); t.setStyle(TableStyle([("BACKGROUND",(0,0),(0,-1),colors.HexColor("#e8eaf6")),("FONTNAME",(0,0),(0,-1),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,-1),9),("VALIGN",(0,0),(-1,-1),"TOP"),("GRID",(0,0),(-1,-1),0.4,colors.HexColor("#bdbdbd")),("TOPPADDING",(0,0),(-1,-1),5),("BOTTOMPADDING",(0,0),(-1,-1),5),("LEFTPADDING",(0,0),(-1,-1),6)]))
    story.append(t); story.append(Spacer(1,8))
    story.append(Paragraph("MANAGEMENT OUTLOOK",s["h2"])); story.append(Paragraph(report.get("outlook","N/A"),s["body"])); story.append(Spacer(1,6))
    story.append(Paragraph("THEME DISTRIBUTION",s["h2"]))
    tm=max(sum(report["totals"].values()),1); td=[["Theme","Mentions","Share"]]
    for tn in THEMES: c=report["totals"].get(tn,0); td.append([tn,str(c),f"{c/tm*100:.0f}%"])
    tt=Table(td,colWidths=[190,70,70]); tt.setStyle(_ts()); story.append(tt)

    # === PAGE 2: THEME INSIGHTS ===
    story.append(PageBreak()); story.append(Paragraph("THEME INSIGHTS BY SECTION",s["h1"])); story.append(_hr()); story.append(Spacer(1,6))
    for sn,d in report["sections"].items():
        story.append(Paragraph(f"{sn}  (Pages {d['pages'][0]}–{d['pages'][1]})",s["h2"]))
        story.append(Paragraph(f"<b>Sentiment:</b> {d['sentiment']:+.3f} ({d['tone']})  |  <b>Dominant:</b> {d['dominant_theme']}",s["body"]))
        story.append(Paragraph(f"<b>Insight:</b> {d['insight']}",s["body"]))
        story.append(Paragraph(f"<b>Keywords:</b> {', '.join(d['keywords'][:8])}",s["small"])); story.append(Spacer(1,4))

    # === PAGE 3: DASHBOARD CHART ===
    if charts.get("dashboard") and os.path.exists(charts["dashboard"]):
        story.append(PageBreak()); story.append(Paragraph("VISUAL DASHBOARD",s["h1"]))
        story.append(RLImage(charts["dashboard"],width=W,height=W*0.78))

    # === PAGE 4: SECTION SUMMARIES ===
    story.append(PageBreak()); story.append(Paragraph("SECTION SUMMARIES",s["h1"])); story.append(_hr()); story.append(Spacer(1,6))
    for sn,d in report["sections"].items():
        story.append(Paragraph(sn,s["h2"])); sm=d.get("summary","")
        if len(sm)>1200: sm=sm[:1200]+"..."
        story.append(Paragraph(sm,s["body"])); story.append(Spacer(1,4))

    # === PAGE 5: MANAGEMENT GUIDANCE ===
    story.append(PageBreak()); story.append(Paragraph("MANAGEMENT GUIDANCE (Forward-Looking Statements)",s["h1"])); story.append(_hr()); story.append(Spacer(1,6))
    for cat,items in report.get("guidance",{}).items():
        story.append(Paragraph(cat,s["h2"]))
        if not items: story.append(Paragraph("No specific forward-looking statements identified.",s["small"]))
        else:
            for it in items:
                pfx="<font color='#1b5e20'><b>★</b></font> " if it.get("has_numbers") else "• "
                st=it["sentence"]; st=st[:400]+"..." if len(st)>400 else st
                story.append(Paragraph(f"{pfx}{st}",s["metric"] if it.get("has_numbers") else s["body"]))
        story.append(Spacer(1,4))
    story.append(Paragraph("<i>★ = Contains quantitative target or metric</i>",s["small"]))

    # === PAGE 6: PMI SCORE + CHART ===
    story.append(PageBreak()); story.append(Paragraph("TEXT-DERIVED PURCHASE MANAGER INDEX (PMI)",s["h1"])); story.append(_hr()); story.append(Spacer(1,6))
    story.append(Paragraph("The PMI is calculated by scanning every sentence for language signals across 5 manufacturing components. Each sentence is classified as 'Improved' or 'Deteriorated'. Sub-Index = (% Improved signals). Final PMI = weighted average of sub-indices. PMI above 50 signals expansion; below 50 signals contraction.",s["body"]))
    story.append(Spacer(1,6))
    # PMI table
    pt_data=[["Component","Weight","Improved","Deteriorated","Sub-Index","Signal"]]
    for cn,cd in pmi["components"].items():
        si=cd["sub_index"]; sig="Expansion" if si>55 else "Contraction" if si<45 else "Neutral"
        pt_data.append([cn.split("(")[0].strip(),f"{cd['weight']*100:.0f}%",str(cd["improved"]),str(cd["deteriorated"]),f"{si:.1f}",sig])
    pt_data.append(["WEIGHTED PMI","100%","","",f"{pmi['pmi_score']:.1f}","EXPANSION" if pmi["pmi_score"]>50 else "CONTRACTION"])
    pt=Table(pt_data,colWidths=[110,50,60,70,60,75]); sty_pt=_ts()
    sty_pt.add("BACKGROUND",(0,-1),(-1,-1),colors.HexColor("#e8eaf6")); sty_pt.add("FONTNAME",(0,-1),(-1,-1),"Helvetica-Bold")
    pt.setStyle(sty_pt); story.append(pt); story.append(Spacer(1,6))
    story.append(Paragraph(f"<b>Interpretation:</b> {pmi['interpretation']}",s["body"]))

    # === PMI CHART EMBEDDED HERE ===
    if charts.get("pmi") and os.path.exists(charts["pmi"]):
        story.append(Spacer(1,10))
        story.append(Paragraph("PMI COMPONENT VISUALIZATION",s["h2"]))
        story.append(RLImage(charts["pmi"],width=W,height=W*0.40))

    # PMI evidence
    story.append(Spacer(1,10)); story.append(Paragraph("Supporting Evidence by Component",s["h2"]))
    for cn,cd in pmi["components"].items():
        story.append(Paragraph(f"<b>{cn}</b>",s["body"]))
        for ev in cd["improved_evidence"][:1]:
            evt=ev[:300]+"..." if len(ev)>300 else ev
            story.append(Paragraph(f"  <font color='#2e7d32'>(+)</font> {evt}",s["small"]))
        for ev in cd["deteriorated_evidence"][:1]:
            evt=ev[:300]+"..." if len(ev)>300 else ev
            story.append(Paragraph(f"  <font color='#c62828'>(-)</font> {evt}",s["small"]))

    # === PAGE 7: WORD CLOUDS ===
    if charts.get("wordclouds") and os.path.exists(charts["wordclouds"]):
        story.append(PageBreak()); story.append(Paragraph("WORD CLOUDS BY SECTION",s["h1"]))
        story.append(RLImage(charts["wordclouds"],width=W,height=W*0.32))

    # === FINAL PAGE: REFERENCES ===
    story.append(PageBreak()); story.append(Paragraph("REFERENCES &amp; SOURCE MAPPING",s["h1"])); story.append(_hr()); story.append(Spacer(1,6))
    story.append(Paragraph("All data was extracted from the following sections of the annual report PDF. Page numbers refer to the PDF page count (1-indexed).",s["body"])); story.append(Spacer(1,6))
    rd=[["Section","PDF Pages","Words","Analyses Run"]]
    for sn,d in report["sections"].items():
        rd.append([sn,f"pp. {d['pages'][0]}–{d['pages'][1]}",str(len(d["text"].split())),"Sentiment, Themes, TF-IDF, Summary, Guidance, PMI"])
    rt=Table(rd,colWidths=[120,70,55,W-255]); rt.setStyle(_ts()); story.append(rt)
    story.append(Spacer(1,12)); story.append(Paragraph("Methodology",s["h2"]))
    story.append(Paragraph("<b>Sentiment:</b> VADER — compound score, chunked at 500 words and averaged. <b>Themes:</b> Keyword frequency against 6 FMCG-specific dictionaries. <b>Keywords:</b> TF-IDF with bigram support (cross-section distinctiveness). <b>Summary:</b> Extractive — scores sentences on position, keyword density, and quantitative content. <b>Guidance:</b> Forward-looking statements via keyword + temporal marker co-occurrence. <b>PMI:</b> Text-derived index — improved/deteriorated signal classification across 5 manufacturing components, weighted per standard PMI formula.",s["body"]))
    story.append(Spacer(1,20)); story.append(_hr())
    story.append(Paragraph(f"Generated by FMCG Annual Report Decoder v3.1  |  {datetime.now().strftime('%d %B %Y, %H:%M')}",s["footer"]))
    doc.build(story); print(f"  ✓ PDF Dashboard → {output_path}")

# ═══════════════════════════════════════════════════════════════════
# EXCEL EXPORT
# ═══════════════════════════════════════════════════════════════════
def build_excel(report, output_path):
    label=report["label"]
    with pd.ExcelWriter(output_path,engine="openpyxl") as w:
        rows=[]
        for sn,d in report["sections"].items():
            row={"Section":sn,"Pages":f"{d['pages'][0]}-{d['pages'][1]}","Sentiment":round(d["sentiment"],4),"Tone":d["tone"],"Dominant Theme":d["dominant_theme"],"Insight":d["insight"],"Summary":d["summary"][:500],"Keywords":", ".join(d["keywords"][:10])}
            row.update({f"[{t}]":d["theme_counts"][t] for t in THEMES}); rows.append(row)
        pd.DataFrame(rows).to_excel(w,sheet_name=f"{label}_Summary"[:31],index=False)
        sr=[]
        for sn,d in report["sections"].items():
            for th,ss in d["theme_sentences"].items():
                for i,st in enumerate(ss,1): sr.append({"Section":sn,"Theme":th,"Rank":i,"Sentence":st})
        if sr: pd.DataFrame(sr).to_excel(w,sheet_name=f"{label}_KeySentences"[:31],index=False)
        gr=[]
        for cat,items in report.get("guidance",{}).items():
            for i,it in enumerate(items,1): gr.append({"Category":cat,"Rank":i,"Quantitative":it.get("has_numbers",False),"Statement":it["sentence"]})
        if gr: pd.DataFrame(gr).to_excel(w,sheet_name=f"{label}_Guidance"[:31],index=False)
        pr=[]
        for cn,cd in report.get("pmi",{}).get("components",{}).items():
            pr.append({"Component":cn,"Weight":f"{cd['weight']*100:.0f}%","Improved":cd["improved"],"Deteriorated":cd["deteriorated"],"Sub-Index":cd["sub_index"],"Evidence+":"; ".join(cd["improved_evidence"])[:500],"Evidence-":"; ".join(cd["deteriorated_evidence"])[:500]})
        pr.append({"Component":"WEIGHTED PMI","Sub-Index":report["pmi"]["pmi_score"],"Evidence+":report["pmi"]["interpretation"]})
        pd.DataFrame(pr).to_excel(w,sheet_name=f"{label}_PMI"[:31],index=False)
        pd.DataFrame([{"Report":label,"Narrative":report["narrative"],"Explanation":report["explanation"],"Outlook":report.get("outlook",""),"Avg Sentiment":round(report["avg_sentiment"],4),"PMI":report["pmi"]["pmi_score"],**{f"[{t}]":report["totals"].get(t,0) for t in THEMES}}]).to_excel(w,sheet_name="Narrative_Summary",index=False)
    print(f"  ✓ Excel → {output_path}")

# ═══════════════════════════════════════════════════════════════════
# PUBLIC API — call this from your web app
# ═══════════════════════════════════════════════════════════════════
def process_report(pdf_path, output_dir=None, label=None):
    """
    End-to-end pipeline.  PDF in → Dashboard PDF + Excel out.

    Parameters:
        pdf_path   : str — path to the annual report PDF
        output_dir : str — directory for outputs (default: same dir as PDF)
        label      : str — report label (default: filename stem)

    Returns:
        dict with keys: pdf, excel, charts, report
    """
    if output_dir is None: output_dir = os.path.dirname(pdf_path) or "."
    os.makedirs(output_dir, exist_ok=True)
    if label is None:
        label = re.sub(r'[^a-zA-Z0-9_\-]','_',os.path.splitext(os.path.basename(pdf_path))[0])[:30]
    print(f"\n{'='*60}\n  FMCG ANNUAL REPORT DECODER v3.1\n{'='*60}")
    report = analyse_report(label, pdf_path)
    if not report: return {"error":"No sections could be extracted."}
    logo = None
    try:
        logo = extract_logo(pdf_path, output_dir)
    except Exception as e:
        print(f"  ⚠ Logo extraction skipped: {e}")
    charts = {}
    try:
        charts["dashboard"] = generate_dashboard_chart(report, output_dir)
    except Exception as e:
        print(f"  ⚠ Dashboard chart failed: {e}")
    try:
        charts["pmi"] = generate_pmi_chart(report["pmi"], output_dir)
    except Exception as e:
        print(f"  ⚠ PMI chart failed: {e}")
    try:
        charts["wordclouds"] = generate_wordclouds(report, output_dir)
    except Exception as e:
        print(f"  ⚠ Wordcloud chart failed: {e}")
    pdf_out = os.path.join(output_dir, f"FMCG_Dashboard_{label}.pdf")
    build_pdf(report, logo, charts, pdf_out)
    xlsx_out = os.path.join(output_dir, f"FMCG_Analysis_{label}.xlsx")
    build_excel(report, xlsx_out)
    print(f"\n{'='*60}\n  COMPLETE  ✓\n  PDF  : {pdf_out}\n  Excel: {xlsx_out}\n{'='*60}\n")
    return {"pdf":pdf_out,"excel":xlsx_out,"charts":charts,"report":report}

# ═══════════════════════════════════════════════════════════════════
# CLI ENTRY POINT
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    PDF_PATH   = "/home/claude/itc_fy25.pdf"
    OUTPUT_DIR = "/home/claude/output"
    LABEL      = "ITC_FY25"
    result = process_report(PDF_PATH, OUTPUT_DIR, LABEL)
    if "error" in result: print(f"\n  ⚠  {result['error']}")
