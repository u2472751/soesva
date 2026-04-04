#!/usr/bin/env python3
"""SoE SciVal Publication Analyser v5"""
import io,re,math,warnings,textwrap,smtplib
from collections import Counter
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from pathlib import Path
import numpy as np,pandas as pd
import matplotlib;matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import matplotlib.ticker as mticker
from matplotlib.lines import Line2D
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
warnings.filterwarnings("ignore",category=UserWarning,module="openpyxl")

ACADIA={"yellow":"#FED789","teal":"#023743","olive":"#72874E","steel":"#476F84","sky":"#A4BED5","plum":"#453947"}
ACADIA_LIST=list(ACADIA.values())
CC={"B&B":"#FED789","BES":"#023743","EPC":"#72874E","F&T":"#476F84","MDM":"#A4BED5","PM":"#453947"}
JC={"3":"#023743","2":"#476F84","1":"#A4BED5","0":"#FED789","unidentified":"#d5d5d5","":"#eee"}
JO=["3","2","1","0","unidentified"]
JL={"3":"JUFO 3 (Highest)","2":"JUFO 2 (Leading)","1":"JUFO 1 (Basic)","0":"JUFO 0","unidentified":"Unidentified"}
FC="Field-Weighted Citation Impact"
PF="Roboto,DejaVu Sans,sans-serif"
CO=["B&B","BES","EPC","F&T","MDM","PM"]
# OA category mapping
def merge_oa(val):
    """Categorise OA status with hybrid groupings."""
    if pd.isna(val) or str(val).strip()=="":return "Closed"
    v=str(val).strip()
    vl=v.lower()
    if vl=="gold":return "Gold"
    if vl=="green":return "Green"
    if vl=="bronze":return "Bronze"
    if "gold" in vl and "green" in vl:return "Hybrid Green/Gold"
    if "hybrid" in vl and "gold" in vl:return "Hybrid Green/Gold"
    if "bronze" in vl and "green" in vl:return "Hybrid Green/Bronze"
    if "hybrid" in vl and "green" in vl:return "Hybrid Green/Gold"
    return v

def setup_fonts():
    rp=list(Path("/usr/share/fonts").rglob("Roboto*.ttf"))
    if not rp:
        try:
            import subprocess;subprocess.run(["apt-get","install","-y","-qq","fonts-roboto"],capture_output=True,timeout=30)
            rp=list(Path("/usr/share/fonts").rglob("Roboto*.ttf"))
        except:pass
    if rp:
        for p in rp:fm.fontManager.addfont(str(p))
        plt.rcParams["font.family"]="Roboto"
    else:plt.rcParams["font.family"]="DejaVu Sans"
    plt.rcParams.update({"axes.labelsize":11,"axes.titlesize":13,"xtick.labelsize":10,"ytick.labelsize":10,"legend.fontsize":9})
setup_fonts()

# ═══ DATA PROCESSING ═══
def parse_scival(f):
    raw=pd.read_excel(f,header=None);hr=None
    for i in range(min(30,len(raw))):
        for c in range(raw.shape[1]):
            if pd.notna(raw.iloc[i,c]) and str(raw.iloc[i,c]).strip()=="Title":hr=i;break
        if hr is not None:break
    if hr is None:st.error("No 'Title' column found");return pd.DataFrame()
    nc=sum(1 for c in range(raw.shape[1]) if pd.notna(raw.iloc[hr,c]))
    hdrs=[str(raw.iloc[hr,c]).strip() for c in range(nc)]
    df=raw.iloc[hr+1:,:nc].copy();df.columns=hdrs
    return df[df["Title"].apply(lambda x:pd.notna(x) and not str(x).startswith("\u00a9"))].reset_index(drop=True)

def match_researchers(pubs,researchers,sov,mprof,anotes):
    sl={}
    for _,r in researchers.iterrows():
        sid=str(r["Scopus ID"]).strip();cl_val=r["Research Cluster"]
        sl[sid]={"name":f"{r['First Name']} {r['Last Name']}","cluster":str(cl_val).strip() if pd.notna(cl_val) else ""}
    for sid,info in sov.items():sl[sid]=info
    sm={}
    for _,r in researchers.iterrows():
        ln=r["Last Name"].strip().lower()
        if ln not in sm:sm[ln]=[]
        sm[ln].append(f"{r['First Name']} {r['Last Name']}")
    wn,wi,wc,wt=[],[],[],[]
    for _,row in pubs.iterrows():
        ids_s=str(row.get("Scopus Author Ids",""));auth_s=str(row.get("Authors",""));inst_s=str(row.get("Institutions",""))
        if ids_s=="nan" or not ids_s.strip():wn.append("");wi.append("");wc.append("");wt.append("No Scopus Author IDs");continue
        ids=[x.strip() for x in ids_s.split("|")];mn,ms,mc=[],[],[]
        for sid in ids:
            if sid in sl:mn.append(sl[sid]["name"]);ms.append(sid);mc.append(sl[sid]["cluster"])
        if mn:
            wn.append("; ".join(mn));wi.append("; ".join(ms))
            wc.append("; ".join(sorted(c for c in set(mc) if isinstance(c,str) and c.strip())))
            hw="warwick" in inst_s.lower();np2=[]
            for name in mn:
                if name in anotes and not hw:np2.append(f"{name}: {anotes[name]}")
            if not hw and not np2:np2.append("Warwick not in Institutions - verify affiliation")
            wt.append("; ".join(np2))
        else:
            wn.append("");wi.append("");wc.append("");mh=False
            for mid,exp in mprof.items():
                if mid in ids:wt.append(f"Merged profile ({mid}). {exp}");mh=True;break
            if not mh:
                authors=[a.strip() for a in auth_s.split("|")] if auth_s!="nan" else []
                coll=[f"{a} shares surname with {sm[a.split(',')[0].strip().lower()][0]}" for a in authors if a.split(',')[0].strip().lower() in sm]
                wt.append("No match: "+"; ".join(coll[:2]) if coll else "")
    pubs=pubs.copy();pubs.insert(0,"Warwick Researcher",wn);pubs.insert(1,"Warwick Scopus ID",wi)
    pubs.insert(2,"Research Cluster",wc);pubs.insert(3,"Match Notes",wt)
    for col in ["Authors","Scopus Author Ids"]:
        if col in pubs.columns:pubs=pubs.drop(columns=[col])
    if "Scopus Source title" in pubs.columns:pubs=pubs.rename(columns={"Scopus Source title":"Journal"})
    return pubs

def match_jufo(pubs,jdb):
    def norm(s):
        s=s.strip().lower();s=re.sub(r"\s*\([^)]*\)\s*$","",s);s=re.sub(r"^the\s+","",s)
        s=re.sub(r"[:\-\u2013\u2014/,&]"," ",s);return re.sub(r"\s+"," ",s).strip()
    jdb=jdb.copy();jdb["LC"]=jdb["Level"].apply(lambda x:"0" if str(x).strip()=="Other identified publication channels" else str(x).strip())
    je={str(r["Name"]).strip().lower():r for _,r in jdb.iterrows()}
    jo={}
    for _,r in jdb.iterrows():
        ot=r.get("Other_Title")
        if pd.notna(ot) and str(ot).strip():
            for t in str(ot).split("|"):jo[t.strip().lower()]=r
    jn={norm(str(r["Name"])):r for _,r in jdb.iterrows()}
    for _,r in jdb.iterrows():
        ot=r.get("Other_Title")
        if pd.notna(ot) and str(ot).strip():
            for t in str(ot).split("|"):jn[norm(t)]=r
    ck=["conference","workshop","symposium","proceedings","congress","forum","colloquium","meeting","encyclopedia","handbook","guide to","lecture notes","edition"]
    if "Journal" not in pubs.columns:pubs["JUFO Level"]="";return pubs
    jm={}
    for j in pubs["Journal"].dropna().unique():
        if not j.strip():continue
        jl_=j.strip().lower();jnr=norm(j)
        if jl_ in je:jm[j]=je[jl_]["LC"]
        elif jl_ in jo:jm[j]=jo[jl_]["LC"]
        elif jnr in jn:jm[j]=jn[jnr]["LC"]
        elif any(kw in jl_ for kw in ck):jm[j]="unidentified"
        else:jm[j]="unidentified"
    pubs=pubs.copy();pubs["JUFO Level"]=pubs["Journal"].map(jm).fillna("");return pubs

# ═══ HELPERS ═══
def fig_buf(fig,dpi=300):
    b=io.BytesIO();fig.savefig(b,format="png",dpi=dpi,bbox_inches="tight",transparent=True,facecolor="none",edgecolor="none");b.seek(0);return b
def xl_buf(df,s="Sheet1"):
    b=io.BytesIO()
    with pd.ExcelWriter(b,engine="openpyxl") as w:df.to_excel(w,index=False,sheet_name=s)
    b.seek(0);return b
def expl_cl(df):
    rows=[]
    for _,row in df.iterrows():
        for cl in [c.strip() for c in str(row.get("Research Cluster","")).split(";") if c.strip()]:
            r=row.copy();r["_Cl"]=cl;rows.append(r)
    return pd.DataFrame(rows) if rows else pd.DataFrame()
def expl_res(df):
    rows=[]
    for _,row in df.iterrows():
        for n in [x.strip() for x in str(row.get("Warwick Researcher","")).split(";") if x.strip()]:
            r=row.copy();r["_Res"]=n;rows.append(r)
    return pd.DataFrame(rows) if rows else pd.DataFrame()
def oa_pct(s):
    t=len(s);return round(100*s.apply(lambda x:pd.notna(x) and str(x).strip()!="").sum()/t,1) if t else 0.0
def send_email(srv,port,frm,pwd,to,subj,body,att):
    try:
        msg=MIMEMultipart();msg["From"]=frm;msg["To"]=", ".join(to);msg["Subject"]=subj
        msg.attach(MIMEText(body,"plain"))
        for name,data in att:
            p=MIMEBase("application","octet-stream");p.set_payload(data)
            encoders.encode_base64(p);p.add_header("Content-Disposition",f"attachment; filename={name}");msg.attach(p)
        with smtplib.SMTP(srv,port) as s:s.starttls();s.login(frm,pwd);s.sendmail(frm,to,msg.as_string())
        return True,"Email sent!"
    except Exception as e:return False,f"Failed: {e}"

def apply_bar_filters(edf,jufo_min,fwci_above1,yr_range):
    """Apply JUFO slider, FWCI toggle, year range filters."""
    if "JUFO Level" in edf.columns and jufo_min is not None and jufo_min>0:
        edf=edf[edf["JUFO Level"].apply(lambda x:str(x).isdigit() and int(x)>=jufo_min)]
    if fwci_above1 and FC in edf.columns:
        edf=edf.copy();edf[FC]=pd.to_numeric(edf[FC],errors="coerce");edf=edf[edf[FC]>1.0]
    if yr_range:
        edf=edf.copy();edf["Year"]=pd.to_numeric(edf["Year"],errors="coerce")
        edf=edf[(edf["Year"]>=yr_range[0])&(edf["Year"]<=yr_range[1])]
    return edf

# ═══ SUMMARY VIEW ═══
def researcher_summary(pubs,name,yr_range=None):
    rdf=expl_res(pubs[pubs["Warwick Researcher"]!=""])
    rdf=rdf[rdf["_Res"]==name].copy()
    if rdf.empty:return None
    rdf[FC]=pd.to_numeric(rdf.get(FC),errors="coerce");rdf["Year"]=pd.to_numeric(rdf["Year"],errors="coerce")
    if yr_range:rdf=rdf[(rdf["Year"]>=yr_range[0])&(rdf["Year"]<=yr_range[1])]
    if rdf.empty:return None
    cluster=rdf["Research Cluster"].iloc[0] if len(rdf)>0 else ""
    yearly=rdf.groupby("Year").agg(Count=(FC,"size"),Avg_FWCI=(FC,"mean")).sort_index()
    # Top 5 per year
    top5_by_year={}
    t5_cols=[c for c in ["Title","Journal","Year",FC,"JUFO Level"] if c in rdf.columns]
    for yr in sorted(rdf["Year"].dropna().unique()):
        yr_df=rdf[rdf["Year"]==yr].nlargest(5,FC)
        if not yr_df.empty:
            top5_by_year[int(yr)]=yr_df[t5_cols].copy()
    t5_all_cols=[c for c in ["Title","Journal","Year",FC,"JUFO Level","Open Access","DOI"] if c in rdf.columns]
    top5=rdf.nlargest(5,FC)[t5_all_cols].copy() if FC in rdf.columns else pd.DataFrame()
    return {"name":name,"cluster":cluster,"total":len(rdf),"avg_fwci":rdf[FC].mean(),"med_fwci":rdf[FC].median(),
        "oa_pct":oa_pct(rdf.get("Open Access",pd.Series())),"yearly":yearly,"top5":top5,"top5_by_year":top5_by_year}

def cluster_summary(pubs,cl,yr_range=None):
    edf=expl_cl(pubs[pubs["Warwick Researcher"]!=""])
    edf=edf[edf["_Cl"]==cl].copy()
    if edf.empty:return None
    edf[FC]=pd.to_numeric(edf.get(FC),errors="coerce");edf["Year"]=pd.to_numeric(edf["Year"],errors="coerce")
    if yr_range:edf=edf[(edf["Year"]>=yr_range[0])&(edf["Year"]<=yr_range[1])]
    if edf.empty:return None
    yearly=edf.groupby("Year").agg(Count=(FC,"size"),Avg_FWCI=(FC,"mean")).sort_index()
    res_names=sorted(set(n.strip() for ns in edf["Warwick Researcher"] for n in str(ns).split("; ") if n.strip()))
    top5_by_year={}
    t5_cols=[c for c in ["Title","Warwick Researcher","Journal","Year",FC,"JUFO Level"] if c in edf.columns]
    for yr in sorted(edf["Year"].dropna().unique()):
        yr_df=edf[edf["Year"]==yr].nlargest(5,FC)
        if not yr_df.empty:
            top5_by_year[int(yr)]=yr_df[t5_cols].copy()
    top5=edf.nlargest(5,FC)[t5_cols].copy() if FC in edf.columns else pd.DataFrame()
    return {"name":cl,"total":len(edf),"researchers":res_names,"avg_fwci":edf[FC].mean(),"med_fwci":edf[FC].median(),
        "oa_pct":oa_pct(edf.get("Open Access",pd.Series())),"yearly":yearly,"top5":top5,"top5_by_year":top5_by_year}

def render_entity_summary(s,etype="researcher",show_top5_by_year=False):
    if s is None:st.warning("No data.");return
    label=s["name"]+(f" ({s.get('cluster','')})" if etype=="researcher" and s.get("cluster") else "")
    st.markdown(f"##### {label}")
    mc=st.columns(4)
    mc[0].metric("Publications",s["total"])
    mc[1].metric("Avg FWCI",f"{s['avg_fwci']:.2f}" if pd.notna(s['avg_fwci']) else "–")
    mc[2].metric("Median FWCI",f"{s['med_fwci']:.2f}" if pd.notna(s['med_fwci']) else "–")
    mc[3].metric("% Open Access",f"{s['oa_pct']}%")
    if etype=="cluster" and s.get("researchers"):
        st.caption(f"Researchers: {', '.join(s['researchers'][:15])}{'...' if len(s['researchers'])>15 else ''}")
    if not s["yearly"].empty:
        st.markdown("**Performance by Year**")
        yt=s["yearly"].copy();yt.index=yt.index.astype(int);yt["Avg_FWCI"]=yt["Avg_FWCI"].round(2)
        st.dataframe(yt.rename(columns={"Count":"Pubs","Avg_FWCI":"Avg FWCI"}),use_container_width=True)
    if not s["top5"].empty:
        st.markdown("**Top 5 Publications (by FWCI)**")
        t5=s["top5"].copy()
        if FC in t5.columns:t5[FC]=t5[FC].round(2)
        st.dataframe(t5,use_container_width=True,hide_index=True)
    if show_top5_by_year and s.get("top5_by_year"):
        st.markdown("**Top 5 by Year**")
        for yr in sorted(s["top5_by_year"].keys()):
            st.markdown(f"*{yr}:*")
            t=s["top5_by_year"][yr].copy()
            if FC in t.columns:t[FC]=t[FC].round(2)
            st.dataframe(t,use_container_width=True,hide_index=True)

def plotly_compare(summaries,etype="researcher",by_year=False):
    if not summaries:return go.Figure()
    if by_year:
        fig=go.Figure()
        for s in summaries:
            if s["yearly"].empty:continue
            yt=s["yearly"].copy();yt.index=yt.index.astype(int)
            fig.add_trace(go.Scatter(x=yt.index,y=yt["Avg_FWCI"].round(2),mode="lines+markers",name=s["name"],
                marker=dict(size=8),line=dict(width=2)))
        fig.add_hline(y=1.0,line_dash="dash",line_color="red",line_width=1.5,opacity=0.6)
        fig.update_layout(title="Avg FWCI by Year",xaxis_title="Year",yaxis_title="FWCI",
            font=dict(family=PF),template="plotly_white",height=450)
        return fig
    names=[s["name"] for s in summaries];totals=[s["total"] for s in summaries]
    avgs=[s["avg_fwci"] for s in summaries];meds=[s["med_fwci"] for s in summaries]
    oa=[s["oa_pct"] for s in summaries]
    colours=[CC.get(n,ACADIA_LIST[i%len(ACADIA_LIST)]) for i,n in enumerate(names)]
    fig=make_subplots(rows=1,cols=4,subplot_titles=["Publications","Avg FWCI","Median FWCI","% OA"],horizontal_spacing=0.06)
    for ann in fig.layout.annotations:ann.update(y=-0.08,yanchor="top",font=dict(size=12,family=PF))
    fig.add_trace(go.Bar(x=names,y=totals,marker_color=colours,text=totals,textposition="auto",showlegend=False),row=1,col=1)
    fig.add_trace(go.Bar(x=names,y=avgs,marker_color=colours,text=[f"{v:.2f}" for v in avgs],textposition="auto",showlegend=False),row=1,col=2)
    fig.add_hline(y=1.0,line_dash="dash",line_color=ACADIA["plum"],line_width=1,opacity=0.5,row=1,col=2)
    fig.add_trace(go.Bar(x=names,y=meds,marker_color=colours,text=[f"{v:.2f}" for v in meds],textposition="auto",showlegend=False),row=1,col=3)
    fig.add_hline(y=1.0,line_dash="dash",line_color=ACADIA["plum"],line_width=1,opacity=0.5,row=1,col=3)
    fig.add_trace(go.Bar(x=names,y=oa,marker_color=colours,text=[f"{v}%" for v in oa],textposition="auto",showlegend=False),row=1,col=4)
    fig.update_layout(font=dict(family=PF),template="plotly_white",height=420,title=f"Comparison",margin=dict(b=80))
    return fig

# ═══ PLOTLY CHARTS ═══
def plotly_faceted_year(df,show_jufo=True,jufo_min=0,fwci_above1=False,yr_range=None,cluster_sel=None):
    edf=expl_cl(df[df["Warwick Researcher"]!=""])
    if edf.empty:return go.Figure()
    edf["Year"]=pd.to_numeric(edf["Year"],errors="coerce");edf=edf.dropna(subset=["Year"]);edf["Year"]=edf["Year"].astype(int)
    edf=apply_bar_filters(edf,jufo_min,fwci_above1,yr_range)
    clusters=cluster_sel if cluster_sel else [c for c in CO if c in edf["_Cl"].unique()]
    if not clusters:return go.Figure().update_layout(title="No data")
    edf=edf[edf["_Cl"].isin(clusters)]
    nc=min(3,len(clusters));nr=math.ceil(len(clusters)/nc)
    fig=make_subplots(rows=nr,cols=nc,subplot_titles=clusters,shared_yaxes=True,horizontal_spacing=0.06,vertical_spacing=0.15)
    for ann in fig.layout.annotations:ann.update(yanchor="top",y=ann.y-0.03,font=dict(size=13,family=PF))
    for idx,cl in enumerate(clusters):
        r2,c2=divmod(idx,nc);r2+=1;c2+=1
        sub=edf[edf["_Cl"]==cl]
        if show_jufo and "JUFO Level" in sub.columns:
            for jl_ in reversed(JO):
                jsub=sub[sub["JUFO Level"].astype(str)==jl_]
                if len(jsub)==0:continue
                ct=jsub.groupby("Year").size().reset_index(name="Count")
                fig.add_trace(go.Bar(x=ct["Year"].astype(str),y=ct["Count"],name=JL.get(jl_,jl_),
                    marker_color=JC.get(jl_,"#ccc"),legendgroup=jl_,showlegend=(idx==0),
                    hovertemplate=f"{cl} | {JL.get(jl_,jl_)}<br>Year: %{{x}}<br>Count: %{{y}}<extra></extra>"),row=r2,col=c2)
        else:
            ct=sub.groupby("Year").size().reset_index(name="Count")
            fig.add_trace(go.Bar(x=ct["Year"].astype(str),y=ct["Count"],marker_color=CC.get(cl,"#999"),showlegend=False,
                hovertemplate=f"{cl}<br>Year: %{{x}}<br>Count: %{{y}}<extra></extra>"),row=r2,col=c2)
    for idx in range(len(clusters),nr*nc):
        r2,c2=divmod(idx,nc);r2+=1;c2+=1
        fig.update_xaxes(visible=False,row=r2,col=c2);fig.update_yaxes(visible=False,row=r2,col=c2)
    fig.update_layout(barmode="stack" if show_jufo else "group",font=dict(family=PF),template="plotly_white",
        height=max(400,nr*340),legend=dict(orientation="h",yanchor="bottom",y=1.02,xanchor="right",x=1),
        title="Publications by Year"+ (" — JUFO" if show_jufo else ""))
    return fig

def plotly_cluster_summary(df,show_jufo=True,jufo_min=0,fwci_above1=False,yr_range=None):
    edf=expl_cl(df[df["Warwick Researcher"]!=""])
    if edf.empty:return go.Figure()
    edf[FC]=pd.to_numeric(edf.get(FC),errors="coerce")
    edf=apply_bar_filters(edf,jufo_min,fwci_above1,yr_range)
    clusters=[c for c in CO if c in edf["_Cl"].unique()]
    if show_jufo and "JUFO Level" in edf.columns:
        fig=make_subplots(rows=1,cols=2,subplot_titles=["Publication Count (JUFO)","Average FWCI"],horizontal_spacing=0.12)
        for ann in fig.layout.annotations:ann.update(y=-0.1,yanchor="top",font=dict(size=13,family=PF))
        for jl_ in reversed(JO):
            jsub=edf[edf["JUFO Level"].astype(str)==jl_]
            if len(jsub)==0:continue
            ct=jsub.groupby("_Cl").size().reindex(clusters,fill_value=0)
            fig.add_trace(go.Bar(x=ct.index,y=ct.values,name=JL.get(jl_,jl_),marker_color=JC.get(jl_,"#ccc"),legendgroup=jl_),row=1,col=1)
        stats=edf.groupby("_Cl")[FC].mean().reindex(clusters)
        fig.add_trace(go.Bar(x=stats.index,y=stats.values,marker_color=[CC.get(c,"#999") for c in stats.index],
            text=[f"{v:.2f}" for v in stats.values],textposition="auto",showlegend=False),row=1,col=2)
        fig.add_hline(y=1.0,line_dash="dash",line_color=ACADIA["plum"],line_width=1.5,opacity=0.6,row=1,col=2)
        fig.update_layout(barmode="stack",height=520)
    else:
        fig=make_subplots(rows=1,cols=2,subplot_titles=["Publication Count","Average FWCI"],horizontal_spacing=0.12)
        for ann in fig.layout.annotations:ann.update(y=-0.1,yanchor="top",font=dict(size=13,family=PF))
        stats=edf.groupby("_Cl").agg(Count=(FC,"size"),Avg=(FC,"mean")).reindex(clusters)
        fig.add_trace(go.Bar(x=stats.index,y=stats["Count"],marker_color=[CC.get(c,"#999") for c in stats.index],
            text=[str(int(v)) for v in stats["Count"]],textposition="auto",showlegend=False),row=1,col=1)
        fig.add_trace(go.Bar(x=stats.index,y=stats["Avg"],marker_color=[CC.get(c,"#999") for c in stats.index],
            text=[f"{v:.2f}" for v in stats["Avg"]],textposition="auto",showlegend=False),row=1,col=2)
        fig.add_hline(y=1.0,line_dash="dash",line_color=ACADIA["plum"],line_width=1.5,opacity=0.6,row=1,col=2)
        fig.update_layout(height=520)
    fig.update_layout(font=dict(family=PF),template="plotly_white",title="Cluster Summary",margin=dict(b=80))
    return fig

def plotly_researcher_chart(df,cluster_filter=None,show_jufo=True,jufo_min=0,fwci_above1=False,yr_range=None):
    m=df[df["Warwick Researcher"]!=""].copy()
    if cluster_filter and cluster_filter!="All":m=m[m["Research Cluster"].str.contains(cluster_filter,na=False)]
    rdf=expl_res(m);
    if rdf.empty:return go.Figure()
    rdf[FC]=pd.to_numeric(rdf.get(FC),errors="coerce")
    rdf=apply_bar_filters(rdf,jufo_min,fwci_above1,yr_range)
    stats=rdf.groupby("_Res").agg(Count=(FC,"size"),Avg=(FC,"mean")).sort_values("Count",ascending=True)
    n=len(stats)
    if show_jufo and "JUFO Level" in rdf.columns:
        fig=go.Figure()
        for jl_ in reversed(JO):
            jsub=rdf[rdf["JUFO Level"].astype(str)==jl_]
            if len(jsub)==0:continue
            ct=jsub.groupby("_Res").size().reindex(stats.index,fill_value=0)
            fig.add_trace(go.Bar(y=ct.index,x=ct.values,orientation="h",name=JL.get(jl_,jl_),marker_color=JC.get(jl_,"#ccc"),legendgroup=jl_))
        fig.update_layout(barmode="stack",title="Researcher Publications (JUFO)")
    else:
        fig=make_subplots(rows=1,cols=2,subplot_titles=["Publication Count","Average FWCI"],shared_yaxes=True,horizontal_spacing=0.08)
        for ann in fig.layout.annotations:ann.update(y=-0.05,yanchor="top",font=dict(size=13,family=PF))
        fig.add_trace(go.Bar(y=stats.index,x=stats["Count"],orientation="h",marker_color=ACADIA["steel"],showlegend=False),row=1,col=1)
        fig.add_trace(go.Bar(y=stats.index,x=stats["Avg"],orientation="h",marker_color=ACADIA["yellow"],showlegend=False),row=1,col=2)
        fig.add_vline(x=1.0,line_dash="dash",line_color=ACADIA["plum"],line_width=1.2,opacity=0.6,row=1,col=2)
        fig.update_layout(title="Researcher Summary")
    fig.update_layout(font=dict(family=PF),template="plotly_white",height=max(450,n*24),margin=dict(b=60))
    return fig

# ═══ BEAMPLOT ═══
BEAM_DIAMOND = dict(size=13,color="white",symbol="diamond",line=dict(width=2,color="#dc267f"))
BEAM_WORLD_LINE = dict(line_dash="dash",line_color="red",line_width=2.5,opacity=0.8)

def interactive_beamplot(df,entity_name,entity_type="cluster"):
    matched=df[df["Warwick Researcher"]!=""].copy()
    if entity_type=="cluster":
        subset=matched[matched["Research Cluster"].str.contains(entity_name,na=False)]
        title=f"Beamplot: {entity_name} Cluster";dc=CC.get(entity_name,ACADIA["steel"])
    else:
        subset=matched[matched["Warwick Researcher"].str.contains(entity_name,na=False,regex=False)]
        title=f"Beamplot: {entity_name}";dc=ACADIA["steel"]
    subset=subset.copy();subset["FWCI"]=pd.to_numeric(subset.get(FC),errors="coerce")
    subset["Year"]=pd.to_numeric(subset.get("Year"),errors="coerce")
    subset=subset.dropna(subset=["FWCI","Year"]);subset["Year"]=subset["Year"].astype(int)
    if len(subset)==0:return go.Figure().update_layout(title=f"No data for {entity_name}"),None
    subset["FR"]=subset["FWCI"].round(2)
    gr=subset.groupby(["Year","FR"]).agg(n=("Title","size"),titles=("Title",lambda x:list(x)),
        researchers=("Warwick Researcher",lambda x:list(set(n2.strip() for ns in x for n2 in str(ns).split(";") if n2.strip())))).reset_index()
    hover_texts=[]
    for _,row in gr.iterrows():
        lines=[f"<b>FWCI: {row['FR']:.2f} | Year: {int(row['Year'])} | Papers: {row['n']}</b>","","<b>Researchers:</b>"]
        for r in row["researchers"][:5]:lines.append(f"  {r}")
        lines+=["","<b>Titles (click to expand):</b>"]
        for t in row["titles"][:5]:lines.append(f"  - {textwrap.shorten(str(t),75,placeholder='...')}")
        if len(row["titles"])>5:lines.append(f"  ... +{len(row['titles'])-5} more")
        hover_texts.append("<br>".join(lines))
    fig=go.Figure()
    fig.add_trace(go.Scatter(x=gr["FR"],y=gr["Year"],mode="markers",
        marker=dict(size=np.clip(gr["n"]*8,7,45),color=dc,opacity=0.75,line=dict(width=0.6,color="black")),
        text=hover_texts,hovertemplate="%{text}<extra></extra>",name="Publications"))
    meds=subset.groupby("Year")["FWCI"].median().reset_index()
    fig.add_trace(go.Scatter(x=meds["FWCI"],y=meds["Year"],mode="markers+lines",
        marker=BEAM_DIAMOND,line=dict(color="#dc267f",width=1.5),
        hovertemplate="<b>Median FWCI: %{x:.2f}</b><br>Year: %{y}<extra></extra>",name="Annual median"))
    fig.add_vline(x=1.0,annotation_text="World avg (1.0)",annotation_position="top right",**BEAM_WORLD_LINE)
    np_=len(subset);om=subset["FWCI"].median()
    fig.update_layout(title=dict(text=f"{title}<br><sub>n={np_} | median FWCI={om:.2f}</sub>",font=dict(family=PF,size=16)),
        xaxis_title="FWCI",yaxis_title="Year",yaxis=dict(autorange="reversed",dtick=1,tickvals=sorted(subset["Year"].unique())),
        xaxis=dict(type="log" if subset["FWCI"].max()>10 else "linear"),font=dict(family=PF,size=13),template="plotly_white",
        height=max(450,len(subset["Year"].unique())*90+120),showlegend=True,
        legend=dict(yanchor="top",y=0.99,xanchor="right",x=0.99,font=dict(size=12)),
        hoverlabel=dict(font_size=13,font_family=PF,bgcolor="white",bordercolor="#ccc"))
    return fig,gr

def interactive_multi_beamplot(df,entities,entity_type):
    n=len(entities);cols=min(3,n);rows=math.ceil(n/cols)
    fig=make_subplots(rows=rows,cols=cols,subplot_titles=[textwrap.shorten(e,25,placeholder="...") for e in entities],
        horizontal_spacing=0.08,vertical_spacing=0.12)
    matched=df[df["Warwick Researcher"]!=""].copy()
    matched["FWCI"]=pd.to_numeric(matched.get(FC),errors="coerce");matched["Year"]=pd.to_numeric(matched.get("Year"),errors="coerce")
    for idx,entity in enumerate(entities):
        r2,c2=divmod(idx,cols);r2+=1;c2+=1
        if entity_type=="cluster":sub=matched[matched["Research Cluster"].str.contains(entity,na=False)];colour=CC.get(entity,ACADIA["steel"])
        else:sub=matched[matched["Warwick Researcher"].str.contains(entity,na=False,regex=False)];colour=ACADIA_LIST[idx%len(ACADIA_LIST)]
        sub=sub.dropna(subset=["FWCI","Year"]).copy()
        if len(sub)==0:continue
        sub["Year"]=sub["Year"].astype(int);sub["FR"]=sub["FWCI"].round(2)
        gr=sub.groupby(["Year","FR"]).agg(n=("Title","size"),titles=("Title",lambda x:"<br>".join([f"- {textwrap.shorten(str(t),65,placeholder='...')}" for t in x]))).reset_index()
        fig.add_trace(go.Scatter(x=gr["FR"],y=gr["Year"],mode="markers",
            marker=dict(size=np.clip(gr["n"]*7,6,35),color=colour,opacity=0.75,line=dict(width=0.4,color="black")),
            customdata=np.stack([gr["n"],gr["titles"]],axis=-1),
            hovertemplate="<b>FWCI: %{x:.2f}</b><br>Year: %{y}<br>Papers: %{customdata[0]}<br>%{customdata[1]}<extra></extra>",
            showlegend=False),row=r2,col=c2)
        meds=sub.groupby("Year")["FWCI"].median().reset_index()
        fig.add_trace(go.Scatter(x=meds["FWCI"],y=meds["Year"],mode="markers",
            marker=BEAM_DIAMOND,hovertemplate="Median: %{x:.2f}<extra></extra>",showlegend=False),row=r2,col=c2)
        fig.add_vline(x=1.0,row=r2,col=c2,**BEAM_WORLD_LINE)
        fig.update_yaxes(autorange="reversed",dtick=1,row=r2,col=c2)
        if sub["FWCI"].max()>10:fig.update_xaxes(type="log",row=r2,col=c2)
    fig.update_layout(font=dict(family=PF,size=12),template="plotly_white",height=max(450,rows*380),showlegend=False,
        hoverlabel=dict(font_size=12,font_family=PF))
    return fig

def static_beamplot(df,entities,etype):
    matched=df[df["Warwick Researcher"]!=""].copy()
    matched["FWCI"]=pd.to_numeric(matched.get(FC),errors="coerce");matched["Year"]=pd.to_numeric(matched.get("Year"),errors="coerce")
    if len(entities)==1:
        e=entities[0]
        if etype=="cluster":sub=matched[matched["Research Cluster"].str.contains(e,na=False)];col=CC.get(e,ACADIA["steel"])
        else:sub=matched[matched["Warwick Researcher"].str.contains(e,na=False,regex=False)];col=ACADIA["steel"]
        sub=sub.dropna(subset=["FWCI","Year"]).copy();sub["Year"]=sub["Year"].astype(int)
        if len(sub)==0:fig,ax=plt.subplots(figsize=(10,4));fig.patch.set_facecolor("none");ax.text(0.5,0.5,"No data",ha="center",va="center",transform=ax.transAxes);return fig
        yrs=sorted(sub["Year"].unique());fig,ax=plt.subplots(figsize=(11,max(4,len(yrs)*0.8+1)));fig.patch.set_facecolor("none")
        sub["FR"]=sub["FWCI"].round(2);cts=sub.groupby(["Year","FR"]).size().reset_index(name="n")
        for _,r in cts.iterrows():ax.scatter(r["FR"],r["Year"],s=max(20,min(r["n"]*30,300)),c=col,edgecolors="black",linewidth=0.5,alpha=0.7,zorder=2)
        meds=sub.groupby("Year")["FWCI"].median()
        for yr,med in meds.items():ax.scatter(med,yr,s=120,facecolors="white",edgecolors="#dc267f",marker="D",linewidth=2,zorder=3)
        ax.axvline(x=1.0,color="red",linestyle="--",linewidth=2,alpha=0.7)
        ax.set_yticks(yrs);ax.set_xlabel("FWCI");ax.set_ylabel("Year");ax.set_title(f"Beamplot: {e}",fontweight="bold",fontsize=14,pad=14)
        if sub["FWCI"].max()>10:ax.set_xscale("log");ax.xaxis.set_major_formatter(mticker.ScalarFormatter())
        ax.spines[["top","right"]].set_visible(False);ax.grid(axis="x",alpha=0.15);ax.invert_yaxis()
        lels=[Line2D([0],[0],marker="o",color="w",markerfacecolor=col,markeredgecolor="black",markersize=8,label="Publication"),
              Line2D([0],[0],marker="D",color="w",markerfacecolor="white",markeredgecolor="#dc267f",markersize=8,markeredgewidth=2,label="Annual median"),
              Line2D([0],[0],color="red",linestyle="--",linewidth=2,label="World avg (1.0)")]
        ax.legend(handles=lels,loc="upper right",framealpha=0.85)
        ax.text(0.02,0.02,f"n={len(sub)} | median FWCI={sub['FWCI'].median():.2f}",transform=ax.transAxes,fontsize=9,color="#555")
        fig.tight_layout(pad=1.5);return fig
    else:
        n=len(entities);nc=min(3,n);nr=math.ceil(n/nc)
        fig,axes=plt.subplots(nr,nc,figsize=(5.5*nc,3.5*nr),squeeze=False);fig.patch.set_facecolor("none")
        for idx,e in enumerate(entities):
            r2,c2=divmod(idx,nc);ax=axes[r2][c2]
            if etype=="cluster":sub=matched[matched["Research Cluster"].str.contains(e,na=False)];col=CC.get(e,ACADIA["steel"])
            else:sub=matched[matched["Warwick Researcher"].str.contains(e,na=False,regex=False)];col=ACADIA_LIST[idx%len(ACADIA_LIST)]
            sub=sub.dropna(subset=["FWCI","Year"]).copy()
            if len(sub)==0:ax.text(0.5,0.5,"No data",ha="center",va="center",transform=ax.transAxes);ax.set_title(e,fontweight="bold",fontsize=10);continue
            sub["Year"]=sub["Year"].astype(int);sub["FR"]=sub["FWCI"].round(2)
            cts=sub.groupby(["Year","FR"]).size().reset_index(name="n")
            for _,r in cts.iterrows():ax.scatter(r["FR"],r["Year"],s=max(15,min(r["n"]*20,200)),c=col,edgecolors="black",linewidth=0.4,alpha=0.7,zorder=2)
            meds=sub.groupby("Year")["FWCI"].median()
            for yr,med in meds.items():ax.scatter(med,yr,s=80,facecolors="white",edgecolors="#dc267f",marker="D",linewidth=1.5,zorder=3)
            ax.axvline(x=1.0,color="red",linestyle="--",linewidth=1.5,alpha=0.6)
            ax.set_yticks(sorted(sub["Year"].unique()));ax.spines[["top","right"]].set_visible(False);ax.grid(axis="x",alpha=0.1);ax.invert_yaxis()
            if sub["FWCI"].max()>10:ax.set_xscale("log");ax.xaxis.set_major_formatter(mticker.ScalarFormatter())
            ax.set_title(f"{textwrap.shorten(e,25,placeholder='...')}\n(n={len(sub)}, med={sub['FWCI'].median():.2f})",fontweight="bold",fontsize=10,pad=8)
        for idx in range(n,nr*nc):r2,c2=divmod(idx,nc);axes[r2][c2].set_visible(False)
        fig.supxlabel("FWCI",fontsize=11,y=0.02);fig.tight_layout(pad=2,rect=[0,0.04,1,1]);return fig

# ═══ OA ═══
def plotly_oa(df,by="cluster",show_breakdown=True,exclude_closed=False):
    m=df[df["Warwick Researcher"]!=""].copy();oa="Open Access"
    if oa not in m.columns:return go.Figure().update_layout(title="No OA data")
    m["OA_Cat"]=m[oa].apply(merge_oa)
    if exclude_closed:m=m[m["OA_Cat"]!="Closed"]
    oa_order=["Gold","Hybrid Green/Gold","Green","Hybrid Green/Bronze","Bronze","Closed"]
    oa_colours={"Gold":"#FED789","Hybrid Green/Gold":"#476F84","Green":"#72874E","Hybrid Green/Bronze":"#A4BED5","Bronze":"#dc267f","Closed":"#d9d9d9"}
    if by=="cluster":
        edf=expl_cl(m)
        if edf.empty:return go.Figure()
        if show_breakdown:
            present=[t for t in oa_order if t in edf["OA_Cat"].values]
            order=[c for c in CO if c in edf["_Cl"].unique()]
            fig=go.Figure()
            for ot in present:
                sub=edf[edf["OA_Cat"]==ot]
                ct=sub.groupby("_Cl").size().reindex(order,fill_value=0)
                fig.add_trace(go.Bar(x=ct.index,y=ct.values,name=ot,marker_color=oa_colours.get(ot,"#999")))
            fig.update_layout(barmode="stack",title="Open Access by Cluster (Breakdown)")
        else:
            g=edf.groupby("_Cl",group_keys=False).apply(lambda g2:pd.Series({"OA":g2["OA_Cat"].apply(lambda x:x!="Closed").sum(),"Total":len(g2)}),include_groups=False)
            g["Pct"]=(100*g["OA"]/g["Total"]).round(1);g["Closed"]=g["Total"]-g["OA"]
            order=[c for c in CO if c in g.index];g=g.reindex(order)
            fig=go.Figure()
            fig.add_trace(go.Bar(x=g.index,y=g["OA"],name="Open Access",marker_color=[CC.get(c,"#999") for c in g.index],opacity=0.9,text=[f"{p}%" for p in g["Pct"]],textposition="auto"))
            fig.add_trace(go.Bar(x=g.index,y=g["Closed"],name="Closed",marker_color="#d9d9d9",opacity=0.6))
            fig.update_layout(barmode="stack",title="Open Access by Cluster")
    else:
        rdf=expl_res(m)
        if rdf.empty:return go.Figure()
        g=rdf.groupby("_Res",group_keys=False).apply(lambda g2:pd.Series({"OA":g2["OA_Cat"].apply(lambda x:x!="Closed").sum(),"Total":len(g2)}),include_groups=False)
        g["Pct"]=(100*g["OA"]/g["Total"]).round(1);g=g.sort_values("Pct",ascending=True)
        fig=go.Figure()
        fig.add_trace(go.Bar(y=g.index,x=g["Pct"],orientation="h",marker_color=ACADIA["steel"],text=[f"{p}%" for p in g["Pct"]],textposition="auto"))
        fig.update_layout(title="% Open Access by Researcher",xaxis_title="% OA",height=max(400,len(g)*22))
    fig.update_layout(font=dict(family=PF),template="plotly_white")
    return fig

def plotly_oa_trend(df):
    """OA publications by year per cluster — stacked area chart."""
    m=df[df["Warwick Researcher"]!=""].copy();oa="Open Access"
    if oa not in m.columns:return go.Figure().update_layout(title="No OA data")
    m["Year"]=pd.to_numeric(m["Year"],errors="coerce");m=m.dropna(subset=["Year"]);m["Year"]=m["Year"].astype(int)
    m["is_OA"]=m[oa].apply(lambda x:pd.notna(x) and str(x).strip()!="")
    edf=expl_cl(m)
    if edf.empty:return go.Figure()
    clusters=[c for c in CO if c in edf["_Cl"].unique()]
    years=sorted(edf["Year"].unique())
    fig=go.Figure()
    for cl in clusters:
        sub=edf[(edf["_Cl"]==cl)&(edf["is_OA"])]
        ct=sub.groupby("Year").size().reindex(years,fill_value=0)
        fig.add_trace(go.Bar(x=[str(y) for y in years],y=ct.values,name=cl,
            marker_color=CC.get(cl,"#999"),
            hovertemplate=f"{cl}<br>Year: %{{x}}<br>OA papers: %{{y}}<extra></extra>"))
    # Add total line
    total_oa=edf[edf["is_OA"]].groupby("Year").size().reindex(years,fill_value=0)
    fig.add_trace(go.Scatter(x=[str(y) for y in years],y=total_oa.values,mode="lines+markers",
        name="Total OA",line=dict(color=ACADIA["plum"],width=2.5,dash="dot"),
        marker=dict(size=8,color=ACADIA["plum"]),
        hovertemplate="Total OA: %{y}<br>Year: %{x}<extra></extra>"))
    fig.update_layout(barmode="stack",title="Open Access Publications by Year and Cluster",
        xaxis_title="Year",yaxis_title="OA Publications",
        font=dict(family=PF),template="plotly_white",height=480,
        legend=dict(orientation="h",yanchor="bottom",y=1.02,xanchor="right",x=1))
    return fig

# ═══════════════════════════════════════════════════════════════════════════════
# STREAMLIT APP
# ═══════════════════════════════════════════════════════════════════════════════
def main():
    st.set_page_config(page_title="SoE Output Evaluation",page_icon="📊",layout="wide",initial_sidebar_state="expanded")
    st.markdown("""<style>
    .main-header{background:linear-gradient(135deg,#023743 0%,#453947 100%);padding:1.5rem 2rem;border-radius:12px;margin-bottom:1.5rem;}
    .main-header h1{color:white !important;margin:0;font-size:1.8rem;font-weight:700;}
    .main-header p{color:#A4BED5 !important;margin:0.3rem 0 0 0;font-size:0.95rem;}
    .metric-card{border-radius:10px;padding:1.2rem;box-shadow:0 2px 8px rgba(0,0,0,0.08);border-left:4px solid;text-align:center;}
    .metric-value{font-size:2rem;font-weight:700;margin:0.3rem 0;}
    .metric-label{font-size:0.85rem;opacity:0.7;text-transform:uppercase;letter-spacing:0.5px;}
    .info-box{background:rgba(164,190,213,0.15);border-left:4px solid #476F84;padding:0.8rem 1rem;border-radius:0 8px 8px 0;margin:0.5rem 0;font-size:0.9rem;}
    .info-box a{color:#476F84 !important;font-weight:500;}
    </style>""",unsafe_allow_html=True)
    st.markdown("""<div class="main-header"><h1>📊 School of Engineering Output Evaluation</h1>
    <p>Publication metrics collected from SciVal (Scopus, Elsevier). Upload files can be found
    <a href="https://livewarwickac-my.sharepoint.com/:f:/g/personal/u2472751_live_warwick_ac_uk/IgBuWi_Oq0WYQaB6E6ZGZsLBAe3DSCOSLFadzPdAMI7MKYQ" style="color:#FED789 !important;text-decoration:underline;" target="_blank">here</a>.</p>
    <p style="margin-top:0.4rem;font-size:0.85rem;color:#88a0b8 !important;">Questions and feedback:
    <b>Amy Phelps</b>, Research Office Assistant (A311,
    <a href="mailto:Amy.I.Phelps@warwick.ac.uk" style="color:#FED789 !important;">Amy.I.Phelps@warwick.ac.uk</a>)</p>
    </div>""",unsafe_allow_html=True)

    # ── Default data files (bundled in repo alongside this script) ──
    # Try multiple path strategies for Streamlit Cloud compatibility
    _candidates = [Path(__file__).parent if "__file__" in dir() else Path("."), Path("."), Path("/mount/src/soesva")]
    SCRIPT_DIR = Path(".")
    for _cd in _candidates:
        if (_cd / "Researchers_updated.csv").exists():
            SCRIPT_DIR = _cd; break
    DEFAULT_SCIVAL = SCRIPT_DIR / "SoE_SciVal_Outputs.xlsx"
    DEFAULT_RESEARCHERS = SCRIPT_DIR / "Researchers_updated.csv"
    DEFAULT_JUFO = SCRIPT_DIR / "jfp-export.csv"

    with st.sidebar:
        st.markdown("### 📁 Data Files")
        if DEFAULT_SCIVAL.exists():
            st.success("Default SciVal loaded", icon="✅")
        scival_upload=st.file_uploader("Upload newer SciVal Export (.xlsx)",type=["xlsx"],
            help="Leave blank to use the bundled default file" if DEFAULT_SCIVAL.exists() else None)
        if DEFAULT_RESEARCHERS.exists():
            st.success("Default Researchers loaded", icon="✅")
        researchers_upload=st.file_uploader("Upload newer Researchers CSV",type=["csv"],
            help="Leave blank to use the bundled default file" if DEFAULT_RESEARCHERS.exists() else None)
        if DEFAULT_JUFO.exists():
            st.success("Default JUFO loaded", icon="✅")
        jufo_upload=st.file_uploader("Upload newer JUFO Export (.csv)",type=["csv"],
            help="Leave blank to use the bundled default file" if DEFAULT_JUFO.exists() else None)
        st.markdown("---")
        st.markdown("### ⚙️ Settings")
        year_range=st.slider("Publication years",2010,2026,(2021,2026))
        st.caption("Hungyen Lin (56023981300) override applied automatically.")

    # Resolve files: uploaded overrides repo defaults
    scival_file = scival_upload if scival_upload is not None else (str(DEFAULT_SCIVAL) if DEFAULT_SCIVAL.exists() else None)
    researchers_file = researchers_upload if researchers_upload is not None else (str(DEFAULT_RESEARCHERS) if DEFAULT_RESEARCHERS.exists() else None)
    jufo_file = jufo_upload if jufo_upload is not None else (str(DEFAULT_JUFO) if DEFAULT_JUFO.exists() else None)

    if scival_file is None or researchers_file is None:
        st.info("👈 Upload a **SciVal export** and **Researchers CSV** to begin, or place default files in the app directory.");return
    if jufo_file is None:
        st.warning("⚠️ No JUFO export found. Upload a JUFO file or place `jfp-export.csv` in the app directory. JUFO-dependent features will be unavailable.")

    with st.spinner("Parsing…"):pubs=parse_scival(scival_file)
    if pubs.empty:return
    researchers=pd.read_csv(researchers_file)
    if "Year" in pubs.columns:
        pubs["Year"]=pd.to_numeric(pubs["Year"],errors="coerce")
        pubs=pubs[(pubs["Year"]>=year_range[0])&(pubs["Year"]<=year_range[1])].reset_index(drop=True)
    with st.spinner("Matching…"):
        pubs=match_researchers(pubs,researchers,
            {"56023981300":{"name":"Hungyen Lin","cluster":"MDM"}},
            {"7203017474":"Merged Scopus: Warwick Christopher James (BCI, 57652552800) + C.J. James (Toulouse)."},
            {"Hungyen Lin":"Warwick academic, Lancaster affiliation in Scopus"})
    pubs=pubs[pubs["Warwick Researcher"]!=""].reset_index(drop=True)
    if jufo_file is not None:
        with st.spinner("Matching JUFO…"):pubs=match_jufo(pubs,pd.read_csv(jufo_file,low_memory=False))
    # Ensure JUFO Level column always exists (even if empty)
    if "JUFO Level" not in pubs.columns:
        pubs["JUFO Level"]=""
    if FC in pubs.columns:pubs[FC]=pd.to_numeric(pubs[FC],errors="coerce")
    all_names=sorted(set(n.strip() for ns in pubs["Warwick Researcher"] for n in str(ns).split("; ") if n.strip()))
    yr_opts=sorted(pubs["Year"].dropna().unique().astype(int))
    yr_min,yr_max=(min(yr_opts),max(yr_opts)) if yr_opts else (2021,2026)

    # Metrics
    cols=st.columns(5)
    mdata=[(str(len(pubs)),"Publications",ACADIA["teal"]),(str(len(all_names)),"Researchers",ACADIA["olive"]),
        (f"{pubs[FC].median():.2f}" if FC in pubs.columns else "–","Median FWCI",ACADIA["steel"]),
        (f"{pubs[FC].mean():.2f}" if FC in pubs.columns else "–","Mean FWCI",ACADIA["plum"]),
        (f"{oa_pct(pubs.get('Open Access',pd.Series()))}%","Open Access",ACADIA["yellow"])]
    for col,(val,label,colour) in zip(cols,mdata):
        col.markdown(f'<div class="metric-card" style="border-left-color:{colour};"><div class="metric-label">{label}</div><div class="metric-value" style="color:{colour};">{val}</div></div>',unsafe_allow_html=True)
    st.markdown("<br>",unsafe_allow_html=True)

    tab1,tab2,tab3,tab4,tab5,tab6,tab7=st.tabs(["📋 Data","🔍 Summary View","📊 Clusters","👤 Researchers","📈 Beamplots","🔓 Open Access","📧 Share"])

    # ── TAB 1: DATA ──
    with tab1:
        st.markdown("#### Publication Data")
        fc1,fc2,fc3,fc4=st.columns(4)
        with fc1:f_cl=st.multiselect("Cluster",sorted(CC.keys()),default=[],key="fc")
        with fc2:f_res=st.multiselect("Researcher",all_names,default=[],key="fr")
        with fc3:
            f_fwci=st.checkbox("FWCI > 1.0 only",value=False,key="ff",
                help="**FWCI** (Field-Weighted Citation Impact) measures citation impact normalised by field, document type, and year. FWCI = 1.0 is the world average; above 1.0 means more cited than expected.")
        with fc4:
            f_jufo=st.selectbox("Min JUFO",["Any","≥1","≥2","≥3"],key="fj")
        fc5,fc6=st.columns([1,2])
        with fc5:f_yr=st.slider("Year range",yr_min,yr_max,(yr_min,yr_max),key="fyr")
        filt=pubs.copy()
        if f_cl:filt=filt[filt["Research Cluster"].apply(lambda x:any(c in str(x) for c in f_cl))]
        if f_res:filt=filt[filt["Warwick Researcher"].apply(lambda x:any(r in str(x) for r in f_res))]
        if f_fwci and FC in filt.columns:filt=filt[filt[FC].fillna(0)>1.0]
        if f_jufo!="Any" and "JUFO Level" in filt.columns:
            mj=int(f_jufo.replace("≥",""));filt=filt[filt["JUFO Level"].apply(lambda x:str(x).isdigit() and int(x)>=mj)]
        filt=filt[(filt["Year"]>=f_yr[0])&(filt["Year"]<=f_yr[1])]
        st.caption(f"Showing **{len(filt)}** of {len(pubs)} publications")
        dcols=[c for c in filt.columns if c not in ["Match Notes","Warwick Scopus ID"]]
        st.dataframe(filt[dcols],use_container_width=True,height=500)
        c1,c2=st.columns(2)
        with c1:st.download_button("⬇️ Export filtered",xl_buf(filt,"Filtered"),"SoE_Filtered.xlsx",mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        with c2:st.download_button("⬇️ Export all",xl_buf(pubs,"All"),"SoE_All.xlsx",mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        flagged=pubs[pubs["Match Notes"].apply(lambda x:pd.notna(x) and str(x).strip()!="")]
        if len(flagged)>0:
            if st.checkbox(f"Show {len(flagged)} match notes",value=False,key="show_flags"):
                st.dataframe(flagged[["Warwick Researcher","Match Notes","Title","Year"]].head(50),use_container_width=True)

    # ── TAB 2: SUMMARY VIEW ──
    with tab2:
        st.markdown("#### Summary View")
        sv_type=st.radio("Summarise by:",["Cluster","Researcher","Compare Clusters","Compare Researchers"],horizontal=True,key="sv_type")
        sv_yc1,sv_yc2=st.columns([1,2])
        with sv_yc1:sv_yr=st.slider("Year range",yr_min,yr_max,(yr_min,yr_max),key="sv_yr")
        sv_yr_range=(sv_yr[0],sv_yr[1])

        if sv_type=="Cluster":
            sv_cl=st.selectbox("Select cluster:",CO,key="sv_cl")
            show_t5y=st.checkbox("Show top 5 by year",value=False,key="sv_t5y_cl")
            if sv_cl:
                s=cluster_summary(pubs,sv_cl,sv_yr_range)
                render_entity_summary(s,"cluster",show_top5_by_year=show_t5y)
                if s and st.button("📈 Visualise as beamplot",key="sv_cl_viz"):
                    st.session_state["_goto_beam"]="cluster";st.session_state["_beam_entity"]=sv_cl

        elif sv_type=="Researcher":
            sv_name=st.selectbox("Select researcher:",all_names,key="sv_res")
            show_t5y=st.checkbox("Show top 5 by year",value=False,key="sv_t5y_res")
            if sv_name:
                s=researcher_summary(pubs,sv_name,sv_yr_range)
                render_entity_summary(s,"researcher",show_top5_by_year=show_t5y)
                if s and st.button("📈 Visualise as beamplot",key="sv_res_viz"):
                    st.session_state["_goto_beam"]="researcher";st.session_state["_beam_entity"]=sv_name

        elif sv_type=="Compare Clusters":
            sv_cls=st.multiselect("Select clusters:",CO,default=CO[:3],key="sv_cmp_cl")
            by_year=st.checkbox("View by year",value=False,key="sv_cby")
            if sv_cls:
                summaries=[cluster_summary(pubs,c,sv_yr_range) for c in sv_cls]
                summaries=[s for s in summaries if s]
                if summaries:
                    fig=plotly_compare(summaries,"cluster",by_year=by_year)
                    st.plotly_chart(fig,use_container_width=True)

        else:  # Compare Researchers
            sv_names=st.multiselect("Select researchers:",all_names,default=all_names[:2] if len(all_names)>=2 else all_names,key="sv_cmp_res")
            by_year=st.checkbox("View by year",value=False,key="sv_rby")
            if sv_names:
                summaries=[researcher_summary(pubs,n,sv_yr_range) for n in sv_names]
                summaries=[s for s in summaries if s]
                if summaries:
                    fig=plotly_compare(summaries,"researcher",by_year=by_year)
                    st.plotly_chart(fig,use_container_width=True)

    # ── TAB 3: CLUSTERS ──
    with tab3:
        st.markdown("#### Cluster Overview")
        cm=st.radio("View:",["Summary","Publications by year"],horizontal=True,key="cm")
        show_jufo=st.checkbox("Show JUFO breakdown",value=True,key="cj",
            help="JUFO: Finland's national journal classification. 3=highest, 2=leading, 1=basic, 0=below criteria.")
        cc1,cc2,cc3=st.columns(3)
        with cc1:jufo_min=st.slider("Min JUFO level",0,3,0,key="cl_jm")
        with cc2:fwci_above1=st.checkbox("FWCI > 1.0 only",value=False,key="cl_fwci")
        with cc3:cl_yr=st.slider("Year range",yr_min,yr_max,(yr_min,yr_max),key="cl_yr")
        if cm=="Summary":
            fig=plotly_cluster_summary(pubs,show_jufo=show_jufo,jufo_min=jufo_min,fwci_above1=fwci_above1,yr_range=cl_yr)
            st.plotly_chart(fig,use_container_width=True)
        else:
            cluster_sel=st.multiselect("Select clusters:",CO,default=CO,key="cl_sel")
            fig=plotly_faceted_year(pubs,show_jufo=show_jufo,jufo_min=jufo_min,fwci_above1=fwci_above1,yr_range=cl_yr,cluster_sel=cluster_sel)
            st.plotly_chart(fig,use_container_width=True)

    # ── TAB 4: RESEARCHERS ──
    with tab4:
        st.markdown("#### Researcher Analytics")
        rc1,rc2=st.columns([2,1])
        with rc1:sc=st.selectbox("Filter by cluster:",["All"]+sorted(CC.keys()),key="rc")
        with rc2:rj=st.checkbox("Show JUFO breakdown",value=True,key="rj")
        rc3,rc4,rc5=st.columns(3)
        with rc3:r_jm=st.slider("Min JUFO level",0,3,0,key="res_jm")
        with rc4:r_fwci=st.checkbox("FWCI > 1.0 only",value=False,key="res_fwci")
        with rc5:r_yr=st.slider("Year range",yr_min,yr_max,(yr_min,yr_max),key="res_yr")
        fig=plotly_researcher_chart(pubs,sc,show_jufo=rj,jufo_min=r_jm,fwci_above1=r_fwci,yr_range=r_yr)
        st.plotly_chart(fig,use_container_width=True)

    # ── TAB 5: BEAMPLOTS ──
    with tab5:
        st.markdown("#### Citation Impact Beamplots")
        st.markdown("""**Beamplots** visualise citation impact over time (inspired by Web of Science InCites).

- **Each dot** = one or more publications at a given FWCI and year. Larger dots = more papers.
- **Pink diamonds** (◇) = **annual median FWCI** for that year.
- **Red dashed line** at FWCI = 1.0 = **world average**. Papers above are cited more than the global field average.

**FWCI** is normalised by field, document type, and year. FWCI of 2.0 = twice the expected citations.

**Hover** over dots for details. **Click** a point to expand the full title list below.""")
        st.markdown('<div class="info-box"><b>💡 JUFO:</b> Finland\'s national journal classification (0–3, 3=highest). Levels 2–3 = leading/top-tier outlets. <a href="https://jfp.csc.fi/jufoportal">Search JUFO →</a></div>',unsafe_allow_html=True)

        bt=st.radio("View by:",["Cluster","Researcher"],horizontal=True,key="bt")
        if bt=="Cluster":
            avail=sorted(CC.keys())
            sel=st.multiselect("Select cluster(s):",avail,default=avail,key="bc")
            if sel:
                if len(sel)==1:
                    fig,gr=interactive_beamplot(pubs,sel[0],"cluster")
                    event=st.plotly_chart(fig,use_container_width=True,on_select="rerun",key="bp_cl")
                    if event and gr is not None and hasattr(event,"selection") and event.selection and event.selection.get("points"):
                        st.markdown("##### 📄 Selected publications")
                        for pt in event.selection["points"]:
                            pi=pt.get("point_index")
                            if pi is not None and pi<len(gr):
                                row=gr.iloc[pi]
                                st.markdown(f"**FWCI {row['FR']:.2f} | Year {int(row['Year'])} | {row['n']} paper(s)**")
                                for t in row["titles"]:st.markdown(f"- {t}")
                                st.markdown("---")
                else:
                    fig=interactive_multi_beamplot(pubs,sel,"cluster")
                    st.plotly_chart(fig,use_container_width=True)
                mfig=static_beamplot(pubs,sel,"cluster")
                st.download_button("💾 Save as PNG",fig_buf(mfig),f"beamplot_{'_'.join(sel)}.png",mime="image/png")
                plt.close(mfig)
        else:
            cf=st.selectbox("Filter by cluster:",["All"]+sorted(CC.keys()),key="brc")
            mdf=pubs.copy()
            if cf!="All":mdf=mdf[mdf["Research Cluster"].str.contains(cf,na=False)]
            names=sorted(set(n.strip() for ns in mdf["Warwick Researcher"] for n in str(ns).split("; ") if n.strip()))
            sel=st.multiselect("Select researcher(s):",names,default=names[:1] if names else [],key="br")
            if sel:
                if len(sel)==1:
                    fig,gr=interactive_beamplot(pubs,sel[0],"researcher")
                    event=st.plotly_chart(fig,use_container_width=True,on_select="rerun",key="bp_res")
                    if event and gr is not None and hasattr(event,"selection") and event.selection and event.selection.get("points"):
                        st.markdown("##### 📄 Selected publications")
                        for pt in event.selection["points"]:
                            pi=pt.get("point_index")
                            if pi is not None and pi<len(gr):
                                row=gr.iloc[pi]
                                st.markdown(f"**FWCI {row['FR']:.2f} | Year {int(row['Year'])} | {row['n']} paper(s)**")
                                for t in row["titles"]:st.markdown(f"- {t}")
                                st.markdown("---")
                else:
                    fig=interactive_multi_beamplot(pubs,sel,"researcher")
                    st.plotly_chart(fig,use_container_width=True)
                mfig=static_beamplot(pubs,sel,"researcher")
                st.download_button("💾 Save as PNG",fig_buf(mfig),"beamplot_researchers.png",mime="image/png")
                plt.close(mfig)

    # ── TAB 6: OA ──
    with tab6:
        st.markdown("#### Open Access Analysis")
        st.markdown('<div class="info-box">Empty OA field = <b>closed access</b>. Categories: <b>Gold</b> (pure Gold OA), <b>Hybrid Green/Gold</b> (includes Hybrid Gold, Gold/Green, Hybrid Gold/Green), <b>Green</b> (pure Green OA), <b>Hybrid Green/Bronze</b> (Bronze/Green), <b>Bronze</b>. UKRI requires OA for REF submissions.</div>',unsafe_allow_html=True)
        ov=st.radio("View by:",["Cluster","Researcher"],horizontal=True,key="ov")
        oa_c1,oa_c2=st.columns(2)
        with oa_c1:show_bd=st.checkbox("Show OA type breakdown",value=True,key="oa_bd")
        with oa_c2:excl_closed=st.checkbox("Exclude closed access papers",value=False,key="oa_excl")
        st.plotly_chart(plotly_oa(pubs,by="cluster" if ov=="Cluster" else "researcher",show_breakdown=show_bd,exclude_closed=excl_closed),use_container_width=True)
        st.markdown("##### Open Access Trend Over Time")
        st.plotly_chart(plotly_oa_trend(pubs),use_container_width=True)
        if "Open Access" in pubs.columns:
            st.markdown("##### OA Type Summary")
            oa_cats=pubs["Open Access"].apply(merge_oa).value_counts()
            st.dataframe(oa_cats.reset_index().rename(columns={"index":"Type","Open Access":"Type","count":"Count"}),use_container_width=True)

    # ── TAB 7: SHARE ──
    with tab7:
        st.markdown("#### Share via Email")
        with st.form("ef"):
            ec1,ec2=st.columns(2)
            with ec1:
                srv=st.text_input("SMTP Server",value="smtp.office365.com")
                port=st.number_input("Port",value=587,step=1)
                frm=st.text_input("Your email")
                pwd=st.text_input("Password",type="password")
            with ec2:
                to=st.text_area("Recipient(s)",placeholder="colleague@warwick.ac.uk")
                subj=st.text_input("Subject",value="SoE Publication Analysis")
                body=st.text_area("Message",value="Please find the publication analysis data attached.")
            st.markdown("**Attach:**")
            ac1,ac2=st.columns(2)
            with ac1:a1=st.checkbox("Full data (Excel)",value=True)
            with ac2:a2=st.checkbox("Filtered data (Excel)")
            if st.form_submit_button("📧 Send"):
                if not frm or not pwd or not to.strip():st.error("Fill all email fields.")
                else:
                    att=[]
                    if a1:att.append(("SoE_All.xlsx",xl_buf(pubs,"All").read()))
                    if a2:att.append(("SoE_Filtered.xlsx",xl_buf(filt if "filt" in dir() else pubs,"Filtered").read()))
                    rl=[r.strip() for r in to.strip().split("\n") if r.strip()]
                    with st.spinner("Sending…"):ok,msg=send_email(srv,port,frm,pwd,rl,subj,body,att)
                    if ok:st.success(msg)
                    else:st.error(msg)

if __name__=="__main__":main()
