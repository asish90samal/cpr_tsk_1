<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>AML Build Roadmap</title>

<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Nunito:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">

<style>

body{
background:#08080c;
color:#e2e8f0;
font-family:'Nunito',sans-serif;
margin:0;
}

.header{
padding:16px 22px 12px;
border-bottom:1px solid #1a1a24;
}

.title{
font-family:'Bebas Neue';
font-size:26px;
letter-spacing:0.05em;
}

.subtitle{
font-size:12px;
color:#52525e;
}

.container{
display:flex;
height:calc(100vh - 70px);
}

.sidebar{
width:220px;
border-right:1px solid #1a1a24;
overflow:auto;
}

.phase{
padding:12px;
border-bottom:1px solid #0d0d14;
cursor:pointer;
}

.phase:hover{
background:#12121a;
}

.phase.active{
background:#1f1f2b;
}

.main{
flex:1;
padding:24px;
overflow:auto;
}

.phaseTitle{
font-size:28px;
font-family:'Bebas Neue';
}

.modules span{
display:inline-block;
background:#12121a;
padding:4px 8px;
margin:4px;
border-radius:4px;
font-family:'JetBrains Mono';
font-size:11px;
}

.tabs button{
margin-right:6px;
padding:6px 14px;
cursor:pointer;
background:#12121a;
border:1px solid #1f1f28;
border-radius:4px;
color:#aaa;
}

.tabs button.active{
color:#f97316;
border-color:#f97316;
}

.box{
margin-top:14px;
background:#12121a;
padding:16px;
border-radius:8px;
border:1px solid #1f1f28;
}

ul{
line-height:1.8;
}

</style>
</head>

<body>

<div class="header">
<div style="font-family:'JetBrains Mono';font-size:10px;color:#3f3f52">
AML ENTERPRISE — BUILD ORDER
</div>

<div class="title">WHAT TO BUILD FIRST</div>

<div class="subtitle">
10 phases · click any phase to see what it does
</div>
</div>


<div class="container">

<div class="sidebar" id="sidebar"></div>

<div class="main">

<div class="phaseTitle" id="title"></div>

<div id="subtitle" style="color:#777;margin-bottom:10px"></div>

<div class="modules" id="modules"></div>

<div class="tabs">
<button onclick="setTab('detail')" id="tab_detail">Why</button>
<button onclick="setTab('test')" id="tab_test">Tests</button>
<button onclick="setTab('pitfalls')" id="tab_pitfalls">Pitfalls</button>
</div>

<div class="box" id="content"></div>

</div>

</div>


<script>

let activePhase=0
let tab="detail"

const PHASES=[

{
title:"Data Layer",
subtitle:"Build raw data first",
modules:[
"utils/alias_engine.py",
"utils/transliteration_engine.py",
"generators/san_individual.py"
],
why:"Everything depends on data existing. Matching engine needs records.",
tests:[
"registry.load_all() runs",
"registry.summary() prints counts",
"datasets return DataFrame"
],
pitfalls:[
"Build utils before generators",
"aliases must exist for every record"
]
},

{
title:"ETL Layer",
subtitle:"Clean names before comparison",
modules:[
"etl_layer/normalization.py"
],
why:"Without cleaning, Mohammed vs MOHAMMED will not match.",
tests:[
"normalize_name('Möhammed') → MOHAMMED",
"normalize_aliases works"
],
pitfalls:[
"normalize both input and candidates"
]
},

{
title:"Blocking Engine",
subtitle:"Reduce candidates from 17000 to ~200",
modules:[
"blocking_engine/inverted_index.py",
"blocking_engine/blocking.py"
],
why:"Fuzzy matching against 17000 rows is too slow.",
tests:[
"inverted index builds",
"query returns <500 candidates"
],
pitfalls:[
"index must rebuild if data changes"
]
}

]

function renderSidebar(){

const sb=document.getElementById("sidebar")
sb.innerHTML=""

PHASES.forEach((p,i)=>{

const d=document.createElement("div")
d.className="phase"+(i===activePhase?" active":"")

d.innerHTML=
"<b>"+(i+1)+". "+p.title+"</b><br><span style='font-size:12px;color:#777'>"+p.subtitle+"</span>"

d.onclick=()=>{
activePhase=i
render()
}

sb.appendChild(d)

})

}

function render(){

const phase=PHASES[activePhase]

document.getElementById("title").innerText=phase.title
document.getElementById("subtitle").innerText=phase.subtitle

const modules=document.getElementById("modules")
modules.innerHTML=""
phase.modules.forEach(m=>{
modules.innerHTML+="<span>"+m+"</span>"
})

renderTab()

renderSidebar()

}

function setTab(t){
tab=t
renderTab()
}

function renderTab(){

const phase=PHASES[activePhase]

let html=""

if(tab==="detail"){

html="<p>"+phase.why+"</p>"

}

if(tab==="test"){

html="<ul>"
phase.tests.forEach(t=>html+="<li>"+t+"</li>")
html+="</ul>"

}

if(tab==="pitfalls"){

html="<ul>"
phase.pitfalls.forEach(p=>html+="<li>"+p+"</li>")
html+="</ul>"

}

document.getElementById("content").innerHTML=html

}

render()

</script>

</body>
</html>
