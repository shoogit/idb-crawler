// scripts/idb_export_to_json.mjs  (v7 — Patch A: header mapping + robust dates)
import { chromium } from "playwright";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

/** ==================== CONFIG ==================== **/
const PAGE_URL   = "https://projectprocurement.iadb.org/en/procurement-notices";
const IFRAME_CSS = 'iframe[src*="app.powerbi.com"]';
const OUT_DIR    = path.resolve("data");
const DEBUG_DIR  = path.resolve("debug");

// If your machine/browser is slow, bump these
const PAGE_LOAD_WAIT_MS   = 5000;   // initial wait after goto
const GRID_WAIT_TIMEOUTMS = 30000;  // wait until grid appears
const RETRIES             = 3;      // whole-parse attempts

/** ==================== SETUP ==================== **/
fs.mkdirSync(OUT_DIR, { recursive: true });
fs.mkdirSync(DEBUG_DIR, { recursive: true });

const sleep  = (ms) => new Promise(r => setTimeout(r, ms));
const sha256 = (s)  => crypto.createHash("sha256").update(s).digest("hex");

/** ==================== UTILS ==================== **/
// Robust date parser for DD/MM/YYYY, MM/DD/YYYY, YYYY/MM/DD and variants
function toISO(raw){
  if(!raw) return null;
  const s = String(raw).trim().replace(/[.]/g,"/").replace(/-/g,"/");
  const parts = s.split(/[\/\s]/).filter(Boolean);
  const nums = parts.map(p => parseInt(p,10)).filter(n => Number.isFinite(n));
  let y,m,d;

  if (nums.length >= 3) {
    // YYYY/MM/DD
    if (nums[0] > 1900 && nums[0] <= 2100) {
      [y,m,d] = nums;
    } else {
      // Ambiguous: assume DD/MM/YYYY when unclear (IDB commonly uses this)
      const [a,b,c] = nums;
      if (a > 12) { d=a; m=b; y=c; }
      else if (b > 12) { m=a; d=b; y=c; }
      else { d=a; m=b; y=c; }
    }
    const dt = new Date(Date.UTC(y ?? 1970, (m ?? 1)-1, d ?? 1));
    if (Number.isFinite(dt.getTime())) return dt.toISOString().slice(0,10);
  }
  const dt2 = new Date(s);
  return Number.isFinite(dt2.getTime()) ? dt2.toISOString().slice(0,10) : null;
}

function toNum(v){ if(v==null) return null; const n=Number(String(v).replace(/[^\d.]/g,"")); return Number.isFinite(n)?n:null; }
function inferSectors(title=""){ const t=title.toLowerCase(); if(/(energy|power|electric)/.test(t))return["Energy"]; if(/(road|rail|transport)/.test(t))return["Transport"]; if(/(water|waste)/.test(t))return["Water"]; if(/(ict|digital)/.test(t))return["ICT"]; return["Other"]; }
function classifyType(s=""){ const t=s.toLowerCase(); if(/expression of interest|reoi/.test(t))return"REOI"; if(/invitation to bid|rfb|itb/.test(t))return"RFB/ITB"; if(/request for proposals|rfp/.test(t))return"RFP"; if(/general procurement notice|gpn/.test(t))return"GPN"; if(/award/.test(t))return"Award"; return s||"Other"; }
function classifyCategory(s=""){ const t=s.toLowerCase(); if(/works/.test(t))return"Works"; if(/goods|supply/.test(t))return"Goods"; if(/consult/.test(t))return"Consulting"; return s||"Other"; }
function mapCountryToISO2(name=""){ const M={Argentina:"AR",Brazil:"BR",Chile:"CL",Colombia:"CO","Costa Rica":"CR","Dominican Republic":"DO",Ecuador:"EC","El Salvador":"SV",Guatemala:"GT",Haiti:"HT",Honduras:"HN",Jamaica:"JM",Mexico:"MX",Nicaragua:"NI",Panama:"PA",Paraguay:"PY",Peru:"PE",Uruguay:"UY","Trinidad and Tobago":"TT",Barbados:"BB",Belize:"BZ",Bolivia:"BO",Guyana:"GY",Suriname:"SR",Bahamas:"BS"}; return M[String(name).trim()] ?? null; }
function stableIdFromURLorTitle(url,title){ if(url){ try{ const u=new URL(url); const last=u.pathname.split("/").filter(Boolean).pop()||u.searchParams.toString()||url; return last.replace(/[^a-z0-9\-_.]/gi,"").slice(0,80)||sha256(url).slice(0,16);}catch{ return sha256(url).slice(0,16);} } return sha256(title||"").slice(0,16); }

// Pick first non-empty among keys (case-insensitive)
function headerPick(row, ...keys){
  for (const k of keys) {
    if (k in row && row[k] != null && String(row[k]).trim() !== "") return row[k];
    const ku = k.toUpperCase();
    const kl = k.toLowerCase();
    if (ku in row && row[ku] != null && String(row[ku]).trim() !== "") return row[ku];
    if (kl in row && row[kl] != null && String(row[kl]).trim() !== "") return row[kl];
  }
  return null;
}

// Map raw Power BI rows to our normalized schema
function normalizeRows(rows){
  return rows.map(r=>{
    // Match the labels you reported from the Power BI table
    const title       = headerPick(r, "Notice Title", "Title");
    const url         = headerPick(r, "URL", "Link", "Notice URL");
    const countryName = headerPick(r, "Country");
    const noticeType  = headerPick(r, "Type", "Notice Type");
    const projName    = headerPick(r, "Project Name", "Project");
    const projNo      = headerPick(r, "Project Number", "Project ID");
    const pub         = headerPick(r, "Publication Date", "Posted");
    const due         = headerPick(r, "Due Date", "Deadline", "Closing Date");
    const buyer       = headerPick(r, "Executing Agency", "Buyer", "Agency");
    const category    = headerPick(r, "Category");
    const method      = headerPick(r, "Method");
    const currency    = headerPick(r, "Currency") || null;
    const budget      = headerPick(r, "Budget", "Estimate") || null;

    const titleSafe   = (title || "").trim() || "(untitled)";
    const id          = `IDB:${stableIdFromURLorTitle(url, titleSafe)}`;

    const obj = {
      id,
      source: "IDB",
      noticeId: id.split(":")[1],
      projectId: projNo || null,
      projectName: projName || null,
      title: titleSafe,
      summary_original: null,
      summary_ko: null,

      countryName: countryName || null,                 // human-readable
      country: mapCountryToISO2(countryName || "") || null, // ISO2 (if known)

      sector: inferSectors(titleSafe),
      category: (category || "").trim() || classifyCategory(String(noticeType||"")),
      method: method || null,
      noticeType: classifyType(String(noticeType||"")),

      currency,
      budgetEstimate: toNum(budget),

      publicationDate: toISO(pub),
      deadline: toISO(due),

      buyer: buyer || null,
      city: null,
      language: ["en"],
      url: url || PAGE_URL,
      documents: [],
      lastSeenAt: new Date().toISOString(),
      hash: ""
    };

    obj.hash = sha256([obj.title, obj.countryName||"", obj.deadline||"", obj.url].join("|"));
    return obj;
  }).filter(x=>x.title && x.url);
}

function dedupeByIdHash(arr){ const m=new Map(); for(const n of arr){ const k=`${n.id}:${n.hash}`; if(!m.has(k)) m.set(k,n);} return [...m.values()]; }

/** ==================== POWER BI HELPERS ==================== **/
async function getPowerBIFrame(page){
  const handle = await page.$(IFRAME_CSS).catch(()=>null);
  if(handle){ const f = await handle.contentFrame(); if(f) return f; }
  for(const f of page.frames()){ if(f.url().includes("app.powerbi.com")) return f; }
  return null;
}

async function waitForPowerBiGrid(rootFrame, timeoutMs = GRID_WAIT_TIMEOUTMS) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const count =
        (await rootFrame.locator('[role="grid"]').count()) +
        (await rootFrame.locator('[role="table"]').count()) +
        (await rootFrame.locator('[aria-label*="table" i]').count());
      if (count > 0) return true;
    } catch {}
    await rootFrame.waitForTimeout(500);
  }
  return false;
}

async function nudgeRender(frame){
  try{
    await frame.evaluate(() => window.scrollTo(0, 0));
    await frame.waitForTimeout(300);
    await frame.evaluate(() => window.scrollBy(0, 800));
    await frame.waitForTimeout(600);
  }catch{}
}

/** ==================== PARSER ==================== **/
async function parseAccessibleTable(rootFrame){
  const frames = [rootFrame, ...rootFrame.childFrames(), ...rootFrame.childFrames().flatMap(f=>f.childFrames())].filter(Boolean);

  for(let i=0;i<frames.length;i++){
    const fr = frames[i];
    await nudgeRender(fr);

    try{
      // Collect headers
      const headerCells = await fr.locator('[role="columnheader"]').all();
      let headers = [];
      for(const h of headerCells) headers.push((await h.innerText().catch(()=>"" )).trim());

      // If no explicit headers, try first row as headers
      if(!headers.length){
        const firstRowCells = await fr.locator('[role="row"]').first().locator('[role="cell"], [role="gridcell"]').all();
        for(const c of firstRowCells) headers.push((await c.innerText().catch(()=>"" )).trim());
      }

      // Collect rows
      const rowLocs = await fr.locator('[role="row"]').all();
      const recs = [];
      for(const r of rowLocs.slice(1)){ // skip header row
        const cells = await r.locator('[role="cell"], [role="gridcell"]').all().catch(()=>[]);
        if(!cells.length) continue;
        const vals = [];
        for(const c of cells) vals.push((await c.innerText().catch(()=>"" )).trim().replace(/\s+/g," "));
        if(vals.some(v=>v)){
          const obj = {};
          headers.forEach((h,idx)=>obj[h || `col_${idx}`] = vals[idx] ?? "");
          recs.push(obj);
        }
      }

      if(recs.length) {
        // One-time header sample for verification
        console.log("HEADERS SAMPLE:", Object.keys(recs[0]));
        return recs;
      }

      // Debug per-frame if nothing parsed
      await fr.page().screenshot({ path: path.join(DEBUG_DIR, `fallback-grid-${i}.png`), fullPage:true }).catch(()=>{});
    }catch{}
  }
  return [];
}

/** ==================== MAIN ==================== **/
(async () => {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    locale: 'en-US',
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36'
  });
  const page = await context.newPage();

  await page.goto(PAGE_URL, { waitUntil: "domcontentloaded", timeout: 60000 });
  await sleep(PAGE_LOAD_WAIT_MS);

  const powerBI = await getPowerBIFrame(page);
  if(!powerBI) throw new Error("Power BI iframe not found (embed may be slow)");

  let rawRows = [];
  for (let attempt = 1; attempt <= RETRIES; attempt++) {
    const ready = await waitForPowerBiGrid(powerBI, GRID_WAIT_TIMEOUTMS);
    if (!ready) {
      if (attempt === RETRIES) throw new Error("Power BI grid not visible after waiting");
      await sleep(1500 * attempt);
      continue;
    }

    rawRows = await parseAccessibleTable(powerBI);
    if (rawRows.length) break;

    if (attempt < RETRIES) await sleep(1500 * attempt);
  }

  const notices = dedupeByIdHash(normalizeRows(rawRows || []));
  const today = new Date().toISOString().slice(0,10);
  fs.writeFileSync(path.join(OUT_DIR, "notices.json"), JSON.stringify(notices,null,2));
  fs.writeFileSync(path.join(OUT_DIR, `notices-${today}.json`), JSON.stringify(notices,null,2));
  console.log(`OK: ${notices.length} notices → ${path.join("data","notices.json")}`);

  await browser.close();
})().catch(e => { console.error(e); process.exit(1); });
