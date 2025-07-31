### **Full patch‑list for Abdul’s prototype**

(each bullet tells you what to change, where in your script, and why Vanessa is seeing the errors)

---

#### **1  |  Data‑prep corrections**

| Problem in current run | Fix to apply |
| ----- | ----- |
| Loads outside Q2 are still being grouped (March & July rows drag the wrong trips into Q2). | Line 1 in your pipeline:pcs \= pcs\[(pcs\['PU Date F'\] \>= '2025‑04‑01') & (pcs\['PU Date F'\] \<= '2025‑06‑30')\]Do this before any grouping or filtering. |
| Five‑digit “permit cards” don’t match the 4‑digit inventory Unit field → company lookup fails → rows drop incorrectly. | After you .str.strip() the Truck column add:pcs\['Truck\_clean'\] \= pcs\['Truck'\].str\[:4\]Run the inventory VLOOKUP on Truck\_clean. |
| Owner‑operators with ‘OP’ in the Truck column slip through when Trailer is numeric. | Keep your OP filter but expand it:pcs \= pcs\[\~pcs\['Truck'\].str.contains(r'\\bOP\\b', na=False, case=False)\] |

---

#### **2  |  Trip grouping & Ref No. logic**

| Problem | Code amendment |
| ----- | ----- |
| Same‑trailer rows months apart share the integer because your rule ignores calendar gaps. | In your “same‑trailer” test add a gap filter:gap \= (cur\['PU Date F'\] \- prev\['Del Date F'\]).daysif same\_trailer and gap \<= 3: keep integer; else new integer. |
| CA tails are collapsed into .1, deleting CA miles (“CA Consolidation” bullet). | Remove that consolidation step entirely—every loaded leg keeps its own decimal; CA → TX is .1, CA → AL is .2, etc. |
| Route optimisation shuffles stops → miles don’t reconcile with Direct Freight. | Delete the nearest‑neighbour block. Trips must stay in the exact chronological order of PCS PU dates so state sequences match auditors’ ELD view. |
| AZ/NV trips stay open and never add CA empty miles. | After writing each loaded row, check:if row\['Cons St'\] in ('AZ','NV') and not future\_ca\_delivery():  add\_virtual\_leg(row\['Cons City'\], row\['Cons St'\], 'Rex Cole Rd, San Bernardino CA')  set row\['Note'\] \+= ' – Inbound (empty ' \+ row\['Cons St'\] \+ '→CA)'  close\_trip() |

---

#### **3  |  HERE routing parameters**

| Bad output symptom | Parameter fix |
| ----- | ----- |
| Total miles off by \~2 000 mi | transportMode=truck & routingMode=fast (you used car defaults). |
| Missing state splits | Add return=summary,polyline,spans=state and read sections\[0\]\['spans'\] for per‑state mileage—no GIS overlay needed. |
| Large CA mileage missing | Pass only City, ST strings ("CITY,CA") to HERE; strip street/warehouse IDs that often geocode to the wrong side of a state line. |

---

#### **4  |  Validation loop (catch errors before full run)**

1. Pick three representative trips Vanessa cited (e.g. Loads 174418/174520, 175029‑031‑150).

2. Run them alone through the pipeline.

3. Compare your state splits to Direct Freight; if any state differs by \> 5 %, adjust the routing flags (often avoid\[features\]=tollRoad or fix origin/consignee spelling).

4. Repeat until the sample passes, then unlock the full Q2 batch.

---

#### **5  |  Reporting tweaks**

* Ref No. column stays blank for rows you drop (OP, intrastate, outside Q2).

* Miles column shows “GEOCODE\_ERR” and logs the Load \# if HERE returns status ≠ 200; handle these manually.

* Output filename: output/Q2\_2025\_state\_miles\_v2.xlsx so Vanessa can compare versions.

---

### **What success looks like**

* Every Ref No. is between 1.x and n.x and all PU/DEL dates fall inside 1 Apr – 30 Jun 2025.

* Direct Freight totals for the three test trips match your HERE totals within ±5 %.

* CA miles now appear on loads ending back in CA and on empty AZ/NV returns.

Apply the amendments above, rerun the three‑trip smoke test, then the whole quarter. Vanessa should see all the discrepancies disappear.

