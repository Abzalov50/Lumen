(in-package :cl)

(defpackage :lumen.dev.inspector
  (:use :cl)
  (:import-from :lumen.core.http :respond-html :req-params)
  (:import-from :lumen.obs.metrics :get-path-stats)
  (:import-from :lumen.dev.module :find-module :get-modules
   :module-meta-path-prefix :module-meta-name :module-meta-doc
	   :module-meta-entities :module-meta-resources :module-meta-routes)
  (:export :mount-inspector!))

(in-package :lumen.dev.inspector)

;;; ---------------------------------------------------------------------------
;;; 1. DATA HELPERS
;;; ---------------------------------------------------------------------------

(defun get-unique-routes ()
  "Récupère les routes uniques (Method + Path) depuis le routeur."
  (let ((seen (make-hash-table :test 'equal))
        (unique-list '())
        (routes-vec (if (boundp 'lumen.core.router::*routes*)
                        (symbol-value 'lumen.core.router::*routes*)
                        #())))
    (loop for i downfrom (1- (length routes-vec)) to 0
          for r = (aref routes-vec i)
          do (ignore-errors
               (let* ((m (lumen.core.router::route-method r))
                      (p (lumen.core.router::route-source-path r)))
                 (when (and m p)
                   (let ((k (format nil "~A ~A" m p)))
                     (unless (gethash k seen)
                       (setf (gethash k seen) t)
                       (push r unique-list)))))))
    unique-list))

(defun find-module-for-path (path)
  (unless (stringp path) (return-from find-module-for-path nil))
  (let ((modules (lumen.dev.module:get-modules))
        (best-match nil)
        (max-len -1))
    (dolist (m modules)
      (let* ((prefix (lumen.dev.module:module-meta-path-prefix m))
             (len (length prefix)))
        (when (and (lumen.utils:str-prefix-p prefix path)
                   (> len max-len)
                   (or (= (length path) len)
                       (char= (char path len) #\/)))
          (setf max-len len
                best-match (lumen.dev.module:module-meta-name m)))))
    best-match))

(defun find-route-summary (method path)
  (ignore-errors 
    (let ((entry (find-if (lambda (x) 
                            (and (string= (getf x :method) method)
                                 (string= (getf x :path) path)))
                          lumen.http.crud::*custom-route-registry*)))
      (getf entry :summary))))

;;; ---------------------------------------------------------------------------
;;; 2. FRONTEND ASSETS
;;; ---------------------------------------------------------------------------

(defparameter *svg-icons*
  (list :module "<svg xmlns='http://www.w3.org/2000/svg' width='16' height='16' viewBox='0 0 24 24' fill='none' stroke='currentColor' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'><path d='M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z'></path><polyline points='3.27 6.96 12 12.01 20.73 6.96'></polyline><line x1='12' y1='22.08' x2='12' y2='12'></line></svg>"
        :search "<svg xmlns='http://www.w3.org/2000/svg' width='16' height='16' viewBox='0 0 24 24' fill='none' stroke='currentColor' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'><circle cx='11' cy='11' r='8'></circle><line x1='21' y1='21' x2='16.65' y2='16.65'></line></svg>"
        :check  "<svg xmlns='http://www.w3.org/2000/svg' width='14' height='14' viewBox='0 0 24 24' fill='none' stroke='currentColor' stroke-width='3' stroke-linecap='round' stroke-linejoin='round'><polyline points='20 6 9 17 4 12'></polyline></svg>"
        :sort   "<svg class='sort-icon' xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='none' stroke='currentColor' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'><path d='M7 15l5 5 5-5'/><path d='M7 9l5-5 5 5'/></svg>"))

(defparameter *inspector-css* "
  :root { --bg: #f3f4f6; --card: #ffffff; --primary: #4f46e5; --text: #1f2937; --border: #e5e7eb; --success: #10b981; }
  body { font-family: 'Inter', system-ui, -apple-system, sans-serif; background: var(--bg); color: var(--text); padding: 2rem; margin:0; padding-bottom: 80px; }
  .container { max-width: 1400px; margin: 0 auto; }
  
  h1 { display: flex; align-items: center; gap: 10px; font-size: 1.5rem; margin-bottom: 1.5rem; color: #111827; }
  .card { background: var(--card); border-radius: 12px; box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1); padding: 1.5rem; margin-bottom: 2rem; border: 1px solid var(--border); }
  
  .mod-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(240px, 1fr)); gap: 1rem; }
  .mod-card { 
      border: 1px solid var(--border); padding: 1rem; border-radius: 8px; cursor: pointer; transition: all 0.2s; background: white; 
      position: relative; overflow: hidden; display: flex; flex-direction: column; gap: 4px;
  }
  .mod-card:hover { border-color: var(--primary); transform: translateY(-1px); box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1); }
  .mod-card.active { background: #eef2ff; border-color: var(--primary); ring: 2px solid var(--primary); }
  .mod-icon { color: #9ca3af; margin-bottom: 4px; }
  .mod-card.active .mod-icon { color: var(--primary); }
  .check-icon { position: absolute; top: 10px; right: 10px; color: var(--primary); opacity: 0; transition: opacity 0.2s; }
  .mod-card.active .check-icon { opacity: 1; }

  .controls { display: flex; gap: 10px; margin-bottom: 15px; }
  .input-group { position: relative; flex-grow: 1; display: flex; align-items: center; }
  .input-icon { position: absolute; left: 12px; color: #9ca3af; }
  .search-input { width: 100%; padding: 10px 10px 10px 36px; border: 1px solid #d1d5db; border-radius: 6px; font-size: 14px; outline: none; transition: border-color 0.15s; }
  .search-input:focus { border-color: var(--primary); box-shadow: 0 0 0 2px rgba(79, 70, 229, 0.1); }
  .limit-select { padding: 0 12px; border: 1px solid #d1d5db; border-radius: 6px; background: white; cursor: pointer; font-size: 14px; }

  .table-wrapper { overflow-x: auto; border: 1px solid var(--border); border-radius: 8px; }
  table { width: 100%; border-collapse: collapse; font-size: 0.875rem; min-width: 900px; }
  th { text-align: left; padding: 12px 16px; background: #f9fafb; border-bottom: 1px solid var(--border); font-weight: 600; color: #6b7280; cursor: pointer; user-select: none; transition: background 0.1s; }
  th:hover { background: #f3f4f6; color: #111827; }
  th svg.sort-icon { vertical-align: middle; opacity: 0.3; margin-left: 4px; }
  th.sort-asc svg.sort-icon, th.sort-desc svg.sort-icon { opacity: 1; color: var(--primary); }
  td { padding: 12px 16px; border-bottom: 1px solid var(--border); vertical-align: middle; color: #374151; }
  tr:hover { background-color: #f9fafb; }
  
  .badge { padding: 2px 8px; border-radius: 4px; font-weight: 600; font-size: 0.7rem; text-transform: uppercase; display: inline-block; min-width: 55px; text-align: center; letter-spacing: 0.05em; }
  .GET { background: #eff6ff; color: #1e40af; border: 1px solid #bfdbfe; } 
  .POST { background: #ecfdf5; color: #065f46; border: 1px solid #a7f3d0; }
  .PUT, .PATCH { background: #fff7ed; color: #9a3412; border: 1px solid #fed7aa; } 
  .DELETE { background: #fef2f2; color: #991b1b; border: 1px solid #fecaca; }
  
  .metric-bar { height: 6px; background: #e5e7eb; border-radius: 3px; width: 60px; display: inline-block; margin-right: 10px; overflow: hidden; vertical-align: middle; }
  .metric-fill { height: 100%; background: var(--primary); border-radius: 3px; }
  .module-tag { background: #e0e7ff; color: #4338ca; border-radius: 12px; padding: 2px 10px; font-size: 0.75rem; font-weight: 600; display: inline-block; }
  .no-mod { color: #9ca3af; font-style: italic; font-size: 0.8rem; }
  .err-high { color: #dc2626; font-weight: bold; }
  
  .pagination { display: flex; justify-content: space-between; align-items: center; margin-top: 20px; padding-top: 15px; border-top: 1px solid var(--border); }
  .btn { padding: 8px 16px; border: 1px solid #d1d5db; background: white; border-radius: 6px; cursor: pointer; font-size: 0.875rem; font-weight: 500; color: #374151; transition: all 0.15s; }
  .btn:hover:not(:disabled) { background: #f9fafb; border-color: #9ca3af; }
  .btn:disabled { opacity: 0.5; cursor: not-allowed; background: #f3f4f6; }
")

(defparameter *inspector-js* "
<script>
  var appState = { 
    page: 1, 
    limit: 50, 
    filterModule: null, 
    filterSearch: '', 
    sortCol: 2, 
    sortAsc: true,
    rows: [] 
  };

  function initInspector() {
    var tbody = document.querySelector('#routesTable tbody');
    if(!tbody) return;
    appState.rows = Array.from(tbody.querySelectorAll('tr'));
    
    // Tri initial (silencieux)
    sortTable(2, true); 
    
    // IMPORTANT: Mise à jour immédiate de l'UI (pagination, textes)
    renderTable(); 
  }

  function selectModule(name, el) {
    document.querySelectorAll('.mod-card').forEach(function(c){ c.classList.remove('active'); });
    if (appState.filterModule === name) {
      appState.filterModule = null; 
    } else {
      appState.filterModule = name;
      if(el) el.classList.add('active');
    }
    appState.page = 1;
    renderTable();
  }

  function updateSearch(val) {
    appState.filterSearch = val.toUpperCase();
    appState.page = 1;
    renderTable();
  }
  
  function changeLimit(val) {
    appState.limit = parseInt(val);
    appState.page = 1;
    renderTable();
  }

  function changePage(delta) {
    appState.page += delta;
    renderTable();
  }

  function sortTable(colIndex, silent) {
    if (!silent) {
      if (appState.sortCol === colIndex) {
        appState.sortAsc = !appState.sortAsc;
      } else {
        appState.sortCol = colIndex;
        appState.sortAsc = true;
      }
    }

    var headers = document.querySelectorAll('#routesTable th');
    headers.forEach(function(th, idx) {
      th.classList.remove('sort-asc', 'sort-desc');
      if (idx === colIndex) {
        th.classList.add(appState.sortAsc ? 'sort-asc' : 'sort-desc');
      }
    });

    appState.rows.sort(function(a, b) {
      var valA = getVal(a, colIndex);
      var valB = getVal(b, colIndex);
      
      var numA = parseFloat(valA);
      var numB = parseFloat(valB);
      
      if (!isNaN(numA) && !isNaN(numB) && colIndex >= 4) {
         return appState.sortAsc ? (numA - numB) : (numB - numA);
      }
      
      if (valA < valB) return appState.sortAsc ? -1 : 1;
      if (valA > valB) return appState.sortAsc ? 1 : -1;
      return 0;
    });

    // Si silent est true, on ne render pas ICI, mais l'appelant (init) le fera.
    if (!silent) renderTable();
  }

  function getVal(row, idx) {
    var cell = row.children[idx];
    if (cell.hasAttribute('data-val')) return cell.getAttribute('data-val');
    return cell.innerText.trim().toUpperCase();
  }

  function renderTable() {
    // 1. Filtrage
    var filtered = appState.rows.filter(function(row) {
      var matchMod = !appState.filterModule || row.getAttribute('data-module') === appState.filterModule;
      var matchTxt = !appState.filterSearch || row.innerText.toUpperCase().indexOf(appState.filterSearch) > -1;
      return matchMod && matchTxt;
    });

    // 2. Calcul Pagination
    var total = filtered.length;
    var maxPage = Math.ceil(total / appState.limit) || 1;
    if (appState.page > maxPage) appState.page = maxPage;
    if (appState.page < 1) appState.page = 1;

    var start = (appState.page - 1) * appState.limit;
    var end = start + appState.limit;
    var pageItems = filtered.slice(start, end);

    // 3. Mise à jour Tableau
    appState.rows.forEach(function(r) { r.style.display = 'none'; });
    var tbody = document.querySelector('#routesTable tbody');
    pageItems.forEach(function(r) { 
      r.style.display = ''; 
      tbody.appendChild(r);
    });

    // 4. Mise à jour Contrôles Pagination
    var elInfo = document.getElementById('pageInfo');
    if(elInfo) elInfo.innerText = 'Page ' + appState.page + ' / ' + maxPage + ' (' + total + ' routes)';
    
    var btnPrev = document.getElementById('btnPrev');
    if(btnPrev) btnPrev.disabled = (appState.page === 1);
    
    var btnNext = document.getElementById('btnNext');
    if(btnNext) btnNext.disabled = (appState.page === maxPage);
  }

  document.addEventListener('DOMContentLoaded', initInspector);
</script>
")

;;; ---------------------------------------------------------------------------
;;; 3. RENDU
;;; ---------------------------------------------------------------------------

(defun render-modules-card (s)
  (format s "<div class='card'><h2>~A Modules</h2>" (getf *svg-icons* :module))
  (format s "<div class='mod-grid'>")
  (ignore-errors
   (dolist (m (lumen.dev.module:get-modules))
      (let ((name (string-downcase (lumen.dev.module:module-meta-name m))))
        (format s "<div class='mod-card' onclick='selectModule(\"~A\", this)'>
                     <div class='check-icon'>~A</div>
                     <div style='font-weight:700; font-size:1.05em; color:#111827'>~A</div>
                     <div style='font-family:monospace; color:#4f46e5; font-size:0.85em;'>~A</div>
                     <div style='font-size:0.8em; color:#6b7280; margin-top:auto'>
                        ~A tables &bull; ~A routes
                     </div>
                   </div>"
                name 
                (getf *svg-icons* :check)
                name
                (lumen.dev.module:module-meta-path-prefix m)
                (length (lumen.dev.module:module-meta-entities m))
                (length (lumen.dev.module:module-meta-routes m))))))
  (format s "</div></div>"))

(defun safe-render-rows (s)
  (let ((routes (handler-case (get-unique-routes) 
                  (error () nil))))
    
    (dolist (r routes)
      (handler-case
          (let* ((m (or (lumen.core.router::route-method r) "UNK")) 
                 (p (or (lumen.core.router::route-source-path r) "/?"))
                 (module-name (find-module-for-path p))
                 (mod-str (if module-name (string-downcase module-name) "-"))
                 (summary (or (find-route-summary m p) ""))
                 ;; Metrics
                 (stats   (ignore-errors (lumen.obs.metrics:get-path-stats m p)))
                 (count   (or (getf stats :count) 0))
                 (total-ms (or (getf stats :sum-ms) 0.0))
                 (errors  (or (getf stats :errors) 0))
                 (avg-ms  (if (> count 0) (/ total-ms count) 0.0))
                 (err-rate (if (> count 0) (* 100.0 (/ errors count)) 0.0)))

            (format s "<tr data-module='~A'>" mod-str)
            
            ;; 0. Module
            (format s "<td><span class='~A'>~A</span></td>" 
                    (if module-name "module-tag" "no-mod") mod-str)
            
            ;; 1. Method
            (format s "<td><span class='badge ~A'>~A</span></td>" m m)
            
            ;; 2. Path
            (format s "<td style='font-family:monospace; font-weight:600; color:#333'>~A</td>" p)
            
            ;; 3. Summary
            (format s "<td style='color:#666;'>~A</td>" summary)
            
            ;; 4. Reqs
            (format s "<td class='metric' data-val='~D'>~D</td>" count count)
            
            ;; 5. Latency
            (format s "<td class='metric' data-val='~F'><div class='metric-bar'><div class='metric-fill' style='width:~D%'></div></div>~,1F ms</td>"
                    avg-ms (min 100 (floor avg-ms)) avg-ms)
            
            ;; 6. Error
            (format s "<td class='metric ~A' data-val='~F'>~,1F%</td>" 
                    (if (> err-rate 0) "err-high" "") err-rate err-rate)
            
            (write-string "</tr>" s))
        
        (error (c)
          (format s "<tr><td colspan='7' style='color:red'>Render Error: ~A</td></tr>" c))))))

(defun render-dashboard-page ()
  (with-output-to-string (s)
    (write-string "<!doctype html><html><head><title>Lumen Cockpit</title>" s)
    (write-string "<meta charset='utf-8'>" s)
    (format s "<style>~A</style>" *inspector-css*)
    (write-string *inspector-js* s)
    (write-string "</head><body><div class='container'>" s)
    
    (format s "<h1>~A Lumen Cockpit</h1>" (getf *svg-icons* :module))

    (render-modules-card s)

    (write-string "<div class='card'>" s)
    
    (write-string "<div class='controls'>" s)
    (format s "<div class='input-group'>
                 <div class='input-icon'>~A</div>
                 <input type='text' class='search-input' onkeyup='updateSearch(this.value)' placeholder='Rechercher...'>
               </div>" (getf *svg-icons* :search))
    
    (write-string "<select class='limit-select' onchange='changeLimit(this.value)'>
         <option value='10'>10 / page</option>
         <option value='50' selected>50 / page</option>
         <option value='100'>100 / page</option>
         <option value='9999'>Tout</option>
       </select>
    </div>" s)

    (write-string "<div class='table-wrapper'>" s)
    (let ((icon (getf *svg-icons* :sort)))
      (format s "<table id='routesTable'><thead><tr>
          <th onclick='sortTable(0)'>Module ~A</th>
          <th onclick='sortTable(1)'>Method ~A</th>
          <th onclick='sortTable(2)'>Route ~A</th>
          <th onclick='sortTable(3)'>Summary ~A</th>
          <th onclick='sortTable(4)'>Reqs ~A</th>
          <th onclick='sortTable(5)'>Lat (avg) ~A</th>
          <th onclick='sortTable(6)'>Err % ~A</th>
        </tr></thead><tbody>" icon icon icon icon icon icon icon))

    (safe-render-rows s)

    (write-string "</tbody></table></div>" s) 

    ;; BOUTONS PREV DÉSACTIVÉ PAR DÉFAUT
    (write-string "<div class='pagination'>
       <button id='btnPrev' class='btn' onclick='changePage(-1)' disabled>&#8592; Prev</button>
       <span id='pageInfo' style='color:#666; font-weight:500; font-size:0.9rem'>Chargement...</span>
       <button id='btnNext' class='btn' onclick='changePage(1)'>Next &#8594;</button>
    </div>" s)
    
    (write-string "</div></div></body></html>" s)))

(defun mount-inspector! (&key (path "/_inspector"))
  (lumen.core.router:defroute :GET path (req)
    (respond-html (render-dashboard-page))))
