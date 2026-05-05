# IMQ Worker Thread Monitoring & Self-Recycling

**Status**: implemented (single-phase delivery, thread-delta semantics). Tests and follow-ups pending — see end of document.

## Context

### Point de départ

Muppy voit apparaître en production des exceptions `RuntimeError: can't start new thread` qui tuent des workers IMQ de manière inopinée et propagent l'erreur à **toutes les tasks** du process, pas seulement la fautive. Symptôme classique d'exhaustion `RLIMIT_NPROC`.

La cause première connue (en cours de résolution séparée) est la gestion des connexions `fabric2` dans les fabric tasks : certaines connexions ne ferment pas proprement et laissent derrière elles des threads paramiko liés au channel SSH (typiquement 2-4 threads : Connection + Transport + Packetizer + éventuellement Channel). Ces threads s'accumulent au fil des messages et finissent par saturer le quota `RLIMIT_NPROC` du user qui exécute les workers.

Cette feature est un **filet de sécurité** : elle observe la dérive, l'expose via métriques et logs, et recycle le worker proprement avant qu'il ne devienne dangereux. Elle **ne corrige pas** la fuite Fabric — c'est un travail distinct — elle la **contient**.

### Architecture IMQ pertinente

- [parts/inouk_addons/inouk_message_queue/cli/imq_worker.py](parts/inouk_addons/inouk_message_queue/cli/imq_worker.py) : CLI `imq-worker`. Exposait déjà `--max-messages` et `--max-rss-memory` ; on ajoute `--max-thread-delta`, `--thread-warn-percent`, `--status-summary-interval-s`.
- [parts/inouk_addons/inouk_message_queue/workers/standalone.py](parts/inouk_addons/inouk_message_queue/workers/standalone.py) : boucle principale. Modèle d'exécution **strictement synchrone** — un seul message à la fois, pas de pool de threads. Les limites existantes sont vérifiées en début d'itération et déclenchent un `break` propre. Même pattern pour les threads.
- [parts/inouk_addons/inouk_message_queue/workers/monitoring.py](parts/inouk_addons/inouk_message_queue/workers/monitoring.py) : `MemoryMonitor` a servi de modèle à `ThreadMonitor`. `MetricsCollector` Prometheus était déjà en place.
- Pas de thread pool IMQ : l'architecture synchrone rend le comptage **sans ambiguïté** — tout thread vivant entre deux tasks au-delà du baseline est un résidu.

### Conséquence architecturale

Comme le worker est mono-thread d'exécution, la **fin d'une task** est le point de mesure idéal : la task vient de libérer ce qu'elle devait libérer, la suivante n'est pas encore démarrée, le compte de threads reflète le résidu. Même logique que `--max-rss-memory`, checké à chaque itération.

## Sémantique : delta, pas absolu

Le seuil de recyclage est un **delta vs. baseline**, pas une valeur absolue. On mesure `os_threads - baseline_os_threads` où `baseline_os_threads` est capturé **une fois** au démarrage du worker, après mise en route du serveur d'observability et avant la première task.

Pourquoi delta plutôt qu'absolu :

- **Portable entre environnements** : le baseline varie (nombre de queues, observability activée ou non, version d'Odoo) ; un seuil absolu calibré sur un worker casse sur un autre.
- **Lecture directe** : `delta = 20` → « ce worker a accumulé 20 threads qu'il n'avait pas au démarrage ». C'est littéralement la fuite.
- **Même seuil dev/staging/prod** : un défaut universel est réaliste, là où un seuil absolu demande calibrage par déploiement.

## Choix techniques

- **`psutil.Process().num_threads()` comme source de vérité** (threads OS) plutôt que `threading.active_count()` (threads Python). Paramiko crée des threads en code natif que `threading.active_count()` peut sous-compter.
- **FDs instrumentés en parallèle** via `psutil.num_fds()` (gauge + delta, pas de seuil de recyclage actuellement). Une fuite Fabric laisse souvent socket + thread en paire ; l'exhaustion FD tue un worker plus salement.
- **Default `0` = désactivé** (comme `--max-rss-memory`). Les workers existants conservent strictement leur comportement actuel tant qu'on n'active pas explicitement `--max-thread-delta`.
- **Pas de limite hard** : on ne peut pas empêcher la création d'un thread en Python sans patcher le runtime. C'est un seuil de **recyclage** (analogue à `gunicorn --max-requests`), pas une barrière.
- **Summary périodique wall-clock** : `--status-summary-interval-s` temporel (défaut 300 s), pas `processed_count % N`. Les tasks IMQ vont de 500 ms à plusieurs heures (backups) ; un compteur de messages ne donne pas un rythme régulier.

## CLI

```
--max-thread-delta N            # (int, default 0) Exit worker post-task when
                                # (os_threads - baseline_os_threads) > N.
                                # Requires external supervisor to restart.
                                # 0 = disabled (no thread-based exit).

--thread-warn-percent P         # (int, default 50) Log post-task sample at INFO
                                # when thread_delta >= P% of --max-thread-delta.
                                # Otherwise DEBUG. Ignored if --max-thread-delta=0.

--status-summary-interval-s S   # (int, default 300) Wall-clock interval between
                                # periodic status summary logs. 0 disables
                                # (shutdown + leak-detection summaries still fire).
```

Validations : `max_thread_delta >= 0`, `0 < thread_warn_percent <= 100`, `status_summary_interval_s >= 0`.

## Observability

### Prometheus metrics

| Metric | Type | Description |
|---|---|---|
| `imq_worker_thread_count` | gauge | Python threads actifs (`threading.active_count()`) |
| `imq_worker_os_thread_count` | gauge | Threads OS (`psutil.num_threads()`) — **source de vérité** |
| `imq_worker_baseline_os_thread_count` | gauge | Baseline capturé au démarrage |
| `imq_worker_thread_delta` | gauge | `os_thread_count - baseline_os_thread_count` — **métrique pivot** |
| `imq_worker_fd_count` | gauge | FDs ouverts (`psutil.num_fds()`) |
| `imq_worker_baseline_fd_count` | gauge | Baseline FDs |
| `imq_worker_fd_delta` | gauge | `fd_count - baseline_fd_count` |
| `imq_worker_max_thread_delta` | gauge | Valeur configurée de `--max-thread-delta` (0 = disabled) |

Pas (encore) exposés en gauge mais dans `/status` et les logs : `os_threads_max` (kernel), `user_nproc_soft/hard` (RLIMIT_NPROC), `user_threads_usage` (somme threads tous procs du user).

### Endpoint `/status`

`HealthChecker.get_detailed_status()` ([workers/monitoring.py](parts/inouk_addons/inouk_message_queue/workers/monitoring.py)) expose un bloc `threads` :

```json
"threads": {
  "enabled": true,
  "python_threads": 3,
  "os_threads": 12,
  "thread_delta": 9,
  "baseline_os_threads": 3,
  "fds": 47,
  "fd_delta": 29,
  "baseline_fds": 18,
  "max_thread_delta": 20,
  "warn_threshold_delta": 10
}
```

Plus un flag `limits.thread_limit_reached: bool` au même niveau que `memory_limit_reached`.

### Logs

- **Post-task sans fuite** : log à chaque task `processed`/`failed` (pas `empty`). DEBUG par défaut, INFO quand `thread_delta >= warn_threshold_delta`. Format : `Post-task threads: os=N (delta=M) py=X fds=Y (fd_delta=Z) queue=Q`.
- **Post-task avec fuite** (per-task leak) : si `os_threads` a crû entre la fin de la task précédente (ou le baseline pour la toute première task) et la fin de la task courante, on émet un log `WARNING "Thread leak detected on last task: queue=Q leaked=+N (os_threads X -> Y, cumulative delta=D, fd_delta=F)"` **puis** on dump le résumé complet via `_log_status_summary(level=WARNING)`. Volume à surveiller en fuite chronique ; rate-limit possible à ajouter si bruyant.
- **Status summary** (périodique toutes les `--status-summary-interval-s` + shutdown) : bloc `Threads`, `FDs`, `System Thread Ceiling` (os_max + % du worker), `User Thread Usage` (somme user + proc_count + % de `nproc_soft`), et `Thread Delta Limit`. Niveau paramétrable (INFO par défaut, WARNING en cas de fuite détectée).
- **Recyclage cumulatif** : log WARNING au moment de l'exit : `Thread delta limit exceeded: os_threads=X baseline=Y delta=+D (max_delta=+N), exiting for recycling`.

### Two independent detection signals

Distinguer les deux mécanismes :

| Signal | Référence | Action | But |
|---|---|---|---|
| **Per-task leak** | `os_threads` à la task n vs. task n-1 | WARNING + summary | Identifier **quelle queue/task** fuit (signal diagnostic) |
| **Cumulative delta** | `os_threads` vs. baseline worker | Exit + restart | Contenir la fuite avant exhaustion (signal opérationnel) |

Les deux peuvent coexister : une fuite chronique à +1 thread/task produira un flot de WARNING per-task et déclenchera le recyclage une fois `max_thread_delta` atteint.

## Calibration

### Pourquoi calibrer

`RLIMIT_NPROC` est **par-user**, partagé entre tous les procs de l'user `muppy` (tous les workers, `mpy-srv`, les fabric tasks actives, etc.). Un worker qui fuite à lui seul peut consommer le budget collectif.

### Formule pragmatique

```
max_thread_delta_par_worker ≈ (RLIMIT_NPROC - baseline_cluster) / (replicas × safety_factor)

avec :
  baseline_cluster ≈ somme threads startup de tous procs de l'user
  safety_factor    ≈ 3-4 (marge pour pics simultanés + restart gap)
```

### Ordres de grandeur par déploiement

Pour **8 workers** (cas prod courant) :

| `RLIMIT_NPROC` | Budget fuite cluster | Budget par worker (÷8) | `max_thread_delta` conseillé |
|---|---|---|---|
| 4096 | ~3000 | ~375 | **80-120** |
| 8192 | ~7000 | ~875 | **200-300** |
| 16384 | ~15000 | ~1800 | **400-500** |
| 65536+ | non contraignant | — | viser `p95(thread_delta) × 2`, plancher 50 |

### Procédure

1. **Vérifier `RLIMIT_NPROC` réel** dans l'environnement cible. Muppy ne tourne que sur Ubuntu (host systemd ou pod Ubuntu) — les commandes ci-dessous sont celles à utiliser :

   ```bash
   # Ubuntu host — sanity check rapide dans ton shell (montre la limite
   # de TON shell, pas forcément celle du worker si systemd override) :
   ulimit -u

   # Ubuntu host, worker sous systemd — ce que le unit impose vraiment :
   systemctl show imq-worker@<instance>.service \
       --property=LimitNPROC,LimitNPROCSoft

   # Ubuntu host ou pod — valeur effective pour un process qui tourne
   # (source de vérité, prend en compte toutes les couches) :
   cat /proc/$(pgrep -f 'imq-worker')/limits | grep "Max processes"

   # Pod Ubuntu en K8s :
   kubectl exec <pod> -- sh -c 'cat /proc/1/limits | grep "Max processes"'

   # Ou plus simple : le log de démarrage du worker crache la valeur
   #   "System thread limits: os_max=..., user_nproc=<soft>/<hard>"
   ```

   Si les valeurs divergent entre `ulimit -u` shell et `/proc/<pid>/limits` worker, c'est `/proc/<pid>/limits` qui fait foi (c'est la limite active pour le process).
2. **Déployer désactivé** (`--max-thread-delta=0`) sur une phase d'observation — les métriques et logs DEBUG/INFO sont déjà là, la gauge `imq_worker_thread_delta` remonte.
3. **Observer 7-14 jours**. Par worker :
   - `baseline_os_threads` (gauge au T0),
   - `p95` et `max` de `thread_delta`,
   - fréquence et ampleur des WARNING `Thread leak detected on last task`.
4. **Activer** avec `--max-thread-delta = max(p95 × 2, 50)` capé par le budget cluster ci-dessus.
5. **Surveiller** la fréquence des WARNING `Thread delta limit exceeded, exiting for recycling`.
   - **< 2 recyclages/h/worker** : bon point.
   - **\> 5 recyclages/h/worker** : le problème est la fuite, pas le seuil — investiguer les tasks coupables via les WARNING per-task.
   - **Jamais déclenché sur 7 jours** : seuil trop haut, divise par 2.

### Point de départ raisonnable sans données

Avec 8 workers, `RLIMIT_NPROC` inconnu (à vérifier), **démarrer à `--max-thread-delta 80`**. Conservateur même si `RLIMIT_NPROC=4096`.

## Orchestration

### Superviseur requis

Le self-recycling suppose un superviseur qui relance le worker après exit :

- **K8s** : `Deployment` avec `restartPolicy: Always` suffit. Templates/charts existants du module `inouk_message_queue` dans [parts/inouk_addons/inouk_message_queue/k8s/](parts/inouk_addons/inouk_message_queue/k8s/) à revoir pour documenter l'usage combiné avec `--max-thread-delta`.
- **systemd** : `Restart=always` + `RestartSec=5s` dans le template systemd du worker. Auditer les templates Muppy (`muppy_dev_server` et consorts) pour confirmer avant activation en prod.
- **Dev local** : désactivé par défaut, pas de régression.

### Configuration de `RLIMIT_NPROC`

Configurer la limite au niveau de l'orchestrateur (ne pas la modifier depuis le code du worker) :

- **K8s** : `spec.template.spec.containers[].resources.limits.pids` (cgroup pids controller).
- **systemd** : `LimitNPROC=` dans le unit file.
- **Docker** : `--ulimit nproc=X`.

Lecture côté worker via `resource.getrlimit(resource.RLIMIT_NPROC)` dans `ThreadMonitor._read_system_limits()` — suffisant, déjà en place.

## Implémentation livrée

### `parts/inouk_addons/inouk_message_queue/workers/monitoring.py`

- Nouvelle classe `ThreadMonitor(max_thread_delta=0, warn_percent=50)` :
  - `capture_baseline()` : à appeler **après** `observability_server.start()` (pour compter son thread daemon dans le baseline) et **avant** la première task.
  - `sample()` : renvoie un dict complet (threads py/os/baseline/delta + fds/baseline/delta + max & warn thresholds). Stocké dans `last_sample` pour relecture sans resample.
  - `check_threads()` : True si pas de baseline ou `max_thread_delta=0` ou `delta <= max_thread_delta`. False sinon → le caller doit break.
  - `_num_fds_safe()` : fallback silencieux si `num_fds()` indisponible (non-Linux, `AccessDenied`, `NotImplementedError`).
  - `_read_system_limits()` (static) : lit `/proc/sys/kernel/threads-max` et `resource.getrlimit(RLIMIT_NPROC)`. Gère `RLIM_INFINITY` → `None`. Défensif sur erreur I/O.
  - `get_current_user_thread_usage()` : itère `psutil.process_iter` et somme `num_threads` pour tous procs du real UID courant. Retourne `{total_threads, proc_count, skipped}`. **Coûteux** (~10-50 ms), à appeler uniquement au rythme du summary.
- `MetricsCollector._init_metrics` : ajout des 8 gauges ci-dessus.
- `MetricsCollector.update_metrics(memory_monitor, worker_ref=None, thread_monitor=None)` : publie les gauges depuis `thread_monitor.sample()`.
- `HealthChecker.__init__(..., thread_monitor=None)` et nouveau `_threads_block()`.
- `ObservabilityServer.__init__(..., thread_monitor=None)` : passe `thread_monitor` à `HealthChecker`.

### `parts/inouk_addons/inouk_message_queue/workers/standalone.py`

- `StandaloneWorker.__init__` :
  - Nouveaux kwargs `max_thread_delta`, `thread_warn_percent`, `status_summary_interval_s`.
  - Instancie `ThreadMonitor`, le passe à `ObservabilityServer`.
  - Track `self.last_status_summary_time`.
- `run()` :
  - Log « Will exit when OS thread delta vs baseline exceeds +N » si configuré.
  - `thread_monitor.capture_baseline()` après `observability_server.start()`.
  - **Check d'exit** dans la boucle, juste après le check mémoire :
    ```python
    if not self.thread_monitor.check_threads():
        sample = self.thread_monitor.last_sample or self.thread_monitor.sample()
        self.logger.warning(
            "Thread delta limit exceeded: os_threads=%d baseline=%d "
            "delta=+%d (max_delta=+%d), exiting for recycling", ...
        )
        break
    ```
  - `update_metrics(...)` reçoit `thread_monitor=self.thread_monitor`.
  - Summary périodique : **wall-clock** (`time.time() - last_status_summary_time >= interval`), plus `processed_count % N`.
- `_post_task_thread_check(queue_name)` : sample post-task, appelé pour `processed` et `failed` (pas `empty`). Compare `os_threads` à la valeur précédente (`last_sample['os_threads']`, ou baseline pour la toute première task) → si croissance, log WARNING + appelle `_log_status_summary(level=WARNING)`. Sinon, log DEBUG/INFO classique selon `warn_threshold_delta`.
- `_log_status_summary(level=logging.INFO)` : enrichi avec `Threads`, `FDs`, `System Thread Ceiling`, `User Thread Usage` (via `get_current_user_thread_usage()`), `Thread Delta Limit`. **Paramétrable** (niveau promu à WARNING sur détection de fuite par task).
- **Bonus** : dédoublonnage des `_log_status_summary()` — `_initiate_graceful_shutdown` ne le réappelle plus (le post-loop dans `run()` fait foi).

### `parts/inouk_addons/inouk_message_queue/cli/imq_worker.py`

- Args `--max-thread-delta` (default `0`), `--thread-warn-percent` (default `50`), `--status-summary-interval-s` (default `300`).
- Validations + propagation à `StandaloneWorker(...)`.

## Follow-ups (à faire avant ou après activation prod)

### Tests à écrire

- **Unit `ThreadMonitor`** :
  - `capture_baseline()` set les attrs et log.
  - `sample()` renvoie toutes les clés attendues, `py_threads <= os_threads`.
  - `check_threads()` : True si `max_thread_delta=0`, True si `baseline=None`, True/False selon delta sinon.
  - Création explicite de N threads dummy (via `threading.Event`) → delta attendu, `check_threads()` bascule à False au-dessus du seuil.
  - `_num_fds_safe()` : monkeypatch `psutil` pour simuler `AttributeError` / `AccessDenied` → retourne `None` sans raise.
  - `_read_system_limits()` : simule `/proc/sys/kernel/threads-max` manquant, `RLIMIT_NPROC = RLIM_INFINITY` → retourne `None` sans raise.
  - `get_current_user_thread_usage()` : valeurs cohérentes, `skipped >= 0`, pas de crash si un proc disparaît pendant le scan.
- **Parsing CLI** : valeurs valides + rejets (`--max-thread-delta=-1`, `--thread-warn-percent=0`, `--thread-warn-percent=101`, `--status-summary-interval-s=-1`).
- **Intégration smoke** : worker lancé avec `--max-thread-delta=0` sur queue vide, `/metrics` expose `imq_worker_thread_delta` et `imq_worker_max_thread_delta`, `/status.threads.enabled=true`.
- **Intégration recyclage** : worker + task test injectant N threads non-joinés, vérifier log WARNING + exit code 0.
- **Intégration summary wall-clock** : worker avec `--status-summary-interval-s=2`, vérifier que le summary sort toutes les ~2 s même sans task processed.

### Enhancements optionnels

- **Gauges Prometheus pour `user_nproc` et `user_threads_usage`** : permettrait des alertes du type `sum(imq_worker_thread_delta) / imq_worker_user_nproc_soft > 0.5`. Actuellement seulement dans `/status` et les logs.
- **Counter `imq_worker_recycle_total{reason="threads|memory|messages|signal"}`** : corréler les raisons de recyclage dans Grafana.
- **WARNING au démarrage si `RLIMIT_NPROC` est trop bas** pour la charge attendue : seuil = `(max_thread_delta × replicas × 4) + baseline_cluster`. Déclenche un WARN explicite invitant l'ops à ajuster l'infra plutôt que modifier RLIMIT côté code.
- **Rate-limit du dump `_log_status_summary(level=WARNING)`** en fuite chronique : en cas de fuite chaque task, on dump ~10 lignes par task. Garder le WARNING par-task mais dumper le summary complet 1×/N leaks ou 1× toutes les X secondes.

### Doc utilisateur

**À ajouter à la fin du dev** : section dans [parts/inouk_addons/inouk_message_queue/README.md](parts/inouk_addons/inouk_message_queue/README.md) titrée **« Worker Threads Consumption and Management »** (titre à affiner). Contenu à reprendre dans les sessions de travail qui ont abouti à ce spec :

1. **Introduction — le point de départ** : les exceptions `RuntimeError: can't start new thread` observées en production, qui tuent un worker entier alors qu'une seule task est fautive. Lien vers la cause racine (fuite Fabric, en cours de résolution).
2. Architecture (pourquoi le monitoring est post-task, pourquoi delta).
3. Flags CLI (`--max-thread-delta`, `--thread-warn-percent`, `--status-summary-interval-s`).
4. Les deux signaux indépendants (per-task WARNING vs. cumulative exit).
5. Métriques Prometheus exposées + endpoints `/status` pertinents.
6. Calibration (formule, tableau RLIMIT/delta, procédure 7-14 jours).
7. Configuration orchestrateur (K8s, systemd) et pourquoi on ne touche pas à `RLIMIT_NPROC` depuis le code.
8. Troubleshooting : comment interpréter les différents WARNING, que faire si recyclages trop fréquents.

Le README doit être **operator-oriented** (comment utiliser, comment interpréter), alors que ce spec est **dev-oriented** (pourquoi on a fait comme ça).

## Interactions et hors scope

- **Ne corrige pas la fuite Fabric** (issue séparée : audit des `cnx.close()` dans `finally`, revue `Group`/`ThreadingGroup`, éventuellement cleanup global par task).
- Pas de préemption d'une task en cours (attente systématique de sa fin avant exit).
- Pas de limite hard de création de threads.
- Pas de gestion des workers `ir.cron` (architecture différente, problème principalement sur standalone long-lived).
- Pas d'auto-tuning du seuil.
- Pas de modification de `RLIMIT_NPROC` côté worker (décision explicite — voir § Configuration de `RLIMIT_NPROC`).
