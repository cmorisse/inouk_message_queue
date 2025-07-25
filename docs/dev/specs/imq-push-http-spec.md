# IMQ Push-Based HTTP Delivery - Spécification Technique v2

## 1. Vue d'ensemble

Cette spécification décrit l'implémentation du pattern Push-Based Delivery dans IMQ, permettant de pousser automatiquement les messages vers des endpoints HTTP externes, avec support des tâches longues et gestion des limitations du protocole HTTP.

## 2. Architecture

### 2.1 Nouveau type de processeur

Ajout d'un troisième type de processeur dans IMQ :
- **`simple`** : Processeur pour messages simples (existant)
- **`rpc`** : Processeur pour appels RPC (existant)
- **`push_http`** : Processeur pour push HTTP avec stratégies adaptatives (nouveau)

### 2.2 Workflow général

```
Client → API REST IMQ → Queue (PG/SQS) → IMQ Worker → Push HTTP Processor → API Cible
                                                              ↓
                                                     Stratégie + Mapping
```

### 2.3 Limitations du protocole HTTP et solutions

Le push-based HTTP impose les contraintes du protocole HTTP (timeout, stabilité connexion) sur l'exécution métier. Pour gérer cela, IMQ supporte plusieurs stratégies de delivery.

## 3. Stratégies de delivery Push HTTP

### 3.1 Types de stratégies

| Stratégie | Durée task | Description | Use case |
|-----------|------------|-------------|----------|
| `sync` | < 30s | Push HTTP synchrone standard | APIs rapides, webhooks simples |
| `async_202` | 30s - 5min | Push avec 202 Accepted + polling | Traitements moyens |
| `claim_check` | > 5min | Push référence + pull worker | Traitements lourds |
| `callback` | Variable | Push + callback worker | Découplage total |
| `hybrid` | Variable | Notification + pull à la demande | Flexibilité maximale |

### 3.2 Configuration adaptative

```python
class IMQQueue(models.Model):
    # ... champs existants ...
    
    # Stratégie de delivery
    push_strategy = fields.Selection([
        ('sync', 'Synchronous (< 30s)'),
        ('async_202', 'Async with 202 Accepted'),
        ('claim_check', 'Claim Check Pattern'),
        ('callback', 'Callback Pattern'),
        ('hybrid', 'Hybrid Push/Pull'),
        ('adaptive', 'Adaptive (Auto-detect)')
    ], default='adaptive', string='Push Strategy')
    
    # Configuration spécifique par stratégie
    push_strategy_config = fields.Text('Strategy Configuration',
        default="""{
            "adaptive_rules": [
                {
                    "condition": "response.status_code == 202",
                    "use_strategy": "async_202"
                },
                {
                    "condition": "response.headers.get('X-IMQ-Strategy') == 'callback'",
                    "use_strategy": "callback"
                }
            ],
            "timeout_escalation": {
                "30s": "sync",
                "5m": "async_202",
                "30m": "claim_check"
            }
        }""")
```

## 4. Configuration détaillée par stratégie

### 4.1 Stratégie Synchrone (sync)

Configuration standard pour tâches rapides :

```json
{
    "name": "quick-webhooks",
    "delivery_mode": "push_http",
    "push_strategy": "sync",
    "http_target_url": "https://api.example.com/webhook",
    "http_timeout": 30,
    "http_retry_config": {
        "max_attempts": 3,
        "retry_on_status": [408, 429, 500, 502, 503, 504]
    }
}
```

### 4.2 Stratégie Async 202 Accepted

Pour tâches moyennes avec polling de statut :

```json
{
    "name": "processing-tasks",
    "delivery_mode": "push_http",
    "push_strategy": "async_202",
    "push_strategy_config": {
        "status_polling": {
            "enabled": true,
            "initial_delay": 5,
            "interval": 30,
            "max_duration": 3600,
            "status_url_from_response": "$.status_url",
            "completion_indicators": {
                "status_field": "$.status",
                "complete_values": ["completed", "done"],
                "failed_values": ["failed", "error"],
                "in_progress_values": ["processing", "pending"]
            }
        }
    }
}
```

Exemple de réponse worker attendue :
```json
HTTP/1.1 202 Accepted
{
    "status": "accepted",
    "task_id": "task-123",
    "status_url": "/api/tasks/task-123/status",
    "estimated_duration": 120
}
```

### 4.3 Stratégie Claim Check

Pour tâches longues, IMQ pousse une référence :

```json
{
    "name": "heavy-processing",
    "delivery_mode": "push_http",
    "push_strategy": "claim_check",
    "push_strategy_config": {
        "claim_check": {
            "push_content": "reference_only",
            "reference_data": {
                "message_id": "{{ message.id }}",
                "claim_url": "{{ imq.base_url }}/api/v1/messages/{{ message.id }}/claim",
                "claim_token": "{{ message.claim_token }}",
                "expires_at": "{{ message.expiry_time }}"
            },
            "message_retention": "72h",
            "claim_notification": {
                "on_claim": true,
                "on_complete": true
            }
        }
    }
}
```

Worker récupère le message complet quand prêt :
```python
# 1. IMQ pousse la référence
POST /notify-heavy-task
{
    "message_id": "msg-123",
    "claim_url": "https://imq.api/messages/msg-123/claim",
    "claim_token": "secret-token"
}

# 2. Worker claim le message quand prêt
GET https://imq.api/messages/msg-123/claim
Authorization: Bearer secret-token

# 3. Worker notifie la completion
POST https://imq.api/messages/msg-123/complete
{
    "status": "completed",
    "result": { ... }
}
```

### 4.4 Stratégie Callback

Worker rappelle IMQ après traitement :

```json
{
    "name": "async-tasks",
    "delivery_mode": "push_http",
    "push_strategy": "callback",
    "push_strategy_config": {
        "callback": {
            "callback_url": "{{ imq.base_url }}/api/v1/messages/{{ message.id }}/callback",
            "callback_token": "{{ generate_token() }}",
            "callback_timeout": "24h",
            "callback_retry": {
                "max_attempts": 5,
                "backoff": "exponential"
            },
            "include_in_push": {
                "callback_url": true,
                "callback_token": true,
                "callback_deadline": true
            }
        }
    }
}
```

### 4.5 Stratégie Hybride

Notification légère + pull à la demande :

```json
{
    "name": "flexible-processing",
    "delivery_mode": "push_http",
    "push_strategy": "hybrid",
    "push_strategy_config": {
        "hybrid": {
            "notification": {
                "url": "https://worker.api/notify",
                "content": {
                    "queue": "{{ queue.name }}",
                    "message_count": "{{ queue.pending_count }}",
                    "priority": "{{ message.priority }}"
                }
            },
            "pull_endpoint": {
                "enabled": true,
                "url": "{{ imq.base_url }}/api/v1/queues/{{ queue.name }}/messages",
                "batch_size": 10,
                "retention": "48h"
            }
        }
    }
}
```

### 4.6 Stratégie Adaptive (Auto-détection)

IMQ détecte automatiquement la stratégie selon la réponse :

```json
{
    "push_strategy": "adaptive",
    "push_strategy_config": {
        "detection_rules": [
            {
                "name": "detect_202_pattern",
                "condition": "response.status_code == 202 and 'status_url' in response.body",
                "apply_strategy": "async_202",
                "extract_config": {
                    "status_url": "response.body.status_url"
                }
            },
            {
                "name": "detect_callback_pattern",
                "condition": "response.headers.get('X-IMQ-Pattern') == 'callback'",
                "apply_strategy": "callback"
            },
            {
                "name": "detect_timeout",
                "condition": "response.timeout == true",
                "apply_strategy": "claim_check"
            }
        ],
        "fallback_strategy": "sync"
    }
}
```

## 5. Implémentation du processeur Push HTTP v2

### 5.1 Processeur avec gestion des stratégies

```python
class PushHTTPProcessor:
    """Processeur Push HTTP avec support multi-stratégies"""
    
    def __init__(self, queue_obj, message_obj, processing_obj, logger):
        self.queue = queue_obj
        self.message = message_obj
        self.processing = processing_obj
        self.logger = logger
        self.strategy = self._determine_strategy()
        
    def process(self):
        """Route vers la stratégie appropriée"""
        strategies = {
            'sync': self._process_sync,
            'async_202': self._process_async_202,
            'claim_check': self._process_claim_check,
            'callback': self._process_callback,
            'hybrid': self._process_hybrid,
            'adaptive': self._process_adaptive
        }
        
        handler = strategies.get(self.strategy, self._process_sync)
        return handler()
    
    def _process_sync(self):
        """Stratégie synchrone standard"""
        request_data = self._prepare_request()
        response = self._execute_request(request_data)
        
        if 200 <= response.status_code < 300:
            return self._mark_success(response)
        else:
            return self._handle_failure(response)
    
    def _process_async_202(self):
        """Stratégie avec 202 Accepted et polling"""
        # 1. Push initial
        request_data = self._prepare_request()
        response = self._execute_request(request_data)
        
        if response.status_code != 202:
            return self._process_sync()  # Fallback
        
        # 2. Extraire l'URL de statut
        status_config = self._get_strategy_config().get('status_polling', {})
        status_url = self._extract_from_response(
            response, 
            status_config.get('status_url_from_response')
        )
        
        if not status_url:
            raise IMQError("202 Accepted but no status URL provided")
        
        # 3. Créer une tâche de polling
        self._create_polling_task(status_url, status_config)
        
        return json.dumps({
            'status': 'accepted',
            'status_url': status_url,
            'polling_scheduled': True
        })
    
    def _process_claim_check(self):
        """Stratégie Claim Check"""
        # 1. Préparer la référence
        claim_config = self._get_strategy_config().get('claim_check', {})
        reference_data = self._apply_mapping(
            claim_config.get('reference_data', {}),
            self._get_context()
        )
        
        # 2. Pousser uniquement la référence
        request_data = self._prepare_request()
        request_data['json'] = reference_data
        
        response = self._execute_request(request_data)
        
        # 3. Marquer le message comme "waiting_claim"
        self.message.write({
            'state': 'waiting_claim',
            'claim_token': reference_data.get('claim_token'),
            'claim_expires': datetime.now() + timedelta(
                hours=int(claim_config.get('message_retention', '72h')[:-1])
            )
        })
        
        return json.dumps({
            'status': 'claim_check_sent',
            'claim_url': reference_data.get('claim_url')
        })
    
    def _process_callback(self):
        """Stratégie avec callback"""
        # 1. Préparer le message avec infos de callback
        callback_config = self._get_strategy_config().get('callback', {})
        callback_data = {
            'callback_url': self._apply_template(callback_config.get('callback_url')),
            'callback_token': self._generate_callback_token(),
            'callback_deadline': (
                datetime.now() + 
                timedelta(hours=24)
            ).isoformat()
        }
        
        # 2. Enrichir le message avec les infos de callback
        request_data = self._prepare_request()
        if callback_config.get('include_in_push', {}).get('callback_url'):
            request_data['json']['callback'] = callback_data
        
        # 3. Envoyer et attendre juste l'acquittement
        response = self._execute_request(request_data)
        
        if response.status_code in [200, 201, 202]:
            # 4. Marquer comme "waiting_callback"
            self.message.write({
                'state': 'waiting_callback',
                'callback_token': callback_data['callback_token'],
                'callback_deadline': callback_data['callback_deadline']
            })
            
            return json.dumps({
                'status': 'callback_pending',
                'callback_url': callback_data['callback_url']
            })
    
    def _process_adaptive(self):
        """Stratégie adaptive - détecte le pattern"""
        # 1. Essayer un push standard
        request_data = self._prepare_request()
        response = self._execute_request(request_data, timeout=5)  # Timeout court
        
        # 2. Analyser la réponse pour détecter le pattern
        detection_rules = self._get_strategy_config().get('detection_rules', [])
        
        for rule in detection_rules:
            if self._evaluate_condition(rule['condition'], response):
                # 3. Basculer vers la stratégie détectée
                self.strategy = rule['apply_strategy']
                self.logger.info(f"Adaptive: Detected pattern '{rule['name']}', switching to {self.strategy}")
                
                # 4. Ré-exécuter avec la bonne stratégie
                return self.process()
        
        # 5. Fallback vers sync si aucune règle ne match
        return self._process_sync()
```

### 5.2 Support du polling pour async_202

```python
class StatusPollingTask(models.Model):
    """Tâche de polling pour stratégie 202"""
    _name = 'imq.status_polling_task'
    
    message_id = fields.Many2one('imq.message', required=True)
    status_url = fields.Char(required=True)
    next_poll = fields.Datetime(required=True)
    poll_count = fields.Integer(default=0)
    max_polls = fields.Integer(default=120)  # 1h avec 30s d'intervalle
    
    @api.model
    def process_pending_polls(self):
        """Cron job pour traiter les polling en attente"""
        tasks = self.search([
            ('next_poll', '<=', fields.Datetime.now()),
            ('poll_count', '<', 'max_polls')
        ])
        
        for task in tasks:
            try:
                response = requests.get(
                    task.status_url,
                    headers=self._get_auth_headers(task.message_id)
                )
                
                status = response.json().get('status')
                
                if status in ['completed', 'done']:
                    task.message_id.write({
                        'state': 'done',
                        'result': json.dumps(response.json())
                    })
                    task.unlink()
                    
                elif status in ['failed', 'error']:
                    task.message_id.write({
                        'state': 'failed',
                        'result': json.dumps(response.json())
                    })
                    task.unlink()
                    
                else:
                    # Continuer le polling
                    task.poll_count += 1
                    task.next_poll = fields.Datetime.now() + timedelta(seconds=30)
                    
            except Exception as e:
                _logger.error(f"Polling failed for task {task.id}: {str(e)}")
                task.poll_count += 1
```

### 5.3 Endpoints pour claim et callback

```python
# Dans controllers/message.py

@route('/api/v1/messages/<string:message_id>/claim', methods=['GET'], auth='token')
def claim_message(self, message_id, **kwargs):
    """Endpoint pour claim check pattern"""
    message = request.env['imq.message'].sudo().browse(message_id)
    
    # Vérifier le token
    auth_header = request.httprequest.headers.get('Authorization', '')
    if not auth_header.startswith('Bearer '):
        return Response(status=401)
        
    token = auth_header[7:]
    if token != message.claim_token:
        return Response(status=403)
    
    # Marquer comme claimed
    message.write({
        'state': 'claimed',
        'claimed_at': fields.Datetime.now()
    })
    
    # Retourner le message complet
    return Response(
        json.dumps({
            'message_id': message.id,
            'payload': json.loads(message.payload),
            'context': json.loads(message.context or '{}'),
            'attempts': message.attempt
        }),
        content_type='application/json'
    )

@route('/api/v1/messages/<string:message_id>/callback', methods=['POST'], auth='token')
def message_callback(self, message_id, **kwargs):
    """Endpoint pour callback pattern"""
    message = request.env['imq.message'].sudo().browse(message_id)
    
    # Vérifier le token
    token = request.jsonrequest.get('callback_token')
    if token != message.callback_token:
        return Response(status=403)
    
    # Traiter le callback
    status = request.jsonrequest.get('status')
    result = request.jsonrequest.get('result', {})
    
    if status == 'completed':
        message.write({
            'state': 'done',
            'result': json.dumps(result),
            'end_time': fields.Datetime.now()
        })
    elif status == 'failed':
        message.write({
            'state': 'failed',
            'result': json.dumps(result),
            'end_time': fields.Datetime.now()
        })
    
    return Response(
        json.dumps({'status': 'callback_received'}),
        content_type='application/json'
    )
```

## 6. Configuration de queue - Interface utilisateur

### 6.1 Vue formulaire améliorée

```xml
<notebook>
    <page string="Push HTTP Configuration" attrs="{'invisible': [('delivery_mode', '!=', 'push_http')]}">
        <group>
            <field name="push_strategy" widget="radio"/>
            <field name="http_target_url" attrs="{'required': [('delivery_mode', '=', 'push_http')]}"/>
            <field name="http_timeout" attrs="{'invisible': [('push_strategy', 'not in', ['sync', 'adaptive'])]}"/>
        </group>
        
        <group string="Strategy Configuration" attrs="{'invisible': [('push_strategy', '=', 'sync')]}">
            <field name="push_strategy_config" widget="ace" options="{'mode': 'json'}"/>
            <button name="test_strategy" string="Test Strategy" type="object" class="btn-primary"/>
            <button name="preview_transformation" string="Preview Message" type="object"/>
        </group>
        
        <group string="Statistics" attrs="{'invisible': [('id', '=', False)]}">
            <field name="push_success_count" readonly="1"/>
            <field name="push_failure_count" readonly="1"/>
            <field name="avg_response_time" readonly="1"/>
            <field name="last_push_error" readonly="1" attrs="{'invisible': [('last_push_error', '=', False)]}"/>
        </group>
    </page>
</notebook>
```

## 7. Monitoring et métriques

### 7.1 Métriques par stratégie

```python
# Prometheus metrics
imq_push_http_requests_total{queue, strategy, status}
imq_push_http_duration_seconds{queue, strategy, quantile}
imq_push_strategy_changes_total{queue, from_strategy, to_strategy}
imq_push_polling_tasks_active{queue}
imq_push_claim_check_claims_total{queue, status}
imq_push_callback_received_total{queue, status}
```

### 7.2 Dashboard de monitoring

- **Vue globale** : Répartition des stratégies utilisées
- **Performance** : Temps de réponse par stratégie
- **Fiabilité** : Taux de succès par stratégie
- **Adaptation** : Changements de stratégie (pour adaptive)

## 8. Guide de choix de stratégie

### 8.1 Arbre de décision

```
Durée de traitement estimée ?
├─ < 30 secondes
│  └─ Strategy: sync
├─ 30s - 5 minutes
│  └─ API supporte 202 Accepted ?
│     ├─ Oui → Strategy: async_202
│     └─ Non → Strategy: callback
└─ > 5 minutes
   └─ Worker peut pull ?
      ├─ Oui → Strategy: claim_check ou hybrid
      └─ Non → Strategy: callback
```

### 8.2 Recommandations par cas d'usage

| Use Case | Stratégie recommandée | Raison |
|----------|----------------------|---------|
| Webhooks simples | `sync` | Réponse immédiate requise |
| Génération PDF | `async_202` | 1-2 min, status consultable |
| Encodage vidéo | `claim_check` | Très long, worker autonome |
| Notifications | `callback` | Fire-and-forget avec confirmation |
| Mix de tâches | `adaptive` | Auto-détection du meilleur pattern |

## 9. Migration et compatibilité

- Les queues existantes restent en mode `pull` par défaut
- La stratégie `sync` est équivalente à l'ancien comportement
- Migration progressive possible via `adaptive`
- API backward compatible

## 10. Sécurité et limites

### 10.1 Timeouts par stratégie

```python
STRATEGY_TIMEOUTS = {
    'sync': 30,      # Standard HTTP timeout
    'async_202': 5,  # Juste pour l'acquittement
    'claim_check': 5, # Juste pour la notification
    'callback': 5,    # Juste pour l'acquittement
    'hybrid': 2      # Notification ultra-légère
}
```

### 10.2 Protection contre les abus

- Rate limiting par endpoint cible
- Circuit breaker si trop d'échecs
- Validation des URLs (pas de localhost, etc.)
- Tokens à usage unique pour claim/callback

## 11. Conclusion

Cette architecture multi-stratégies permet à IMQ de :
1. **S'adapter** aux contraintes réelles des workers
2. **Supporter** des tâches de toute durée (1s à plusieurs heures)
3. **Optimiser** selon le cas d'usage
4. **Évoluer** avec détection automatique des patterns

Le mode `adaptive` est recommandé par défaut, laissant IMQ choisir la meilleure stratégie selon les réponses des workers.