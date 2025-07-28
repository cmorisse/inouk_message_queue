# IMQ Smart Mapper - Adapter les messages vers des API REST existantes

## Concept

Le Smart Mapper permet à IMQ de pousser des messages vers n'importe quelle API REST existante SANS modifier cette API. IMQ traduit intelligemment le message vers le format attendu par l'API cible.

## 1. Architecture du Smart Mapper

```
[Message IMQ] → [Smart Mapper] → [Transformation] → [API REST existante]
                        ↓
                 [Mapping Rules]
```

## 2. Configuration d'un Smart Mapper

### Exemple : Connecter IMQ à une API de paiement existante

```json
POST /api/v1/queues
{
  "name": "payment-queue",
  "provider": "pgsql",
  "deliveryMode": "push",
  "smartMapper": {
    "enabled": true,
    "target": {
      "url": "https://payment-provider.com/api/v2/charges",
      "method": "POST",
      "headers": {
        "Authorization": "Bearer ${env.PAYMENT_API_KEY}",
        "Content-Type": "application/json"
      }
    },
    "mapping": {
      "request": {
        "body": {
          // Transformation Jinja2/JSONPath
          "amount": "{{ payload.amount * 100 }}",  // Convertir en centimes
          "currency": "{{ payload.currency | upper }}",
          "customer": {
            "email": "{{ payload.customer_email }}",
            "name": "{{ payload.customer_name }}"
          },
          "metadata": {
            "order_id": "{{ attributes.order_id }}",
            "imq_message_id": "{{ messageId }}"
          }
        },
        "queryParams": {
          "idempotency_key": "{{ messageId }}"  // Utiliser messageId pour idempotence
        }
      },
      "response": {
        // Comment interpréter la réponse
        "success": {
          "condition": "statusCode >= 200 and statusCode < 300",
          "extract": {
            "chargeId": "$.id",
            "status": "$.status"
          }
        },
        "retry": {
          "condition": "statusCode == 429 or statusCode >= 500",
          "extractRetryAfter": "headers['Retry-After']"
        },
        "failure": {
          "condition": "statusCode >= 400 and statusCode < 500",
          "extractError": "$.error.message"
        }
      }
    }
  }
}
```

## 3. Exemples de mappings pour APIs courantes

### Stripe Payment API
```json
{
  "smartMapper": {
    "target": {
      "url": "https://api.stripe.com/v1/payment_intents",
      "method": "POST",
      "headers": {
        "Authorization": "Bearer ${env.STRIPE_SECRET_KEY}",
        "Content-Type": "application/x-www-form-urlencoded"
      }
    },
    "mapping": {
      "request": {
        "body": {
          "_format": "form-urlencoded",  // Spécial pour Stripe
          "amount": "{{ (payload.amount * 100) | int }}",
          "currency": "{{ payload.currency | lower }}",
          "payment_method": "{{ payload.payment_method_id }}",
          "confirm": "true",
          "metadata[order_id]": "{{ payload.order_id }}",
          "metadata[imq_id]": "{{ messageId }}"
        }
      }
    }
  }
}
```

### SendGrid Email API
```json
{
  "smartMapper": {
    "target": {
      "url": "https://api.sendgrid.com/v3/mail/send",
      "method": "POST",
      "headers": {
        "Authorization": "Bearer ${env.SENDGRID_API_KEY}",
        "Content-Type": "application/json"
      }
    },
    "mapping": {
      "request": {
        "body": {
          "personalizations": [{
            "to": [{"email": "{{ payload.recipient }}"}],
            "dynamic_template_data": "{{ payload.template_data }}"
          }],
          "from": {"email": "{{ payload.sender or 'noreply@company.com' }}"},
          "template_id": "{{ payload.template_id }}",
          "custom_args": {
            "imq_message_id": "{{ messageId }}",
            "correlation_id": "{{ attributes.correlation_id }}"
          }
        }
      }
    }
  }
}
```

### Slack Webhook
```json
{
  "smartMapper": {
    "target": {
      "url": "{{ env.SLACK_WEBHOOK_URL }}",
      "method": "POST"
    },
    "mapping": {
      "request": {
        "body": {
          "text": "{{ payload.title }}",
          "blocks": [
            {
              "type": "section",
              "text": {
                "type": "mrkdwn",
                "text": "*{{ payload.title }}*\n{{ payload.message }}"
              }
            },
            {
              "type": "context",
              "elements": [{
                "type": "mrkdwn",
                "text": "Message ID: {{ messageId }} | Time: {{ metadata.enqueuedTime }}"
              }]
            }
          ]
        }
      }
    }
  }
}
```

## 4. Fonctionnalités avancées du Smart Mapper

### Transformations disponibles
```yaml
# Fonctions de transformation
{{ payload.amount * 100 }}                    # Calculs
{{ payload.name | upper }}                    # Filtres string
{{ payload.date | format_date('YYYY-MM-DD') }} # Formatage dates
{{ payload.items | map('price') | sum }}      # Agrégations
{{ attributes.tags | join(',') }}             # Arrays
{{ payload.user?.email or 'default@example.com' }} # Valeurs par défaut
```

### Conditions complexes
```json
{
  "mapping": {
    "request": {
      "body": {
        "_if": {
          "condition": "payload.amount > 100",
          "then": {
            "requiresApproval": true,
            "approvalLevel": "{{ payload.amount > 1000 ? 'executive' : 'manager' }}"
          },
          "else": {
            "autoApprove": true
          }
        }
      }
    }
  }
}
```

### Authentification dynamique
```json
{
  "target": {
    "headers": {
      "_switch": {
        "on": "{{ attributes.auth_type }}",
        "cases": {
          "oauth": {
            "Authorization": "Bearer {{ oauth_token(env.CLIENT_ID, env.CLIENT_SECRET) }}"
          },
          "apikey": {
            "X-API-Key": "{{ env.API_KEY }}"
          },
          "basic": {
            "Authorization": "Basic {{ base64(env.USERNAME + ':' + env.PASSWORD) }}"
          }
        }
      }
    }
  }
}
```

## 5. Gestion des réponses

### Interprétation intelligente
```json
{
  "response": {
    "success": {
      // Conditions multiples possibles
      "condition": "statusCode == 200 and body.status in ['completed', 'processing']",
      "store": {
        // Stocker des infos de la réponse
        "externalId": "$.data.id",
        "processedAt": "$.data.timestamp",
        "receipt": "$.data"  // Stocker toute la réponse
      }
    },
    "retry": {
      "conditions": [
        {
          "when": "statusCode == 429",
          "backoff": "{{ headers['Retry-After'] or 60 }}"
        },
        {
          "when": "statusCode >= 500",
          "backoff": "exponential"
        },
        {
          "when": "body.error.code == 'RATE_LIMIT'",
          "backoff": "{{ body.error.retry_after }}"
        }
      ]
    }
  }
}
```

## 6. Patterns de mapping réutilisables

### Créer un template de mapping
```json
POST /api/v1/mapper-templates
{
  "name": "stripe-payment",
  "description": "Template pour Stripe Payment Intents API",
  "mapping": { /* ... */ }
}
```

### Utiliser un template
```json
{
  "smartMapper": {
    "template": "stripe-payment",
    "overrides": {
      "target.url": "https://api.stripe.com/v1/payment_intents"
    }
  }
}
```

## 7. Mode découverte automatique

### API Learning
```json
POST /api/v1/mapper/discover
{
  "targetUrl": "https://api.example.com/orders",
  "sampleRequest": {
    "method": "POST",
    "headers": {"Authorization": "Bearer token"},
    "body": {"orderId": "12345", "total": 99.99}
  },
  "sampleIMQMessage": {
    "payload": {
      "order_id": "12345",
      "amount": 99.99,
      "currency": "USD"
    }
  }
}

// Réponse : suggestion de mapping
{
  "suggestedMapping": {
    "request": {
      "body": {
        "orderId": "{{ payload.order_id }}",
        "total": "{{ payload.amount }}"
      }
    }
  }
}
```

## 8. Cas d'usage concrets

### 1. Intégration CRM existant
```python
# Message IMQ
{
  "type": "simple",
  "selector": "create_contact",
  "payload": {
    "email": "john@example.com",
    "name": "John Doe",
    "company": "Acme Corp"
  }
}

# Transformé automatiquement pour Salesforce API
POST https://instance.salesforce.com/services/data/v53.0/sobjects/Contact
{
  "LastName": "Doe",
  "FirstName": "John",
  "Email": "john@example.com",
  "Company": "Acme Corp",
  "Description": "Created by IMQ - Message ID: xxx"
}
```

### 2. Notification multi-canal
```python
# Un seul message IMQ
{
  "selector": "send_alert",
  "payload": {
    "level": "critical",
    "message": "Server CPU > 90%",
    "server": "prod-01"
  }
}

# Mappé vers plusieurs APIs selon rules
# → Slack: Format Slack blocks
# → PagerDuty: Format incident
# → Email: Format HTML
# → SMS: Format texte court
```

## 9. Monitoring et debug

### Mode test/dry-run
```bash
POST /api/v1/mapper/test
{
  "queueName": "payment-queue",
  "testMessage": {
    "payload": {"amount": 99.99, "currency": "EUR"}
  }
}

# Réponse
{
  "transformedRequest": {
    "url": "https://api.stripe.com/v1/payment_intents",
    "method": "POST",
    "headers": {"Authorization": "Bearer sk_test_***"},
    "body": "amount=9999&currency=eur"
  },
  "wouldExecute": false
}
```

### Logs détaillés
```json
{
  "messageId": "abc-123",
  "mapping": {
    "input": { /* message original */ },
    "transformed": { /* après transformation */ },
    "response": { /* réponse de l'API */ },
    "duration": 234,
    "transformationTime": 2
  }
}
```

## 10. Avantages du Smart Mapper

1. **Zero modification** : Les APIs existantes restent intactes
2. **Réutilisabilité** : Templates de mapping partageables
3. **Évolutivité** : Ajouter de nouvelles APIs sans code
4. **Standardisation** : Messages IMQ uniformes, APIs variées
5. **Debugging** : Transformation visible et testable
6. **Resilience** : Retry intelligent selon les réponses

## Conclusion

Le Smart Mapper transforme IMQ en un **hub d'intégration universel**. Au lieu de modifier chaque API pour accepter les messages IMQ, on configure des règles de transformation qui adaptent les messages au format attendu. C'est l'approche "Adapter Pattern" appliquée aux queues de messages !