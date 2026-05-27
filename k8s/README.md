# IMQ Workers v3 - Kubernetes Deployment Guide

This directory contains production-ready Kubernetes manifests for deploying IMQ Workers v3 in various configurations.

## 📁 Directory Structure

```
k8s/
├── README.md                    # This file
└── examples/
    ├── deployment-basic.yaml    # Simple deployment for getting started
    ├── deployment-production.yaml # Full production deployment with HPA
    ├── deployment-multiqueue.yaml # Multiple worker pools for different queues
    ├── servicemonitor.yaml      # Prometheus monitoring configuration
    ├── rbac.yaml               # RBAC policies and service accounts
    ├── secrets-template.yaml    # Secret templates and examples
    └── job-examples.yaml        # Job and CronJob examples
```

## 🚀 Quick Start

### 1. Prerequisites

- Kubernetes cluster (1.19+)
- kubectl configured
- Container registry with Muppy image
- PostgreSQL database accessible from cluster
- (Optional) Prometheus Operator for monitoring

### 2. Create Namespace

```bash
kubectl create namespace muppy-workers
```

### 3. Create Secrets

Edit `secrets-template.yaml` with your values:

```bash
# Copy template
cp examples/secrets-template.yaml my-secrets.yaml

# Edit with your values
vim my-secrets.yaml

# Apply secrets
kubectl apply -f my-secrets.yaml
```

### 4. Deploy Basic Worker

```bash
# Deploy basic worker
kubectl apply -f examples/deployment-basic.yaml

# Check deployment
kubectl get pods -l app=imq-worker
kubectl logs -l app=imq-worker
```

## 📋 Deployment Examples

### Basic Deployment (`deployment-basic.yaml`)

Simple deployment suitable for:
- Development environments
- Small workloads
- Getting started with IMQ Workers v3

Features:
- 2 replicas processing default queue
- Basic resource limits
- Health checks
- Observability endpoint

### Production Deployment (`deployment-production.yaml`)

Enterprise-ready deployment with:
- 5 replicas with rolling updates
- Horizontal Pod Autoscaler (HPA)
- Pod Disruption Budget (PDB)
- Advanced affinity rules
- Resource quotas and limits
- Security contexts
- Prometheus monitoring

### Multi-Queue Deployment (`deployment-multiqueue.yaml`)

Separate worker pools for different priorities:
- **High-priority workers**: 3 replicas, aggressive scaling
- **Default workers**: 5 replicas, regex pattern matching
- **Batch workers**: 2 replicas, higher resources, scheduled nodes

## 🔧 Configuration Options

### Worker Arguments

| Argument | Description | Example |
|----------|-------------|---------|
| `--database` | Database name | `muppy_prod` |
| `--queues` | Queue name or regex pattern (was `--queue`, deprecated alias) | `default.*` |
| `--max-messages` | Message limit (0=unlimited) | `1000` |
| `--max-rss-memory` | Memory limit | `1024M` |
| `--observability-port` | Metrics port | `8080` |
| `--worker-name` | Worker identifier | `worker-$(HOSTNAME)` |
| `--log-level` | Logging level | `INFO` |
| `--message` | Process specific message | `49665` |

### Environment Variables

Required:
- `DATABASE_NAME` / `PGDATABASE`
- `PGHOST`
- `PGUSER`
- `PGPASSWORD`

Optional:
- `PGPORT` (default: 5432)
- `AWS_ACCESS_KEY_ID` (for SQS)
- `AWS_SECRET_ACCESS_KEY` (for SQS)
- `AWS_DEFAULT_REGION` (for SQS)

## 📊 Monitoring

### Prometheus Integration

1. Deploy ServiceMonitor:
```bash
kubectl apply -f examples/servicemonitor.yaml
```

2. Available Metrics:
- `imq_worker_messages_processed_total`
- `imq_worker_messages_failed_total`
- `imq_worker_processing_duration`
- `imq_worker_memory_rss_bytes`
- `imq_worker_memory_limit_bytes`
- `imq_worker_active_processings`
- `imq_worker_queue_wait_time`

3. Example Prometheus Queries:
```promql
# Message processing rate
rate(imq_worker_messages_processed_total[5m])

# Error rate by queue
rate(imq_worker_messages_failed_total[5m]) / rate(imq_worker_messages_processed_total[5m])

# Memory usage percentage
imq_worker_memory_rss_bytes / imq_worker_memory_limit_bytes * 100

# 95th percentile processing time
histogram_quantile(0.95, rate(imq_worker_processing_duration_bucket[5m]))
```

### Grafana Dashboard

Import the dashboard from `servicemonitor.yaml` ConfigMap or use dashboard ID: `TBD`

## 🔐 Security

### RBAC Configuration

Apply RBAC policies:
```bash
kubectl apply -f examples/rbac.yaml
```

Includes:
- Service Accounts
- Roles and RoleBindings
- NetworkPolicies
- PriorityClasses

### Security Best Practices

1. **Run as non-root user**
   ```yaml
   securityContext:
     runAsNonRoot: true
     runAsUser: 1001
   ```

2. **Drop all capabilities**
   ```yaml
   capabilities:
     drop:
     - ALL
   ```

3. **Use secrets for sensitive data**
   - Never hardcode passwords
   - Use External Secrets Operator or Sealed Secrets
   - Rotate credentials regularly

4. **Network policies**
   - Restrict egress to database only
   - Allow ingress only from Prometheus

## 🚦 Health Checks

### Liveness Probe
- Endpoint: `/livez`
- Checks: Worker main loop is responsive
- Failure action: Restart container

### Readiness Probe
- Endpoint: `/readyz`
- Checks: Database connectivity, queue discovery
- Failure action: Remove from service endpoints

### Startup Probe (Production)
- Allows longer startup time
- Prevents premature liveness failures

## 🔄 Scaling

### Horizontal Pod Autoscaler (HPA)

```bash
# Check HPA status
kubectl get hpa -n muppy-workers

# Manual scaling
kubectl scale deployment imq-worker-production --replicas=10 -n muppy-workers
```

### Scaling Metrics
- CPU utilization (default)
- Memory utilization
- Custom metrics (queue depth, wait time)

### Scaling Policies
- Scale up: Fast (60s stabilization)
- Scale down: Slow (600s stabilization)

## 🛠️ Operations

### Viewing Logs

```bash
# All worker logs
kubectl logs -l app=imq-worker -n muppy-workers

# Follow logs
kubectl logs -f deployment/imq-worker-production -n muppy-workers

# Specific worker
kubectl logs imq-worker-production-abc123 -n muppy-workers
```

### Debugging

```bash
# Execute shell in worker pod
kubectl exec -it imq-worker-production-abc123 -n muppy-workers -- /bin/bash

# Check worker status
curl http://imq-worker-production-metrics:8080/statusz

# Get metrics
curl http://imq-worker-production-metrics:8080/metrics
```

### Rolling Updates

```bash
# Update image
kubectl set image deployment/imq-worker-production \
  imq-worker=your-registry/muppy:v18.0.2 \
  -n muppy-workers

# Check rollout status
kubectl rollout status deployment/imq-worker-production -n muppy-workers

# Rollback if needed
kubectl rollout undo deployment/imq-worker-production -n muppy-workers
```

### Maintenance Mode

Stop workers using system parameters:

```bash
# Create ConfigMap with stop parameter
kubectl create configmap imq-stop-workers \
  --from-literal=imq.STOP_STANDALONE_WORKERS="*" \
  -n muppy-workers

# Or use Odoo UI to set system parameter
```

## 📝 Jobs and CronJobs

### One-time Queue Processing

```bash
# Process specific queue until empty
kubectl apply -f examples/job-examples.yaml
kubectl logs -f job/imq-queue-drain-job
```

### Scheduled Batch Processing

```bash
# Deploy nightly batch job
kubectl apply -f - <<EOF
apiVersion: batch/v1
kind: CronJob
metadata:
  name: imq-nightly-batch
spec:
  schedule: "0 2 * * *"
  # ... (see job-examples.yaml)
EOF
```

### Debug Single Message

```bash
# Process specific message
kubectl create job imq-debug-12345 \
  --from=cronjob/imq-worker-template \
  -- imq-worker --database $PGDATABASE --queues=default --message=12345
```

## 🚨 Troubleshooting

### Common Issues

1. **Workers not starting**
   - Check secrets: `kubectl get secrets`
   - Check database connectivity
   - Review pod events: `kubectl describe pod <pod-name>`

2. **High memory usage**
   - Reduce `--max-rss-memory`
   - Check for memory leaks in processors
   - Scale horizontally instead of vertically

3. **Slow processing**
   - Check database performance
   - Review processor code
   - Monitor network latency

4. **Messages not processing**
   - Verify queue exists in database
   - Check worker logs for errors
   - Ensure no stop parameters are set

### Getting Help

1. Check worker logs
2. Review pod events
3. Check metrics and dashboards
4. Enable debug logging: `--log-level=DEBUG`

## 📚 Additional Resources

- [IMQ Workers v3 Specification](../docs/dev/specs/workers_v3_spec.md)
- [Implementation Plan](../docs/dev/specs/workers_v3_implementation_plan.md)
- [Odoo Deployment Best Practices](https://www.odoo.com/documentation/deployment)
- [Kubernetes Documentation](https://kubernetes.io/docs/)

## 🏷️ Version Compatibility

| Component | Version |
|-----------|---------|
| IMQ Workers v3 | 1.0.0+ |
| Kubernetes | 1.19+ |
| Prometheus Operator | 0.50+ |
| Odoo | 18.0+ |
| PostgreSQL | 10+ |