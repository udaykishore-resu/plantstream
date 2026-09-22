# Config-as-code and the GitOps flow (k3s edge + Argo CD)

The whole behaviour of a site is `plant.yaml` plus a handful of environment
variables. Both live in Git; nothing is configured by hand on the edge box.

## Repository layout

```
plants/                                # one directory per site
  austin/
    plant.yaml                         # asset model + sources (this repo's schema)
    values.yaml                        # Helm values: broker URL, Kafka, queue budget
  hamburg/
    plant.yaml
    values.yaml
apps/
  plantstream-austin.yaml              # Argo CD Application per site
  plantstream-hamburg.yaml
```

## Argo CD Application (per site)

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: plantstream-austin
  namespace: argocd
spec:
  project: edge
  destination:
    name: k3s-austin                   # the site's k3s cluster registered in Argo CD
    namespace: plantstream
  sources:
    - repoURL: https://github.com/udaykishore-resu/plantstream
      targetRevision: v0.1.0
      path: deploy/helm/plantstream
      helm:
        valueFiles:
          - $plants/plants/austin/values.yaml
        fileParameters:
          - name: plant.yaml            # inject the site's plant.yaml into the ConfigMap
            path: $plants/plants/austin/plant.yaml
    - repoURL: https://github.com/acme/plants
      targetRevision: main
      ref: plants
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
```

Argo CD renders the chart with the site's values and plant.yaml. The
Deployment carries a `checksum/plant` annotation, so any change to the model
restarts the single replica; the store-and-forward PVC survives the restart
and the bridge replays whatever was queued.

## Change flow

1. An engineer edits `plants/austin/plant.yaml` (new tag, new range, new PLC)
   and opens a PR.
2. CI in the plants repo runs the validator:
   `PLANTSTREAM_PLANT_FILE=plants/austin/plant.yaml PLANTSTREAM_HTTP_ADDR=:0 plantstream` exits
   non-zero with every problem listed if the file is invalid.
3. Review, merge. Argo CD syncs within its poll interval (or via webhook).
4. The node restarts, publishes a new retained `BIRTH` per source that
   consumers use to discover the changed metric set.

## Edge cluster notes

- **k3s** single-node (or 3-node) per site; the plantstream pod is pinned to
  the node with the OT network interface via `nodeSelector`.
- **NetworkPolicy** egress lists the PLC subnets explicitly; the pod cannot
  reach anything else on the OT network.
- **Secrets** (MQTT/Kafka credentials) come from External Secrets Operator or
  SealedSecrets committed to the plants repo; the chart only references them.
- **Offline operation**: Argo CD runs centrally, but the site keeps running
  its last synced version when the WAN is down. Store-and-forward covers the
  data path for the same outage.
- **Upgrades**: bump `targetRevision` in the Application; roll back by
  reverting the commit.
