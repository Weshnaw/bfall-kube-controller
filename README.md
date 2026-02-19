# Helpful automation controllers for kubernetes
## Tailscale Ingress Controller
Creates an ingress for any services labeled with 
`bfall.me/tailscale-ingress: ${port-name}`

the ingress will be named `tsi-${service-name}`, and the controller should automatically clean up the ingress if the label is removed, as well as updating the existing ingress if the label changes or the controller is updated.

the controller uses leader election to ensure that only one instance is updating kubernetes at a time, and automatically attempts to failover if one instance fails.

the ingresses are marked as owned by the service so even without the controller if the service is deleted the ingress will be deleted as well.

see the example.yaml within the tailscale-ingress-controller so see a working service yaml, that will create an ingress using the port named http

## Deployment Service Controller
WIP

# TODO:
- create a controller that will create a service for labeled deployments
- maybe: create a controller that will create a PVC, would like to look into creating custom fields for an existing spec 
- considering: creating macros for the reconciler and error policy traits
- considering: do I even need the reconciler to check leadership status?
