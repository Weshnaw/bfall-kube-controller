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

## Pangolin Gateway Controller
download gateway CRDs: https://gateway-api.sigs.k8s.io/guides/getting-started/#installing-gateway-api
`kubectl apply --server-side -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.5.0/standard-install.yaml`
README WIP

## Pangolin Newt Init
A utility init script that will register the current pod to a pangolin server, and additionally it will update the pod's annotations with the pangolin nice_id such that it might be able to be picked up by the gateway controller

### TODOs:
- Refactor `pangolin.rs` into the shared lib
- Better handling of if there is an existing site
- migrate over to use provisioning: https://website.fossorial.io/news/templated-provisioning-and-rollouts-for-the-edge
  - how to handle the nice_id label? maybe a seperate controller that does a site lookup for any pods with a selector label
  - for the actual provisioning file we could simplify the init container to a script that copies the provisioning file secret if one does not already exist in the pvc, and have a generic provisioning file as a secret

# Global TODOs:
- Handle metrics somehow either via pushing to an metrics service, or implementing some way to pull metrics
  - a pull example might require refactoring such that we patch the current pod with leadership status, and then using that tag in the selector
- create a controller that will create a service for labeled deployments
- maybe: create a controller that will create a PVC, would like to look into creating custom fields for an existing spec 
- considering: creating macros for the reconciler and error policy traits
- considering: do I even need the reconciler to check leadership status?
