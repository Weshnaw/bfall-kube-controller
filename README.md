# Helpful automation controllers for kubernetes
- ingress-controller: create a tailscale ingress from a service based on a couple tags

# TODO:
- flesh out the controller deployment
- refactor when/if I decide to add more controllers (give each controller it's own example, deployment, and docker package)
  - move generic initialization logic to shared lib
  - move leader election logic to shared lib
