# Helpful automation controllers for kubernetes
- ingress-controller: create a tailscale ingress from a service based on a couple tags

# TODO:
- kubernetes leasing to do leader election
- some basic logic to detect "does the ingress already exist"
- flesh out the controller deployment
- refactor when/if I decide to add more controllers (give each controller it's own example, deployment, and docker package)