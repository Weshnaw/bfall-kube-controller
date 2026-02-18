# Helpful automation controllers for kubernetes
- ingress-controller: create a tailscale ingress from a service based on a couple tags

# TODO:
- some basic logic to detect "does the ingress already exist"
- logic to delete if the labels are removed
- flesh out the controller deployment
- maybe instead of specifying the port name, I can look for ports where the app protocol is http
- add labels such as:
  - app.kubernetes.io/managed-by: ingress-controller
  - ingress-controller.bfall.me/parent: parent.name
  - ingress-controller.bfall.me/revision: parent.generation.to_string()
- maybe add annotions for:
  - ingress-controller.bfall.me/version: 0.1.0
  - ingress-controller.bfall.me/commit: foo-bar
  - ingress-controller.bfall.me/config-hash: hash-of-ingress // this is mainly for auditing and a quick shortcut for inequality, since equality would still need to be double checked via checking actual definition, could take the next step and use  ingress-controller.bfall.me/drifted to detect and account for drift
- refactor when/if I decide to add more controllers (give each controller it's own example, deployment, and docker package)
