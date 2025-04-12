
1. # KIND Installieren und Cluster erstellen
https://kind.sigs.k8s.io/docs/user/quick-start/
kind to run Kubernetes locally in a docker container
curl.exe -Lo kind-windows-amd64.exe https://kind.sigs.k8s.io/dl/v0.27.0/kind-windows-amd64
Move-Item .\kind-windows-amd64.exe "C:\Program Files (x86)\kind.exe"
Zur PATH Systemvariable hinzufügen: C:\Program Files (x86)
Docker Desktop starten
kind create cluster

#nach docker compose up vom kafka compose 
docker network connect sfr2_my_network kind-control-plane

2. # K9s installieren und mit Cluster verbinden
k9s um sich den Zustand von seinem Kubernetes anzuschauen 
In devop tools cmd öffnen k9s
k9s -c pod

3. # ArgoCD installieren
https://argo-cd.readthedocs.io/en/stable/getting_started/
Kubeconfig existiert local nachdem man kind create cluster ausgeführt hat
C:\Users\%USERPROFILE%\.kube\config

kubectl create namespace argocd
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
kubectl config set-context --current --namespace=argocd
kubectl port-forward svc/argocd-server -n argocd 8080:443
http://localhost:8080
(argocd_windows_amd64.exe haben wir umbenannt auf argocd.exe um sie ausführen zu können)
argocd admin initial-password -n argocd
Dann unter http://localhost:8080 mit admin und dem angezeigten pw einloggen
https://argo-cd.readthedocs.io/en/stable/getting_started/#creating-apps-via-ui

4. # ArgoCD App hinzufügen per GUI (einfach Anweisungen von GUI befolgen)

